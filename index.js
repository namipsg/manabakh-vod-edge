require('./instrument');
const express = require('express');
const redis = require('redis');
const { Client } = require('minio');
const { EventEmitter } = require('events');
const { Readable } = require('stream');
const path = require('path');
const { gzip, gunzip } = require('zlib');
const { promisify } = require('util');
const mime = require('mime-types');
const Sentry = require("@sentry/node");
const app = express();
const port = process.env.PORT || 3000;
const minioBucket = process.env.MINIO_BUCKET_NAME || 'test';
const cacheTtlSeconds = parseInt(process.env.CACHE_TTL_SECONDS, 10) || 3600;
const ttlExtensions = new Set(['.json', '.vtt', '.png']);
const cacheExcludedExtensions = new Set(['.mp4']);
const rangeEnabledExtensions = new Set(['.mp4']);
const compressibleImageExtensions = new Set(['.png', '.jpg', '.jpeg']);
const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

const redisClient = redis.createClient({
  socket: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    reconnectStrategy: (retries) => Math.min(retries * 50, 1000)
  },
  retry_delay_on_cluster_down: 300,
  retry_delay_on_failover: 100
});

const minioClient = new Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  port: parseInt(process.env.MINIO_PORT) || 9000,
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || 'minioadmin',
  secretKey: process.env.MINIO_SECRET_KEY || 'minioadmin'
});

const cacheEvents = new EventEmitter();

async function initializeRedis() {
  await redisClient.connect();
  console.log('Connected to Redis');
}

async function incrementHitCount(filename) {
  await redisClient.hIncrBy('vod_hit_counts', filename, 1);
}

function getFileExtension(filename) {
  return path.extname(filename).toLowerCase();
}

function shouldUseTtl(filename) {
  return ttlExtensions.has(getFileExtension(filename));
}

function shouldCompressImage(filename) {
  return compressibleImageExtensions.has(getFileExtension(filename));
}

function isCacheExcluded(filename) {
  return cacheExcludedExtensions.has(getFileExtension(filename));
}

function supportsRangeRequests(filename) {
  return rangeEnabledExtensions.has(getFileExtension(filename));
}

function getContentType(filename) {
  if (getFileExtension(filename) === '.mp4') {
    return 'video/mp4';
  }

  return mime.lookup(filename) || 'application/octet-stream';
}

async function streamToBuffer(stream) {
  const chunks = [];

  for await (const chunk of stream) {
    chunks.push(chunk);
  }

  return Buffer.concat(chunks);
}

async function encodeCacheValue(filename, buffer) {
  if (!shouldCompressImage(filename)) {
    return buffer.toString('base64');
  }

  const compressedBuffer = await gzipAsync(buffer);

  return JSON.stringify({
    encoding: 'gzip',
    data: compressedBuffer.toString('base64')
  });
}

async function decodeCacheValue(cachedFile) {
  try {
    const parsedCacheValue = JSON.parse(cachedFile);

    if (parsedCacheValue.encoding === 'gzip' && parsedCacheValue.data) {
      return gunzipAsync(Buffer.from(parsedCacheValue.data, 'base64'));
    }
  } catch (error) {
    return Buffer.from(cachedFile, 'base64');
  }

  return Buffer.from(cachedFile, 'base64');
}

async function getCachedFile(filename) {
  if (isCacheExcluded(filename)) {
    return null;
  }

  const cacheKey = `vod_file:${filename}`;
  const cachedFile = await redisClient.get(cacheKey);

  if (!cachedFile) {
    return null;
  }

  if (shouldUseTtl(filename)) {
    const ttl = await redisClient.ttl(cacheKey);

    if (ttl === -1) {
      console.log(`Cache entry for ${filename} has no TTL, re-caching from Minio`);
      await redisClient.del(cacheKey);
      return null;
    }
  }

  return decodeCacheValue(cachedFile);
}

async function cacheFileFromMinio(filename) {
  if (isCacheExcluded(filename)) {
    return;
  }

  try {
    const stream = await minioClient.getObject(minioBucket, filename);
    const buffer = await streamToBuffer(stream);
    const cacheKey = `vod_file:${filename}`;
    const cacheValue = await encodeCacheValue(filename, buffer);

    if (shouldUseTtl(filename)) {
      await redisClient.set(cacheKey, cacheValue, { EX: cacheTtlSeconds });
    } else {
      await redisClient.set(cacheKey, cacheValue);
    }

    console.log(`Cached ${filename} to Redis`);
  } catch (error) {
    console.error(`Failed to cache ${filename}:`, error.message);
  }
}

cacheEvents.on('cache_file', cacheFileFromMinio);

function parseRangeHeader(rangeHeader, fileSize) {
  if (!rangeHeader) return null;
  if (!rangeHeader.startsWith('bytes=')) return { invalid: true };

  const firstRange = rangeHeader.replace('bytes=', '').split(',')[0].trim();
  const [rawStart, rawEnd] = firstRange.split('-');

  let start;
  let end;

  if (rawStart === '') {
    const suffixLength = Number(rawEnd);
    if (!Number.isInteger(suffixLength) || suffixLength <= 0) {
      return { invalid: true };
    }
    start = Math.max(fileSize - suffixLength, 0);
    end = fileSize - 1;
  } else {
    start = Number(rawStart);
    end = rawEnd ? Number(rawEnd) : fileSize - 1;
  }

  if (
    !Number.isInteger(start) ||
    !Number.isInteger(end) ||
    start < 0 ||
    end < start ||
    start >= fileSize
  ) {
    return { invalid: true };
  }

  return {
    invalid: false,
    start,
    end: Math.min(end, fileSize - 1),
  };
}

async function getPartialObjectStream(filename, start, length) {
  if (typeof minioClient.getPartialObject === 'function') {
    return minioClient.getPartialObject(minioBucket, filename, start, length);
  }

  const stream = await minioClient.getObject(minioBucket, filename);
  return stream;
}

async function serveRangeEnabledFile(req, res, filename) {
  const stat = await minioClient.statObject(minioBucket, filename);
  const fileSize = Number(stat.size);
  const contentType = getContentType(filename);
  const range = parseRangeHeader(req.headers.range, fileSize);

  res.set('Accept-Ranges', 'bytes');
  res.set('Content-Type', contentType);
  res.set('Cache-Control', 'public, max-age=3600');

  if (range?.invalid) {
    res.set('Content-Range', `bytes */${fileSize}`);
    return res.status(416).end();
  }

  if (range) {
    const chunkSize = range.end - range.start + 1;
    const stream = await getPartialObjectStream(filename, range.start, chunkSize);

    res.status(206);
    res.set('Content-Range', `bytes ${range.start}-${range.end}/${fileSize}`);
    res.set('Content-Length', String(chunkSize));
    res.set('X-Cache-Layer', 'L1-Minio-Range');
    return stream.pipe(res);
  }

  const stream = await minioClient.getObject(minioBucket, filename);

  res.set('Content-Length', String(fileSize));
  res.set('X-Cache-Layer', 'L1-Minio-Stream');
  return stream.pipe(res);
}

async function serveFile(req, res) {
  const filename = req.params[0];

  try {
    await incrementHitCount(filename);

    if (supportsRangeRequests(filename)) {
      return serveRangeEnabledFile(req, res, filename);
    }

    const cachedFile = await getCachedFile(filename);

    if (cachedFile) {
      console.log(`Cache hit for ${filename}`);
      res.set('Content-Type', getContentType(filename));
      res.set('Cache-Control', 'public, max-age=3600');
      res.set('X-Cache-Layer', 'L0-Redis');
      const stream = Readable.from(cachedFile);
      return stream.pipe(res);
    }

    console.log(`Cache miss for ${filename}, fetching from Minio`);

    const stream = await minioClient.getObject(minioBucket, filename);

    res.set('Content-Type', getContentType(filename));
    res.set('Cache-Control', 'public, max-age=3600');
    res.set('X-Cache-Layer', 'L1-Minio');

    stream.pipe(res);

    if (!isCacheExcluded(filename)) {
      cacheEvents.emit('cache_file', filename);
    }

  } catch (error) {
    console.error(`Error serving ${filename}:`, error.message);

    if (error.code === 'NoSuchKey') {
      return res.status(404).json({ error: 'File not found' });
    }

    if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND') {
      return res.status(502).json({ error: 'Backend service unavailable' });
    }

    res.status(500).json({ error: 'Internal server error' });
  }
}

app.get('/nami/*', serveFile);
app.get('/vod/*', serveFile);

app.get('/stats', async (req, res) => {
  try {
    const redisHitCounts = await redisClient.hGetAll('vod_hit_counts');
    res.json({ hitCounts: redisHitCounts });
  } catch (error) {
    res.status(500).json({ error: 'Failed to retrieve stats' });
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', timestamp: new Date().toISOString() });
});

app.get('/ready', async (req, res) => {
  try {
    await redisClient.ping();
    res.status(200).json({ status: 'ready' });
  } catch (error) {
    res.status(503).json({ status: 'not ready', error: error.message });
  }
});

function startServer() {
  // Sentry error handler setup
  Sentry.setupExpressErrorHandler(app);

  const server = app.listen(port, async () => {
    try {
      await initializeRedis();
      console.log(`FCDN server running on port ${port}`);
    } catch (error) {
      console.error('Failed to start server:', error);
      process.exit(1);
    }
  });

  return server;
}

let server;

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, shutting down gracefully`);

  server.close(async () => {
    try {
      await redisClient.quit();
      console.log('Redis connection closed');
    } catch (error) {
      console.error('Error closing Redis:', error);
    }
    process.exit(0);
  });

  setTimeout(() => {
    console.log('Force shutdown after 10s');
    process.exit(1);
  }, 10000);
}

if (require.main === module) {
  server = startServer();
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
}

module.exports = {
  app,
  cacheFileFromMinio,
  getCachedFile,
  gracefulShutdown,
  isCacheExcluded,
  parseRangeHeader,
  serveRangeEnabledFile,
  startServer,
  supportsRangeRequests,
  getContentType,
};
