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
const cacheKeyPrefix = process.env.VOD_CACHE_REDIS_PREFIX || 'vod_file:';
const metricsKey = process.env.VOD_CDN_METRICS_KEY || 'vod_metrics';
const hitCountsKey = process.env.VOD_CDN_HIT_COUNTS_KEY || 'vod_hit_counts';
const requestLogEnabled = process.env.VOD_CDN_REQUEST_LOGS === undefined
  ? process.env.NODE_ENV !== 'production'
  : process.env.VOD_CDN_REQUEST_LOGS === 'true';
const ttlExtensions = new Set(['.json', '.vtt', '.png', '.ts']);
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

function logRequestDetail(message) {
  if (requestLogEnabled) {
    console.log(message);
  }
}

async function incrementHitCount(filename) {
  await redisClient.hIncrBy(hitCountsKey, filename, 1);
}

async function incrementMetrics(updates) {
  if (!updates.length) {
    return;
  }

  try {
    const batch = redisClient.multi();
    for (const [field, amount] of updates) {
      batch.hIncrBy(metricsKey, field, amount);
    }
    await batch.exec();
  } catch (error) {
    console.error('Failed to increment VOD CDN metrics:', error.message);
  }
}

async function incrementMetric(field, amount = 1) {
  await incrementMetrics([[field, amount]]);
}

async function incrementRequestMetrics(filename) {
  await Promise.all([
    incrementHitCount(filename),
    incrementMetrics([
      ['totalServedRequests', 1],
      [`extension:${getFileExtension(filename) || 'none'}:requests`, 1],
    ]),
  ]);
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

  const cacheKey = `${cacheKeyPrefix}${filename}`;
  const cachedFile = await redisClient.get(cacheKey);

  if (!cachedFile) {
    return null;
  }

  if (shouldUseTtl(filename)) {
    const ttl = await redisClient.ttl(cacheKey);

    if (ttl === -1) {
      console.log(`Cache entry for ${filename} has no TTL, re-caching from Minio`);
      await redisClient.del(cacheKey);
      await incrementMetrics([
        ['evictedFiles', 1],
        ['evictedNoTtlFiles', 1],
      ]);
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
    const cacheKey = `${cacheKeyPrefix}${filename}`;
    const cacheValue = await encodeCacheValue(filename, buffer);

    if (shouldUseTtl(filename)) {
      await redisClient.set(cacheKey, cacheValue, { EX: cacheTtlSeconds });
    } else {
      await redisClient.set(cacheKey, cacheValue);
    }

    console.log(`Cached ${filename} to Redis`);
    await incrementMetrics([
      ['cachedFiles', 1],
      ['cachedBytes', buffer.length],
      ['cachedByFallbackFiles', 1],
    ]);
  } catch (error) {
    console.error(`Failed to cache ${filename}:`, error.message);
    await incrementMetric('cacheWriteFailures');
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
    await incrementMetric('rangeInvalidRequests');
    return res.status(416).end();
  }

  if (range) {
    const chunkSize = range.end - range.start + 1;
    const stream = await getPartialObjectStream(filename, range.start, chunkSize);

    await incrementMetrics([
      ['rangeRequests', 1],
      ['rangeServedBytes', chunkSize],
      ['cacheBypassRequests', 1],
    ]);
    res.status(206);
    res.set('Content-Range', `bytes ${range.start}-${range.end}/${fileSize}`);
    res.set('Content-Length', String(chunkSize));
    res.set('X-Cache-Layer', 'L1-Minio-Range');
    return stream.pipe(res);
  }

  const stream = await minioClient.getObject(minioBucket, filename);

  await incrementMetrics([
    ['minioStreamRequests', 1],
    ['cacheBypassRequests', 1],
    ['minioStreamBytes', fileSize],
  ]);
  res.set('Content-Length', String(fileSize));
  res.set('X-Cache-Layer', 'L1-Minio-Stream');
  return stream.pipe(res);
}

async function serveFile(req, res) {
  const filename = req.params[0];

  try {
    await incrementRequestMetrics(filename);

    if (supportsRangeRequests(filename)) {
      return serveRangeEnabledFile(req, res, filename);
    }

    const cachedFile = await getCachedFile(filename);

    if (cachedFile) {
      logRequestDetail(`Cache hit for ${filename}`);
      await incrementMetrics([
        ['cacheHits', 1],
        ['redisServedRequests', 1],
        ['redisServedBytes', cachedFile.length],
      ]);
      res.set('Content-Type', getContentType(filename));
      res.set('Cache-Control', 'public, max-age=3600');
      res.set('X-Cache-Layer', 'L0-Redis');
      const stream = Readable.from(cachedFile);
      return stream.pipe(res);
    }

    logRequestDetail(`Cache miss for ${filename}, fetching from Minio`);
    await incrementMetric('cacheMisses');

    const stream = await minioClient.getObject(minioBucket, filename);

    res.set('Content-Type', getContentType(filename));
    res.set('Cache-Control', 'public, max-age=3600');
    res.set('X-Cache-Layer', 'L1-Minio');

    await incrementMetric('minioFallbackRequests');
    stream.pipe(res);

    if (!isCacheExcluded(filename)) {
      cacheEvents.emit('cache_file', filename);
    }

  } catch (error) {
    console.error(`Error serving ${filename}:`, error.message);

    if (error.code === 'NoSuchKey') {
      await incrementMetric('notFoundResponses');
      return res.status(404).json({ error: 'File not found' });
    }

    if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND') {
      await incrementMetric('backendUnavailableResponses');
      return res.status(502).json({ error: 'Backend service unavailable' });
    }

    await incrementMetric('internalErrorResponses');
    res.status(500).json({ error: 'Internal server error' });
  }
}

app.get('/nami/*', serveFile);
app.get('/vod/*', serveFile);

async function countCachedFiles() {
  let cursor = '0';
  let count = 0;

  do {
    const scanResult = await redisClient.scan(cursor, {
      MATCH: `${cacheKeyPrefix}*`,
      COUNT: 100,
    });

    cursor = String(scanResult.cursor);
    count += scanResult.keys.length;
  } while (cursor !== '0');

  return count;
}

function parseMetricValue(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : value;
}

function normalizeMetrics(metrics) {
  return Object.entries(metrics).reduce((acc, [key, value]) => {
    acc[key] = parseMetricValue(value);
    return acc;
  }, {});
}

async function buildMetricsSnapshot() {
  const [metrics, hitCounts, currentCachedFiles] = await Promise.all([
    redisClient.hGetAll(metricsKey),
    redisClient.hGetAll(hitCountsKey),
    countCachedFiles(),
  ]);

  return {
    service: 'vod-cdn',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    redis: {
      cacheKeyPrefix,
      metricsKey,
      hitCountsKey,
      currentCachedFiles,
    },
    config: {
      cacheTtlSeconds,
      requestLogEnabled,
      ttlExtensions: Array.from(ttlExtensions),
      cacheExcludedExtensions: Array.from(cacheExcludedExtensions),
      rangeEnabledExtensions: Array.from(rangeEnabledExtensions),
    },
    totals: normalizeMetrics(metrics),
    hitCounts,
  };
}

async function sendMetrics(req, res) {
  try {
    res.json(await buildMetricsSnapshot());
  } catch (error) {
    res.status(500).json({ error: 'Failed to retrieve stats' });
  }
}

function prometheusMetricName(field) {
  const names = {
    backendUnavailableResponses: 'vod_cdn_backend_unavailable_responses_total',
    cacheBypassRequests: 'vod_cdn_cache_bypass_requests_total',
    cachedByFallbackFiles: 'vod_cdn_cached_by_fallback_files_total',
    cachedBytes: 'vod_cdn_cached_bytes_total',
    cachedFiles: 'vod_cdn_cached_files_total',
    cacheHits: 'vod_cdn_cache_hits_total',
    cacheMisses: 'vod_cdn_cache_misses_total',
    cacheWriteFailures: 'vod_cdn_cache_write_failures_total',
    evictedFiles: 'vod_cdn_evicted_files_total',
    evictedNoTtlFiles: 'vod_cdn_evicted_no_ttl_files_total',
    internalErrorResponses: 'vod_cdn_internal_error_responses_total',
    minioFallbackRequests: 'vod_cdn_minio_fallback_requests_total',
    minioStreamBytes: 'vod_cdn_minio_stream_bytes_total',
    minioStreamRequests: 'vod_cdn_minio_stream_requests_total',
    notFoundResponses: 'vod_cdn_not_found_responses_total',
    rangeInvalidRequests: 'vod_cdn_range_invalid_requests_total',
    rangeRequests: 'vod_cdn_range_requests_total',
    rangeServedBytes: 'vod_cdn_range_served_bytes_total',
    redisServedBytes: 'vod_cdn_redis_served_bytes_total',
    redisServedRequests: 'vod_cdn_redis_served_requests_total',
    totalServedRequests: 'vod_cdn_requests_total',
  };

  return names[field];
}

function escapePrometheusLabelValue(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/"/g, '\\"');
}

function formatPrometheusSample(name, value, labels = {}) {
  const labelEntries = Object.entries(labels);
  const labelText = labelEntries.length
    ? `{${labelEntries.map(([key, labelValue]) => `${key}="${escapePrometheusLabelValue(labelValue)}"`).join(',')}}`
    : '';

  return `${name}${labelText} ${Number(value) || 0}`;
}

function buildPrometheusMetrics(snapshot) {
  const lines = [
    '# HELP vod_cdn_info Static service identity.',
    '# TYPE vod_cdn_info gauge',
    formatPrometheusSample('vod_cdn_info', 1, { service: snapshot.service }),
    '# HELP vod_cdn_uptime_seconds Process uptime in seconds.',
    '# TYPE vod_cdn_uptime_seconds gauge',
    formatPrometheusSample('vod_cdn_uptime_seconds', snapshot.uptime),
    '# HELP vod_cdn_current_cached_files Current number of VOD CDN files in Redis.',
    '# TYPE vod_cdn_current_cached_files gauge',
    formatPrometheusSample('vod_cdn_current_cached_files', snapshot.redis.currentCachedFiles),
  ];

  for (const [field, value] of Object.entries(snapshot.totals)) {
    const extensionMatch = field.match(/^extension:(.*):requests$/);

    if (extensionMatch) {
      lines.push(formatPrometheusSample('vod_cdn_extension_requests_total', value, {
        extension: extensionMatch[1],
      }));
      continue;
    }

    const metricName = prometheusMetricName(field);
    if (metricName) {
      lines.push(formatPrometheusSample(metricName, value));
    }
  }

  return `${lines.join('\n')}\n`;
}

async function sendPrometheusMetrics(req, res) {
  try {
    res.type('text/plain; version=0.0.4; charset=utf-8');
    res.send(buildPrometheusMetrics(await buildMetricsSnapshot()));
  } catch (error) {
    res.status(500).type('text/plain').send('Failed to retrieve VOD CDN metrics\n');
  }
}

app.get('/stats', sendMetrics);
app.get('/metrics', sendMetrics);
app.get('/metrics/prometheus', sendPrometheusMetrics);

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
  buildMetricsSnapshot,
  buildPrometheusMetrics,
  parseRangeHeader,
  serveRangeEnabledFile,
  startServer,
  supportsRangeRequests,
  getContentType,
};
