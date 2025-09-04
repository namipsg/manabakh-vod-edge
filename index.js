require('dotenv').config();
const express = require('express');
const redis = require('redis');
const { Client } = require('minio');
const { EventEmitter } = require('events');
const { Readable } = require('stream');
const mime = require('mime-types');
const app = express();
const port = process.env.PORT || 3000;
const minioBucket = process.env.MINIO_BUCKET_NAME || 'test'

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

async function cacheFileFromMinio(filename) {
  try {
    const stream = await minioClient.getObject(minioBucket, filename);
    const chunks = [];

    for await (const chunk of stream) {
      chunks.push(chunk);
    }

    const buffer = Buffer.concat(chunks);
    await redisClient.setEx(`vod_file:${filename}`, 3600, buffer.toString('base64'));
    console.log(`Cached ${filename} to Redis`);
  } catch (error) {
    console.error(`Failed to cache ${filename}:`, error.message);
  }
}

cacheEvents.on('cache_file', cacheFileFromMinio);

app.get('/nami/*', async (req, res) => {
  const filename = req.params[0];

  try {
    await incrementHitCount(filename);

    const cachedFile = await redisClient.get(`vod_file:${filename}`);

    if (cachedFile) {
      console.log(`Cache hit for ${filename}`);
      res.set('Content-Type', mime.lookup(filename) || 'application/octet-stream');
      res.set('Cache-Control', 'public, max-age=3600');
      res.set('X-Cache-Layer', 'L0-Redis');
      const buffer = Buffer.from(cachedFile, 'base64');
      const stream = Readable.from(buffer);
      return stream.pipe(res);
    }

    console.log(`Cache miss for ${filename}, fetching from Minio`);

    const stream = await minioClient.getObject(minioBucket, filename);

    res.set('Content-Type', mime.lookup(filename) || 'application/octet-stream');
    res.set('Cache-Control', 'public, max-age=3600');
    res.set('X-Cache-Layer', 'L1-Minio');

    stream.pipe(res);

    cacheEvents.emit('cache_file', filename);

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
});

app.get('/vod/*', async (req, res) => {
  const filename = req.params[0];

  try {
    await incrementHitCount(filename);

    const cachedFile = await redisClient.get(`vod_file:${filename}`);

    if (cachedFile) {
      console.log(`Cache hit for ${filename}`);
      res.set('Content-Type', mime.lookup(filename) || 'application/octet-stream');
      res.set('Cache-Control', 'public, max-age=3600');
      res.set('X-Cache-Layer', 'L0-Redis');
      const buffer = Buffer.from(cachedFile, 'base64');
      const stream = Readable.from(buffer);
      return stream.pipe(res);
    }

    console.log(`Cache miss for ${filename}, fetching from Minio`);

    const stream = await minioClient.getObject(minioBucket, filename);

    res.set('Content-Type', mime.lookup(filename) || 'application/octet-stream');
    res.set('Cache-Control', 'public, max-age=3600');
    res.set('X-Cache-Layer', 'L1-Minio');

    stream.pipe(res);

    cacheEvents.emit('cache_file', filename);

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
});

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

const server = app.listen(port, async () => {
  try {
    await initializeRedis();
    console.log(`FCDN server running on port ${port}`);
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
});

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

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));