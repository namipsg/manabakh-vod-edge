const express = require('express');
const redis = require('redis');
const cassandra = require('cassandra-driver');
const Minio = require('minio');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

const minioEndpoint = process.env.MINIO_ENDPOINT?.replace(/^https?:\/\//, '');

const minioClient = new Minio.Client({
  endPoint: minioEndpoint,
  port: parseInt(process.env.MINIO_PORT) || 9000,
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
});

const BUCKET_NAME = process.env.MINIO_BUCKET_NAME;
const VOD_BUCKET_NAME = process.env.VOD_BUCKET_NAME || 'vod-files';
const CASSANDRA_HOSTS = process.env.CASSANDRA_HOSTS || 'localhost:9042';
const CASSANDRA_KEYSPACE = process.env.CASSANDRA_KEYSPACE || 'cdn_cache';
const REDIS_MEMORY_THRESHOLD = 0.8;
const CASSANDRA_MAX_FILES = parseInt(process.env.CASSANDRA_MAX_FILES) || 10000;

let redisClient;
let cassandraClient;

async function initRedis() {
  redisClient = redis.createClient({
    url: REDIS_URL
  });

  redisClient.on('error', (err) => {
    console.error('Redis Client Error:', err);
  });

  redisClient.on('connect', () => {
    console.log('Connected to Redis');
  });

  await redisClient.connect();
}

async function initCassandra() {
  cassandraClient = new cassandra.Client({
    contactPoints: CASSANDRA_HOSTS.split(','),
    localDataCenter: 'DC1-VoD'
  });

  try {
    await cassandraClient.execute(`
      CREATE KEYSPACE IF NOT EXISTS ${CASSANDRA_KEYSPACE}
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    `);

    await cassandraClient.execute(`USE ${CASSANDRA_KEYSPACE}`);

    await cassandraClient.execute(`
      CREATE TABLE IF NOT EXISTS vod_cache (
        file_key text PRIMARY KEY,
        buffer blob,
        content_length int,
        content_type text,
        last_accessed timestamp
      )
    `);

    await cassandraClient.execute(`
      CREATE TABLE IF NOT EXISTS hit_counts (
        file_key text PRIMARY KEY,
        hit_count counter
      )
    `);

    console.log('Connected to Cassandra and created tables');
  } catch (error) {
    console.error('Cassandra initialization error:', error);
    throw error;
  }
}

async function getRedisMemoryUsage() {
  try {
    const info = await redisClient.info('memory');
    const lines = info.split('\r');
    const usedMemory = parseInt(lines.find(line => line.startsWith('used_memory:'))?.split(':')[1] || '0');
    const maxMemory = parseInt(lines.find(line => line.startsWith('maxmemory:'))?.split(':')[1] || '0');

    if (maxMemory === 0) return 0;
    return usedMemory / maxMemory;
  } catch (error) {
    console.error('Error getting Redis memory usage:', error);
    return 0;
  }
}

async function incrementHitCount(fileKey, location = 'redis') {
  try {
    if (location === 'redis') {
      await redisClient.incr(`hit_count:${fileKey}`);
    } else {
      await cassandraClient.execute(
        'UPDATE hit_counts SET hit_count = hit_count + 1 WHERE file_key = ?',
        [fileKey]
      );
    }
  } catch (error) {
    console.error('Error incrementing hit count:', error);
  }
}

async function getHitCount(fileKey, location = 'redis') {
  try {
    if (location === 'redis') {
      const count = await redisClient.get(`hit_count:${fileKey}`);
      return parseInt(count) || 0;
    } else {
      const result = await cassandraClient.execute(
        'SELECT hit_count FROM hit_counts WHERE file_key = ?',
        [fileKey]
      );
      return result.rows[0]?.hit_count || 0;
    }
  } catch (error) {
    console.error('Error getting hit count:', error);
    return 0;
  }
}

async function getCassandraStorageInfo() {
  try {
    const result = await cassandraClient.execute('SELECT COUNT(*) as count FROM vod_cache');
    const totalFiles = result.rows[0]?.count || 0;
    
    const sizeResult = await cassandraClient.execute(
      'SELECT SUM(content_length) as total_size FROM vod_cache'
    );
    const totalSize = sizeResult.rows[0]?.total_size || 0;
    
    return {
      totalFiles,
      totalSize,
      utilizationPercent: (totalFiles / CASSANDRA_MAX_FILES) * 100,
      isNearCapacity: totalFiles > (CASSANDRA_MAX_FILES * 0.9)
    };
  } catch (error) {
    console.error('Error getting Cassandra storage info:', error);
    return {
      totalFiles: 0,
      totalSize: 0,
      utilizationPercent: 0,
      isNearCapacity: false
    };
  }
}

async function moveToSecondaryCache(fileKey) {
  try {
    const redisData = await redisClient.get(`vod:${fileKey}`);
    if (!redisData) return false;

    const data = JSON.parse(redisData);
    const hitCount = await getHitCount(fileKey, 'redis');

    await cassandraClient.execute(
      'INSERT INTO vod_cache (file_key, buffer, content_length, content_type, last_accessed) VALUES (?, ?, ?, ?, ?)',
      [fileKey, Buffer.from(data.buffer, 'base64'), data.contentLength, data.contentType, new Date()]
    );

    await cassandraClient.execute(
      'UPDATE hit_counts SET hit_count = hit_count + ? WHERE file_key = ?',
      [hitCount, fileKey]
    );

    await redisClient.del(`vod:${fileKey}`);
    await redisClient.del(`hit_count:${fileKey}`);

    console.log(`Moved ${fileKey} from Redis to Cassandra`);
    return true;
  } catch (error) {
    console.error('Error moving file to secondary cache:', error);
    return false;
  }
}

async function cleanupLowHitFiles() {
  try {
    const redisMemUsage = await getRedisMemoryUsage();

    if (redisMemUsage > REDIS_MEMORY_THRESHOLD) {
      const keys = await redisClient.keys('vod:*');
      const hitCounts = [];

      for (const key of keys) {
        const fileKey = key.replace('vod:', '');
        const hitCount = await getHitCount(fileKey, 'redis');
        hitCounts.push({ key: fileKey, hitCount });
      }

      hitCounts.sort((a, b) => a.hitCount - b.hitCount);
      const filesToMove = Math.ceil(keys.length * 0.2);

      for (let i = 0; i < filesToMove && i < hitCounts.length; i++) {
        await moveToSecondaryCache(hitCounts[i].key);
      }
    }

    const cassandraInfo = await getCassandraStorageInfo();
    
    if (cassandraInfo.isNearCapacity) {
      const cassandraResult = await cassandraClient.execute(
        'SELECT file_key, hit_count FROM hit_counts'
      );
      
      const sortedFiles = cassandraResult.rows
        .sort((a, b) => a.hit_count - b.hit_count)
        .slice(0, Math.ceil(cassandraResult.rows.length * 0.1));

      for (const file of sortedFiles) {
        await cassandraClient.execute(
          'DELETE FROM vod_cache WHERE file_key = ?',
          [file.file_key]
        );
        await cassandraClient.execute(
          'DELETE FROM hit_counts WHERE file_key = ?',
          [file.file_key]
        );
      }

      console.log(`Cleaned up ${sortedFiles.length} low-hit files from Cassandra (${cassandraInfo.utilizationPercent.toFixed(1)}% capacity)`);
    }
  } catch (error) {
    console.error('Error during cleanup:', error);
  }
}

app.get('/vod/*', async (req, res) => {
  const vodPath = req.params[0];
  const fileKey = `vod:${vodPath}`;

  try {
    if (vodPath.endsWith('.mp4')) {
      const s3Url = await minioClient.presignedGetObject(VOD_BUCKET_NAME, vodPath, 24 * 60 * 60);
      return res.redirect(302, s3Url);
    }

    let cachedData = await redisClient.get(fileKey);
    let cacheLocation = 'redis';

    if (cachedData) {
      console.log(`Redis cache hit for: ${vodPath}`);
      await incrementHitCount(vodPath, 'redis');
    } else {
      try {
        const cassandraResult = await cassandraClient.execute(
          'SELECT buffer, content_length, content_type FROM vod_cache WHERE file_key = ?',
          [vodPath]
        );

        if (cassandraResult.rows.length > 0) {
          console.log(`Cassandra cache hit for: ${vodPath}`);
          const row = cassandraResult.rows[0];
          cachedData = JSON.stringify({
            buffer: row.buffer.toString('base64'),
            contentLength: row.content_length,
            contentType: row.content_type
          });
          cacheLocation = 'cassandra';
          await incrementHitCount(vodPath, 'cassandra');

          await cassandraClient.execute(
            'UPDATE vod_cache SET last_accessed = ? WHERE file_key = ?',
            [new Date(), vodPath]
          );
        }
      } catch (cassandraError) {
        console.error('Cassandra lookup error:', cassandraError);
      }
    }

    if (cachedData) {
      const data = JSON.parse(cachedData);

      res.set({
        'Content-Type': data.contentType || 'application/octet-stream',
        'Content-Length': data.contentLength,
        'Content-Disposition': `inline; filename=\"${encodeURIComponent(vodPath.split('/').pop())}\"`,
        'X-Cache': `HIT-${cacheLocation.toUpperCase()}`
      });

      return res.send(Buffer.from(data.buffer, 'base64'));
    }

    console.log(`Cache miss for VOD: ${vodPath}`);

    if (!VOD_BUCKET_NAME) {
      return res.status(500).json({ error: 'VOD_BUCKET_NAME not configured' });
    }

    const fileBuffer = await minioClient.getObject(VOD_BUCKET_NAME, vodPath);

    const chunks = [];
    fileBuffer.on('data', (chunk) => chunks.push(chunk));
    fileBuffer.on('end', async () => {
      try {
        const buffer = Buffer.concat(chunks);
        const contentType = getContentType(vodPath);

        const cacheData = {
          buffer: buffer.toString('base64'),
          contentLength: buffer.length,
          contentType
        };

        await redisClient.set(fileKey, JSON.stringify(cacheData));
        await incrementHitCount(vodPath, 'redis');

        console.log(`Cached VOD file: ${vodPath}`);

        res.set({
          'Content-Type': contentType,
          'Content-Length': buffer.length,
          'Content-Disposition': `inline; filename=\"${encodeURIComponent(vodPath.split('/').pop())}\"`,
          'X-Cache': 'MISS'
        });

        res.send(buffer);

        setTimeout(() => cleanupLowHitFiles(), 1000);
      } catch (cacheError) {
        console.error(`Error caching VOD ${vodPath}:`, cacheError.message);
        res.status(500).json({ error: 'Cache error' });
      }
    });

    fileBuffer.on('error', (error) => {
      console.error(`Error reading VOD file ${vodPath}:`, error.message);

      if (error.code === 'NoSuchKey' || error.code === 'NotFound') {
        return res.status(404).json({ error: 'VOD file not found' });
      }

      res.status(500).json({ error: 'Failed to fetch VOD file from storage' });
    });

  } catch (error) {
    console.error(`Error processing VOD ${vodPath}:`, error.message);

    if (error.code === 'NoSuchKey' || error.code === 'NotFound') {
      return res.status(404).json({ error: 'VOD file not found' });
    }

    if (error.code === 'NoSuchBucket') {
      return res.status(503).json({ error: 'VOD storage bucket not found' });
    }

    res.status(500).json({ error: 'Internal server error' });
  }
});

function getContentType(filename) {
  const ext = filename.split('.').pop().toLowerCase();
  const mimeTypes = {
    'mp4': 'video/mp4',
    'webm': 'video/webm',
    'avi': 'video/x-msvideo',
    'mov': 'video/quicktime',
    'wmv': 'video/x-ms-wmv',
    'flv': 'video/x-flv',
    'mkv': 'video/x-matroska',
    'm3u8': 'application/vnd.apple.mpegurl',
    'ts': 'video/mp2t',
    'vtt': 'text/vtt',
    'srt': 'application/x-subrip',
    'json': 'application/json',
    'xml': 'application/xml',
    'txt': 'text/plain'
  };
  return mimeTypes[ext] || 'application/octet-stream';
}

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    redis: redisClient?.isOpen ? 'connected' : 'disconnected',
    cassandra: cassandraClient?.getState()?.getConnectedHosts()?.length > 0 ? 'connected' : 'disconnected',
    timestamp: new Date().toISOString()
  });
});

async function startServer() {
  try {
    await initRedis();
    await initCassandra();

    setInterval(() => {
      cleanupLowHitFiles().catch(error => {
        console.error('Cleanup error:', error);
      });
    }, 5 * 60 * 1000);

    app.listen(PORT, () => {
      console.log(`CDN cache service running on port ${PORT}`);
      console.log(`Health check available at http://localhost:${PORT}/health`);
      console.log(`Cache stats available at http://localhost:${PORT}/cache/stats`);
      console.log(`VOD files available at http://localhost:${PORT}/vod/*`);
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down gracefully...');
  await redisClient?.quit();
  await cassandraClient?.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('Received SIGINT, shutting down gracefully...');
  await redisClient?.quit();
  await cassandraClient?.shutdown();
  process.exit(0);
});

startServer();