import { Router, Request, Response } from 'express';
import { s3Service } from './services/s3-service.js';
import { logger } from './middleware.js';
import { processM3U8ContentForCDN } from './utils/m3u8-handler.js';
import { detectContentType } from './utils/helpers.js';
import { generateCacheKey, getCacheItem, setCacheItem } from './utils/cache-v2.js';
import { pipeline } from 'stream';
import { promisify } from 'util';

const pipelineAsync = promisify(pipeline);
const router = Router();

interface CDNRequest extends Request {
  cdnPath?: string;
}

// Middleware to extract CDN path
router.use('/*', (req: CDNRequest, _res, next) => {
  req.cdnPath = req.params[0] || '';
  next();
});

// Main CDN route handler
router.get('/*', async (req: CDNRequest, res: Response) => {
  const startTime = Date.now();
  const cdnPath = req.cdnPath?.includes('/cdn') ? req.cdnPath : 'nami/' + req.cdnPath || '';

  try {
    if (!cdnPath) {
      return res.status(400).json({
        error: {
          code: 400,
          message: 'Object path is required',
        },
        success: false,
        timestamp: new Date().toISOString(),
      });
    }

    // Parse bucket and object key from path
    const { bucket, key } = s3Service.parseObjectPath(cdnPath);

    logger.info({
      type: 'cdn-request',
      bucket,
      key,
      userAgent: req.get('User-Agent'),
      ip: req.ip,
    }, `CDN request: ${bucket}/${key}`);

    // Generate cache key for this request
    const cacheKey = generateCacheKey(`${bucket}/${key}`, {
      range: req.get('Range'),
    });

    // Check cache for smaller files (not for range requests)
    const range = req.get('Range');
    if (!range) {
      const cachedItem = await getCacheItem(cacheKey);
      if (cachedItem) {
        logger.debug({
          type: 'cdn-cache',
          bucket,
          key,
          size: cachedItem.size,
        }, 'Serving from cache');

        // Determine content type
        let contentType = cachedItem.contentType || detectContentType(key);

        // Set headers
        res.set({
          'Content-Type': contentType,
          'Content-Length': cachedItem.size.toString(),
          'Cache-Control': 'public, max-age=3600',
          'X-Cache': 'HIT',
          'ETag': cachedItem.etag || '',
          'Last-Modified': cachedItem.lastModified?.toUTCString() || '',
        });

        return res.send(cachedItem.data);
      }
    }

    // Handle range requests
    let rangeHeader: string | undefined;

    if (range) {
      rangeHeader = range;
      logger.debug({
        type: 'cdn-request',
        bucket,
        key,
        range,
      }, 'Range request received');
    }

    // Get object from S3
    const s3Response = await s3Service.getObject(bucket, key, rangeHeader);

    if (!s3Response.body) {
      return res.status(404).json({
        error: {
          code: 404,
          message: 'Object not found',
        },
        success: false,
        timestamp: new Date().toISOString(),
      });
    }

    // Set CDN headers
    const headers: Record<string, string> = {
      'Cache-Control': 'public, max-age=3600', // 1 hour cache
      'Accept-Ranges': 'bytes',
    };

    if (s3Response.etag) {
      headers['ETag'] = s3Response.etag;
    }

    if (s3Response.lastModified) {
      headers['Last-Modified'] = s3Response.lastModified.toUTCString();
    }

    if (s3Response.contentLength) {
      headers['Content-Length'] = s3Response.contentLength.toString();
    }

    // Determine content type
    let contentType = s3Response.contentType;
    if (!contentType || contentType === 'application/octet-stream') {
      contentType = detectContentType(key);
    }

    headers['Content-Type'] = contentType;

    // Handle range requests
    if (range && s3Response.contentRange) {
      res.status(206); // Partial Content
      headers['Content-Range'] = s3Response.contentRange;
    } else {
      res.status(200);
    }

    // Apply headers
    Object.entries(headers).forEach(([name, value]) => {
      res.set(name, value);
    });

    // Add cache status header
    res.set('X-Cache', 'MISS');

    // Special handling for M3U8 playlists
    if (contentType?.includes('application/vnd.apple.mpegurl') ||
      contentType?.includes('application/x-mpegURL') ||
      key.endsWith('.m3u8')) {

      logger.debug({
        type: 'cdn-request',
        bucket,
        key,
      }, 'Processing M3U8 playlist');

      // Read the entire stream for M3U8 processing
      const chunks: Buffer[] = [];
      const stream = s3Response.body as NodeJS.ReadableStream;

      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => {
        try {
          const content = Buffer.concat(chunks).toString('utf-8');
          const baseUrl = `${req.protocol}://${req.get('host')}/vod/`;
          const processedContent = processM3U8ContentForCDN(content, baseUrl, key);

          const processedBuffer = Buffer.from(processedContent, 'utf-8');
          res.set('Content-Length', processedBuffer.length.toString());
          res.send(processedContent);

          // Cache processed M3U8 content (they're usually small)
          if (!range && processedBuffer.length < 1024 * 1024) { // Cache if < 1MB
            setCacheItem(cacheKey, processedBuffer, processedBuffer.length, {
              contentType,
              etag: s3Response.etag,
              lastModified: s3Response.lastModified,
            }).catch((error) => {
              logger.warn({
                type: 'cdn-cache',
                error: error instanceof Error ? error.message : String(error),
                key: cacheKey.substring(0, 50),
              }, 'Failed to cache M3U8 content');
            });
          }

          logger.info({
            type: 'cdn-response',
            bucket,
            key,
            statusCode: res.statusCode,
            duration: Date.now() - startTime,
            size: processedBuffer.length,
          }, 'M3U8 playlist served');
        } catch (error) {
          logger.error({
            type: 'cdn-error',
            error: error instanceof Error ? error.message : String(error),
            bucket,
            key,
          }, 'Failed to process M3U8 content');

          res.status(500).json({
            error: {
              code: 500,
              message: 'Failed to process playlist',
            },
            success: false,
            timestamp: new Date().toISOString(),
          });
        }
      });

      stream.on('error', (error) => {
        logger.error({
          type: 'cdn-error',
          error: error instanceof Error ? error.message : String(error),
          bucket,
          key,
        }, 'Stream error while processing M3U8');

        if (!res.headersSent) {
          res.status(500).json({
            error: {
              code: 500,
              message: 'Stream error',
            },
            success: false,
            timestamp: new Date().toISOString(),
          });
        }
      });

      return;
    }

    // Stream other content directly
    const stream = s3Response.body as NodeJS.ReadableStream;

    // For small files (< 5MB), collect data for caching
    const shouldCache = !range && s3Response.contentLength && s3Response.contentLength < 5 * 1024 * 1024;
    let cacheBuffer: Buffer[] = [];
    let totalSize = 0;

    if (shouldCache) {
      // Collect data for caching while streaming
      stream.on('data', (chunk: Buffer) => {
        cacheBuffer.push(chunk);
        totalSize += chunk.length;

        // If it gets too big while streaming, stop collecting for cache
        if (totalSize > 5 * 1024 * 1024) {
          cacheBuffer = [];
          totalSize = 0;
        }
      });

      stream.on('end', () => {
        // Cache the collected data if it's still reasonable size
        if (cacheBuffer.length > 0 && totalSize > 0 && totalSize < 5 * 1024 * 1024) {
          const fullBuffer = Buffer.concat(cacheBuffer);
          setCacheItem(cacheKey, fullBuffer, fullBuffer.length, {
            contentType,
            etag: s3Response.etag,
            lastModified: s3Response.lastModified,
          }).then((success) => {
            if (success) {
              logger.debug({
                type: 'cdn-cache',
                bucket,
                key,
                size: totalSize,
              }, 'File cached for future requests');
            }
          }).catch((error) => {
            logger.warn({
              type: 'cdn-cache',
              error: error instanceof Error ? error.message : String(error),
              bucket,
              key,
            }, 'Failed to cache file');
          });
        }
      });
    }

    // Handle streaming with proper error handling
    stream.on('error', (error) => {
      logger.error({
        type: 'cdn-error',
        error: error instanceof Error ? error.message : String(error),
        bucket,
        key,
      }, 'Stream error');

      if (!res.headersSent) {
        res.status(500).json({
          error: {
            code: 500,
            message: 'Stream error',
          },
          success: false,
          timestamp: new Date().toISOString(),
        });
      }
    });

    res.on('close', () => {
      logger.debug({
        type: 'cdn-response',
        bucket,
        key,
        statusCode: res.statusCode,
        duration: Date.now() - startTime,
      }, 'Connection closed');
    });

    await pipelineAsync(stream, res);

    logger.info({
      type: 'cdn-response',
      bucket,
      key,
      statusCode: res.statusCode,
      duration: Date.now() - startTime,
      contentType,
    }, 'Object served successfully');

  } catch (error) {
    logger.error({
      type: 'cdn-error',
      error: error instanceof Error ? error.message : String(error),
      cdnPath,
      duration: Date.now() - startTime,
    }, 'CDN request failed');

    if (!res.headersSent) {
      const statusCode = s3Service.isS3Error(error) ? s3Service.getS3ErrorStatus(error) : 500;

      res.status(statusCode).json({
        error: {
          code: statusCode,
          message: statusCode === 404 ? 'Object not found' : 'Internal server error',
        },
        success: false,
        timestamp: new Date().toISOString(),
      });
    }
  }
});

// HEAD request support for object metadata
router.head('/*', async (req: CDNRequest, res: Response) => {
  const cdnPath = req.cdnPath || '';

  try {
    if (!cdnPath) {
      return res.status(400).end();
    }

    const { bucket, key } = s3Service.parseObjectPath(cdnPath);
    const metadata = await s3Service.headObject(bucket, key);

    // Set headers
    const headers: Record<string, string> = {
      'Cache-Control': 'public, max-age=3600',
      'Accept-Ranges': 'bytes',
    };

    if (metadata.etag) {
      headers['ETag'] = metadata.etag;
    }

    if (metadata.lastModified) {
      headers['Last-Modified'] = metadata.lastModified.toUTCString();
    }

    if (metadata.contentLength) {
      headers['Content-Length'] = metadata.contentLength.toString();
    }

    let contentType = metadata.contentType;
    if (!contentType || contentType === 'application/octet-stream') {
      contentType = detectContentType(key);
    }

    headers['Content-Type'] = contentType;

    Object.entries(headers).forEach(([name, value]) => {
      res.set(name, value);
    });

    res.status(200).end();

  } catch (error) {
    const statusCode = s3Service.isS3Error(error) ? s3Service.getS3ErrorStatus(error) : 500;
    res.status(statusCode).end();
  }
});

export default router;