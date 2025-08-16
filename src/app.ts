import express, { Express } from 'express';
import compression from 'compression';
import helmet from 'helmet';
import { SERVER, ROUTES } from './config/constants.js';
import { corsMiddleware, errorHandler, requestLogger,  } from './middleware.js';
import proxyRoutes from './proxy-routes.js';
import cdnRoutes from './cdn-routes.js';
import { cacheManager } from './services/cache/cache-manager.js';
import { getCacheStats, clearCache, switchCacheBackend, isCacheHealthy } from './utils/cache-v2.js';

/**
 * Create and configure the Express application
 */
const app: Express = express();

// Apply global middleware
app.use(compression());
app.use(helmet({
  contentSecurityPolicy: false // Disable CSP for proxy functionality
}));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// Apply custom middleware
app.use(corsMiddleware);
app.use(requestLogger);


// Set up routes
app.use(ROUTES.PROXY_BASE, proxyRoutes);
app.use(ROUTES.CDN_BASE, cdnRoutes);

// Root route
app.get('/', (req, res) => {
  res.json({
    name: 'MCDN - Edge CDN Proxy',
    version: process.env.npm_package_version || '0.2.0',
    description: 'An edge CDN proxy for S3/MinIO VOD archives with streaming media support',
    usage: {
      cdn: {
        withBucket: `${ROUTES.CDN_BASE}/bucket-name/path/to/video.m3u8`,
        defaultBucket: `${ROUTES.CDN_BASE}/path/to/video.m3u8`,
      },
      proxy: {
        queryParam: `${ROUTES.PROXY_BASE}?url=https://example.com`,
        pathParam: `${ROUTES.PROXY_BASE}/https://example.com`,
        base64: `${ROUTES.PROXY_BASE}/base64/${Buffer.from('https://example.com').toString('base64')}`,
      },
    },
    status: `${ROUTES.PROXY_BASE}/status`,
  });
});

// Status endpoint
app.get(`${ROUTES.PROXY_BASE}/status`, (req, res) => {
  const uptime = process.uptime();
  const memory = process.memoryUsage();
  
  return res.json({
    status: 'ok',
    version: process.env.npm_package_version || '0.2.0',
    uptime,
    timestamp: new Date().toISOString(),
    environment: SERVER.NODE_ENV,
    memory: {
      rss: Math.round(memory.rss / 1024 / 1024 * 100) / 100,
      heapTotal: Math.round(memory.heapTotal / 1024 / 1024 * 100) / 100,
      heapUsed: Math.round(memory.heapUsed / 1024 / 1024 * 100) / 100,
      external: Math.round(memory.external / 1024 / 1024 * 100) / 100,
    },
  });
});

// Cache endpoints
app.get(`${ROUTES.PROXY_BASE}/cache/stats`, async (req, res) => {
  try {
    const stats = await getCacheStats();
    res.json({
      status: 'ok',
      data: stats,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: {
        code: 500,
        message: 'Failed to get cache stats',
      },
      success: false,
      timestamp: new Date().toISOString(),
    });
  }
});

app.post(`${ROUTES.PROXY_BASE}/cache/clear`, async (req, res) => {
  try {
    const success = await clearCache();
    if (success) {
      res.json({
        status: 'ok',
        message: 'Cache cleared successfully',
        timestamp: new Date().toISOString(),
      });
    } else {
      res.status(500).json({
        error: {
          code: 500,
          message: 'Failed to clear cache',
        },
        success: false,
        timestamp: new Date().toISOString(),
      });
    }
  } catch (error) {
    res.status(500).json({
      error: {
        code: 500,
        message: 'Failed to clear cache',
      },
      success: false,
      timestamp: new Date().toISOString(),
    });
  }
});

app.post(`${ROUTES.PROXY_BASE}/cache/switch`, async (req, res) => {
  try {
    const { mode } = req.body;
    
    if (!mode || !['memory', 'redis', 'cassandra', 'redis-cassandra'].includes(mode)) {
      return res.status(400).json({
        error: {
          code: 400,
          message: 'Invalid cache mode. Must be one of: memory, redis, cassandra, redis-cassandra',
        },
        success: false,
        timestamp: new Date().toISOString(),
      });
    }

    const success = await switchCacheBackend(mode);
    if (success) {
      res.json({
        status: 'ok',
        message: `Cache backend switched to ${mode}`,
        mode: cacheManager.getMode(),
        timestamp: new Date().toISOString(),
      });
    } else {
      res.status(500).json({
        error: {
          code: 500,
          message: `Failed to switch to ${mode} cache backend`,
        },
        success: false,
        timestamp: new Date().toISOString(),
      });
    }
  } catch (error) {
    res.status(500).json({
      error: {
        code: 500,
        message: 'Failed to switch cache backend',
      },
      success: false,
      timestamp: new Date().toISOString(),
    });
  }
});

app.get(`${ROUTES.PROXY_BASE}/cache/health`, async (req, res) => {
  try {
    const healthy = await isCacheHealthy();
    res.json({
      status: 'ok',
      data: {
        healthy,
        mode: cacheManager.getMode(),
        initialized: cacheManager.isInitialized(),
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: {
        code: 500,
        message: 'Failed to check cache health',
      },
      success: false,
      timestamp: new Date().toISOString(),
    });
  }
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: {
      code: 404,
      message: 'Not Found',
      path: req.path,
    },
    success: false,
    timestamp: new Date().toISOString(),
  });
});

// Global error handler
app.use(errorHandler);

export default app;