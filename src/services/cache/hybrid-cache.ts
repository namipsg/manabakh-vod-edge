import { CacheBackend, CacheItem, CacheStats } from './cache-interface.js';
import { RedisCache } from './redis-cache.js';
import { CassandraCache } from './cassandra-cache.js';
import { CACHE } from '../../config/constants.js';
import { logger } from '../../middleware.js';

/**
 * Hybrid cache implementation using Redis as L1 cache and Cassandra as L2 cache
 * This provides fast access through Redis while maintaining persistence through Cassandra
 */
export class HybridCache implements CacheBackend {
  private redisCache: RedisCache;
  private cassandraCache: CassandraCache;
  private redisConnected = false;
  private cassandraConnected = false;

  constructor() {
    this.redisCache = new RedisCache();
    this.cassandraCache = new CassandraCache();
  }

  async initialize(): Promise<void> {
    const initPromises: Promise<void>[] = [];

    // Initialize Redis (L1 cache)
    initPromises.push(
      this.redisCache.initialize()
        .then(() => {
          this.redisConnected = true;
          logger.info({
            type: 'hybrid-cache',
            component: 'redis',
            status: 'connected',
          }, 'Hybrid cache: Redis L1 connected');
        })
        .catch((error) => {
          this.redisConnected = false;
          logger.error({
            type: 'hybrid-cache',
            component: 'redis',
            error: error instanceof Error ? error.message : String(error),
          }, 'Hybrid cache: Redis L1 failed to connect');
        })
    );

    // Initialize Cassandra (L2 cache)
    initPromises.push(
      this.cassandraCache.initialize()
        .then(() => {
          this.cassandraConnected = true;
          logger.info({
            type: 'hybrid-cache',
            component: 'cassandra',
            status: 'connected',
          }, 'Hybrid cache: Cassandra L2 connected');
        })
        .catch((error) => {
          this.cassandraConnected = false;
          logger.error({
            type: 'hybrid-cache',
            component: 'cassandra',
            error: error instanceof Error ? error.message : String(error),
          }, 'Hybrid cache: Cassandra L2 failed to connect');
        })
    );

    // Wait for both to complete (but don't fail if one fails)
    await Promise.allSettled(initPromises);

    if (!this.redisConnected && !this.cassandraConnected) {
      throw new Error('Both Redis and Cassandra failed to connect');
    }

    logger.info({
      type: 'hybrid-cache',
      redisConnected: this.redisConnected,
      cassandraConnected: this.cassandraConnected,
    }, 'Hybrid cache initialized');
  }

  async get(key: string): Promise<CacheItem | null> {
    try {
      // Try Redis first (L1 cache)
      if (this.redisConnected) {
        const redisResult = await this.redisCache.get(key);
        if (redisResult) {
          logger.debug({
            type: 'hybrid-cache',
            action: 'l1-hit',
            key: key.substring(0, 50),
          }, 'Hybrid cache L1 hit');
          return redisResult;
        }
      }

      // Try Cassandra if Redis miss (L2 cache)
      if (this.cassandraConnected) {
        const cassandraResult = await this.cassandraCache.get(key);
        if (cassandraResult) {
          logger.debug({
            type: 'hybrid-cache',
            action: 'l2-hit',
            key: key.substring(0, 50),
          }, 'Hybrid cache L2 hit');

          // Promote to Redis for faster future access
          if (this.redisConnected) {
            // Don't await this - fire and forget
            this.redisCache.set(key, cassandraResult.data, {
              ttl: Math.max(1, Math.floor((cassandraResult.expiresAt.getTime() - Date.now()) / 1000)),
              contentType: cassandraResult.contentType,
              etag: cassandraResult.etag,
              lastModified: cassandraResult.lastModified,
            }).catch((error) => {
              logger.warn({
                type: 'hybrid-cache',
                error: error instanceof Error ? error.message : String(error),
                key: key.substring(0, 50),
              }, 'Failed to promote L2 hit to L1');
            });
          }

          return cassandraResult;
        }
      }

      // Cache miss on both levels
      logger.debug({
        type: 'hybrid-cache',
        action: 'miss',
        key: key.substring(0, 50),
      }, 'Hybrid cache miss');

      return null;
    } catch (error) {
      logger.error({
        type: 'hybrid-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Hybrid cache get error');
      return null;
    }
  }

  async set(key: string, value: Buffer, options: {
    ttl?: number;
    contentType?: string;
    etag?: string;
    lastModified?: Date;
  } = {}): Promise<boolean> {
    const promises: Promise<boolean>[] = [];
    let redisSuccess = false;
    let cassandraSuccess = false;

    // Set in Redis (L1 cache)
    if (this.redisConnected) {
      promises.push(
        this.redisCache.set(key, value, options)
          .then((success) => {
            redisSuccess = success;
            return success;
          })
          .catch((error) => {
            logger.error({
              type: 'hybrid-cache',
              component: 'redis',
              error: error instanceof Error ? error.message : String(error),
              key: key.substring(0, 50),
            }, 'Hybrid cache Redis set error');
            return false;
          })
      );
    }

    // Set in Cassandra (L2 cache)
    if (this.cassandraConnected) {
      promises.push(
        this.cassandraCache.set(key, value, options)
          .then((success) => {
            cassandraSuccess = success;
            return success;
          })
          .catch((error) => {
            logger.error({
              type: 'hybrid-cache',
              component: 'cassandra',
              error: error instanceof Error ? error.message : String(error),
              key: key.substring(0, 50),
            }, 'Hybrid cache Cassandra set error');
            return false;
          })
      );
    }

    // Wait for both operations
    await Promise.allSettled(promises);

    const success = redisSuccess || cassandraSuccess;

    if (success) {
      logger.debug({
        type: 'hybrid-cache',
        action: 'set',
        key: key.substring(0, 50),
        redis: redisSuccess,
        cassandra: cassandraSuccess,
        size: value.length,
      }, 'Hybrid cache set');
    }

    return success;
  }

  async delete(key: string): Promise<boolean> {
    const promises: Promise<boolean>[] = [];

    // Delete from Redis
    if (this.redisConnected) {
      promises.push(this.redisCache.delete(key));
    }

    // Delete from Cassandra
    if (this.cassandraConnected) {
      promises.push(this.cassandraCache.delete(key));
    }

    const results = await Promise.allSettled(promises);
    const success = results.some(result => result.status === 'fulfilled' && result.value);

    if (success) {
      logger.debug({
        type: 'hybrid-cache',
        action: 'delete',
        key: key.substring(0, 50),
      }, 'Hybrid cache delete');
    }

    return success;
  }

  async exists(key: string): Promise<boolean> {
    try {
      // Check Redis first
      if (this.redisConnected) {
        const redisExists = await this.redisCache.exists(key);
        if (redisExists) {
          return true;
        }
      }

      // Check Cassandra if not in Redis
      if (this.cassandraConnected) {
        return await this.cassandraCache.exists(key);
      }

      return false;
    } catch (error) {
      logger.error({
        type: 'hybrid-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Hybrid cache exists error');
      return false;
    }
  }

  async clear(): Promise<boolean> {
    const promises: Promise<boolean>[] = [];

    // Clear Redis
    if (this.redisConnected) {
      promises.push(this.redisCache.clear());
    }

    // Clear Cassandra
    if (this.cassandraConnected) {
      promises.push(this.cassandraCache.clear());
    }

    const results = await Promise.allSettled(promises);
    const success = results.some(result => result.status === 'fulfilled' && result.value);

    if (success) {
      logger.info({
        type: 'hybrid-cache',
        action: 'clear',
      }, 'Hybrid cache cleared');
    }

    return success;
  }

  async getStats(): Promise<CacheStats> {
    try {
      let redisStats: CacheStats | null = null;
      let cassandraStats: CacheStats | null = null;

      // Get Redis stats
      if (this.redisConnected) {
        try {
          redisStats = await this.redisCache.getStats();
        } catch (error) {
          logger.error({
            type: 'hybrid-cache',
            component: 'redis',
            error: error instanceof Error ? error.message : String(error),
          }, 'Failed to get Redis stats');
        }
      }

      // Get Cassandra stats
      if (this.cassandraConnected) {
        try {
          cassandraStats = await this.cassandraCache.getStats();
        } catch (error) {
          logger.error({
            type: 'hybrid-cache',
            component: 'cassandra',
            error: error instanceof Error ? error.message : String(error),
          }, 'Failed to get Cassandra stats');
        }
      }

      // Combine stats
      const totalHits = (redisStats?.hits || 0) + (cassandraStats?.hits || 0);
      const totalMisses = (redisStats?.misses || 0) + (cassandraStats?.misses || 0);
      const totalErrors = (redisStats?.errors || 0) + (cassandraStats?.errors || 0);

      return {
        enabled: true,
        mode: 'hybrid (redis-cassandra)',
        size: `Redis: ${redisStats?.size || 'N/A'}, Cassandra: ${cassandraStats?.size || 'N/A'}`,
        maxSize: `Redis: ${redisStats?.maxSize || 'N/A'}, Cassandra: ${cassandraStats?.maxSize || 'N/A'}`,
        items: (redisStats?.items || 0) + (cassandraStats?.items || 0),
        maxItems: Math.max(redisStats?.maxItems || 0, cassandraStats?.maxItems || 0),
        hitRatio: totalHits + totalMisses > 0 ? 
          (totalHits / (totalHits + totalMisses)).toFixed(2) : '0.00',
        hits: totalHits,
        misses: totalMisses,
        errors: totalErrors,
        connected: this.redisConnected || this.cassandraConnected,
      };
    } catch (error) {
      logger.error({
        type: 'hybrid-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Hybrid cache stats error');

      return {
        enabled: true,
        mode: 'hybrid (redis-cassandra)',
        size: 'Unknown',
        maxSize: 'Unknown',
        items: 0,
        maxItems: 0,
        hitRatio: '0.00',
        hits: 0,
        misses: 0,
        errors: 1,
        connected: false,
      };
    }
  }

  async close(): Promise<void> {
    const promises: Promise<void>[] = [];

    if (this.redisConnected) {
      promises.push(this.redisCache.close());
    }

    if (this.cassandraConnected) {
      promises.push(this.cassandraCache.close());
    }

    await Promise.allSettled(promises);

    this.redisConnected = false;
    this.cassandraConnected = false;

    logger.info({
      type: 'hybrid-cache',
      action: 'closed',
    }, 'Hybrid cache connections closed');
  }

  async isHealthy(): Promise<boolean> {
    try {
      const healthChecks: Promise<boolean>[] = [];

      if (this.redisConnected) {
        healthChecks.push(this.redisCache.isHealthy());
      }

      if (this.cassandraConnected) {
        healthChecks.push(this.cassandraCache.isHealthy());
      }

      if (healthChecks.length === 0) {
        return false;
      }

      const results = await Promise.allSettled(healthChecks);
      return results.some(result => result.status === 'fulfilled' && result.value);
    } catch (error) {
      logger.error({
        type: 'hybrid-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Hybrid cache health check failed');
      return false;
    }
  }
}