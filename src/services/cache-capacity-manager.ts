import { cacheManager } from './cache/cache-manager.js';
import { CacheBackend } from './cache/cache-interface.js';
import { RedisCache } from './cache/redis-cache.js';
import { CassandraCache } from './cache/cassandra-cache.js';
import { HybridCache } from './cache/hybrid-cache.js';
import { CACHE } from '../config/constants.js';
import { logger } from '../middleware.js';

/**
 * Cache Capacity Manager
 * Monitors cache capacity and manages eviction policies
 * - Redis: 85% threshold -> move to Cassandra
 * - Cassandra: 90% threshold -> remove entirely
 */
export class CacheCapacityManager {
  private redisThreshold = CACHE.REDIS_CAPACITY_THRESHOLD; // 85%
  private cassandraThreshold = CACHE.CASSANDRA_CAPACITY_THRESHOLD; // 90%
  private monitoringInterval = 60000; // 1 minute
  private intervalId?: NodeJS.Timeout;
  private isRunning = false;

  constructor() {
    this.startMonitoring();
  }

  /**
   * Start capacity monitoring
   */
  public startMonitoring(): void {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;
    this.intervalId = setInterval(async () => {
      try {
        await this.checkAndManageCapacity();
      } catch (error) {
        logger.error({
          type: 'cache-capacity-manager',
          error: error instanceof Error ? error.message : String(error),
        }, 'Error during capacity management');
      }
    }, this.monitoringInterval);

    logger.info({
      type: 'cache-capacity-manager',
      action: 'started',
      redisThreshold: this.redisThreshold,
      cassandraThreshold: this.cassandraThreshold,
      intervalMs: this.monitoringInterval,
    }, 'Cache capacity monitoring started');
  }

  /**
   * Stop capacity monitoring
   */
  public stopMonitoring(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
    this.isRunning = false;

    logger.info({
      type: 'cache-capacity-manager',
      action: 'stopped',
    }, 'Cache capacity monitoring stopped');
  }

  /**
   * Check capacity and manage eviction
   */
  private async checkAndManageCapacity(): Promise<void> {
    if (!cacheManager.isInitialized()) {
      return;
    }

    const mode = cacheManager.getMode();
    const backend = (cacheManager as any).backend as CacheBackend;

    try {
      const capacityInfo = await backend.getCapacityInfo();

      logger.debug({
        type: 'cache-capacity-manager',
        mode,
        usedPercentage: capacityInfo.usedPercentage,
        usedBytes: capacityInfo.usedBytes,
        itemCount: capacityInfo.itemCount,
      }, 'Capacity check');

      // Handle different cache modes
      switch (mode) {
        case 'redis':
          await this.manageRedisCapacity(backend as RedisCache, capacityInfo);
          break;

        case 'cassandra':
          await this.manageCassandraCapacity(backend as CassandraCache, capacityInfo);
          break;

        case 'redis-cassandra':
          await this.manageHybridCapacity(backend as HybridCache);
          break;

        case 'memory':
          // Memory cache uses built-in LRU, no special management needed
          break;
      }
    } catch (error) {
      logger.error({
        type: 'cache-capacity-manager',
        error: error instanceof Error ? error.message : String(error),
        mode,
      }, 'Failed to check cache capacity');
    }
  }

  /**
   * Manage Redis capacity
   */
  private async manageRedisCapacity(
    redisCache: RedisCache, 
    capacityInfo: { usedPercentage: number; itemCount: number }
  ): Promise<void> {
    if (capacityInfo.usedPercentage >= this.redisThreshold) {
      logger.warn({
        type: 'cache-capacity-manager',
        component: 'redis',
        usedPercentage: capacityInfo.usedPercentage,
        threshold: this.redisThreshold,
      }, 'Redis capacity threshold reached');

      await this.evictLeastUsedItems(redisCache, Math.ceil(capacityInfo.itemCount * 0.2));
    }
  }

  /**
   * Manage Cassandra capacity
   */
  private async manageCassandraCapacity(
    cassandraCache: CassandraCache, 
    capacityInfo: { usedPercentage: number; itemCount: number }
  ): Promise<void> {
    if (capacityInfo.usedPercentage >= this.cassandraThreshold) {
      logger.warn({
        type: 'cache-capacity-manager',
        component: 'cassandra',
        usedPercentage: capacityInfo.usedPercentage,
        threshold: this.cassandraThreshold,
      }, 'Cassandra capacity threshold reached');

      await this.evictLeastUsedItems(cassandraCache, Math.ceil(capacityInfo.itemCount * 0.1));
    }
  }

  /**
   * Manage hybrid cache capacity
   */
  private async manageHybridCapacity(hybridCache: HybridCache): Promise<void> {
    const redisCache = (hybridCache as any).redisCache as RedisCache;
    const cassandraCache = (hybridCache as any).cassandraCache as CassandraCache;

    // Check Redis capacity first
    if ((hybridCache as any).redisConnected) {
      try {
        const redisCapacity = await redisCache.getCapacityInfo();
        
        if (redisCapacity.usedPercentage >= this.redisThreshold) {
          logger.warn({
            type: 'cache-capacity-manager',
            component: 'hybrid-redis',
            usedPercentage: redisCapacity.usedPercentage,
            threshold: this.redisThreshold,
          }, 'Hybrid cache Redis L1 capacity threshold reached');

          await this.moveRedisItemsToCassandra(redisCache, cassandraCache, Math.ceil(redisCapacity.itemCount * 0.2));
        }
      } catch (error) {
        logger.error({
          type: 'cache-capacity-manager',
          component: 'hybrid-redis',
          error: error instanceof Error ? error.message : String(error),
        }, 'Failed to manage Redis capacity in hybrid mode');
      }
    }

    // Check Cassandra capacity
    if ((hybridCache as any).cassandraConnected) {
      try {
        const cassandraCapacity = await cassandraCache.getCapacityInfo();
        
        if (cassandraCapacity.usedPercentage >= this.cassandraThreshold) {
          logger.warn({
            type: 'cache-capacity-manager',
            component: 'hybrid-cassandra',
            usedPercentage: cassandraCapacity.usedPercentage,
            threshold: this.cassandraThreshold,
          }, 'Hybrid cache Cassandra L2 capacity threshold reached');

          await this.evictLeastUsedItems(cassandraCache, Math.ceil(cassandraCapacity.itemCount * 0.1));
        }
      } catch (error) {
        logger.error({
          type: 'cache-capacity-manager',
          component: 'hybrid-cassandra',
          error: error instanceof Error ? error.message : String(error),
        }, 'Failed to manage Cassandra capacity in hybrid mode');
      }
    }
  }

  /**
   * Move items from Redis to Cassandra based on hit count
   */
  private async moveRedisItemsToCassandra(
    redisCache: RedisCache, 
    cassandraCache: CassandraCache, 
    itemCount: number
  ): Promise<void> {
    try {
      const itemsToMove = await redisCache.getItemsByHitCount(itemCount);
      
      let movedCount = 0;
      let failedCount = 0;

      for (const item of itemsToMove) {
        try {
          // Get the full item from Redis
          const cacheItem = await redisCache.get(item.key);
          if (cacheItem) {
            // Move to Cassandra
            const success = await cassandraCache.set(item.key, cacheItem.data, {
              contentType: cacheItem.contentType,
              etag: cacheItem.etag,
              lastModified: cacheItem.lastModified,
            });

            if (success) {
              // Remove from Redis
              await redisCache.delete(item.key);
              movedCount++;
            } else {
              failedCount++;
            }
          }
        } catch (error) {
          failedCount++;
          logger.debug({
            type: 'cache-capacity-manager',
            action: 'move-item-failed',
            key: item.key.substring(0, 50),
            error: error instanceof Error ? error.message : String(error),
          }, 'Failed to move item from Redis to Cassandra');
        }
      }

      logger.info({
        type: 'cache-capacity-manager',
        action: 'redis-to-cassandra-migration',
        requested: itemCount,
        moved: movedCount,
        failed: failedCount,
      }, 'Completed Redis to Cassandra migration');
    } catch (error) {
      logger.error({
        type: 'cache-capacity-manager',
        action: 'redis-to-cassandra-migration',
        error: error instanceof Error ? error.message : String(error),
      }, 'Failed to move items from Redis to Cassandra');
    }
  }

  /**
   * Evict least used items from cache
   */
  private async evictLeastUsedItems(cache: CacheBackend, itemCount: number): Promise<void> {
    try {
      const itemsToEvict = await cache.getItemsByHitCount(itemCount);
      
      let evictedCount = 0;
      let failedCount = 0;

      for (const item of itemsToEvict) {
        try {
          const success = await cache.delete(item.key);
          if (success) {
            evictedCount++;
          } else {
            failedCount++;
          }
        } catch (error) {
          failedCount++;
          logger.debug({
            type: 'cache-capacity-manager',
            action: 'evict-item-failed',
            key: item.key.substring(0, 50),
            error: error instanceof Error ? error.message : String(error),
          }, 'Failed to evict cache item');
        }
      }

      logger.info({
        type: 'cache-capacity-manager',
        action: 'eviction',
        requested: itemCount,
        evicted: evictedCount,
        failed: failedCount,
      }, 'Completed cache eviction');
    } catch (error) {
      logger.error({
        type: 'cache-capacity-manager',
        action: 'eviction',
        error: error instanceof Error ? error.message : String(error),
      }, 'Failed to evict cache items');
    }
  }

  /**
   * Get current monitoring status
   */
  public getStatus() {
    return {
      isRunning: this.isRunning,
      redisThreshold: this.redisThreshold,
      cassandraThreshold: this.cassandraThreshold,
      monitoringInterval: this.monitoringInterval,
    };
  }

  /**
   * Update thresholds
   */
  public updateThresholds(redisThreshold?: number, cassandraThreshold?: number): void {
    if (redisThreshold !== undefined && redisThreshold > 0 && redisThreshold < 100) {
      this.redisThreshold = redisThreshold;
    }
    
    if (cassandraThreshold !== undefined && cassandraThreshold > 0 && cassandraThreshold < 100) {
      this.cassandraThreshold = cassandraThreshold;
    }

    logger.info({
      type: 'cache-capacity-manager',
      action: 'thresholds-updated',
      redisThreshold: this.redisThreshold,
      cassandraThreshold: this.cassandraThreshold,
    }, 'Cache capacity thresholds updated');
  }

  /**
   * Force capacity check
   */
  public async forceCapacityCheck(): Promise<void> {
    logger.info({
      type: 'cache-capacity-manager',
      action: 'force-check',
    }, 'Forcing capacity check');

    await this.checkAndManageCapacity();
  }
}

// Export singleton instance
export const cacheCapacityManager = new CacheCapacityManager();