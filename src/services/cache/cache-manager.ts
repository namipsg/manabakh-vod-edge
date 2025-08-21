import { CacheBackend, CacheItem, CacheStats, CacheMode } from './cache-interface.js';
import { RedisCache } from './redis-cache.js';
import { CassandraCache } from './cassandra-cache.js';
import { HybridCache } from './hybrid-cache.js';
import { CACHE } from '../../config/constants.js';
import { logger } from '../../middleware.js';

// Import the legacy memory cache functionality
import NodeCache from 'node-cache';

/**
 * Legacy memory cache implementation for backwards compatibility
 */
class MemoryCache implements CacheBackend {
  private cache: NodeCache;
  private currentCacheSize = 0;
  private hits = 0;
  private misses = 0;
  private errors = 0;

  constructor() {
    this.cache = new NodeCache({
      stdTTL: CACHE.TTL,
      checkperiod: CACHE.CHECK_PERIOD,
      maxKeys: CACHE.MAX_ITEMS,
      useClones: false,
    });
  }

  async initialize(): Promise<void> {
    logger.info({
      type: 'memory-cache',
      action: 'initialized',
      maxItems: CACHE.MAX_ITEMS,
      maxSize: this.formatBytes(CACHE.MAX_SIZE),
      ttl: CACHE.TTL,
    }, 'Memory cache initialized');
  }

  async get(key: string): Promise<CacheItem | null> {
    try {
      const item = this.cache.get<{ data: Buffer, size: number, contentType?: string, etag?: string, lastModified?: Date, createdAt: Date, expiresAt: Date, hitCount?: number }>(key);
      if (item) {
        // Increment hit count
        item.hitCount = (item.hitCount || 0) + 1;
        this.cache.set(key, item, this.cache.getTtl(key) || CACHE.TTL);
        
        this.hits++;
        return {
          data: item.data,
          size: item.size,
          contentType: item.contentType,
          etag: item.etag,
          lastModified: item.lastModified,
          createdAt: item.createdAt,
          expiresAt: item.expiresAt,
          hitCount: item.hitCount,
        };
      } else {
        this.misses++;
        return null;
      }
    } catch (error) {
      this.errors++;
      this.misses++;
      return null;
    }
  }

  async set(key: string, value: Buffer, options: {
    ttl?: number;
    contentType?: string;
    etag?: string;
    lastModified?: Date;
  } = {}): Promise<boolean> {
    try {
      // Check if adding this item would exceed cache size limit
      if (this.currentCacheSize + value.length > CACHE.MAX_SIZE) {
        // Implement a simple LRU-like cleanup
        const keys = this.cache.keys();
        if (keys.length > 0) {
          const keysToRemove = keys.slice(0, Math.ceil(keys.length * 0.2));
          keysToRemove.forEach(k => {
            const item = this.cache.get<{data: Buffer, size: number}>(k);
            if (item) {
              this.currentCacheSize -= item.size;
              this.cache.del(k);
            }
          });
        }
        
        if (this.currentCacheSize + value.length > CACHE.MAX_SIZE) {
          return false;
        }
      }

      const ttl = options.ttl || CACHE.TTL;
      const now = new Date();
      const expiresAt = new Date(now.getTime() + ttl * 1000);

      const cacheItem = {
        data: value,
        size: value.length,
        contentType: options.contentType,
        etag: options.etag,
        lastModified: options.lastModified,
        createdAt: now,
        expiresAt,
        hitCount: 0,
      };

      const success = this.cache.set(key, cacheItem, ttl);
      if (success) {
        this.currentCacheSize += value.length;
      }
      return success;
    } catch (error) {
      this.errors++;
      return false;
    }
  }

  async delete(key: string): Promise<boolean> {
    try {
      const item = this.cache.get<{data: Buffer, size: number}>(key);
      if (item) {
        this.currentCacheSize -= item.size;
      }
      return this.cache.del(key) > 0;
    } catch (error) {
      this.errors++;
      return false;
    }
  }

  async exists(key: string): Promise<boolean> {
    return this.cache.has(key);
  }

  async clear(): Promise<boolean> {
    try {
      this.cache.flushAll();
      this.currentCacheSize = 0;
      return true;
    } catch (error) {
      this.errors++;
      return false;
    }
  }

  async getStats(): Promise<CacheStats> {
    return {
      enabled: true,
      mode: 'memory',
      size: this.formatBytes(this.currentCacheSize),
      maxSize: this.formatBytes(CACHE.MAX_SIZE),
      items: this.cache.keys().length,
      maxItems: CACHE.MAX_ITEMS,
      hitRatio: this.hits + this.misses > 0 ? 
        (this.hits / (this.hits + this.misses)).toFixed(2) : '0.00',
      hits: this.hits,
      misses: this.misses,
      errors: this.errors,
      connected: true,
    };
  }

  async close(): Promise<void> {
    this.cache.flushAll();
  }

  async isHealthy(): Promise<boolean> {
    return true;
  }

  async getCapacityInfo(): Promise<{
    usedBytes: number;
    maxBytes: number;
    usedPercentage: number;
    itemCount: number;
    maxItems: number;
  }> {
    return {
      usedBytes: this.currentCacheSize,
      maxBytes: CACHE.MAX_SIZE,
      usedPercentage: CACHE.MAX_SIZE > 0 ? (this.currentCacheSize / CACHE.MAX_SIZE) * 100 : 0,
      itemCount: this.cache.keys().length,
      maxItems: CACHE.MAX_ITEMS,
    };
  }

  async getItemsByHitCount(limit: number = 100): Promise<Array<{
    key: string;
    hitCount: number;
    size: number;
    createdAt: Date;
  }>> {
    const keys = this.cache.keys();
    const items: Array<{
      key: string;
      hitCount: number;
      size: number;
      createdAt: Date;
    }> = [];

    for (const key of keys.slice(0, limit)) {
      const item = this.cache.get<{ size: number, hitCount?: number, createdAt: Date }>(key);
      if (item) {
        items.push({
          key,
          hitCount: item.hitCount || 0,
          size: item.size,
          createdAt: item.createdAt,
        });
      }
    }

    // Sort by hit count (ascending - lowest first)
    return items.sort((a, b) => a.hitCount - b.hitCount);
  }

  async incrementHitCount(key: string): Promise<boolean> {
    try {
      const item = this.cache.get<{ hitCount?: number }>(key);
      if (item) {
        item.hitCount = (item.hitCount || 0) + 1;
        const ttl = this.cache.getTtl(key) || CACHE.TTL;
        this.cache.set(key, item, ttl);
        return true;
      }
      return false;
    } catch (error) {
      this.errors++;
      return false;
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}

/**
 * Cache Manager
 * Manages different cache backends and provides a unified interface
 */
export class CacheManager {
  private backend: CacheBackend;
  private mode: CacheMode;
  private initialized = false;

  constructor() {
    this.mode = CACHE.MODE as CacheMode;
    this.backend = this.createBackend(this.mode);
  }

  private createBackend(mode: CacheMode): CacheBackend {
    switch (mode) {
      case 'redis':
        logger.info({ type: 'cache-manager', mode }, 'Using Redis cache backend');
        return new RedisCache();
        
      case 'cassandra':
        logger.info({ type: 'cache-manager', mode }, 'Using Cassandra cache backend');
        return new CassandraCache();
        
      case 'redis-cassandra':
        logger.info({ type: 'cache-manager', mode }, 'Using hybrid Redis-Cassandra cache backend');
        return new HybridCache();
        
      case 'memory':
      default:
        logger.info({ type: 'cache-manager', mode }, 'Using in-memory cache backend');
        return new MemoryCache();
    }
  }

  async initialize(): Promise<void> {
    try {
      await this.backend.initialize();
      this.initialized = true;
      
      logger.info({
        type: 'cache-manager',
        mode: this.mode,
        initialized: true,
      }, 'Cache manager initialized successfully');
    } catch (error) {
      this.initialized = false;
      logger.error({
        type: 'cache-manager',
        mode: this.mode,
        error: error instanceof Error ? error.message : String(error),
      }, 'Failed to initialize cache manager');
      
      // Fallback to memory cache if configured backend fails
      if (this.mode !== 'memory') {
        logger.warn({
          type: 'cache-manager',
          action: 'fallback',
          from: this.mode,
          to: 'memory',
        }, 'Falling back to memory cache');
        
        this.mode = 'memory';
        this.backend = new MemoryCache();
        await this.backend.initialize();
        this.initialized = true;
      } else {
        throw error;
      }
    }
  }

  async get(key: string): Promise<CacheItem | null> {
    if (!this.initialized) {
      return null;
    }
    return this.backend.get(key);
  }

  async set(key: string, value: Buffer, options?: {
    ttl?: number;
    contentType?: string;
    etag?: string;
    lastModified?: Date;
  }): Promise<boolean> {
    if (!this.initialized) {
      return false;
    }
    return this.backend.set(key, value, options);
  }

  async delete(key: string): Promise<boolean> {
    if (!this.initialized) {
      return false;
    }
    return this.backend.delete(key);
  }

  async exists(key: string): Promise<boolean> {
    if (!this.initialized) {
      return false;
    }
    return this.backend.exists(key);
  }

  async clear(): Promise<boolean> {
    if (!this.initialized) {
      return false;
    }
    return this.backend.clear();
  }

  async getStats(): Promise<CacheStats> {
    if (!this.initialized) {
      return {
        enabled: false,
        mode: this.mode,
        size: '0 Bytes',
        maxSize: '0 Bytes',
        items: 0,
        maxItems: 0,
        hitRatio: '0.00',
        hits: 0,
        misses: 0,
        errors: 0,
        connected: false,
      };
    }
    return this.backend.getStats();
  }

  async close(): Promise<void> {
    if (this.initialized) {
      await this.backend.close();
      this.initialized = false;
      
      logger.info({
        type: 'cache-manager',
        mode: this.mode,
      }, 'Cache manager closed');
    }
  }

  async isHealthy(): Promise<boolean> {
    if (!this.initialized) {
      return false;
    }
    return this.backend.isHealthy();
  }

  getMode(): CacheMode {
    return this.mode;
  }

  isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Switch cache backend at runtime (advanced feature)
   */
  async switchBackend(newMode: CacheMode): Promise<boolean> {
    try {
      logger.info({
        type: 'cache-manager',
        action: 'switch-backend',
        from: this.mode,
        to: newMode,
      }, 'Switching cache backend');

      // Close current backend
      if (this.initialized) {
        await this.backend.close();
      }

      // Create and initialize new backend
      this.mode = newMode;
      this.backend = this.createBackend(newMode);
      await this.backend.initialize();
      this.initialized = true;

      logger.info({
        type: 'cache-manager',
        action: 'switch-complete',
        mode: newMode,
      }, 'Cache backend switched successfully');

      return true;
    } catch (error) {
      logger.error({
        type: 'cache-manager',
        action: 'switch-failed',
        error: error instanceof Error ? error.message : String(error),
        targetMode: newMode,
      }, 'Failed to switch cache backend');

      // Try to restore previous backend or fallback to memory
      try {
        this.mode = 'memory';
        this.backend = new MemoryCache();
        await this.backend.initialize();
        this.initialized = true;
      } catch (fallbackError) {
        this.initialized = false;
      }

      return false;
    }
  }
}

// Export singleton instance
export const cacheManager = new CacheManager();