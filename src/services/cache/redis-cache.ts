import { Redis } from 'ioredis';
import { CacheBackend, CacheItem, CacheStats } from './cache-interface.js';
import { REDIS, CACHE } from '../../config/constants.js';
import { logger } from '../../middleware.js';

export class RedisCache implements CacheBackend {
  private client: Redis;
  private connected = false;
  private hits = 0;
  private misses = 0;
  private errors = 0;

  constructor() {
    this.client = new Redis({
      host: REDIS.HOST,
      port: REDIS.PORT,
      password: REDIS.PASSWORD || undefined,
      db: REDIS.DB,
      keyPrefix: REDIS.KEY_PREFIX,
      maxRetriesPerRequest: REDIS.MAX_RETRIES,
      connectTimeout: REDIS.CONNECT_TIMEOUT,
      commandTimeout: REDIS.COMMAND_TIMEOUT,
      lazyConnect: true,
    });

    this.setupEventListeners();
  }

  private setupEventListeners(): void {
    this.client.on('connect', () => {
      this.connected = true;
      logger.info({
        type: 'redis-cache',
        action: 'connected',
        host: REDIS.HOST,
        port: REDIS.PORT,
        db: REDIS.DB,
      }, 'Redis cache connected');
    });

    this.client.on('ready', () => {
      logger.info({
        type: 'redis-cache',
        action: 'ready',
      }, 'Redis cache ready');
    });

    this.client.on('error', (error: Error) => {
      this.errors++;
      this.connected = false;
      logger.error({
        type: 'redis-cache',
        error: error.message,
      }, 'Redis cache error');
    });

    this.client.on('close', () => {
      this.connected = false;
      logger.warn({
        type: 'redis-cache',
        action: 'disconnected',
      }, 'Redis cache disconnected');
    });

    this.client.on('reconnecting', () => {
      logger.info({
        type: 'redis-cache',
        action: 'reconnecting',
      }, 'Redis cache reconnecting');
    });
  }

  async initialize(): Promise<void> {
    try {
      await this.client.connect();
      this.connected = true;
      
      // Test the connection
      await this.client.ping();
      
      logger.info({
        type: 'redis-cache',
        action: 'initialized',
        version: await this.client.info('server'),
      }, 'Redis cache initialized successfully');
    } catch (error) {
      this.connected = false;
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Failed to initialize Redis cache');
      throw error;
    }
  }

  async get(key: string): Promise<CacheItem | null> {
    try {
      if (!this.connected) {
        this.misses++;
        return null;
      }

      // Get all fields for the cache item
      const result = await this.client.hmget(
        key,
        'data',
        'size',
        'contentType',
        'etag',
        'lastModified',
        'createdAt',
        'expiresAt',
        'hitCount'
      );

      const [data, size, contentType, etag, lastModified, createdAt, expiresAt, hitCount] = result;

      if (!data || !size || !createdAt || !expiresAt) {
        this.misses++;
        return null;
      }

      // Check if item has expired
      const expiry = new Date(expiresAt);
      if (expiry < new Date()) {
        // Item expired, delete it
        await this.delete(key);
        this.misses++;
        return null;
      }

      const cacheItem: CacheItem = {
        data: Buffer.from(data, 'base64'),
        size: parseInt(size, 10),
        contentType: contentType || undefined,
        etag: etag || undefined,
        lastModified: lastModified ? new Date(lastModified) : undefined,
        createdAt: new Date(createdAt),
        expiresAt: expiry,
        hitCount: hitCount ? parseInt(hitCount, 10) : 0,
      };

      // Increment hit count
      await this.incrementHitCount(key);

      this.hits++;
      logger.debug({
        type: 'redis-cache',
        action: 'hit',
        key: key.substring(0, 50),
        size: cacheItem.size,
        hitCount: cacheItem.hitCount,
      }, 'Redis cache hit');

      return cacheItem;
    } catch (error) {
      this.errors++;
      this.misses++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Redis cache get error');
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
      if (!this.connected) {
        return false;
      }

      const ttl = options.ttl || CACHE.TTL;
      const now = new Date();
      const expiresAt = new Date(now.getTime() + ttl * 1000);

      const cacheData = {
        data: value.toString('base64'),
        size: value.length.toString(),
        contentType: options.contentType || '',
        etag: options.etag || '',
        lastModified: options.lastModified?.toISOString() || '',
        createdAt: now.toISOString(),
        expiresAt: expiresAt.toISOString(),
        hitCount: '0',
      };

      // Use pipeline for atomic operation
      const pipeline = this.client.pipeline();
      pipeline.hmset(key, cacheData);
      pipeline.expire(key, ttl);
      
      const results = await pipeline.exec();
      
      // Check if all operations succeeded
      const success = results?.every(([error]: [Error | null, any]) => !error) || false;

      if (success) {
        logger.debug({
          type: 'redis-cache',
          action: 'set',
          key: key.substring(0, 50),
          size: value.length,
          ttl,
        }, 'Redis cache set');
      }

      return success;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Redis cache set error');
      return false;
    }
  }

  async delete(key: string): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      const result = await this.client.del(key);
      const success = result > 0;

      if (success) {
        logger.debug({
          type: 'redis-cache',
          action: 'delete',
          key: key.substring(0, 50),
        }, 'Redis cache delete');
      }

      return success;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Redis cache delete error');
      return false;
    }
  }

  async exists(key: string): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      const result = await this.client.exists(key);
      return result > 0;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Redis cache exists error');
      return false;
    }
  }

  async clear(): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      // Get all keys with our prefix and delete them
      const keys = await this.client.keys(`${REDIS.KEY_PREFIX}*`);
      
      if (keys.length > 0) {
        // Remove prefix from keys for deletion (ioredis adds prefix automatically)
        const keysWithoutPrefix = keys.map((key: string) => 
          key.startsWith(REDIS.KEY_PREFIX) ? key.substring(REDIS.KEY_PREFIX.length) : key
        );
        
        await this.client.del(...keysWithoutPrefix);
      }

      logger.info({
        type: 'redis-cache',
        action: 'clear',
        keysDeleted: keys.length,
      }, 'Redis cache cleared');

      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Redis cache clear error');
      return false;
    }
  }

  async getStats(): Promise<CacheStats> {
    try {
      let items = 0;
      let size = '0 Bytes';

      if (this.connected) {
        // Get approximate key count
        const keys = await this.client.keys(`${REDIS.KEY_PREFIX}*`);
        items = keys.length;

        // Get memory usage info
        const info = await this.client.info('memory');
        const memoryMatch = info.match(/used_memory_human:([^\r\n]+)/);
        if (memoryMatch) {
          size = memoryMatch[1].trim();
        }
      }

      return {
        enabled: true,
        mode: 'redis',
        size,
        maxSize: 'N/A', // Redis manages memory internally
        items,
        maxItems: -1, // No hard limit
        hitRatio: this.hits + this.misses > 0 ? 
          (this.hits / (this.hits + this.misses)).toFixed(2) : '0.00',
        hits: this.hits,
        misses: this.misses,
        errors: this.errors,
        connected: this.connected,
      };
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Redis cache stats error');

      return {
        enabled: true,
        mode: 'redis',
        size: 'Unknown',
        maxSize: 'N/A',
        items: 0,
        maxItems: -1,
        hitRatio: '0.00',
        hits: this.hits,
        misses: this.misses,
        errors: this.errors,
        connected: this.connected,
      };
    }
  }

  async close(): Promise<void> {
    try {
      if (this.client) {
        await this.client.quit();
        this.connected = false;
        logger.info({
          type: 'redis-cache',
          action: 'closed',
        }, 'Redis cache connection closed');
      }
    } catch (error) {
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Redis cache close error');
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      await this.client.ping();
      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Redis cache health check failed');
      return false;
    }
  }

  async getCapacityInfo(): Promise<{
    usedBytes: number;
    maxBytes: number;
    usedPercentage: number;
    itemCount: number;
    maxItems: number;
  }> {
    try {
      if (!this.connected) {
        return {
          usedBytes: 0,
          maxBytes: 0,
          usedPercentage: 0,
          itemCount: 0,
          maxItems: 0,
        };
      }

      const info = await this.client.info('memory');
      const keys = await this.client.keys(`${REDIS.KEY_PREFIX}*`);
      
      const usedMemoryMatch = info.match(/used_memory:(\d+)/);
      const maxMemoryMatch = info.match(/maxmemory:(\d+)/);
      
      const usedBytes = usedMemoryMatch ? parseInt(usedMemoryMatch[1], 10) : 0;
      const maxBytes = maxMemoryMatch ? parseInt(maxMemoryMatch[1], 10) : 0;
      const usedPercentage = maxBytes > 0 ? (usedBytes / maxBytes) * 100 : 0;

      return {
        usedBytes,
        maxBytes,
        usedPercentage,
        itemCount: keys.length,
        maxItems: -1, // Redis doesn't have a hard item limit
      };
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Redis cache capacity info error');
      
      return {
        usedBytes: 0,
        maxBytes: 0,
        usedPercentage: 0,
        itemCount: 0,
        maxItems: 0,
      };
    }
  }

  async getItemsByHitCount(limit: number = 100): Promise<Array<{
    key: string;
    hitCount: number;
    size: number;
    createdAt: Date;
  }>> {
    try {
      if (!this.connected) {
        return [];
      }

      const keys = await this.client.keys(`${REDIS.KEY_PREFIX}*`);
      const items: Array<{
        key: string;
        hitCount: number;
        size: number;
        createdAt: Date;
      }> = [];

      // Get hit count and size for each key
      for (const key of keys.slice(0, limit)) {
        const cleanKey = key.startsWith(REDIS.KEY_PREFIX) ? key.substring(REDIS.KEY_PREFIX.length) : key;
        const result = await this.client.hmget(cleanKey, 'hitCount', 'size', 'createdAt');
        
        const [hitCount, size, createdAt] = result;
        if (hitCount !== null && size !== null && createdAt !== null) {
          items.push({
            key: cleanKey,
            hitCount: parseInt(hitCount, 10) || 0,
            size: parseInt(size, 10) || 0,
            createdAt: new Date(createdAt),
          });
        }
      }

      // Sort by hit count (ascending - lowest first)
      return items.sort((a, b) => a.hitCount - b.hitCount);
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Redis cache getItemsByHitCount error');
      return [];
    }
  }

  async incrementHitCount(key: string): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      await this.client.hincrby(key, 'hitCount', 1);
      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'redis-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Redis cache incrementHitCount error');
      return false;
    }
  }
}