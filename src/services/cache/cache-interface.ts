/**
 * Cache Interface
 * Unified interface for different cache backends
 */

export interface CacheItem {
  data: Buffer;
  size: number;
  contentType?: string;
  etag?: string;
  lastModified?: Date;
  createdAt: Date;
  expiresAt: Date;
  hitCount?: number;
}

export interface CacheStats {
  enabled: boolean;
  mode: string;
  size: string;
  maxSize: string;
  items: number;
  maxItems: number;
  hitRatio: string;
  hits: number;
  misses: number;
  errors: number;
  connected: boolean;
}

export interface CacheBackend {
  /**
   * Initialize the cache backend
   */
  initialize(): Promise<void>;

  /**
   * Get an item from the cache
   */
  get(key: string): Promise<CacheItem | null>;

  /**
   * Set an item in the cache
   */
  set(key: string, value: Buffer, options?: {
    ttl?: number;
    contentType?: string;
    etag?: string;
    lastModified?: Date;
  }): Promise<boolean>;

  /**
   * Delete an item from the cache
   */
  delete(key: string): Promise<boolean>;

  /**
   * Check if an item exists in the cache
   */
  exists(key: string): Promise<boolean>;

  /**
   * Clear all items from the cache
   */
  clear(): Promise<boolean>;

  /**
   * Get cache statistics
   */
  getStats(): Promise<CacheStats>;

  /**
   * Close the cache backend connections
   */
  close(): Promise<void>;

  /**
   * Check if the cache backend is healthy
   */
  isHealthy(): Promise<boolean>;

  /**
   * Get capacity information
   */
  getCapacityInfo(): Promise<{
    usedBytes: number;
    maxBytes: number;
    usedPercentage: number;
    itemCount: number;
    maxItems: number;
  }>;

  /**
   * Get items sorted by hit count (lowest first)
   */
  getItemsByHitCount(limit?: number): Promise<Array<{
    key: string;
    hitCount: number;
    size: number;
    createdAt: Date;
  }>>;

  /**
   * Increment hit count for a cache item
   */
  incrementHitCount(key: string): Promise<boolean>;
}

export type CacheMode = 'memory' | 'redis' | 'cassandra' | 'redis-cassandra';

export interface CacheOptions {
  mode: CacheMode;
  ttl: number;
  maxSize: number;
  maxItems: number;
}