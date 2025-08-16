import { cacheManager } from '../services/cache/cache-manager.js';
import { logger } from '../middleware.js';

/**
 * Enhanced cache utility functions using the new cache manager
 * This maintains backward compatibility while adding new features
 */

// Generate cache key from URL and request headers that might affect response
export function generateCacheKey(url: string, headers: Record<string, any> = {}): string {
  // Extract relevant headers that might affect the response
  const relevantHeaders: Record<string, any> = {};
  const headersToInclude = ['range', 'accept', 'accept-encoding'];
  
  headersToInclude.forEach(header => {
    if (headers[header]) {
      relevantHeaders[header] = headers[header];
    }
  });
  
  return `${url}|${JSON.stringify(relevantHeaders)}`;
}

// Set item in cache with enhanced options
export async function setCacheItem(
  key: string, 
  value: Buffer, 
  size: number,
  options: {
    contentType?: string;
    etag?: string;
    lastModified?: Date;
    ttl?: number;
  } = {}
): Promise<boolean> {
  try {
    const success = await cacheManager.set(key, value, {
      contentType: options.contentType,
      etag: options.etag,
      lastModified: options.lastModified,
      ttl: options.ttl,
    });

    if (success) {
      logger.debug({
        type: 'cache-v2',
        action: 'set',
        key: key.substring(0, 50),
        size: formatBytes(size),
        mode: cacheManager.getMode(),
      }, 'Item cached successfully');
    }

    return success;
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
      key: key.substring(0, 50),
    }, 'Cache set error');
    return false;
  }
}

// Get item from cache with enhanced return type
export async function getCacheItem(key: string): Promise<{ 
  data: Buffer; 
  size: number; 
  contentType?: string;
  etag?: string;
  lastModified?: Date;
} | null> {
  try {
    const item = await cacheManager.get(key);
    
    if (item) {
      logger.debug({
        type: 'cache-v2',
        action: 'hit',
        key: key.substring(0, 50),
        size: formatBytes(item.size),
        mode: cacheManager.getMode(),
      }, 'Cache hit');

      return {
        data: item.data,
        size: item.size,
        contentType: item.contentType,
        etag: item.etag,
        lastModified: item.lastModified,
      };
    } else {
      logger.debug({
        type: 'cache-v2',
        action: 'miss',
        key: key.substring(0, 50),
        mode: cacheManager.getMode(),
      }, 'Cache miss');
      return null;
    }
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
      key: key.substring(0, 50),
    }, 'Cache get error');
    return null;
  }
}

// Check if item exists in cache
export async function cacheItemExists(key: string): Promise<boolean> {
  try {
    return await cacheManager.exists(key);
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
      key: key.substring(0, 50),
    }, 'Cache exists error');
    return false;
  }
}

// Delete item from cache
export async function deleteCacheItem(key: string): Promise<boolean> {
  try {
    const success = await cacheManager.delete(key);
    
    if (success) {
      logger.debug({
        type: 'cache-v2',
        action: 'delete',
        key: key.substring(0, 50),
        mode: cacheManager.getMode(),
      }, 'Cache item deleted');
    }

    return success;
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
      key: key.substring(0, 50),
    }, 'Cache delete error');
    return false;
  }
}

// Helper to format bytes to human-readable format
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Get cache stats
export async function getCacheStats() {
  try {
    return await cacheManager.getStats();
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
    }, 'Cache stats error');
    
    return {
      enabled: false,
      mode: 'unknown',
      size: '0 Bytes',
      maxSize: '0 Bytes',
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

// Clear cache
export async function clearCache(): Promise<boolean> {
  try {
    const success = await cacheManager.clear();
    
    if (success) {
      logger.info({
        type: 'cache-v2',
        action: 'clear',
        mode: cacheManager.getMode(),
      }, 'Cache cleared successfully');
    }

    return success;
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
    }, 'Cache clear error');
    return false;
  }
}

// Health check
export async function isCacheHealthy(): Promise<boolean> {
  try {
    return await cacheManager.isHealthy();
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
    }, 'Cache health check error');
    return false;
  }
}

// Switch cache backend (advanced feature)
export async function switchCacheBackend(mode: 'memory' | 'redis' | 'cassandra' | 'redis-cassandra'): Promise<boolean> {
  try {
    const success = await cacheManager.switchBackend(mode);
    
    if (success) {
      logger.info({
        type: 'cache-v2',
        action: 'backend-switched',
        newMode: mode,
      }, 'Cache backend switched successfully');
    }

    return success;
  } catch (error) {
    logger.error({
      type: 'cache-v2',
      error: error instanceof Error ? error.message : String(error),
      targetMode: mode,
    }, 'Cache backend switch error');
    return false;
  }
}

// Get current cache mode
export function getCacheMode(): string {
  return cacheManager.getMode();
}

// Check if cache is initialized
export function isCacheInitialized(): boolean {
  return cacheManager.isInitialized();
}

// Note: Individual exports are already defined above

export default {
  generateCacheKey,
  setCacheItem,
  getCacheItem,
  cacheItemExists,
  deleteCacheItem,
  getCacheStats,
  clearCache,
  isCacheHealthy,
  switchCacheBackend,
  getCacheMode,
  isCacheInitialized,
};