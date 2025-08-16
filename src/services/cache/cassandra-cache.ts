import { Client, types } from 'cassandra-driver';
import { CacheBackend, CacheItem, CacheStats } from './cache-interface.js';
import { CASSANDRA, CACHE } from '../../config/constants.js';
import { logger } from '../../middleware.js';

export class CassandraCache implements CacheBackend {
  private client: Client;
  private connected = false;
  private hits = 0;
  private misses = 0;
  private errors = 0;
  private keyspace = CASSANDRA.KEYSPACE;
  private tableName = CASSANDRA.TABLE_NAME;

  constructor() {
    this.client = new Client({
      contactPoints: CASSANDRA.HOSTS,
      localDataCenter: CASSANDRA.LOCAL_DATA_CENTER,
      keyspace: this.keyspace,
      credentials: CASSANDRA.USERNAME && CASSANDRA.PASSWORD ? {
        username: CASSANDRA.USERNAME,
        password: CASSANDRA.PASSWORD,
      } : undefined,
      socketOptions: {
        connectTimeout: CASSANDRA.CONNECT_TIMEOUT,
        readTimeout: CASSANDRA.REQUEST_TIMEOUT,
      },
    });

    this.setupEventListeners();
  }

  private setupEventListeners(): void {
    this.client.on('log', (level, className, message, furtherInfo) => {
      if (level === 'error') {
        this.errors++;
        logger.error({
          type: 'cassandra-cache',
          class: className,
          message,
          info: furtherInfo,
        }, 'Cassandra cache error');
      } else if (level === 'warning') {
        logger.warn({
          type: 'cassandra-cache',
          class: className,
          message,
        }, 'Cassandra cache warning');
      }
    });
  }

  async initialize(): Promise<void> {
    try {
      await this.client.connect();
      this.connected = true;

      // Create keyspace if it doesn't exist
      await this.createKeyspaceIfNotExists();

      // Create table if it doesn't exist
      await this.createTableIfNotExists();

      logger.info({
        type: 'cassandra-cache',
        action: 'initialized',
        keyspace: this.keyspace,
        table: this.tableName,
        hosts: CASSANDRA.HOSTS,
      }, 'Cassandra cache initialized successfully');
    } catch (error) {
      this.connected = false;
      this.errors++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Failed to initialize Cassandra cache');
      throw error;
    }
  }

  private async createKeyspaceIfNotExists(): Promise<void> {
    const query = `
      CREATE KEYSPACE IF NOT EXISTS ${this.keyspace}
      WITH REPLICATION = {
        'class': 'SimpleStrategy',
        'replication_factor': ${CASSANDRA.REPLICATION_FACTOR}
      }
    `;

    try {
      await this.client.execute(query);
      logger.debug({
        type: 'cassandra-cache',
        action: 'keyspace-created',
        keyspace: this.keyspace,
      }, 'Cassandra keyspace created or verified');
    } catch (error) {
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
        keyspace: this.keyspace,
      }, 'Failed to create Cassandra keyspace');
      throw error;
    }
  }

  private async createTableIfNotExists(): Promise<void> {
    const query = `
      CREATE TABLE IF NOT EXISTS ${this.keyspace}.${this.tableName} (
        cache_key text PRIMARY KEY,
        data blob,
        size bigint,
        content_type text,
        etag text,
        last_modified timestamp,
        created_at timestamp,
        expires_at timestamp
      ) WITH gc_grace_seconds = 86400
        AND compaction = {'class': 'LeveledCompactionStrategy'}
    `;

    try {
      await this.client.execute(query);
      
      // Create TTL index for automatic cleanup
      const ttlIndexQuery = `
        CREATE INDEX IF NOT EXISTS expires_at_idx 
        ON ${this.keyspace}.${this.tableName} (expires_at)
      `;
      await this.client.execute(ttlIndexQuery);

      logger.debug({
        type: 'cassandra-cache',
        action: 'table-created',
        table: this.tableName,
      }, 'Cassandra cache table created or verified');
    } catch (error) {
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
        table: this.tableName,
      }, 'Failed to create Cassandra cache table');
      throw error;
    }
  }

  async get(key: string): Promise<CacheItem | null> {
    try {
      if (!this.connected) {
        this.misses++;
        return null;
      }

      const query = `
        SELECT data, size, content_type, etag, last_modified, created_at, expires_at
        FROM ${this.keyspace}.${this.tableName}
        WHERE cache_key = ?
      `;

      const result = await this.client.execute(query, [key], {
        consistency: types.consistencies.localQuorum,
      });

      if (result.rows.length === 0) {
        this.misses++;
        return null;
      }

      const row = result.rows[0];

      // Check if item has expired
      const expiresAt = row.expires_at;
      if (expiresAt && expiresAt < new Date()) {
        // Item expired, delete it
        await this.delete(key);
        this.misses++;
        return null;
      }

      const cacheItem: CacheItem = {
        data: Buffer.from(row.data),
        size: row.size ? Number(row.size) : 0,
        contentType: row.content_type || undefined,
        etag: row.etag || undefined,
        lastModified: row.last_modified || undefined,
        createdAt: row.created_at || new Date(),
        expiresAt: expiresAt || new Date(),
      };

      this.hits++;
      logger.debug({
        type: 'cassandra-cache',
        action: 'hit',
        key: key.substring(0, 50),
        size: cacheItem.size,
      }, 'Cassandra cache hit');

      return cacheItem;
    } catch (error) {
      this.errors++;
      this.misses++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Cassandra cache get error');
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

      const query = `
        INSERT INTO ${this.keyspace}.${this.tableName} 
        (cache_key, data, size, content_type, etag, last_modified, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        USING TTL ${ttl}
      `;

      const params = [
        key,
        value,
        value.length,
        options.contentType || null,
        options.etag || null,
        options.lastModified || null,
        now,
        expiresAt,
      ];

      await this.client.execute(query, params, {
        consistency: types.consistencies.localQuorum,
      });

      logger.debug({
        type: 'cassandra-cache',
        action: 'set',
        key: key.substring(0, 50),
        size: value.length,
        ttl,
      }, 'Cassandra cache set');

      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Cassandra cache set error');
      return false;
    }
  }

  async delete(key: string): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      const query = `
        DELETE FROM ${this.keyspace}.${this.tableName}
        WHERE cache_key = ?
      `;

      await this.client.execute(query, [key], {
        consistency: types.consistencies.localQuorum,
      });

      logger.debug({
        type: 'cassandra-cache',
        action: 'delete',
        key: key.substring(0, 50),
      }, 'Cassandra cache delete');

      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Cassandra cache delete error');
      return false;
    }
  }

  async exists(key: string): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      const query = `
        SELECT cache_key FROM ${this.keyspace}.${this.tableName}
        WHERE cache_key = ?
        LIMIT 1
      `;

      const result = await this.client.execute(query, [key], {
        consistency: types.consistencies.localQuorum,
      });

      return result.rows.length > 0;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
        key: key.substring(0, 50),
      }, 'Cassandra cache exists error');
      return false;
    }
  }

  async clear(): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      const query = `TRUNCATE ${this.keyspace}.${this.tableName}`;
      await this.client.execute(query);

      logger.info({
        type: 'cassandra-cache',
        action: 'clear',
        table: this.tableName,
      }, 'Cassandra cache cleared');

      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Cassandra cache clear error');
      return false;
    }
  }

  async getStats(): Promise<CacheStats> {
    try {
      let items = 0;
      let estimatedSize = '0 Bytes';

      if (this.connected) {
        // Get approximate count (this can be expensive on large tables)
        try {
          const countQuery = `
            SELECT COUNT(*) as count 
            FROM ${this.keyspace}.${this.tableName}
            WHERE expires_at > ?
            ALLOW FILTERING
          `;
          
          const countResult = await this.client.execute(countQuery, [new Date()], {
            consistency: types.consistencies.localOne,
          });
          
          if (countResult.rows.length > 0) {
            items = Number(countResult.rows[0].count) || 0;
          }
        } catch (error) {
          // Count query failed, use 0
          logger.debug({
            type: 'cassandra-cache',
            error: error instanceof Error ? error.message : String(error),
          }, 'Failed to get Cassandra item count');
        }

        // Get size estimation (sum of size column)
        try {
          const sizeQuery = `
            SELECT SUM(size) as total_size 
            FROM ${this.keyspace}.${this.tableName}
            WHERE expires_at > ?
            ALLOW FILTERING
          `;
          
          const sizeResult = await this.client.execute(sizeQuery, [new Date()], {
            consistency: types.consistencies.localOne,
          });
          
          if (sizeResult.rows.length > 0 && sizeResult.rows[0].total_size) {
            const totalBytes = Number(sizeResult.rows[0].total_size);
            estimatedSize = this.formatBytes(totalBytes);
          }
        } catch (error) {
          // Size query failed, use default
          logger.debug({
            type: 'cassandra-cache',
            error: error instanceof Error ? error.message : String(error),
          }, 'Failed to get Cassandra size estimation');
        }
      }

      return {
        enabled: true,
        mode: 'cassandra',
        size: estimatedSize,
        maxSize: `${CACHE.MAX_FILES} files`, // File-based limit
        items,
        maxItems: CACHE.MAX_FILES,
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
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Cassandra cache stats error');

      return {
        enabled: true,
        mode: 'cassandra',
        size: 'Unknown',
        maxSize: `${CACHE.MAX_FILES} files`,
        items: 0,
        maxItems: CACHE.MAX_FILES,
        hitRatio: '0.00',
        hits: this.hits,
        misses: this.misses,
        errors: this.errors,
        connected: this.connected,
      };
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  async close(): Promise<void> {
    try {
      if (this.client) {
        await this.client.shutdown();
        this.connected = false;
        logger.info({
          type: 'cassandra-cache',
          action: 'closed',
        }, 'Cassandra cache connection closed');
      }
    } catch (error) {
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Cassandra cache close error');
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      if (!this.connected) {
        return false;
      }

      // Simple health check query
      await this.client.execute('SELECT now() FROM system.local');
      return true;
    } catch (error) {
      this.errors++;
      logger.error({
        type: 'cassandra-cache',
        error: error instanceof Error ? error.message : String(error),
      }, 'Cassandra cache health check failed');
      return false;
    }
  }
}