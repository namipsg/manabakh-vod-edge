import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Get directory path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

/**
 * Server configuration
 */
export const SERVER = {
  PORT: parseInt(process.env.PORT || '3000', 10),
  HOST: process.env.HOST || 'localhost',
  NODE_ENV: process.env.NODE_ENV || 'development',
  IS_PRODUCTION: process.env.NODE_ENV === 'production',
  IS_DEVELOPMENT: process.env.NODE_ENV === 'development',
};

/**
 * Proxy settings
 */
export const PROXY = {
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS?.split(',') || ['*'],
  MAX_REQUEST_SIZE: parseInt(process.env.MAX_REQUEST_SIZE || '10485760', 10), // 10MB
  ALLOWED_DOMAINS: process.env.ALLOWED_DOMAINS?.split(',').filter(Boolean) || [],
  REQUEST_TIMEOUT: parseInt(process.env.REQUEST_TIMEOUT || '30000', 10), // 30 seconds
  ENABLE_DOMAIN_WHITELIST: process.env.ENABLE_DOMAIN_WHITELIST === 'true',
  ENABLE_RATE_LIMITING: process.env.ENABLE_RATE_LIMITING === 'true',
  RATE_LIMIT_WINDOW: parseInt(process.env.RATE_LIMIT_WINDOW || '60000', 10), // 1 minute
  RATE_LIMIT_MAX_REQUESTS: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '60', 10), // 60 per minute
};

/**
 * Logging configuration
 */
export const LOGGING = {
  LEVEL: process.env.LOG_LEVEL || 'info',
};

/**
 * S3 Configuration
 */
export const S3 = {
  ENDPOINT: process.env.S3_ENDPOINT || 'http://localhost:9000',
  ACCESS_KEY_ID: process.env.S3_ACCESS_KEY_ID || 'minioadmin',
  SECRET_ACCESS_KEY: process.env.S3_SECRET_ACCESS_KEY || 'minioadmin',
  REGION: process.env.S3_REGION || 'us-east-1',
  BUCKET_NAME: process.env.S3_BUCKET_NAME || 'vod-archive',
  FORCE_PATH_STYLE: process.env.S3_FORCE_PATH_STYLE === 'true' || true,
  USE_SSL: process.env.S3_USE_SSL === 'true' || false,
};

/**
 * Cache Configuration
 */
export const CACHE = {
  // Cache mode: 'memory' (default), 'redis', 'cassandra', 'redis-cassandra'
  MODE: process.env.CACHE_MODE || 'memory',
  TTL: parseInt(process.env.CACHE_TTL || '300', 10), // 5 minutes default
  CHECK_PERIOD: parseInt(process.env.CACHE_CHECK_PERIOD || '600', 10), // 10 minutes
  MAX_ITEMS: parseInt(process.env.CACHE_MAX_ITEMS || '1000', 10),
  MAX_SIZE: parseInt(process.env.CACHE_MAX_SIZE || '104857600', 10), // 100MB default
  MEMORY_THRESHOLD: parseFloat(process.env.REDIS_MEMORY_THRESHOLD || '0.8'), // 80%
  MAX_FILES: parseInt(process.env.CASSANDRA_MAX_FILES || '100000', 10),
};

/**
 * Redis Configuration
 */
export const REDIS = {
  URL: process.env.REDIS_URL || 'redis://localhost:6379',
  HOST: process.env.REDIS_HOST || 'localhost',
  PORT: parseInt(process.env.REDIS_PORT || '6379', 10),
  PASSWORD: process.env.REDIS_PASSWORD || '',
  DB: parseInt(process.env.REDIS_DB || '0', 10),
  KEY_PREFIX: process.env.REDIS_KEY_PREFIX || 'mcdn:',
  MAX_RETRIES: parseInt(process.env.REDIS_MAX_RETRIES || '3', 10),
  RETRY_DELAY: parseInt(process.env.REDIS_RETRY_DELAY || '1000', 10), // 1 second
  CONNECT_TIMEOUT: parseInt(process.env.REDIS_CONNECT_TIMEOUT || '10000', 10), // 10 seconds
  COMMAND_TIMEOUT: parseInt(process.env.REDIS_COMMAND_TIMEOUT || '5000', 10), // 5 seconds
};

/**
 * Cassandra Configuration
 */
export const CASSANDRA = {
  HOSTS: process.env.CASSANDRA_HOSTS?.split(',') || ['localhost:9042'],
  KEYSPACE: process.env.CASSANDRA_KEYSPACE || 'cdn_cache',
  USERNAME: process.env.CASSANDRA_USERNAME || '',
  PASSWORD: process.env.CASSANDRA_PASSWORD || '',
  LOCAL_DATA_CENTER: process.env.CASSANDRA_LOCAL_DATA_CENTER || 'DC1-VoD',
  CONSISTENCY: process.env.CASSANDRA_CONSISTENCY || 'LOCAL_QUORUM',
  REPLICATION_FACTOR: parseInt(process.env.CASSANDRA_REPLICATION_FACTOR || '1', 10),
  TABLE_NAME: process.env.CASSANDRA_TABLE_NAME || 'cache_data',
  CONNECT_TIMEOUT: parseInt(process.env.CASSANDRA_CONNECT_TIMEOUT || '5000', 10), // 5 seconds
  REQUEST_TIMEOUT: parseInt(process.env.CASSANDRA_REQUEST_TIMEOUT || '12000', 10), // 12 seconds
};

/**
 * Route paths
 */
export const ROUTES = {
  PROXY_BASE: '/proxy',
  PROXY_PATH: '/proxy/:url',
  PROXY_BASE64: '/proxy-base64/:encodedUrl',
  CDN_BASE: '/vod',
  CDN_BUCKET_OBJECT: '/vod/:bucket/*',
  CDN_OBJECT: '/vod/*',
};

/**
 * Common HTTP headers
 */
export const HEADERS = {
  // CORS related headers
  CORS_HEADERS: {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, PUT, PATCH, DELETE',
    'Access-Control-Allow-Headers': 'Origin, X-Requested-With, Content-Type, Accept, Authorization',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Max-Age': '86400', // 24 hours
  },
  
  // Security headers
  SECURITY_HEADERS: {
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Referrer-Policy': 'no-referrer-when-downgrade',
    'Content-Security-Policy': "default-src 'self'",
  },
};

export default {
  SERVER,
  PROXY,
  LOGGING,
  S3,
  CACHE,
  REDIS,
  CASSANDRA,
  ROUTES,
  HEADERS,
};