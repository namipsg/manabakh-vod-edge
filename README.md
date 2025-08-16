# MCDN - Edge CDN Proxy

A powerful edge CDN proxy server for S3/MinIO VOD archives, built with Express.js and TypeScript. Specifically designed to handle streaming media formats like M3U8 playlists, MPEG-TS segments, and various other media formats while providing efficient edge caching and CDN functionality.

## Features

### Core CDN Features
- **S3/MinIO Integration**: Direct integration with self-hosted S3 and MinIO storage
- **Edge Caching**: Intelligent caching of frequently accessed content with configurable TTL
- **Range Request Support**: Efficient partial content delivery for large video files
- **CDN Headers**: Proper ETag, Last-Modified, and Cache-Control headers for optimal caching

### Advanced Media Handling
- **M3U8 Playlist Processing**: Automatic URL rewriting for HLS playlists to work with CDN
- **MPEG-TS Segment Support**: Optimized handling for video segments with proper content detection
- **Content Type Detection**: Automatic detection based on file extensions and binary signatures
- **Streaming Optimization**: Memory-efficient streaming for large video files

### Performance & Caching
- **Worker Thread Pool**: Resource-intensive tasks handled by worker threads
- **Intelligent Caching**: Size-based caching with LRU eviction for optimal memory usage
- **Compression Support**: Built-in GZIP, Brotli, and Zstandard compression
- **Stream Processing**: Efficient streaming without loading entire files into memory

### Legacy CORS Proxy Support
- **Full CORS Support**: Maintains original CORS proxy functionality for external URLs
- **Multiple URL Formats**: Query parameters, path parameters, and base64-encoded URLs
- **Anti-Hotlinking Bypass**: Domain-specific headers for common streaming services

### Monitoring & Management
- **Comprehensive Logging**: Detailed request/response logging with configurable levels  
- **Performance Metrics**: Built-in metrics and statistics endpoints
- **Health Monitoring**: Server status and resource usage endpoints

## Installation

```bash
# Clone the repository
git clone <your-repository-url>
cd mcdn

# Install dependencies
npm install

# Create .env file from example
cp .env.example .env

# Configure your S3/MinIO settings in .env
# Edit S3_ENDPOINT, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, etc.

# Build the project
npm run build

# Start the server
npm start
```

## Usage

### CDN Endpoints (Primary functionality)

Access your S3/MinIO content through the CDN:

#### 1. Using Default Bucket

```
http://localhost:3000/cdn/path/to/video.m3u8
http://localhost:3000/cdn/videos/episode1/playlist.m3u8
```

#### 2. Specifying Bucket Name

```
http://localhost:3000/cdn/my-bucket/path/to/video.m3u8
http://localhost:3000/cdn/archive/shows/series1/episode1.m3u8
```

#### 3. Range Requests (for large files)

```bash
curl -H "Range: bytes=0-1023" http://localhost:3000/cdn/large-video.mp4
```

### Legacy CORS Proxy (Backward compatibility)

#### 1. Query Parameter

```
http://localhost:3000/proxy?url=https://example.com/video.m3u8
```

#### 2. Path Parameter

```
http://localhost:3000/proxy/https://example.com/video.m3u8
```

#### 3. Base64-Encoded Parameter

```
http://localhost:3000/proxy/base64/aHR0cHM6Ly9leGFtcGxlLmNvbS92aWRlby5tM3U4
```

### Streaming Media Support

MCDN has specialized handling for various streaming media formats:

- **HLS Playlists**: Automatically processes M3U8 playlists to rewrite all segment URLs to CDN endpoints
- **MPEG-TS Segments**: Handles TS segments with proper content type detection and range requests
- **WebVTT Subtitles**: Processes VTT files to rewrite image URLs to CDN endpoints  
- **Content Detection**: Automatically detects content types even with misleading file extensions

Example for serving an HLS stream from S3:

```
# Master playlist
http://localhost:3000/cdn/videos/series1/master.m3u8

# Individual segments will be automatically rewritten to:
http://localhost:3000/cdn/videos/series1/segment001.ts
http://localhost:3000/cdn/videos/series1/segment002.ts
```

## Configuration

MCDN is configured using environment variables:

### S3/MinIO Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_ENDPOINT` | S3/MinIO endpoint URL | `http://localhost:9000` |
| `S3_ACCESS_KEY_ID` | S3 access key | `minioadmin` |
| `S3_SECRET_ACCESS_KEY` | S3 secret key | `minioadmin` |
| `S3_REGION` | S3 region | `us-east-1` |
| `S3_BUCKET_NAME` | Default bucket name | `vod-archive` |
| `S3_FORCE_PATH_STYLE` | Use path-style URLs (required for MinIO) | `true` |
| `S3_USE_SSL` | Use SSL for S3 connections | `false` |

### Server Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `3000` |
| `NODE_ENV` | Environment mode | `development` |
| `LOG_LEVEL` | Logging level | `info` |
| `HOST` | Server host | `localhost` |

### Caching Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `ENABLE_CACHE` | Enable response caching | `true` |
| `CACHE_TTL` | Cache TTL in seconds | `300` (5 min) |
| `CACHE_CHECK_PERIOD` | Cache cleanup interval in seconds | `600` (10 min) |
| `CACHE_MAX_ITEMS` | Maximum number of cached items | `1000` |
| `CACHE_MAX_SIZE` | Maximum cache size in bytes | `104857600` (100MB) |

### Performance Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `USE_WORKER_THREADS` | Enable worker threads | `true` |
| `WORKER_THREADS` | Number of worker threads (0 = auto) | `0` |
| `ENABLE_STREAMING` | Enable optimized streaming | `true` |
| `STREAM_SIZE_THRESHOLD` | Size threshold for streaming | `1048576` (1MB) |

### Legacy CORS Proxy Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MAX_REQUEST_SIZE` | Max request size in bytes | `10485760` (10MB) |
| `REQUEST_TIMEOUT` | Request timeout in milliseconds | `30000` (30s) |
| `ENABLE_DOMAIN_WHITELIST` | Enable domain whitelist | `false` |
| `ALLOWED_DOMAINS` | Comma-separated allowed domains | `*` (all) |
| `ALLOWED_ORIGINS` | CORS allowed origins | `*` (all) |

## API Endpoints

### Status Endpoints

- **GET /proxy/status**: Get server status and resource usage
  ```json
  {
    "status": "ok",
    "version": "0.2.0",
    "uptime": 3654.8,
    "timestamp": "2025-05-01T15:30:45.123Z",
    "environment": "production",
    "memory": {
      "rss": 56.23,
      "heapTotal": 32.75,
      "heapUsed": 27.12,
      "external": 2.34
    }
  }
  ```

- **GET /proxy/cache/stats**: Get cache statistics
  ```json
  {
    "status": "ok",
    "data": {
      "enabled": true,
      "size": "45.2 MB",
      "maxSize": "100 MB",
      "items": 328,
      "maxItems": 1000,
      "hitRatio": "0.78",
      "hits": 1249,
      "misses": 352
    },
    "timestamp": "2025-05-01T15:30:45.123Z"
  }
  ```

- **POST /proxy/cache/clear**: Clear the cache
  ```json
  {
    "status": "ok",
    "message": "Cache cleared successfully",
    "timestamp": "2025-05-01T15:30:45.123Z"
  }
  ```

- **GET /proxy/workers/stats**: Get worker thread statistics
  ```json
  {
    "status": "ok",
    "data": {
      "enabled": true,
      "threadsAvailable": 8,
      "threadsRunning": 3,
      "maxThreads": 8,
      "queueSize": 0
    },
    "timestamp": "2025-05-01T15:30:45.123Z"
  }
  ```

- **GET /proxy/metrics**: Get performance metrics
  ```json
  {
    "status": "ok",
    "data": {
      "server": {
        "uptime": "2 hr 45 min",
        "system": {
          "uptime": "5 days 7 hr",
          "cpus": 8,
          "loadAvg": [1.25, 0.86, 0.52],
          "memory": {
            "total": "16 GB",
            "used": "8.7 GB",
            "free": "7.3 GB",
            "usedPercent": "54.38%"
          },
          "platform": "linux",
          "arch": "x64"
        },
        "process": {
          "uptime": "2 hr 45 min",
          "pid": 12345,
          "memory": {
            "rss": "56.23 MB",
            "heapTotal": "32.75 MB",
            "heapUsed": "27.12 MB",
            "external": "2.34 MB"
          },
          "versions": {
            "node": "v18.16.0"
          }
        }
      },
      "requests": {
        "total": 1601,
        "success": 1578,
        "error": 23,
        "successRate": "98.56%"
      },
      "performance": {
        "avgResponseTime": "187 ms",
        "maxResponseTime": "3.24 sec",
        "throughput": {
          "bytesIn": "24.7 MB",
          "bytesOut": "458.2 MB",
          "totalTransferred": "482.9 MB"
        }
      },
      "features": {
        "streaming": {
          "requests": 421,
          "totalSize": "387.5 MB",
          "avgSize": "920.4 KB"
        },
        "cache": {
          "hits": 1249,
          "misses": 352,
          "hitRatio": "78.01%"
        },
        "workers": {
          "tasks": 1892,
          "errors": 7,
          "successRate": "99.63%"
        }
      }
    },
    "timestamp": "2025-05-01T15:30:45.123Z"
  }
  ```

- **POST /proxy/metrics/reset**: Reset performance metrics
  ```json
  {
    "status": "ok",
    "message": "Performance metrics reset successfully",
    "timestamp": "2025-05-01T15:30:45.123Z"
  }
  ```

## Development

```bash
# Start development server with hot reloading
npm run dev

# Run tests
npm run test

# Run linter
npm run lint
```

## Advanced Features

### Domain Templates

Shrina Proxy includes a domain template system to bypass anti-hotlinking protection. The templates in `src/config/domain-templates.ts` define specific headers to use for different domains.

Example domain template:

```typescript
{
  pattern: /\.kwikie\.ru$/i,
  headers: {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:137.0) Gecko/20100101 Firefox/137.0',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.5',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
  },
  headersFn: (url: URL) => {
    return {
      'origin': 'https://kwik.si',
      'referer': 'https://kwik.si/',
    };
  }
}
```

Usage in requests:
```
http://localhost:3000/proxy?url=https://cdn.kwikie.ru/video123.m3u8
```
The proxy will automatically apply the correct headers to bypass the anti-hotlinking protection.

### Custom Content Type Detection

The proxy can detect content types based on binary signatures, overriding incorrect content types returned by servers.

Example of content type detection:

```typescript
// Detect MPEG-TS content even if served with an incorrect content type
if (detectTransportStream(buffer)) {
  return 'video/mp2t';
}
```

Usage example for MPEG-TS segments with misleading extensions:
```
http://localhost:3000/proxy?url=https://example.com/segment-123-v1-a1.jpg
```

In this case, if the content is actually a transport stream despite the `.jpg` extension, Shrina will correctly identify it as `video/mp2t`.

### Adaptive Decompression

Shrina automatically detects and handles various compression formats, even when content-encoding headers are incorrect or missing.

Example compression formats supported:
- GZIP (magic bytes: `0x1F 0x8B`)
- Brotli
- Zstandard (magic bytes: `0x28 0xB5 0x2F 0xFD`)
- Deflate

Example usage:
```
http://localhost:3000/proxy?url=https://example.com/compressed-content
```

If the server returns compressed content without correct headers, Shrina will:
1. Detect the compression format based on magic bytes
2. Decompress the content using the appropriate algorithm
3. Remove the content-encoding header from the response
4. Forward the decompressed content to the client

### M3U8 Playlist Processing

Shrina automatically processes M3U8 playlists to rewrite all URLs to pass through the proxy.

Example original M3U8 content:
```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-KEY:METHOD=AES-128,URI="key.php?id=12345"
segment-0.ts
segment-1.ts
https://cdn2.example.com/segment-2.ts
```

After processing through Shrina:
```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-KEY:METHOD=AES-128,URI="/proxy?url=https%3A%2F%2Fexample.com%2Fkey.php%3Fid%3D12345"
/proxy?url=https%3A%2F%2Fexample.com%2Fsegment-0.ts
/proxy?url=https%3A%2F%2Fexample.com%2Fsegment-1.ts
/proxy?url=https%3A%2F%2Fcdn2.example.com%2Fsegment-2.ts
```

This ensures that all segments and resources referenced in the playlist are also proxied through Shrina.

## License

MIT

## Hosting Recommendations

For those looking to deploy Shrina Proxy in a production environment, here are some recommended hosting options:

- [Jink Host](https://clients.jink.host/aff.php?aff=7) - Affordable hosting with good performance for small to medium proxy deployments
- [Hivelocity](https://my.hivelocity.net/sign-up?referralCode=JKUA) - Enterprise-grade dedicated servers for high-traffic or commercial proxy applications

When choosing a hosting provider, consider factors like bandwidth limits, CPU resources, and geographic location to ensure optimal performance for your specific streaming needs.
