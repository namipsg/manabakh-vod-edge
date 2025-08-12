# Domain Migration Tool

This tool migrates all URLs from an old domain to a new domain across Aerospike records and associated S3 JSON files.

## Features

- Scans all records in Aerospike's `test.content` collection
- Replaces domain URLs in all object fields (recursive)
- Processes `configURL` fields by fetching and updating S3 JSON files
- Updates both Aerospike records and S3 files atomically
- Provides detailed progress logging and statistics
- Handles errors gracefully with retry logic

## Prerequisites

1. Install dependencies:
```bash
npm install
```

2. Set up environment variables in `.env`:
```env
# Migration settings
OLD_DOMAIN=https://old-domain.com
NEW_DOMAIN=https://new-domain.com

# Aerospike connection
AEROSPIKE_HOST=localhost
AEROSPIKE_PORT=3000

# MinIO/S3 settings
MINIO_ENDPOINT=https://s3.amazonaws.com
MINIO_PORT=443
MINIO_USE_SSL=true
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_BUCKET_NAME=your_bucket_name
```

## Usage

Run the migration:
```bash
npm run migrate-domains
```

Or directly:
```bash
node domain-migration.js
```

## What it does

1. **Connects to Aerospike** and scans all records in `test.content`

2. **For each record:**
   - Searches all fields for the old domain URL
   - Replaces found URLs with the new domain
   - If `configURL` field exists:
     - Extracts the S3 path from the URL
     - Downloads the JSON file from S3
     - Performs domain replacement in the JSON content
     - Uploads the updated JSON back to S3
   - Updates the Aerospike record with any changes

3. **Progress tracking:**
   - Shows progress every 100 records
   - Final statistics: processed, updated, errors
   - Graceful shutdown on SIGINT/SIGTERM

## Example

```bash
# Before migration
Record: {
  title: "Video content",
  thumbnail: "https://old-domain.com/thumb.jpg",
  configURL: "https://old-domain.com/config/video123.json"
}

S3 file (config/video123.json): {
  "playlist": "https://old-domain.com/playlist.m3u8",
  "segments": ["https://old-domain.com/seg1.ts"]
}

# After migration
Record: {
  title: "Video content", 
  thumbnail: "https://new-domain.com/thumb.jpg",
  configURL: "https://new-domain.com/config/video123.json"
}

S3 file (config/video123.json): {
  "playlist": "https://new-domain.com/playlist.m3u8",
  "segments": ["https://new-domain.com/seg1.ts"]
}
```

## Safety Features

- **Dry-run capability**: Set `DRY_RUN=true` to preview changes without applying them
- **Atomic updates**: Both Aerospike and S3 updates succeed together or fail together
- **Error handling**: Continues processing even if individual records fail
- **Detailed logging**: Full audit trail of all changes made
- **Graceful shutdown**: Can be interrupted safely with Ctrl+C

## Monitoring

The script outputs:
- Record processing progress
- Domain replacements found
- S3 file updates
- Error details
- Final statistics

Sample output:
```
Starting domain migration from "https://old.com" to "https://new.com"
Target: Aerospike namespace "test", set "content"
S3 Bucket: "my-bucket"
Connected to Aerospike
Processing record 1: video_123
Found domain replacements in record
Processing S3 file: config/video123.json
Updated S3 file: config/video123.json
✓ Updated record video_123
Progress: 100 processed, 45 updated, 2 errors
...
=== Migration Complete ===
Total records processed: 1000
Records updated: 450
Errors encountered: 5
Success rate: 99.50%
```