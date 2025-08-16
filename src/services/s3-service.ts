import { S3Client, GetObjectCommand, HeadObjectCommand, GetObjectCommandInput } from '@aws-sdk/client-s3';
import { S3 } from '../config/constants.js';
import { logger } from '../middleware.js';

export class S3Service {
  private client: S3Client;
  private defaultBucket: string;

  constructor() {
    this.defaultBucket = S3.BUCKET_NAME;
    
    // Ensure endpoint has protocol prefix
    let endpoint = S3.ENDPOINT;
    if (!endpoint.startsWith('http://') && !endpoint.startsWith('https://')) {
      endpoint = S3.USE_SSL ? `https://${endpoint}` : `http://${endpoint}`;
    }
    
    this.client = new S3Client({
      endpoint,
      region: S3.REGION || 'us-east-1',
      credentials: {
        accessKeyId: S3.ACCESS_KEY_ID,
        secretAccessKey: S3.SECRET_ACCESS_KEY,
      },
      forcePathStyle: S3.FORCE_PATH_STYLE,
    });

    logger.info({
      type: 's3-service',
      endpoint: S3.ENDPOINT,
      region: S3.REGION,
      defaultBucket: this.defaultBucket,
      forcePathStyle: S3.FORCE_PATH_STYLE,
    }, 'S3 service initialized');
  }

  async getObject(bucket: string, key: string, range?: string) {
    try {
      const params: GetObjectCommandInput = {
        Bucket: bucket,
        Key: key,
      };

      if (range) {
        params.Range = range;
      }

      const command = new GetObjectCommand(params);
      const response = await this.client.send(command);

      return {
        body: response.Body,
        contentType: response.ContentType,
        contentLength: response.ContentLength,
        etag: response.ETag,
        lastModified: response.LastModified,
        contentRange: response.ContentRange,
        acceptRanges: response.AcceptRanges,
        metadata: response.Metadata,
      };
    } catch (error) {
      logger.error({
        type: 's3-service',
        error: error instanceof Error ? error.message : String(error),
        bucket,
        key,
        range,
      }, 'Failed to get S3 object');
      throw error;
    }
  }

  async getObjectFromDefaultBucket(key: string, range?: string) {
    return this.getObject(this.defaultBucket, key, range);
  }

  async headObject(bucket: string, key: string) {
    try {
      const command = new HeadObjectCommand({
        Bucket: bucket,
        Key: key,
      });

      const response = await this.client.send(command);

      return {
        contentType: response.ContentType,
        contentLength: response.ContentLength,
        etag: response.ETag,
        lastModified: response.LastModified,
        acceptRanges: response.AcceptRanges,
        metadata: response.Metadata,
      };
    } catch (error) {
      logger.error({
        type: 's3-service',
        error: error instanceof Error ? error.message : String(error),
        bucket,
        key,
      }, 'Failed to get S3 object metadata');
      throw error;
    }
  }

  async headObjectFromDefaultBucket(key: string) {
    return this.headObject(this.defaultBucket, key);
  }

  parseObjectPath(path: string): { bucket: string; key: string } {
    const parts = path.split('/').filter(Boolean);
    
    if (parts.length === 0) {
      throw new Error('Invalid object path');
    }

    // If path has multiple segments, first could be bucket name
    if (parts.length === 1) {
      return {
        bucket: this.defaultBucket,
        key: parts[0],
      };
    }

    // Check if first part looks like a bucket name (no file extension)
    const firstPart = parts[0];
    const hasExtension = /\.[a-zA-Z0-9]+$/.test(firstPart);
    
    if (!hasExtension && parts.length > 1) {
      // Treat first part as bucket name
      return {
        bucket: firstPart,
        key: parts.slice(1).join('/'),
      };
    } else {
      // Treat entire path as key in default bucket
      return {
        bucket: this.defaultBucket,
        key: parts.join('/'),
      };
    }
  }

  isS3Error(error: any): boolean {
    return error && (error.name === 'NoSuchKey' || error.name === 'NoSuchBucket' || error.$metadata);
  }

  getS3ErrorStatus(error: any): number {
    if (!this.isS3Error(error)) return 500;
    
    switch (error.name) {
      case 'NoSuchKey':
      case 'NoSuchBucket':
        return 404;
      case 'AccessDenied':
        return 403;
      default:
        return 500;
    }
  }
}

export const s3Service = new S3Service();