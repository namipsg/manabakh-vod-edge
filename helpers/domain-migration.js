const Aerospike = require('aerospike');
const Minio = require('minio');
require('dotenv').config();

const OLD_DOMAIN = process.env.OLD_DOMAIN;
const NEW_DOMAIN = process.env.NEW_DOMAIN;
const S3_PREFIX = process.env.S3_PREFIX;
const AEROSPIKE_HOST = process.env.AEROSPIKE_HOST || 'localhost';
const AEROSPIKE_PORT = parseInt(process.env.AEROSPIKE_PORT) || 3000;
const NAMESPACE = 'test';
const SET = 'content';

const minioEndpoint = process.env.MINIO_ENDPOINT?.replace(/^https?:\/\//, '');
const minioClient = new Minio.Client({
  endPoint: minioEndpoint,
  port: parseInt(process.env.MINIO_PORT) || 9000,
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
});

const BUCKET_NAME = process.env.MINIO_BUCKET_NAME;

let aerospikeClient;
let processedCount = 0;
let updatedCount = 0;
let errorCount = 0;

async function initAerospike() {
  const config = {
    hosts: `${AEROSPIKE_HOST}:${AEROSPIKE_PORT}`,
  };

  aerospikeClient = new Aerospike.Client(config);
  
  try {
    await aerospikeClient.connect();
    console.log('Connected to Aerospike');
  } catch (error) {
    console.error('Failed to connect to Aerospike:', error);
    throw error;
  }
}

function replaceDomainsInObject(obj, oldDomain, newDomain) {
  let hasChanges = false;
  
  function processValue(value) {
    if (typeof value === 'string') {
      if (value.includes(oldDomain)) {
        hasChanges = true;
        return value.replace(new RegExp(oldDomain.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'), newDomain);
      }
      return value;
    } else if (Array.isArray(value)) {
      return value.map(processValue);
    } else if (value && typeof value === 'object') {
      const newObj = {};
      for (const [key, val] of Object.entries(value)) {
        newObj[key] = processValue(val);
      }
      return newObj;
    }
    return value;
  }

  const result = processValue(obj);
  return { result, hasChanges };
}

async function processS3JsonFile(configURL, oldDomain, newDomain) {
  try {
    const s3Path = configURL.replace(S3_PREFIX, '').replace(/^\/+/, '');
    
    console.log(`Processing S3 file: ${s3Path}`);
    
    const fileStream = await minioClient.getObject(BUCKET_NAME, s3Path);
    
    const chunks = [];
    return new Promise((resolve, reject) => {
      fileStream.on('data', (chunk) => chunks.push(chunk));
      fileStream.on('end', async () => {
        try {
          const buffer = Buffer.concat(chunks);
          const jsonContent = JSON.parse(buffer.toString());
          
          const { result: updatedJson, hasChanges } = replaceDomainsInObject(jsonContent, oldDomain, newDomain);
          
          if (hasChanges) {
            const updatedBuffer = Buffer.from(JSON.stringify(updatedJson, null, 2));
            
            await minioClient.putObject(BUCKET_NAME, s3Path, updatedBuffer, {
              'Content-Type': 'application/json'
            });
            
            console.log(`Updated S3 file: ${s3Path}`);
            resolve({ updated: true, content: updatedJson });
          } else {
            console.log(`No changes needed for S3 file: ${s3Path}`);
            resolve({ updated: false, content: jsonContent });
          }
        } catch (error) {
          console.error(`Error processing S3 file ${s3Path}:`, error);
          reject(error);
        }
      });
      fileStream.on('error', (error) => {
        console.error(`Error reading S3 file ${s3Path}:`, error);
        reject(error);
      });
    });
  } catch (error) {
    console.error(`Failed to access S3 file for ${configURL}:`, error);
    throw error;
  }
}

async function processRecord(record) {
  try {
    processedCount++;
    const key = record.key;
    const bins = record.bins;
    
    console.log(`Processing record ${processedCount}: ${key.key || key.digest}`);
    
    let recordUpdated = false;
    const updatedBins = { ...bins };
    
    const { result: updatedBins_domains, hasChanges: domainChanges } = replaceDomainsInObject(bins, OLD_DOMAIN, NEW_DOMAIN);
    
    if (domainChanges) {
      Object.assign(updatedBins, updatedBins_domains);
      recordUpdated = true;
      console.log(`Found domain replacements in record`);
    }
    
    if (bins.configURL && typeof bins.configURL === 'string' && bins.configURL.includes(OLD_DOMAIN)) {
      try {
        const s3Result = await processS3JsonFile(bins.configURL, OLD_DOMAIN, NEW_DOMAIN);
        
        // Update the configURL in the record to point to the new domain
        updatedBins.configURL = bins.configURL.replace(new RegExp(OLD_DOMAIN.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'), NEW_DOMAIN);
        recordUpdated = true;
        console.log(`Updated configURL from ${bins.configURL} to ${updatedBins.configURL}`);
        
        if (s3Result.updated) {
          console.log(`Updated associated S3 JSON file content for record`);
        }
      } catch (s3Error) {
        console.error(`Failed to process S3 file for record ${key.key || key.digest}:`, s3Error.message);
        errorCount++;
      }
    }
    
    if (recordUpdated) {
      const aerospikeKey = new Aerospike.Key(NAMESPACE, SET, key.key || key.digest);
      await aerospikeClient.put(aerospikeKey, updatedBins);
      updatedCount++;
      console.log(`✓ Updated record ${key.key || key.digest}`);
    } else {
      console.log(`- No changes needed for record ${key.key || key.digest}`);
    }
    
    if (processedCount % 100 === 0) {
      console.log(`Progress: ${processedCount} processed, ${updatedCount} updated, ${errorCount} errors`);
    }
    
  } catch (error) {
    console.error(`Error processing record:`, error);
    errorCount++;
  }
}

async function runMigration() {
  if (!OLD_DOMAIN || !NEW_DOMAIN) {
    console.error('OLD_DOMAIN and NEW_DOMAIN environment variables are required');
    process.exit(1);
  }
  
  if (!BUCKET_NAME) {
    console.error('MINIO_BUCKET_NAME environment variable is required');
    process.exit(1);
  }
  
  console.log(`Starting domain migration from "${OLD_DOMAIN}" to "${NEW_DOMAIN}"`);
  console.log(`Target: Aerospike namespace "${NAMESPACE}", set "${SET}"`);
  console.log(`S3 Bucket: "${BUCKET_NAME}"`);
  
  try {
    await initAerospike();
    
    const scan = aerospikeClient.scan(NAMESPACE, SET);
    scan.concurrent = true;
    scan.nobins = false;
    
    const stream = scan.foreach();
    
    stream.on('data', processRecord);
    
    stream.on('error', (error) => {
      console.error('Scan error:', error);
      errorCount++;
    });
    
    stream.on('end', async () => {
      console.log('\n=== Migration Complete ===');
      console.log(`Total records processed: ${processedCount}`);
      console.log(`Records updated: ${updatedCount}`);
      console.log(`Errors encountered: ${errorCount}`);
      console.log(`Success rate: ${((processedCount - errorCount) / processedCount * 100).toFixed(2)}%`);
      
      await aerospikeClient.close();
      console.log('Disconnected from Aerospike');
      process.exit(errorCount > 0 ? 1 : 0);
    });
    
  } catch (error) {
    console.error('Migration failed:', error);
    if (aerospikeClient) {
      await aerospikeClient.close();
    }
    process.exit(1);
  }
}

process.on('SIGINT', async () => {
  console.log('\nMigration interrupted by user');
  if (aerospikeClient) {
    await aerospikeClient.close();
  }
  console.log(`Final stats: ${processedCount} processed, ${updatedCount} updated, ${errorCount} errors`);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  console.log('\nMigration terminated');
  if (aerospikeClient) {
    await aerospikeClient.close();
  }
  console.log(`Final stats: ${processedCount} processed, ${updatedCount} updated, ${errorCount} errors`);
  process.exit(1);
});

if (require.main === module) {
  runMigration().catch((error) => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
}