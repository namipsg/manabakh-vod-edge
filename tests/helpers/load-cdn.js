const { Readable } = require('stream');

function createFakeRedis() {
  const values = new Map();
  const hashes = new Map();
  const expiring = new Set();

  const redisClient = {
    values,
    connect: jest.fn().mockResolvedValue(undefined),
    quit: jest.fn().mockResolvedValue(undefined),
    ping: jest.fn().mockResolvedValue('PONG'),
    get: jest.fn(async (key) => values.get(key) || null),
    set: jest.fn(async (key, value, options) => {
      values.set(key, value);
      if (options?.EX) expiring.add(key);
    }),
    ttl: jest.fn(async (key) => (expiring.has(key) ? 60 : -1)),
    del: jest.fn(async (key) => {
      values.delete(key);
      expiring.delete(key);
    }),
    scan: jest.fn(async (cursor, options = {}) => {
      const pattern = options.MATCH || '*';
      const regex = new RegExp(`^${pattern.replace(/\*/g, '.*')}$`);
      return {
        cursor: '0',
        keys: Array.from(values.keys()).filter((key) => regex.test(key)),
      };
    }),
    hIncrBy: jest.fn(async (key, field, amount) => {
      const hash = hashes.get(key) || {};
      hash[field] = String(Number(hash[field] || 0) + amount);
      hashes.set(key, hash);
    }),
    hGetAll: jest.fn(async (key) => ({ ...(hashes.get(key) || {}) })),
  };

  redisClient.multi = jest.fn(() => {
    const commands = [];
    const batch = {
      hIncrBy: (key, field, amount) => {
        commands.push(() => redisClient.hIncrBy(key, field, amount));
        return batch;
      },
      exec: async () => Promise.all(commands.map((command) => command())),
    };
    return batch;
  });

  return redisClient;
}

function createFakeMinio(objects = {}) {
  const objectMap = new Map(
    Object.entries(objects).map(([key, value]) => [
      key,
      Buffer.isBuffer(value) ? value : Buffer.from(String(value)),
    ])
  );

  function getBuffer(key) {
    const buffer = objectMap.get(key);
    if (!buffer) {
      const error = new Error('File not found');
      error.code = 'NoSuchKey';
      throw error;
    }
    return buffer;
  }

  return {
    statObject: jest.fn(async (bucket, key) => ({ size: getBuffer(key).length })),
    getObject: jest.fn(async (bucket, key) => Readable.from(getBuffer(key))),
    getPartialObject: jest.fn(async (bucket, key, start, length) => {
      const buffer = getBuffer(key);
      return Readable.from(buffer.subarray(start, start + length));
    }),
  };
}

function loadCdn({ objects = {}, redisClient = createFakeRedis(), minioClient = createFakeMinio(objects) } = {}) {
  jest.resetModules();
  jest.doMock('redis', () => ({ createClient: jest.fn(() => redisClient) }));
  jest.doMock('minio', () => ({ Client: jest.fn(() => minioClient) }));
  jest.doMock('@sentry/node', () => ({ setupExpressErrorHandler: jest.fn() }));
  jest.doMock('../../instrument', () => {});

  process.env.MINIO_BUCKET_NAME = 'vod';

  return {
    cdn: require('../../index'),
    redisClient,
    minioClient,
  };
}

module.exports = {
  createFakeMinio,
  createFakeRedis,
  loadCdn,
};
