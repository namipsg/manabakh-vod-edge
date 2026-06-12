const request = require('supertest');
const { loadCdn } = require('../helpers/load-cdn');

function binaryParser(res, callback) {
  const chunks = [];
  res.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
  res.on('end', () => callback(null, Buffer.concat(chunks)));
}

describe('MP4 partial delivery', () => {
  test('serves MP4 byte ranges from MinIO with 206 partial content', async () => {
    const { cdn, minioClient, redisClient } = loadCdn({
      objects: {
        'movies/big.mp4': 'abcdefghijklmnopqrstuvwxyz',
      },
    });

    const response = await request(cdn.app)
      .get('/vod/movies/big.mp4')
      .set('Range', 'bytes=10-19')
      .buffer(true)
      .parse(binaryParser)
      .expect(206);

    expect(response.body.toString()).toBe('klmnopqrst');
    expect(response.headers['accept-ranges']).toBe('bytes');
    expect(response.headers['content-range']).toBe('bytes 10-19/26');
    expect(response.headers['content-length']).toBe('10');
    expect(response.headers['content-type']).toContain('video/mp4');
    expect(response.headers['x-cache-layer']).toBe('L1-Minio-Range');
    expect(minioClient.getPartialObject).toHaveBeenCalledWith('vod', 'movies/big.mp4', 10, 10);
    expect(redisClient.get).not.toHaveBeenCalled();
    expect(redisClient.set).not.toHaveBeenCalled();
  });

  test('serves full MP4 streams without caching when no Range header is present', async () => {
    const { cdn, minioClient, redisClient } = loadCdn({
      objects: {
        'movies/big.mp4': 'abcdefghijklmnopqrstuvwxyz',
      },
    });

    const response = await request(cdn.app)
      .get('/nami/movies/big.mp4')
      .buffer(true)
      .parse(binaryParser)
      .expect(200);

    expect(response.body.toString()).toBe('abcdefghijklmnopqrstuvwxyz');
    expect(response.headers['accept-ranges']).toBe('bytes');
    expect(response.headers['content-length']).toBe('26');
    expect(response.headers['x-cache-layer']).toBe('L1-Minio-Stream');
    expect(minioClient.getObject).toHaveBeenCalledWith('vod', 'movies/big.mp4');
    expect(redisClient.get).not.toHaveBeenCalled();
    expect(redisClient.set).not.toHaveBeenCalled();
  });

  test('returns 416 for unsatisfiable MP4 byte ranges', async () => {
    const { cdn, redisClient } = loadCdn({
      objects: {
        'movies/big.mp4': 'abcdefghijklmnopqrstuvwxyz',
      },
    });

    const response = await request(cdn.app)
      .get('/vod/movies/big.mp4')
      .set('Range', 'bytes=99-120')
      .expect(416);

    expect(response.headers['content-range']).toBe('bytes */26');
    expect(redisClient.get).not.toHaveBeenCalled();
    expect(redisClient.set).not.toHaveBeenCalled();
  });
});
