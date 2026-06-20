const { Readable } = require('stream');
const request = require('supertest');
const { loadCdn } = require('../helpers/load-cdn');

async function waitFor(assertion, attempts = 20) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (assertion()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  throw new Error('Timed out waiting for condition');
}

describe('Concurrent cache fills', () => {
  test('shares one MinIO fetch for simultaneous requests for the same segment', async () => {
    let releaseMinio;
    let resolveMinioStarted;
    const minioStarted = new Promise((resolve) => {
      resolveMinioStarted = resolve;
    });
    const minioGate = new Promise((resolve) => {
      releaseMinio = resolve;
    });

    const minioClient = {
      statObject: jest.fn(),
      getPartialObject: jest.fn(),
      getObject: jest.fn(async () => {
        resolveMinioStarted();
        await minioGate;
        return Readable.from(Buffer.from('segment-data'));
      }),
    };

    const { cdn, redisClient } = loadCdn({ minioClient });

    const firstRequest = request(cdn.app)
      .get('/vod/movie/720p/index1.ts')
      .expect(200);
    const firstPromise = firstRequest.then((response) => response);

    await minioStarted;

    const secondRequest = request(cdn.app)
      .get('/vod/movie/720p/index1.ts')
      .expect(200);
    const secondPromise = secondRequest.then((response) => response);

    await waitFor(() => redisClient.get.mock.calls.length >= 2);

    expect(minioClient.getObject).toHaveBeenCalledTimes(1);

    releaseMinio();

    const [firstResponse, secondResponse] = await Promise.all([
      firstPromise,
      secondPromise,
    ]);

    expect(firstResponse.headers['x-cache-layer']).toBe('L1-Minio');
    expect(secondResponse.headers['x-cache-layer']).toBe('L1-Minio-Coalesced');
    expect(redisClient.set).toHaveBeenCalledTimes(1);
    expect(redisClient.set).toHaveBeenCalledWith(
      'vod_file:movie/720p/index1.ts',
      Buffer.from('segment-data').toString('base64'),
      { EX: 3600 }
    );

    const statsResponse = await request(cdn.app)
      .get('/stats')
      .expect(200);

    expect(statsResponse.body.totals.cacheFillRequests).toBe(1);
    expect(statsResponse.body.totals.coalescedRequests).toBe(1);
    expect(statsResponse.body.cache.inFlightCacheFills).toBe(0);
  });
});
