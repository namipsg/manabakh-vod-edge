const { loadCdn } = require('../helpers/load-cdn');

describe('VOD segment cache TTL policy', () => {
  test('caches TS segments with a Redis TTL', async () => {
    const { cdn, redisClient } = loadCdn({
      objects: {
        'movie/720p/index1.ts': 'segment-data',
      },
    });

    await cdn.cacheFileFromMinio('movie/720p/index1.ts');

    expect(redisClient.set).toHaveBeenCalledWith(
      'vod_file:movie/720p/index1.ts',
      Buffer.from('segment-data').toString('base64'),
      { EX: 3600 }
    );
  });

  test('rejects legacy TS cache entries that have no TTL', async () => {
    const { cdn, redisClient } = loadCdn();
    redisClient.values.set(
      'vod_file:movie/720p/index1.ts',
      Buffer.from('stale-segment').toString('base64')
    );

    await expect(cdn.getCachedFile('movie/720p/index1.ts')).resolves.toBeNull();

    expect(redisClient.del).toHaveBeenCalledWith('vod_file:movie/720p/index1.ts');
  });
});
