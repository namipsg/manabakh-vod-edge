const { loadCdn } = require('../helpers/load-cdn');

describe('MP4 cache policy', () => {
  test('excludes MP4 files from Redis cache and enables range support', async () => {
    const { cdn, redisClient } = loadCdn({
      objects: {
        'movies/big.mp4': '0123456789',
      },
    });

    redisClient.values.set('vod_file:movies/big.mp4', Buffer.from('cached').toString('base64'));

    expect(cdn.isCacheExcluded('movies/big.mp4')).toBe(true);
    expect(cdn.supportsRangeRequests('movies/big.mp4')).toBe(true);
    await expect(cdn.getCachedFile('movies/big.mp4')).resolves.toBeNull();

    await cdn.cacheFileFromMinio('movies/big.mp4');
    expect(redisClient.set).not.toHaveBeenCalled();
  });

  test('parses valid and invalid range headers', () => {
    const { cdn } = loadCdn();

    expect(cdn.parseRangeHeader(undefined, 1000)).toBeNull();
    expect(cdn.parseRangeHeader('items=0-1', 1000)).toEqual({ invalid: true });
    expect(cdn.parseRangeHeader('bytes=0-99', 1000)).toEqual({ invalid: false, start: 0, end: 99 });
    expect(cdn.parseRangeHeader('bytes=100-', 1000)).toEqual({ invalid: false, start: 100, end: 999 });
    expect(cdn.parseRangeHeader('bytes=-50', 1000)).toEqual({ invalid: false, start: 950, end: 999 });
    expect(cdn.parseRangeHeader('bytes=1000-1001', 1000)).toEqual({ invalid: true });
    expect(cdn.parseRangeHeader('bytes=abc-def', 1000)).toEqual({ invalid: true });
  });
});
