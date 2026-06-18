const request = require('supertest');
const { loadCdn } = require('../helpers/load-cdn');

describe('Prometheus metrics', () => {
  test('exports VOD range counters for Grafana scraping', async () => {
    const { cdn } = loadCdn({
      objects: {
        'movies/big.mp4': 'abcdefghijklmnopqrstuvwxyz',
      },
    });

    await request(cdn.app)
      .get('/vod/movies/big.mp4')
      .set('Range', 'bytes=0-9')
      .expect(206);

    const response = await request(cdn.app)
      .get('/metrics/prometheus')
      .expect(200);

    expect(response.text).toContain('vod_cdn_requests_total 1');
    expect(response.text).toContain('vod_cdn_range_requests_total 1');
    expect(response.text).toContain('vod_cdn_range_served_bytes_total 10');
    expect(response.text).toContain('vod_cdn_extension_requests_total{extension=".mp4"} 1');
  });

  test('exports Redis cache hit counters and cached file gauge', async () => {
    const { cdn, redisClient } = loadCdn();
    await redisClient.set('vod_file:posters/cover.jpg', Buffer.from('cover').toString('base64'));

    await request(cdn.app)
      .get('/nami/posters/cover.jpg')
      .expect(200);

    const response = await request(cdn.app)
      .get('/metrics/prometheus')
      .expect(200);

    expect(response.text).toContain('vod_cdn_cache_hits_total 1');
    expect(response.text).toContain('vod_cdn_redis_served_requests_total 1');
    expect(response.text).toContain('vod_cdn_current_cached_files 1');
  });
});
