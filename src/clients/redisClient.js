const Redis = require('ioredis');
const config = require('../config');

// Redis Sentinel/단일 인스턴스 공용 — REDIS_URL 형식으로 접속
// 예: redis://10.1.93.110:6379  (Sentinel 구성 시 ioredis sentinel 옵션으로 교체)
const redis = new Redis(config.redisUrl, {
  retryStrategy: (times) => Math.min(times * 200, 3000),
  maxRetriesPerRequest: 3,
});

redis.on('error', (err) => {
  console.error('[redis] connection error:', err.message);
});

redis.on('connect', () => {
  console.log('[redis] connected:', config.redisUrl);
});

module.exports = redis;
