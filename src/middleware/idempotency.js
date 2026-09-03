const redis = require('../clients/redisClient');
const config = require('../config');

// Worker 장애 → VIP 전환 → 클라이언트 자동 재시도 상황에서
// 같은 요청이 두 번 처리되어 좌석이 중복으로 팔리는 것을 막는 미들웨어.
//
// 동작:
// 1) 요청 헤더의 Idempotency-Key로 Redis에서 처리 기록을 조회
// 2) 이미 처리된 키면 새로 처리하지 않고 저장해둔 응답을 그대로 반환
// 3) 처음 보는 키면 요청을 진행시키고, 응답이 나가는 순간 그 결과를 캐싱

function idempotency({ required = true } = {}) {
  return async function idempotencyMiddleware(req, res, next) {
    const key = req.get('Idempotency-Key');

    if (!key) {
      if (required) {
        return res.status(400).json({ error: 'idempotency_key_required' });
      }
      return next();
    }

    const cacheKey = `idem:${req.method}:${req.originalUrl}:${key}`;

    try {
      const cached = await redis.get(cacheKey);
      if (cached) {
        const { status, body } = JSON.parse(cached);
        res.setHeader('Idempotency-Replayed', 'true');
        return res.status(status).json(body);
      }
    } catch (err) {
      // Redis 조회 실패 시에도 서비스는 계속 — 다만 재시도 시 중복 처리 위험은 남음
      console.error('[idempotency] lookup failed:', err.message);
    }

    // res.json을 가로채서, 실제로 응답이 나가는 시점에 결과를 캐싱한다.
    const originalJson = res.json.bind(res);
    res.json = (body) => {
      const status = res.statusCode;
      // 2xx 성공 응답만 캐싱 — 4xx/5xx는 재시도 시 다시 시도할 수 있어야 하므로 캐싱하지 않음
      if (status >= 200 && status < 300) {
        redis
          .set(cacheKey, JSON.stringify({ status, body }), 'EX', config.idempotencyTtlSeconds)
          .catch((err) => console.error('[idempotency] cache write failed:', err.message));
      }
      return originalJson(body);
    };

    next();
  };
}

module.exports = idempotency;
