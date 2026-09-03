// Idempotency Check 미들웨어
//
// === 이게 왜 필요한지 (쉬운 설명) ===
// 온라인 쇼핑하다가 결제 버튼을 실수로 두 번 눌러도 결제는 한 번만 되는 것,
// 그 원리를 여기서 구현한다.
//
// === 언제 이게 실제로 필요해지는지 ===
// Worker(서버) 장애 → MetalLB가 자동으로 다른 Worker로 트래픽을 돌림(VIP 전환)
// → 그 순간 사용자 요청이 응답을 못 받음 → 클라이언트가 "어? 응답이 안 왔네"
// 하고 같은 요청을 자동으로 재전송 → 이때 서버가 이걸 "완전히 새로운 요청"으로
// 착각하면, 좌석이 중복으로 팔리거나 결제가 두 번 될 위험이 있다.
//
// === 이 미들웨어가 하는 일 ===
// 1) 요청 헤더에 있는 Idempotency-Key(요청을 보낼 때 클라이언트가 붙이는 고유 키)로
//    "이 요청, 전에 처리한 적 있나?"를 Redis에서 확인한다
// 2) 있으면(=재시도로 들어온 요청) → 새로 처리하지 않고, 저장해둔 예전 응답을 그대로 돌려준다
// 3) 없으면(=처음 보는 요청) → 정상적으로 처리를 진행시키고, 응답이 나가는 순간
//    그 결과를 Redis에 저장해서 다음번 재시도에 대비한다

const redis = require('../clients/redisClient');
const config = require('../config');

// required: 이 옵션이 true(기본값)면 Idempotency-Key 헤더가 없는 요청은 무조건 거부한다.
// (좌석 Hold, 예약 확정처럼 "중복되면 안 되는" 요청에는 항상 true로 써야 한다)
function idempotency({ required = true } = {}) {
  // Express 미들웨어는 (req, res, next) 세 개를 받는 함수 형태여야 한다.
  // req  = 들어온 요청 정보, res = 응답을 보낼 도구, next = "다음 단계로 넘어가라"는 신호
  return async function idempotencyMiddleware(req, res, next) {
    const key = req.get('Idempotency-Key');

    if (!key) {
      if (required) {
        // 키가 없는데 필수인 경우 -> 여기서 요청을 끝내고 400 에러로 돌려보낸다.
        // (다음 단계인 실제 좌석 Hold나 예약 로직은 아예 실행되지 않는다)
        return res.status(400).json({ error: 'idempotency_key_required' });
      }
      // 키가 없어도 괜찮은 요청이면(required: false), 그냥 다음 단계로 넘어간다.
      return next();
    }

    // Redis에 저장할 때 쓸 고유한 키를 만든다.
    // 같은 Idempotency-Key라도 "어느 API에 어떤 방식으로 보낸 요청인지"가 다르면
    // 서로 다른 요청으로 취급해야 하므로, method(POST 등)와 URL 경로도 함께 섞는다.
    const cacheKey = `idem:${req.method}:${req.originalUrl}:${key}`;

    try {
      // 이 키로 전에 처리한 기록이 있는지 Redis에서 찾아본다.
      const cached = await redis.get(cacheKey);
      if (cached) {
        // 있다! -> 이건 재시도로 들어온 요청이다.
        // 새로 처리하지 않고, 그때 응답했던 내용을 그대로 다시 돌려준다.
        const { status, body } = JSON.parse(cached);
        // 이 헤더를 보고 "아, 이건 캐시된 응답이구나"를 클라이언트나 개발자가 알 수 있다.
        res.setHeader('Idempotency-Replayed', 'true');
        return res.status(status).json(body);
      }
    } catch (err) {
      // Redis 조회 자체가 실패했을 때도 서비스는 멈추면 안 되므로,
      // 에러 로그만 남기고 그냥 요청을 정상 진행시킨다.
      // (다만 이 경우엔 재시도 시 중복 처리될 위험이 남아있다는 걸 알고 있어야 함)
      console.error('[idempotency] lookup failed:', err.message);
    }

    // === 여기부터는 "처음 보는 요청"이라는 뜻 ===
    // 라우트(queue.js, seat.js, booking.js 등)가 실제 로직을 실행하고
    // res.json(...)을 호출해서 응답을 보내는 순간을 "가로채서" 캐싱하는 트릭이다.
    //
    // 원래의 res.json 함수를 originalJson이라는 이름으로 따로 저장해두고,
    // res.json을 "저장도 하고 원래 응답도 보내는" 새 함수로 바꿔치기한다.
    const originalJson = res.json.bind(res);
    res.json = (body) => {
      const status = res.statusCode;
      // 성공(2xx) 응답만 캐싱한다.
      // 왜 실패(4xx/5xx) 응답은 캐싱 안 하나? -> 실패는 "일시적인 문제였을 수도"
      // 있으니, 같은 키로 다시 요청이 오면 또 시도해볼 수 있게 열어둬야 하기 때문.
      if (status >= 200 && status < 300) {
        redis
          .set(cacheKey, JSON.stringify({ status, body }), 'EX', config.idempotencyTtlSeconds)
          .catch((err) => console.error('[idempotency] cache write failed:', err.message));
      }
      // 바꿔치기했어도 원래 하려던 일(실제로 응답 보내기)은 그대로 수행한다.
      return originalJson(body);
    };

    // 라우트의 실제 로직으로 넘어간다.
    next();
  };
}

module.exports = idempotency;
