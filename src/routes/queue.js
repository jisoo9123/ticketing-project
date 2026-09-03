// F01 — Queue (대기열) 라우트
//
// 사용자가 티켓팅에 참여하겠다고 요청하면, 그 사람에게 "대기 순번표"와
// "입장 토큰"을 발급하는 곳. 은행 창구의 번호표 뽑기와 비슷하다.
//
// 왜 Redis를 쓰나?
//   대기 순번은 트래픽이 몰릴 때 초당 몇 백~몇 천 건씩 발급될 수 있는데,
//   Redis는 이런 단순 카운팅 작업을 아주 빠르게 처리한다. 게다가 이 순번
//   정보는 이벤트가 끝나면 필요 없어지므로, 영구 저장(PostgreSQL)까진 필요 없다.

const express = require('express');
const { v4: uuidv4 } = require('uuid');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');

const router = express.Router();

// POST /queue/join { event_id, user_id }
//
// 처리 순서:
//   1) Redis의 INCR(1씩 증가시키는 명령)로 "이 이벤트의 몇 번째 대기자인지" 번호를 받는다
//   2) 이 사람만 알아볼 수 있는 랜덤 토큰(uuid)을 하나 만든다
//   3) 그 토큰을 Redis에 10분짜리로 저장해둔다 — 이 토큰이 있어야 다음 단계(Auth)를 통과할 수 있다
//   4) 사용자에게 "네 순번은 몇 번이고, 이 토큰을 잘 보관해" 라고 응답한다
router.post('/join', idempotency(), async (req, res) => {
  const { event_id, user_id } = req.body || {};

  // 필수 값이 없으면 아예 처리하지 않고 바로 에러 응답 — 잘못된 요청이 뒤로 넘어가지 않게 막는 방어 코드.
  if (!event_id || !user_id) {
    return res.status(400).json({ error: 'event_id and user_id are required' });
  }

  // 이벤트별로 별도의 순번 카운터를 둔다. 예: "queue:evt1:seq"
  // INCR은 Redis가 원자적으로(다른 요청과 겹치지 않게) 처리해주므로,
  // 수천 명이 동시에 요청해도 같은 번호가 두 명에게 나가는 일은 없다.
  const seqKey = `queue:${event_id}:seq`;
  const position = await redis.incr(seqKey);

  // 이 사용자만의 고유 토큰을 발급한다. 이후 auth.js에서 이 토큰으로 신원을 확인한다.
  const token = uuidv4();
  const tokenKey = `queue:${event_id}:token:${token}`;
  await redis.set(
    tokenKey,
    JSON.stringify({ user_id, position, issued_at: new Date().toISOString() }),
    'EX',
    600 // 대기 토큰은 10분(600초) 동안만 유효 — 그 안에 인증(Auth)을 마쳐야 한다
  );

  // 202 Accepted: "요청은 받았고, 처리(대기열 등록) 완료했다"는 뜻의 상태 코드.
  return res.status(202).json({
    status: 'queued',
    event_id,
    position,
    token,
  });
});

module.exports = router;
