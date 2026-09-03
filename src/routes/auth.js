const express = require('express');
const redis = require('../clients/redisClient');

const router = express.Router();

// POST /auth/verify { event_id, token }
// Queue 단계에서 발급된 토큰이 Redis에 살아있는지 확인 후 입장을 허용한다.
router.post('/verify', async (req, res) => {
  const { event_id, token } = req.body || {};

  if (!event_id || !token) {
    return res.status(400).json({ error: 'event_id and token are required' });
  }

  const tokenKey = `queue:${event_id}:token:${token}`;
  const raw = await redis.get(tokenKey);

  if (!raw) {
    return res.status(401).json({ error: 'invalid_or_expired_token' });
  }

  const data = JSON.parse(raw);

  // 입장 허용 표시 — 좌석 조회/Hold 단계에서 이 값을 다시 확인한다.
  const admissionKey = `queue:${event_id}:admitted:${data.user_id}`;
  await redis.set(admissionKey, '1', 'EX', 600);

  return res.status(200).json({
    status: 'admitted',
    event_id,
    user_id: data.user_id,
    position: data.position,
  });
});

module.exports = router;
