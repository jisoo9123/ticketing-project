const express = require('express');
const { v4: uuidv4 } = require('uuid');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');

const router = express.Router();

// POST /queue/join { event_id, user_id }
// Redis INCR로 대기 순번을 발급하고, 입장 토큰을 TTL과 함께 발급한다.
router.post('/join', idempotency(), async (req, res) => {
  const { event_id, user_id } = req.body || {};

  if (!event_id || !user_id) {
    return res.status(400).json({ error: 'event_id and user_id are required' });
  }

  const seqKey = `queue:${event_id}:seq`;
  const position = await redis.incr(seqKey);

  const token = uuidv4();
  const tokenKey = `queue:${event_id}:token:${token}`;
  await redis.set(
    tokenKey,
    JSON.stringify({ user_id, position, issued_at: new Date().toISOString() }),
    'EX',
    600 // 대기 토큰 10분 유효
  );

  return res.status(202).json({
    status: 'queued',
    event_id,
    position,
    token,
  });
});

module.exports = router;
