const express = require('express');
const pool = require('../clients/pgClient');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');
const config = require('../config');

const router = express.Router();

// GET /seats/:eventId — 좌석 목록 조회 (원본 상태는 PostgreSQL 기준)
router.get('/:eventId', async (req, res) => {
  const { eventId } = req.params;

  const { rows } = await pool.query(
    `SELECT id, seat_no, status
       FROM seats
      WHERE event_id = $1
      ORDER BY seat_no`,
    [eventId]
  );

  return res.status(200).json({ event_id: eventId, seats: rows });
});

// POST /seats/:seatId/hold { event_id, user_id }
// Auth 단계에서 입장 허용된 사용자만 좌석을 임시로 잡을 수 있다.
// Redis SET NX + TTL — 같은 좌석에 두 사람이 동시에 눌러도 한쪽만 Hold에 성공한다.
router.post('/:seatId/hold', idempotency(), async (req, res) => {
  const { seatId } = req.params;
  const { event_id, user_id } = req.body || {};

  if (!event_id || !user_id) {
    return res.status(400).json({ error: 'event_id and user_id are required' });
  }

  const admissionKey = `queue:${event_id}:admitted:${user_id}`;
  const admitted = await redis.get(admissionKey);
  if (!admitted) {
    return res.status(403).json({ error: 'not_admitted' });
  }

  const holdKey = `hold:${event_id}:${seatId}`;
  const acquired = await redis.set(
    holdKey,
    user_id,
    'EX',
    config.seatHoldTtlSeconds,
    'NX'
  );

  if (!acquired) {
    return res.status(409).json({ error: 'seat_already_held' });
  }

  return res.status(200).json({
    status: 'held',
    event_id,
    seat_id: seatId,
    user_id,
    hold_ttl_seconds: config.seatHoldTtlSeconds,
  });
});

module.exports = router;
