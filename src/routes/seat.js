// @author gyustar
// @date 2026-09-03

// F03 — Seat (좌석 조회 및 임시 Hold) 라우트
//
// [2026-09-03 재작성] 실제 운영 스키마(seat 테이블, version 컬럼 포함)에 맞춰
// 다시 짬. 기존엔 우리가 임의로 만든 status만 있는 단순 스키마를 가정했지만,
// 실제로는 낙관적 잠금(version)까지 이미 고려된 스키마가 있었음.
//
// 여전히 이 라우트의 기본 구조(Redis로 임시 Hold, PostgreSQL은 최종 확정 때만
// 건드림)는 그대로 유지한다 — 그게 맞는 설계이기 때문. 바뀐 건 실제 테이블/컬럼
// 이름과, seat 목록 조회 시 version까지 같이 내려주는 부분이다.

const express = require('express');
const pool = require('../clients/pgClient');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');
const config = require('../config');

const router = express.Router();

// GET /seats/:eventId — 특정 이벤트(ticket_event.id)의 좌석 목록을 조회한다.
// version도 같이 내려준다 — 클라이언트가 나중에 "내가 본 시점의 버전"을
// 기준으로 낙관적 잠금 비교를 하고 싶을 때 쓸 수 있도록.
router.get('/:eventId', async (req, res) => {
  const { eventId } = req.params;

  const { rows } = await pool.query(
    `SELECT id, seat_code, status, version
       FROM seat
      WHERE event_id = $1
      ORDER BY seat_code`,
    [eventId]
  );

  return res.status(200).json({ event_id: eventId, seats: rows });
});

// POST /seats/:seatId/hold { event_id, user_id }
//
// Redis Hold 로직 자체는 이전과 동일 — 이건 실제 좌석 확정과 무관하게
// "지금 이 사람이 이 좌석을 보고 있는 중"이라는 임시 표시일 뿐이라
// 스키마가 바뀌었다고 로직을 바꿀 이유가 없다.
//
// 다만 Hold를 걸기 전에, 이 좌석이 DB에서 이미 AVAILABLE이 아닌 상태(이미
// 팔렸거나 존재하지 않는 좌석)인지 미리 확인하는 절차를 추가했다.
// 이걸 안 하면 이미 팔린 좌석도 Redis Hold까지는 걸렸다가 booking 단계에서야
// 실패하는 낭비가 생긴다 — 최종 방어선은 어차피 booking.js의 트랜잭션이지만,
// 여기서 먼저 걸러주면 사용자 경험이 낫다.
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

  // 좌석이 실제로 존재하고 아직 AVAILABLE인지 미리 확인 (선제 체크, 최종 확정은 아님)
  const seatCheck = await pool.query(
    `SELECT status FROM seat WHERE id = $1 AND event_id = $2`,
    [seatId, event_id]
  );
  if (seatCheck.rowCount === 0) {
    return res.status(404).json({ error: 'seat_not_found' });
  }
  if (seatCheck.rows[0].status !== 'AVAILABLE') {
    return res.status(409).json({ error: 'seat_not_available' });
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
