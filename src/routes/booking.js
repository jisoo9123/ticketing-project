const express = require('express');
const { v4: uuidv4 } = require('uuid');
const pool = require('../clients/pgClient');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');
const { publish } = require('../clients/kafkaClient');
const config = require('../config');

const router = express.Router();

// POST /bookings { event_id, seat_id, user_id }
//
// 여기가 아키텍처에서 가장 까다롭다고 표시된 지점이다.
// 같은 좌석에 두 요청이 거의 동시에 들어와도, 조건부 UPDATE(WHERE status='available')로
// 오직 한 요청만 성공하도록 만든다 — 이게 안 되면 한 좌석이 두 명에게 팔린다.
router.post('/', idempotency(), async (req, res) => {
  const { event_id, seat_id, user_id } = req.body || {};

  if (!event_id || !seat_id || !user_id) {
    return res.status(400).json({ error: 'event_id, seat_id and user_id are required' });
  }

  // Seat 단계에서 이 사용자가 실제로 Hold를 잡았는지 확인
  const holdKey = `hold:${event_id}:${seat_id}`;
  const holder = await redis.get(holdKey);
  if (holder !== user_id) {
    return res.status(409).json({ error: 'seat_hold_expired_or_mismatched' });
  }

  const client = await pool.connect();
  const bookingId = uuidv4();

  try {
    await client.query('BEGIN');

    // 조건부 UPDATE — status가 여전히 'available'인 경우에만 성공.
    // 동시 요청 중 하나만 이 UPDATE의 영향을 받고, 나머지는 rowCount 0으로 실패한다.
    const updateResult = await client.query(
      `UPDATE seats
          SET status = 'booked', updated_at = now()
        WHERE id = $1 AND event_id = $2 AND status = 'available'
        RETURNING id, seat_no`,
      [seat_id, event_id]
    );

    if (updateResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'seat_already_booked' });
    }

    await client.query(
      `INSERT INTO bookings (id, event_id, seat_id, user_id, status, created_at)
       VALUES ($1, $2, $3, $4, 'confirmed', now())`,
      [bookingId, event_id, seat_id, user_id]
    );

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[booking] transaction failed:', err.message);
    return res.status(500).json({ error: 'booking_transaction_failed' });
  } finally {
    client.release();
  }

  // 예약이 실제로 확정된 뒤에만 Hold를 해제하고 이벤트를 발행한다.
  await redis.del(holdKey);

  // Kafka 발행은 결제/알림을 위한 "후속" 처리다 — 여기서 실패하더라도
  // 이미 확정된 예약(DB commit)까지 되돌리거나 사용자 응답을 막으면 안 된다.
  // 발행 실패는 로그로 남기고, 재처리는 별도 재시도/보정 배치 몫으로 둔다.
  try {
    await publish(config.topics.bookingCreated, {
      booking_id: bookingId,
      event_id,
      seat_id,
      user_id,
      created_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error(`[booking] booking_id=${bookingId} Kafka 발행 실패 (예약 자체는 확정됨):`, err.message);
  }

  return res.status(201).json({
    status: 'confirmed',
    booking_id: bookingId,
    event_id,
    seat_id,
    user_id,
  });
});

module.exports = router;
