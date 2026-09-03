// @author gyustar
// @date 2026-09-03

// F04 — Booking (예약 확정) 라우트
//
// [2026-09-03 재작성] 실제 운영 스키마에 맞춰 다시 짬. 기존엔 seats/bookings
// 2개 테이블을 우리가 임의로 설계했었는데, 실제로는 booking/booking_seat/
// payment/processed_event까지 포함된 더 정교한 스키마가 이미 있었다.
//
// === 이번에 새로 반영한 것 3가지 ===
//
// 1) booking.idempotency_key UNIQUE 제약 — DB 자체가 중복을 막아준다.
//    기존엔 Redis 캐시로만 Idempotency를 처리했는데, Redis는 TTL 지나면
//    사라지는 휘발성 저장소다. DB에 UNIQUE 제약이 있으면 캐시가 사라진
//    뒤에도 "같은 idempotency_key로는 두 번 못 만든다"는 게 영구적으로
//    보장된다 — Redis 캐시보다 한 단계 더 안전한 방어선.
//
// 2) booking_seat 중간 테이블 — 예약(booking) 하나에 좌석(seat) 여러 개가
//    묶일 수 있는 다대다 구조. 지금 API는 여전히 좌석 1개씩 예약하는
//    방식이지만, 내부적으로는 booking_seat에 1행을 추가하는 식으로
//    맞춰뒀다 — 나중에 "여러 좌석 한 번에 예약"으로 확장하기 쉽다.
//
// 3) seat.version 컬럼 — 그동안 우리가 "WHERE status='available'" 조건 하나로
//    동시성을 막았는데, 이 조건 자체는 여전히 유효하고 그대로 쓴다.
//    (PostgreSQL이 UPDATE 문 처리 중 해당 행에 자동으로 거는 잠금 덕분에,
//     동시에 두 요청이 들어와도 한쪽만 조건을 통과한다 — 이 원리는 안 바뀜)
//    다만 UPDATE가 성공할 때 version도 함께 1 증가시켜서, 이 값을 감사
//    로그나 "마지막으로 언제 바뀌었는지" 추적용으로 남긴다.

const express = require('express');
const pool = require('../clients/pgClient');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');
const { publish } = require('../clients/kafkaClient');
const config = require('../config');

const router = express.Router();

// POST /bookings { event_id, seat_id, user_id }
//
// 주의: user_id는 실제 로그인 시스템이 이 프로젝트 범위 밖이라(README 참고),
// app_user 테이블에 이미 존재하는 id(정수)라고 가정한다. 테스트할 때는
// app_user에 미리 행을 하나 넣어두고 그 id를 써야 한다 (test/seed.sql 참고).
router.post('/', idempotency(), async (req, res) => {
  const { event_id, seat_id, user_id } = req.body || {};
  // Idempotency-Key 헤더 값을 그대로 booking.idempotency_key에 저장한다.
  // 이 값이 없으면 idempotency() 미들웨어가 이미 400으로 막아주므로 여기선 항상 존재.
  const idempotencyKey = req.get('Idempotency-Key');

  if (!event_id || !seat_id || !user_id) {
    return res.status(400).json({ error: 'event_id, seat_id and user_id are required' });
  }

  // seat.js에서 이 사용자가 실제로 Hold를 잡았는지 확인 (기존과 동일)
  const holdKey = `hold:${event_id}:${seat_id}`;
  const holder = await redis.get(holdKey);
  if (holder !== String(user_id)) {
    return res.status(409).json({ error: 'seat_hold_expired_or_mismatched' });
  }

  const client = await pool.connect();
  let bookingId;

  try {
    await client.query('BEGIN');

    // === 핵심: 조건부 UPDATE로 좌석 확정 (동시성 제어의 실체) ===
    // status='AVAILABLE'일 때만 성공하고, 성공 시 version도 1 올린다.
    const seatUpdate = await client.query(
      `UPDATE seat
          SET status = 'BOOKED', version = version + 1
        WHERE id = $1 AND event_id = $2 AND status = 'AVAILABLE'
        RETURNING id, seat_code, version`,
      [seat_id, event_id]
    );

    if (seatUpdate.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'seat_already_booked' });
    }

    // === booking 테이블에 예약 헤더 생성 ===
    // idempotency_key가 UNIQUE라서, 만약 Redis 캐시가 어떤 이유로든 무력화된
    // 상태에서 같은 키로 재요청이 들어와도 여기서 DB가 한 번 더 막아준다.
    let bookingInsert;
    try {
      bookingInsert = await client.query(
        `INSERT INTO booking (user_id, event_id, idempotency_key, status)
         VALUES ($1, $2, $3, 'CONFIRMED')
         RETURNING id`,
        [user_id, event_id, idempotencyKey]
      );
    } catch (err) {
      // PostgreSQL의 unique_violation 에러 코드는 23505.
      // idempotency_key가 중복됐다는 건 = Redis 캐시가 미처 못 잡은 재시도 요청.
      // 이 경우 새로 실패 처리하지 말고, 기존에 만들어졌던 예약을 찾아서 그대로 응답한다
      // — 이게 진짜 의미의 Idempotency(같은 키로는 항상 같은 결과)다.
      if (err.code === '23505' && err.constraint === 'booking_idempotency_key_key') {
        await client.query('ROLLBACK');
        const existing = await pool.query(
          `SELECT id, status FROM booking WHERE idempotency_key = $1`,
          [idempotencyKey]
        );
        if (existing.rowCount > 0) {
          return res.status(200).json({
            status: 'confirmed',
            booking_id: existing.rows[0].id,
            event_id,
            seat_id,
            user_id,
            note: 'idempotency_key로 기존 예약 반환됨 (DB 레벨 중복 방지)',
          });
        }
      }
      throw err; // 다른 종류의 에러면 바깥 catch로 넘겨서 500 처리
    }

    bookingId = bookingInsert.rows[0].id;

    // === booking_seat에 좌석 연결 (다대다 중간 테이블) ===
    // seat_id가 UNIQUE라서, 이 좌석이 이미 다른 booking에 물려있으면
    // 여기서도 한 번 더 막힌다 (이론상 위 조건부 UPDATE에서 이미 걸렀어야 하지만,
    // 이중 방어선으로 남겨두는 것에 의미가 있다).
    await client.query(
      `INSERT INTO booking_seat (booking_id, seat_id) VALUES ($1, $2)`,
      [bookingId, seat_id]
    );

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[booking] transaction failed:', err.message);
    return res.status(500).json({ error: 'booking_transaction_failed' });
  } finally {
    client.release();
  }

  // Hold 해제 (기존과 동일)
  await redis.del(holdKey);

  // Kafka 발행 (기존과 동일 — 실패해도 예약 확정 응답은 그대로 나간다)
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
