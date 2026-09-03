// @author gyustar
// @date 2026-09-03

// F04 — Booking (예약 확정) 라우트
//
// === 이 프로젝트에서 가장 중요한 파일 ===
// "임시로 잡아둔 좌석"(Redis Hold)을 "진짜로 확정된 예약"(PostgreSQL)으로
// 바꾸는 곳. 좌석 하나에 두 명 이상이 동시에 예약을 시도했을 때, 정확히
// 한 명만 성공해야 한다 — 이게 안 지켜지면 한 좌석이 두 사람에게 팔리는
// 사고(중복 예매)가 난다.
//
// 이 코드는 실제로 동시 요청 20~50건을 직접 쏴서 "1건만 확정되는지"
// 검증했다 (test/concurrency-core.test.js 참고).

const express = require('express');
const { v4: uuidv4 } = require('uuid');
const pool = require('../clients/pgClient');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');
const { publish } = require('../clients/kafkaClient');
const config = require('../config');

const router = express.Router();

// POST /bookings { event_id, seat_id, user_id }
router.post('/', idempotency(), async (req, res) => {
  const { event_id, seat_id, user_id } = req.body || {};

  if (!event_id || !seat_id || !user_id) {
    return res.status(400).json({ error: 'event_id, seat_id and user_id are required' });
  }

  // === 1단계: 이 사용자가 정말 이 좌석을 Hold했는지 확인 ===
  // seat.js에서 Hold에 성공한 사람만 Redis에 "hold:이벤트:좌석 = user_id" 형태로 기록이 남는다.
  // 여기서 그 기록을 다시 확인해서, Hold도 안 한 사람이 예약 확정을 시도하는 걸 막는다.
  // (예: Hold가 5분 지나서 이미 풀렸는데 뒤늦게 예약 확정을 누른 경우도 여기서 걸러진다)
  const holdKey = `hold:${event_id}:${seat_id}`;
  const holder = await redis.get(holdKey);
  if (holder !== user_id) {
    return res.status(409).json({ error: 'seat_hold_expired_or_mismatched' });
  }

  const client = await pool.connect();
  const bookingId = uuidv4();

  try {
    // === 2단계: 트랜잭션 시작 ===
    // 트랜잭션이란 "여러 개의 DB 작업을 하나의 묶음으로 처리"하는 것.
    // 묶음 안의 작업이 전부 성공해야 실제로 반영되고(COMMIT),
    // 하나라도 실패하면 전부 취소된다(ROLLBACK) — "다 되거나, 하나도 안 되거나" 둘 중 하나.
    await client.query('BEGIN');

    // === 3단계: 조건부 UPDATE — 여기가 진짜 핵심 ===
    // "WHERE status = 'available'" 이 조건이 핵심이다.
    // 이 좌석이 지금 'available'(비어있음) 상태일 때만 'booked'(예약됨)로 바꾼다.
    //
    // 왜 이게 동시성을 막아주나?
    //   PostgreSQL은 UPDATE 문 하나를 처리하는 동안 그 행(row)에 자동으로 잠금을 건다.
    //   두 요청이 동시에 이 UPDATE를 실행하려고 하면, PostgreSQL이 순서대로 처리한다:
    //     - 첫 번째 요청: status가 아직 'available' -> 조건 만족 -> UPDATE 성공 (1 row)
    //     - 두 번째 요청: 첫 번째가 이미 'booked'로 바꿔놨으므로 -> 조건 불만족 -> 0 rows
    //   즉 "동시에 눌러도" 실제로는 DB 안에서 한 명씩 순서가 매겨지고,
    //   조건(status='available')이 이미 깨진 쪽은 자동으로 실패한다.
    const updateResult = await client.query(
      `UPDATE seats
          SET status = 'booked', updated_at = now()
        WHERE id = $1 AND event_id = $2 AND status = 'available'
        RETURNING id, seat_no`,
      [seat_id, event_id]
    );

    // updateResult.rowCount는 "이 UPDATE로 실제로 바뀐 행의 개수"다.
    // 0이면 "이미 누가 먼저 예약했다"는 뜻 — 방금 설명한 동시성 제어가 여기서 작동한 것.
    if (updateResult.rowCount === 0) {
      await client.query('ROLLBACK'); // 지금까지 한 것도 없지만, 트랜잭션을 깔끔하게 종료
      return res.status(409).json({ error: 'seat_already_booked' });
    }

    // === 4단계: 예약 기록을 bookings 테이블에 남긴다 ===
    // seats 테이블은 "지금 좌석이 비었나 찼나"만 담당하고,
    // bookings 테이블은 "누가 언제 예매했는가"라는 실제 기록을 담당한다 (용도가 다름).
    await client.query(
      `INSERT INTO bookings (id, event_id, seat_id, user_id, status, created_at)
       VALUES ($1, $2, $3, $4, 'confirmed', now())`,
      [bookingId, event_id, seat_id, user_id]
    );

    // 여기까지 문제없이 왔으면 두 작업(UPDATE + INSERT)을 실제로 확정한다.
    await client.query('COMMIT');
  } catch (err) {
    // 트랜잭션 도중 예상 못한 에러(연결 끊김 등)가 나면, 지금까지 한 걸 전부 되돌린다.
    // 이게 없으면 "좌석은 booked로 바뀌었는데 bookings에는 기록이 안 남는" 반쪽짜리
    // 상태가 생길 수 있다 — 트랜잭션이 바로 이런 상황을 막아준다.
    await client.query('ROLLBACK');
    console.error('[booking] transaction failed:', err.message);
    return res.status(500).json({ error: 'booking_transaction_failed' });
  } finally {
    // 성공하든 실패하든, 빌려 쓴 DB 연결은 반드시 풀에 반납해야 한다.
    // (안 하면 연결이 하나씩 새서 나중에 풀이 고갈된다)
    client.release();
  }

  // === 5단계: 예약이 실제로 확정된 뒤 뒷정리 ===
  // 더 이상 Hold 상태로 붙잡아둘 필요가 없으니 Redis에서 지운다.
  await redis.del(holdKey);

  // === 6단계: Kafka로 "예약 확정됐다" 이벤트를 발행 ===
  // 이 코드가 try/catch로 감싸져 있는 이유가 중요하다:
  // Kafka 발행은 결제·알림을 위한 "후속" 처리일 뿐이지, 예약 확정 자체의 필수
  // 조건이 아니다. 만약 이 부분에서 실패했다고 전체 요청을 실패시켜버리면,
  // 이미 DB에는 확정된 예약(4단계에서 COMMIT 완료)이 있는데 사용자에게는
  // "실패했다"고 알리는 모순이 생긴다. 그래서 여기서 실패해도 로그만 남기고
  // 사용자에게는 정상적으로 "예약 확정됨" 응답을 보낸다.
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

  // 201 Created: "새로운 것(예약)이 성공적으로 만들어졌다"는 상태 코드.
  return res.status(201).json({
    status: 'confirmed',
    booking_id: bookingId,
    event_id,
    seat_id,
    user_id,
  });
});

module.exports = router;