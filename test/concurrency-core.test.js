// @author gyustar
// @date 2026-09-03

// booking.js의 핵심 SQL(조건부 UPDATE)이 실제 운영 스키마(seat/booking/
// booking_seat) 기준으로도 동시성을 안전하게 막아주는지 직접 검증한다.
//
// [2026-09-03 재작성] 기존엔 우리가 임의로 만든 seats/bookings 2테이블
// 기준이었는데, 실제 스키마(app_user/ticket_event/seat/booking/
// booking_seat/payment/processed_event) 기준으로 다시 짰다.

const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PGHOST || '127.0.0.1',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'ticketing',
  user: process.env.PGUSER || 'ticketing_app',
  password: process.env.PGPASSWORD || 'localtest',
});

const CONCURRENT = parseInt(process.env.CONCURRENT_REQUESTS || '20', 10);

// 테스트에 필요한 최소 데이터(유저 1명, 이벤트 1개, 좌석 1개)를 만들어둔다.
// FK 제약 때문에 seat -> ticket_event, booking -> app_user/ticket_event가
// 미리 존재해야 한다.
async function setupFixtures() {
  const userEmail = `concurrency-test-${Date.now()}@example.com`;
  const userResult = await pool.query(
    `INSERT INTO app_user (email, password_hash) VALUES ($1, 'test-hash') RETURNING id`,
    [userEmail]
  );
  // 이 테스트는 "같은 좌석에 여러 명이 동시에" 상황을 재현해야 하므로,
  // 실제로는 유저를 여러 명 만들어야 하지만 핵심 SQL 검증이 목적이라
  // 여기서는 하나의 유저 id를 여러 "가상 사용자"가 공유하는 걸로 단순화한다.
  const userId = userResult.rows[0].id;

  const eventResult = await pool.query(
    `INSERT INTO ticket_event (title, opens_at, closes_at)
     VALUES ('동시성 테스트 이벤트', now(), now() + interval '1 day')
     RETURNING id`
  );
  const eventId = eventResult.rows[0].id;

  const seatResult = await pool.query(
    `INSERT INTO seat (event_id, seat_code, status) VALUES ($1, 'A1', 'AVAILABLE') RETURNING id`,
    [eventId]
  );
  const seatId = seatResult.rows[0].id;

  return { userId, eventId, seatId };
}

// booking.js의 트랜잭션 블록을 그대로 재현한다 (실제 코드와 SQL을 동일하게 맞춰야
// 이 테스트가 의미가 있다 — 여기서만 안전하고 실제 코드가 다르면 소용없음).
async function attemptBooking({ userId, eventId, seatId }, idx) {
  const client = await pool.connect();
  const idempotencyKey = `concurrency-test-${idx}-${Date.now()}-${Math.random()}`;
  try {
    await client.query('BEGIN');

    const seatUpdate = await client.query(
      `UPDATE seat
          SET status = 'BOOKED', version = version + 1
        WHERE id = $1 AND event_id = $2 AND status = 'AVAILABLE'
        RETURNING id, version`,
      [seatId, eventId]
    );

    if (seatUpdate.rowCount === 0) {
      await client.query('ROLLBACK');
      return { idx, result: 'rejected', reason: 'seat_already_booked' };
    }

    const bookingInsert = await client.query(
      `INSERT INTO booking (user_id, event_id, idempotency_key, status)
       VALUES ($1, $2, $3, 'CONFIRMED') RETURNING id`,
      [userId, eventId, idempotencyKey]
    );
    const bookingId = bookingInsert.rows[0].id;

    await client.query(
      `INSERT INTO booking_seat (booking_id, seat_id) VALUES ($1, $2)`,
      [bookingId, seatId]
    );

    await client.query('COMMIT');
    return { idx, result: 'confirmed', bookingId };
  } catch (err) {
    await client.query('ROLLBACK');
    return { idx, result: 'error', error: err.message };
  } finally {
    client.release();
  }
}

async function run() {
  console.log(`[동시성 핵심 로직 테스트 - 실제 스키마 기준] 좌석 1개에 ${CONCURRENT}명이 동시에 예약을 시도합니다.`);

  const fixtures = await setupFixtures();
  console.log(`테스트 데이터 준비 완료: user_id=${fixtures.userId} event_id=${fixtures.eventId} seat_id=${fixtures.seatId}\n`);

  const attempts = Array.from({ length: CONCURRENT }, (_, i) => i);

  const startedAt = Date.now();
  const results = await Promise.all(attempts.map((i) => attemptBooking(fixtures, i)));
  const elapsedMs = Date.now() - startedAt;

  const confirmed = results.filter((r) => r.result === 'confirmed');
  const rejected = results.filter((r) => r.result === 'rejected');
  const errored = results.filter((r) => r.result === 'error');

  console.log(`처리 완료 (${elapsedMs}ms)\n`);
  results.forEach((r) => {
    if (r.result === 'confirmed') console.log(`  확정: 시도#${r.idx} -> CONFIRMED (booking_id=${r.bookingId})`);
    else if (r.result === 'rejected') console.log(`  거부: 시도#${r.idx} -> rejected (${r.reason})`);
    else console.log(`  오류: 시도#${r.idx} -> error (${r.error})`);
  });

  console.log(`\n예약 확정: ${confirmed.length}건 / 거부: ${rejected.length}건 / 오류: ${errored.length}건`);

  // DB를 다시 조회해서 실제로 booking_seat에 이 좌석이 몇 번 연결됐는지 재확인.
  // seat_id가 UNIQUE라서 원래 1개 초과로는 절대 안 들어가야 정상.
  const dbCheck = await pool.query(
    `SELECT count(*) FROM booking_seat WHERE seat_id = $1`,
    [fixtures.seatId]
  );
  const dbLinkedCount = parseInt(dbCheck.rows[0].count, 10);
  console.log(`DB 재조회 결과 — booking_seat에 이 좌석이 연결된 건수: ${dbLinkedCount}`);

  console.log('\n--- 최종 판정 ---');
  if (confirmed.length === 1 && dbLinkedCount === 1) {
    console.log(`PASS — 동시 요청 ${CONCURRENT}건 중 정확히 1건만 예약 확정 (실제 운영 스키마 기준으로도 검증됨).`);
    await pool.end();
    process.exit(0);
  } else {
    console.log(`FAIL — confirmed=${confirmed.length}, DB=${dbLinkedCount}. 코드 재검토 필요.`);
    await pool.end();
    process.exit(1);
  }
}

run().catch((err) => {
  console.error('테스트 실행 오류:', err);
  process.exit(1);
});
