// @author gyustar
// @date 2026-09-03

// booking.js 안에 있는 핵심 SQL이 실제로 동시성을 막아주는지,
// Express/Redis/Kafka 없이 PostgreSQL만 놓고 직접 검증한다.
//
// booking.js와 완전히 동일한 SQL을 그대로 가져다 씀:
//   UPDATE seats SET status='booked', updated_at=now()
//    WHERE id=$1 AND event_id=$2 AND status='available'
//   RETURNING id, seat_no
//
// 실행: PGHOST=... PGUSER=... PGPASSWORD=... node test/concurrency-core.test.js
//
// 알려진 이슈: CONCURRENT_REQUESTS를 늘려서 반복 실행하면 종종 0건 확정으로
// 실패하는 케이스가 발견됨 — setupSeat()의 시드(seed) 로직 문제로 추정,
// 아직 원인 미확정. 다음 작업자는 이 부분부터 확인할 것.

const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PGHOST || '127.0.0.1',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'ticketing',
  user: process.env.PGUSER || 'ticketing_app',
  password: process.env.PGPASSWORD || 'localtest',
});

const CONCURRENT = parseInt(process.env.CONCURRENT_REQUESTS || '20', 10);
const EVENT_ID = `concurrency-core-${Date.now()}`;
const SEAT_ID = 'seat-core-1';

async function setupSeat() {
  await pool.query(
    `INSERT INTO seats (id, event_id, seat_no, status)
     VALUES ($1, $2, 'A1', 'available')
     ON CONFLICT (id) DO UPDATE SET status = 'available'`,
    [SEAT_ID, EVENT_ID]
  );
}

// booking.js의 트랜잭션 블록을 그대로 재현 (BEGIN -> 조건부 UPDATE -> INSERT -> COMMIT)
async function attemptBooking(userId) {
  const client = await pool.connect();
  const bookingId = `bk-${userId}`;
  try {
    await client.query('BEGIN');

    const updateResult = await client.query(
      `UPDATE seats
          SET status = 'booked', updated_at = now()
        WHERE id = $1 AND event_id = $2 AND status = 'available'
        RETURNING id, seat_no`,
      [SEAT_ID, EVENT_ID]
    );

    if (updateResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return { userId, result: 'rejected', reason: 'seat_already_booked' };
    }

    await client.query(
      `INSERT INTO bookings (id, event_id, seat_id, user_id, status, created_at)
       VALUES ($1, $2, $3, $4, 'confirmed', now())`,
      [bookingId, EVENT_ID, SEAT_ID, userId]
    );

    await client.query('COMMIT');
    return { userId, result: 'confirmed', bookingId };
  } catch (err) {
    await client.query('ROLLBACK');
    return { userId, result: 'error', error: err.message };
  } finally {
    client.release();
  }
}

async function run() {
  console.log(`[동시성 핵심 로직 테스트] 좌석 1개에 ${CONCURRENT}명이 정확히 동시에 예약을 시도합니다.`);
  await setupSeat();
  console.log(`좌석 준비 완료: event_id=${EVENT_ID} seat_id=${SEAT_ID}\n`);

  const userIds = Array.from({ length: CONCURRENT }, (_, i) => `user-${i}`);

  const startedAt = Date.now();
  // Promise.all로 진짜 동시에 쏜다 — 순차 실행이면 이 테스트는 의미가 없음
  const results = await Promise.all(userIds.map(attemptBooking));
  const elapsedMs = Date.now() - startedAt;

  const confirmed = results.filter((r) => r.result === 'confirmed');
  const rejected = results.filter((r) => r.result === 'rejected');
  const errored = results.filter((r) => r.result === 'error');

  console.log(`처리 완료 (${elapsedMs}ms)\n`);
  results.forEach((r) => {
    if (r.result === 'confirmed') console.log(`  확정: ${r.userId} -> CONFIRMED (${r.bookingId})`);
    else if (r.result === 'rejected') console.log(`  거부: ${r.userId} -> rejected (${r.reason})`);
    else console.log(`  오류: ${r.userId} -> error (${r.error})`);
  });

  console.log(`\n예약 확정: ${confirmed.length}건 / 거부: ${rejected.length}건 / 오류: ${errored.length}건`);

  const dbCheck = await pool.query(
    `SELECT count(*) FROM bookings WHERE seat_id = $1 AND event_id = $2 AND status = 'confirmed'`,
    [SEAT_ID, EVENT_ID]
  );
  const dbConfirmedCount = parseInt(dbCheck.rows[0].count, 10);
  console.log(`DB 재조회 결과 — bookings 테이블의 confirmed 건수: ${dbConfirmedCount}`);

  console.log('\n--- 최종 판정 ---');
  if (confirmed.length === 1 && dbConfirmedCount === 1) {
    console.log(`PASS — 동시 요청 ${CONCURRENT}건 중 정확히 1건만 예약 확정.`);
    await pool.end();
    process.exit(0);
  } else {
    console.log(`FAIL — confirmed=${confirmed.length}, DB=${dbConfirmedCount}. 코드 재검토 필요.`);
    await pool.end();
    process.exit(1);
  }
}

run().catch((err) => {
  console.error('테스트 실행 오류:', err);
  process.exit(1);
});