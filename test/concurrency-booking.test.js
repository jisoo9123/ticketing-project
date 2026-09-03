// @author gyustar
// @date 2026-09-03

// 좌석 동시성 테스트 — 실제 실행 중인 서버(HTTP)를 대상으로 한다.
// concurrency-core.test.js가 SQL 자체를 검증한다면, 이건 Express 라우트를
// 포함한 전체 요청 경로(Idempotency 체크, Redis Hold, PostgreSQL 확정)를 검증한다.
//
// 실행: BASE_URL=http://<서버주소>:3000 node test/concurrency-booking.test.js

const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';
const CONCURRENT_REQUESTS = parseInt(process.env.CONCURRENT_REQUESTS || '10', 10);
const EVENT_ID = `concurrency-test-${Date.now()}`;
const SEAT_ID = 'seat-concurrency-1';

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

async function post(path, body, headers = {}) {
  const res = await fetch(`${BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  let json = null;
  try { json = await res.json(); } catch (e) { /* no body */ }
  return { status: res.status, body: json };
}

async function setupSeat() {
  const { Pool } = require('pg');
  const pool = new Pool({
    host: process.env.PGHOST || '127.0.0.1',
    port: parseInt(process.env.PGPORT || '5432', 10),
    database: process.env.PGDATABASE || 'ticketing',
    user: process.env.PGUSER || 'ticketing_app',
    password: process.env.PGPASSWORD || 'localtest',
  });

  await pool.query(
    `INSERT INTO seats (id, event_id, seat_no, status)
     VALUES ($1, $2, 'A1', 'available')
     ON CONFLICT (id) DO UPDATE SET status = 'available'`,
    [SEAT_ID, EVENT_ID]
  );
  await pool.end();
  log(`테스트 좌석 준비 완료: event_id=${EVENT_ID} seat_id=${SEAT_ID}`);
}

async function admitUser(userId) {
  const joinRes = await post('/queue/join', { event_id: EVENT_ID, user_id: userId }, {
    'Idempotency-Key': `join-${userId}`,
  });
  const token = joinRes.body?.token;
  await post('/auth/verify', { event_id: EVENT_ID, token });
}

async function attemptHoldAndBook(userId) {
  const holdRes = await post(`/seats/${SEAT_ID}/hold`, { event_id: EVENT_ID, user_id: userId }, {
    'Idempotency-Key': `hold-${userId}`,
  });

  if (holdRes.status !== 200) {
    return { userId, stage: 'hold', status: holdRes.status, result: 'blocked_at_hold' };
  }

  const bookRes = await post('/bookings', { event_id: EVENT_ID, seat_id: SEAT_ID, user_id: userId }, {
    'Idempotency-Key': `book-${userId}`,
  });

  return {
    userId,
    stage: 'booking',
    status: bookRes.status,
    result: bookRes.status === 201 ? 'confirmed' : 'rejected',
    body: bookRes.body,
  };
}

async function run() {
  log(`동시성 테스트 시작 — 동시 요청 ${CONCURRENT_REQUESTS}건, 대상: ${BASE_URL}`);

  await setupSeat();

  const userIds = Array.from({ length: CONCURRENT_REQUESTS }, (_, i) => `concurrency-user-${i}`);

  log('사용자 대기열·인증 통과 처리 중...');
  await Promise.all(userIds.map(admitUser));

  log(`이제 ${CONCURRENT_REQUESTS}명이 동시에 같은 좌석(${SEAT_ID})을 잡으려고 시도합니다...`);
  const startedAt = Date.now();
  const results = await Promise.all(userIds.map(attemptHoldAndBook));
  const elapsedMs = Date.now() - startedAt;

  const confirmed = results.filter((r) => r.result === 'confirmed');
  const blockedAtHold = results.filter((r) => r.result === 'blocked_at_hold');
  const rejectedAtBooking = results.filter((r) => r.result === 'rejected');

  log(`처리 완료 (${elapsedMs}ms)`);
  log(`  - 예약 확정(confirmed): ${confirmed.length}건`);
  log(`  - Hold 단계에서 차단:   ${blockedAtHold.length}건`);
  log(`  - Booking 단계에서 거부: ${rejectedAtBooking.length}건`);

  console.log('\n--- 상세 결과 ---');
  results.forEach((r) => {
    console.log(`  ${r.userId}: ${r.stage} -> ${r.status} (${r.result})`);
  });

  console.log('\n--- 판정 ---');
  if (confirmed.length === 1) {
    console.log(`PASS — 동시 요청 ${CONCURRENT_REQUESTS}건 중 정확히 1건만 예약 확정됨.`);
    console.log(`   confirmed booking_id: ${confirmed[0].body?.booking_id}`);
    process.exit(0);
  } else if (confirmed.length === 0) {
    console.log(`FAIL — 아무도 예약에 성공하지 못함. Hold나 Booking 로직에 문제가 있을 수 있음.`);
    process.exit(1);
  } else {
    console.log(`FAIL — ${confirmed.length}건이 동시에 예약 확정됨. 중복 예매 버그. 즉시 확인 필요.`);
    confirmed.forEach((r) => console.log(`   중복 확정: ${r.userId} -> booking_id=${r.body?.booking_id}`));
    process.exit(1);
  }
}

run().catch((err) => {
  console.error('테스트 실행 중 오류:', err);
  process.exit(1);
});