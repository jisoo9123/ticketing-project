// @author gyustar
// @date 2026-09-03

// PostgreSQL(PPAS) 클라이언트
//
// 여기서 "예약이 실제로 확정됐다"는 사실을 영구적으로 저장한다.
// Redis와 다르게, 여기 저장된 데이터는 서버를 껐다 켜도 사라지지 않는다.
//
// Pool(연결 풀)을 쓰는 이유:
//   매 요청마다 새로 DB 연결을 맺으면 느리고 비효율적이다.
//   Pool은 미리 연결 여러 개를 만들어두고, 요청이 올 때마다 빌려주고
//   끝나면 반납받는 방식으로 재사용한다 — 훨씬 빠르다.

const { Pool } = require('pg');
const config = require('../config');

const pool = new Pool({
  host: config.pg.host,
  port: config.pg.port,
  database: config.pg.database,
  user: config.pg.user,
  password: config.pg.password,
  // 동시에 최대 10개까지 연결을 유지한다.
  // 트래픽이 많아지면 이 숫자를 늘려야 할 수 있다 (부하 테스트 결과 보고 조정).
  max: 10,
  // 30초 동안 아무 요청도 안 쓰는 연결은 정리한다 (자원 낭비 방지)
  idleTimeoutMillis: 30000,
});

// 풀에 있는 연결 중 하나가 예상치 못하게 끊기면 여기로 에러가 들어온다.
// 이것도 redisClient.js와 마찬가지로 서버를 죽이지 않고 로그만 남긴다 —
// 실제 쿼리를 실행하는 라우트(booking.js 등)에서 그때그때 에러 처리를 한다.
pool.on('error', (err) => {
  console.error('[postgres] unexpected error on idle client:', err.message);
});

module.exports = pool;