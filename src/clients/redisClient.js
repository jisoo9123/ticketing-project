// @author gyustar
// @date 2026-09-03
//
// Redis 클라이언트
//
// Redis는 "빠르지만 휘발성"인 저장소다. 서버가 재시작되거나 TTL(유효시간)이
// 지나면 데이터가 사라질 수 있다. 그래서 이 프로젝트에서는 "잠깐 있다가
// 없어져도 괜찮은 데이터"만 Redis에 둔다:
//   - 대기열 순번 (queue.js)
//   - 입장 허용 여부 (auth.js)
//   - 좌석 임시 Hold (seat.js)
//   - Idempotency-Key로 캐싱한 응답 (middleware/idempotency.js)
//
// 반대로 "영원히 남아있어야 하는 사실"(누가 어느 좌석을 예매했는가)은
// Redis가 아니라 PostgreSQL(pgClient.js)에 저장한다.
//
// 이 클러스터는 Redis Sentinel 구성(Primary 1 + Replica 2 + Sentinel 3)이다.
// Sentinel이 Primary 장애를 감지하면 Replica 중 하나를 자동 승격시키고,
// ioredis가 새 Primary로 자동 재연결한다.

const Redis = require('ioredis');
const config = require('../config');

// ioredis Sentinel 모드로 연결한다.
// sentinels: Sentinel 프로세스 주소 목록 (최소 1개, 보통 3개)
// name: Sentinel이 감시하는 master 그룹 이름
// password: Redis 데이터 노드 인증 비밀번호
// sentinelPassword: Sentinel 자체 인증 비밀번호
const redis = new Redis({
  sentinels: config.redis.sentinels,
  name: config.redis.name,
  password: config.redis.password,
  sentinelPassword: config.redis.sentinelPassword,
  // 연결이 끊기면 자동으로 재연결을 시도한다.
  // 시도할 때마다 대기 시간을 조금씩 늘려서(200ms, 400ms, 600ms...) 서버에 부담을 덜 준다.
  // 최대 3000ms(3초)까지만 늘어나고 그 이상은 늘리지 않는다.
  retryStrategy: (times) => Math.min(times * 200, 3000),
  // 요청 하나가 실패했을 때 최대 3번까지만 재시도한다 (무한 재시도 방지)
  maxRetriesPerRequest: 3,
});

// 연결 중 오류가 나면 콘솔에 로그만 남긴다.
// 여기서 서버를 죽이지 않는 이유: Redis가 잠깐 끊겨도 서버 전체가
// 다운되면 안 되기 때문이다. 실제로 이 값을 쓰는 라우트(queue.js 등)에서
// 오류가 나면 그때 그 요청만 실패 응답을 보내면 된다.
redis.on('error', (err) => {
  console.error('[redis] connection error:', err.message);
});

redis.on('connect', () => {
  console.log('[redis] connected to sentinel master:', config.redis.name);
});

// Sentinel이 Primary를 전환(failover)하면 이 이벤트가 발생한다.
redis.on('+switch-master', () => {
  console.log('[redis] sentinel failover detected, reconnecting to new master');
});

module.exports = redis;
