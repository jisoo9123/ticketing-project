// @author gyustar
// @date 2026-09-03

// 설정 로더
//
// 이 파일 하나가 프로젝트 전체에서 쓰는 환경변수(.env)를 읽어서
// 다른 파일들이 쓰기 편한 형태(숫자로 변환, 기본값 채움 등)로 정리해준다.
//
// 왜 이렇게 하나로 모아두나?
//   -> 다른 파일에서 매번 process.env.PGHOST 이런 식으로 직접 읽으면,
//      오타 나거나 값이 없을 때 대응(기본값)을 파일마다 따로 챙겨야 해서 실수가 잦아진다.
//      여기 한 군데서만 관리하면 "설정값이 어디서 오는지" 찾기도 쉽고 안전하다.
//
// .env 파일이 실제로 있어야 이 값들이 채워진다 — .env.example을 복사해서
// .env로 만들고, 실제 서버 주소/비밀번호로 채워 넣어야 한다.
require('dotenv').config();

module.exports = {
  // 이 Node.js 서버 자체가 몇 번 포트로 요청을 받을지 (기본 3000)
  port: parseInt(process.env.PORT || '3000', 10),

  // Redis 접속 주소. data namespace의 Redis(대기열/토큰/좌석 Hold 담당) 서버 위치.
  // 형식: redis://호스트:포트  (예: redis://10.1.93.110:6379)
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',

  // PostgreSQL(PPAS) 접속 정보 — 예약 확정 데이터가 최종적으로 저장되는 곳.
  // host/port/database/user/password를 하나로 묶어서 pgClient.js에 넘겨준다.
  pg: {
    host: process.env.PGHOST || '127.0.0.1',
    port: parseInt(process.env.PGPORT || '5432', 10),
    database: process.env.PGDATABASE || 'ticketing',
    user: process.env.PGUSER || 'ticketing_app',
    password: process.env.PGPASSWORD || '',
  },

  // Kafka 접속 정보. booking.js가 예약 확정 이벤트를 발행하고,
  // consumers/ 폴더의 두 워커가 이 이벤트를 받아서 처리한다.
  kafka: {
    // 콤마로 여러 브로커 주소를 나열할 수 있다 (예: "broker1:9092,broker2:9092")
    brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9092').split(','),
    // Kafka 쪽에서 "이 연결이 누구냐"를 구분하는 이름표 같은 것
    clientId: process.env.KAFKA_CLIENT_ID || 'app-namespace',
    // Consumer Group 이름 — 같은 그룹에 속한 Consumer끼리는 메시지를 나눠서 처리한다
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP || 'app-namespace-consumers',
  },

  // Kafka 토픽(메시지가 오가는 채널) 이름들.
  // 실제 토픽 이름 문자열을 코드 여기저기 직접 쓰지 않고 이렇게 한 곳에 모아두면,
  // 나중에 토픽 이름이 바뀌어도 여기 한 줄만 고치면 된다.
  topics: {
    bookingCreated: 'booking.created',           // 예약이 확정됐을 때 발행 (booking.js)
    paymentRequested: 'payment.requested',        // (현재 미사용, 향후 결제 요청 분리 시 사용 예정)
    paymentCompleted: 'payment.completed',        // 결제가 끝났을 때 발행 (paymentConsumer.js)
    notificationRequested: 'notification.requested', // 알림 발송을 기록할 때 발행 (notificationConsumer.js)
  },

  // Idempotency-Key로 캐싱해둔 응답을 Redis에 얼마나 오래 보관할지 (초 단위)
  // 기본 86400초 = 24시간. 이 시간이 지나면 같은 키로 재요청해도 "새 요청"으로 처리된다.
  idempotencyTtlSeconds: parseInt(process.env.IDEMPOTENCY_TTL_SECONDS || '86400', 10),

  // 좌석을 "임시로 잡아둔" 상태(Hold)가 몇 초 동안 유지되는지.
  // 이 시간 안에 예약을 확정하지 않으면 Hold가 자동으로 풀려서 다른 사람이 잡을 수 있게 된다.
  // 기본 300초 = 5분.
  seatHoldTtlSeconds: parseInt(process.env.SEAT_HOLD_TTL_SECONDS || '300', 10),
};