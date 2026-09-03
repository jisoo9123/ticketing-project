// 서버 엔트리 포인트
//
// 이 파일이 "main"이다 — 프로그램을 시작하면 제일 먼저 실행되는 파일.
// 하지만 이 파일 자체는 Redis/PostgreSQL/Kafka를 직접 다루지 않는다.
// 하는 일은 딱 두 가지뿐이다:
//   1) Express 앱을 만들고, 어떤 주소로 요청이 오면 어느 라우트 파일로
//      보낼지 "교통정리"만 한다 (실제 로직은 각 라우트 파일 안에 있다)
//   2) Kafka Consumer 두 개(결제/알림 워커)를 같이 띄운다

const express = require('express');
const config = require('./config');

// === 안전장치: 예상 못한 에러로 서버 전체가 죽는 것을 막는다 ===
// Kafka 연결이 실패하는 것처럼, 코드 어딘가에서 .catch()로 못 잡은 에러(rejection)가
// 발생하면 Node.js는 기본적으로 프로세스 전체를 종료시켜버린다.
// Queue/Auth/Seat/Booking API는 Kafka가 잠깐 안 되더라도 계속 응답해야 하므로,
// 여기서 그런 에러를 붙잡아 "로그만 남기고 서버는 계속 돌린다"로 바꿔준다.
process.on('unhandledRejection', (reason) => {
  console.error('[server] unhandled rejection (서버는 계속 동작):', reason?.message || reason);
});

// 각 기능별 라우트 파일을 불러온다. 이 시점에서는 그냥 "불러오기"만 할 뿐,
// 아직 아무 요청도 처리하지 않는다 — 실제 연결(app.use)은 아래에서 한다.
const queueRoute = require('./routes/queue');
const authRoute = require('./routes/auth');
const seatRoute = require('./routes/seat');
const bookingRoute = require('./routes/booking');

// Kafka Consumer(백그라운드에서 계속 도는 결제/알림 처리 워커) 시작 함수들을 불러온다.
const { startPaymentConsumer } = require('./consumers/paymentConsumer');
const { startNotificationConsumer } = require('./consumers/notificationConsumer');

// Express 앱 객체 생성 — 이게 우리 서버의 몸통이다.
const app = express();
// 요청 본문(body)이 JSON 형식으로 오면 자동으로 파싱해서 req.body에 넣어주는 설정.
// 이게 없으면 각 라우트에서 req.body가 그냥 undefined로 나온다.
app.use(express.json());

// 서버가 살아있는지 확인하는 용도의 아주 단순한 엔드포인트.
// K8s의 헬스체크(liveness/readiness probe)나, 배포 후 "서버 잘 떴나?" 확인할 때 쓴다.
app.get('/healthz', (req, res) => res.status(200).json({ status: 'ok' }));

// === 여기가 "교통정리" 하는 부분 ===
// 요청 주소가 /queue로 시작하면 queueRoute(queue.js)로,
// /auth로 시작하면 authRoute(auth.js)로... 이런 식으로 흘려보낸다.
// 예: POST /queue/join 요청이 오면 -> queue.js 안의 "/join" 핸들러가 처리한다.
app.use('/queue', queueRoute);
app.use('/auth', authRoute);
app.use('/seats', seatRoute);
app.use('/bookings', bookingRoute);

// Express의 "에러 처리 미들웨어" — 함수 인자가 4개(err, req, res, next)인 게 특징이다.
// 어느 라우트에서든 예상 못한 에러가 나서 여기까지 넘어오면, 사용자에게는
// 자세한 에러 내용 대신 뭉뚱그린 500 에러만 보여준다 (보안상 내부 에러 상세를
// 그대로 노출하지 않기 위함). 실제 에러 내용은 서버 콘솔에만 남는다.
app.use((err, req, res, next) => {
  console.error('[unhandled]', err);
  res.status(500).json({ error: 'internal_error' });
});

async function main() {
  // 실제로 config.port(기본 3000) 포트에서 요청을 받기 시작한다.
  app.listen(config.port, () => {
    console.log(`[server] app namespace listening on :${config.port}`);
  });

  // 로컬에서 Kafka 없이 API 서버(Queue/Auth/Seat/Booking)만 빠르게 테스트하고
  // 싶을 때 쓰는 탈출구. SKIP_KAFKA_CONSUMERS=true로 실행하면 아래 Consumer
  // 기동 자체를 건너뛴다. (booking.js의 Kafka 발행은 어차피 try/catch로
  // 감싸져 있으므로, 이 모드에서도 예약 확정 자체는 정상 동작한다.)
  if (process.env.SKIP_KAFKA_CONSUMERS === 'true') {
    console.log('[server] SKIP_KAFKA_CONSUMERS=true — Kafka consumer 기동 생략 (API 서버만 검증할 때 사용)');
    return;
  }

  // Kafka consumer는 원래 서버와 별도의 프로세스로 띄우는 게 정석에 더 가깝지만
  // (예: 결제 워커만 따로 스케일하고 싶을 때), MVP 단계에서는 관리 편의를 위해
  // 같은 프로세스 안에서 함께 띄운다.
  //
  // .catch()로 감싸는 이유: Kafka가 아직 준비 안 된 상태(예: 인프라팀이 아직
  // Kafka를 안 띄웠거나 네트워크 문제)에서도, Queue/Auth/Seat/Booking API
  // 자체는 죽지 않고 계속 응답해야 하기 때문이다. 실제로 개발 중 이 부분을
  // await로 그냥 기다리게 해뒀다가 서버 전체가 죽는 문제를 겪고 이렇게 고쳤다.
  startPaymentConsumer().catch((err) => {
    console.error('[server] payment consumer 기동 실패 (API 서버는 계속 동작):', err.message);
  });
  startNotificationConsumer().catch((err) => {
    console.error('[server] notification consumer 기동 실패 (API 서버는 계속 동작):', err.message);
  });
}

// main() 함수를 실행하고, 혹시 여기서까지 못 잡은 진짜 치명적인 에러가 있으면
// (예: 포트가 이미 사용 중이라 애초에 서버를 못 띄우는 경우) 그때는 로그를
// 남기고 프로세스를 종료한다 — 이런 경우는 살려둬 봐야 정상 동작이 불가능하므로.
main().catch((err) => {
  console.error('[server] fatal startup error:', err);
  process.exit(1);
});
