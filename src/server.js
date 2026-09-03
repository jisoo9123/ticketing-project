const express = require('express');
const config = require('./config');

// Kafka consumer 쪽에서 예상 못한 시점에 reject가 발생해도(예: kafkajs 내부 재연결 로직)
// API 서버(Queue/Auth/Seat/Booking)까지 함께 죽어서는 안 된다. 로그만 남기고 계속 응답한다.
process.on('unhandledRejection', (reason) => {
  console.error('[server] unhandled rejection (서버는 계속 동작):', reason?.message || reason);
});

const queueRoute = require('./routes/queue');
const authRoute = require('./routes/auth');
const seatRoute = require('./routes/seat');
const bookingRoute = require('./routes/booking');

const { startPaymentConsumer } = require('./consumers/paymentConsumer');
const { startNotificationConsumer } = require('./consumers/notificationConsumer');

const app = express();
app.use(express.json());

app.get('/healthz', (req, res) => res.status(200).json({ status: 'ok' }));

app.use('/queue', queueRoute);
app.use('/auth', authRoute);
app.use('/seats', seatRoute);
app.use('/bookings', bookingRoute);

app.use((err, req, res, next) => {
  console.error('[unhandled]', err);
  res.status(500).json({ error: 'internal_error' });
});

async function main() {
  app.listen(config.port, () => {
    console.log(`[server] app namespace listening on :${config.port}`);
  });

  if (process.env.SKIP_KAFKA_CONSUMERS === 'true') {
    console.log('[server] SKIP_KAFKA_CONSUMERS=true — Kafka consumer 기동 생략 (API 서버만 검증할 때 사용)');
    return;
  }

  // Kafka consumer는 서버와 별도 프로세스로 도는 것이 정석에 가깝지만,
  // MVP 단계에서는 같은 프로세스에서 함께 기동한다.
  // Kafka가 아직 준비 안 됐다고 해서 Queue/Auth/Seat/Booking API까지 죽으면 안 되므로,
  // consumer 기동 실패는 로그만 남기고 서버는 계속 응답하게 둔다.
  startPaymentConsumer().catch((err) => {
    console.error('[server] payment consumer 기동 실패 (API 서버는 계속 동작):', err.message);
  });
  startNotificationConsumer().catch((err) => {
    console.error('[server] notification consumer 기동 실패 (API 서버는 계속 동작):', err.message);
  });
}

main().catch((err) => {
  console.error('[server] fatal startup error:', err);
  process.exit(1);
});
