// @author gyustar
// @date 2026-09-03

// Payment Consumer — booking.created 이벤트를 구독(subscribe)하는 워커
//
// "구독"이란 "이 채널(토픽)에 새 메시지가 올라오면 나한테 알려줘"라고
// Kafka에 미리 등록해두는 것이다. booking.js가 booking.created 토픽에
// 메시지를 발행(publish)하면, 이 파일이 그걸 자동으로 받아서 처리한다.
//
// 이 파일이 하는 일 (한 문장으로): "예약 확정됐다"는 소식을 받으면
// Mock 결제 시스템에 결제를 요청하고, 결제가 끝나면 "결제 완료됐다"는
// 새 소식(payment.completed)을 또 발행한다.
//
// 이렇게 예약 확정(booking.js)과 결제 처리(여기)를 분리해둔 이유:
// 결제 처리가 느리거나 실패해도, 이미 확정된 예약 자체는 영향받지 않게
// 하기 위해서다. booking.js를 보면 실제로 Kafka 발행 부분이 try/catch로
// 감싸져 있어서, 이 워커가 아예 안 떠 있어도 예약 확정 API는 정상 동작한다.

const { createConsumer, publish } = require('../clients/kafkaClient');
const mockPayment = require('../mock/mockPayment');
const config = require('../config');

async function startPaymentConsumer() {
  // Kafka에 접속할 Consumer 하나를 만든다. 'payment'는 이 Consumer의 그룹 이름 뒷부분.
  const consumer = createConsumer('payment');
  await consumer.connect();
  // "booking.created 토픽을 구독하겠다"고 등록한다.
  // fromBeginning: false 는 "내가 연결된 시점 이후의 새 메시지만 받겠다"는 뜻
  // (과거에 쌓인 메시지까지 전부 다시 처리하지 않도록).
  await consumer.subscribe({ topic: config.topics.bookingCreated, fromBeginning: false });

  // run()을 호출하면 이 함수는 Kafka로부터 메시지가 올 때마다 eachMessage를 실행하며
  // 계속 대기 상태로 돈다 (서버가 꺼질 때까지 실행되는 백그라운드 작업이라고 보면 된다).
  await consumer.run({
    eachMessage: async ({ message }) => {
      // Kafka 메시지는 원래 이진 데이터(Buffer)라서, 문자열로 바꾼 뒤 JSON으로 해석한다.
      // 이 event 객체 안에 booking.js가 발행했던 booking_id, seat_id 등이 들어있다.
      const event = JSON.parse(message.value.toString());

      try {
        // Mock 결제 시스템에 결제를 요청한다 (진짜 결제사 대신 세워둔 가짜, mockPayment.js 참고).
        const payment = await mockPayment.charge(event);

        // 결제가 성공했으면, 원래 이벤트 정보에 결제 결과를 덧붙여서
        // "payment.completed"라는 새 토픽으로 다시 발행한다.
        // 이걸 notificationConsumer.js가 받아서 유저에게 알림을 보낸다.
        await publish(config.topics.paymentCompleted, {
          ...event, // booking_id, event_id, seat_id, user_id 등 원래 정보를 그대로 유지
          payment_id: payment.payment_id,
          payment_status: payment.status,
          approved_at: payment.approved_at,
        });

        console.log(`[payment-consumer] booking_id=${event.booking_id} 결제 완료`);
      } catch (err) {
        // Mock 결제가 실패한 경우(mockPayment.js에서 일부러 실패시킬 수 있게 해둔 부분).
        // 지금은 로그만 남기고 끝내지만, 나중에 실제 서비스로 갈 때는
        // "재시도 큐로 다시 보내기"나 "실패 전용 토픽(payment.retry)으로 분리하기" 같은
        // 보완이 필요하다 — 지금은 여기까지가 MVP 범위.
        console.error(`[payment-consumer] booking_id=${event.booking_id} 결제 실패:`, err.message);
      }
    },
  });

  console.log('[payment-consumer] started, listening on', config.topics.bookingCreated);
}

module.exports = { startPaymentConsumer };