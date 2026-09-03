const { createConsumer, publish } = require('../clients/kafkaClient');
const mockPayment = require('../mock/mockPayment');
const config = require('../config');

// booking.created 이벤트를 받아 Mock 결제 시스템에 결제를 요청하고,
// 성공하면 payment.completed 이벤트를 발행한다.
async function startPaymentConsumer() {
  const consumer = createConsumer('payment');
  await consumer.connect();
  await consumer.subscribe({ topic: config.topics.bookingCreated, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      try {
        const payment = await mockPayment.charge(event);

        await publish(config.topics.paymentCompleted, {
          ...event,
          payment_id: payment.payment_id,
          payment_status: payment.status,
          approved_at: payment.approved_at,
        });

        console.log(`[payment-consumer] booking_id=${event.booking_id} 결제 완료`);
      } catch (err) {
        // Mock 결제 실패 — 재시도는 Kafka consumer group의 offset 정책에 맡기거나,
        // 별도 재시도 토픽(payment.retry)으로 분리하는 것을 다음 단계로 고려
        console.error(`[payment-consumer] booking_id=${event.booking_id} 결제 실패:`, err.message);
      }
    },
  });

  console.log('[payment-consumer] started, listening on', config.topics.bookingCreated);
}

module.exports = { startPaymentConsumer };
