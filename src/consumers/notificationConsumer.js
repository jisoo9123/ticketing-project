const { createConsumer, publish } = require('../clients/kafkaClient');
const mockNotification = require('../mock/mockNotification');
const config = require('../config');

// payment.completed 이벤트를 받아 유저에게 알림을 보낸다(Mock).
// notification.requested 토픽으로도 기록을 남겨, 알림 발송 이력을 별도로 추적할 수 있게 한다.
async function startNotificationConsumer() {
  const consumer = createConsumer('notification');
  await consumer.connect();
  await consumer.subscribe({ topic: config.topics.paymentCompleted, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      try {
        await publish(config.topics.notificationRequested, event);

        const result = await mockNotification.send(event);

        console.log(`[notification-consumer] booking_id=${event.booking_id} 알림 발송 완료 (${result.notification_id})`);
      } catch (err) {
        console.error(`[notification-consumer] booking_id=${event.booking_id} 알림 발송 실패:`, err.message);
      }
    },
  });

  console.log('[notification-consumer] started, listening on', config.topics.paymentCompleted);
}

module.exports = { startNotificationConsumer };
