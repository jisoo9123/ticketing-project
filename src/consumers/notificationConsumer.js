// @author gyustar
// @date 2026-09-03

// Notification Consumer — payment.completed 이벤트를 구독하는 워커
//
// paymentConsumer.js가 결제를 끝내고 payment.completed 이벤트를 발행하면,
// 이 파일이 그걸 받아서 "예매 확정 + 결제 완료됐습니다" 알림을 유저에게 보낸다.
//
// 흐름 정리: booking.js(예약 확정) -> paymentConsumer.js(결제) -> 여기(알림)
// 이렇게 이벤트가 릴레이 경주처럼 이어지는 구조다. 각 단계는 서로의 존재를
// 직접 알 필요 없이, Kafka 토픽 이름만 맞으면 연결된다 — 이게 이벤트 기반
// 구조(비동기 이벤트 처리)의 장점이다.

const { createConsumer, publish } = require('../clients/kafkaClient');
const mockNotification = require('../mock/mockNotification');
const config = require('../config');

async function startNotificationConsumer() {
  const consumer = createConsumer('notification');
  await consumer.connect();
  await consumer.subscribe({ topic: config.topics.paymentCompleted, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());

      try {
        // notification.requested 토픽에도 같은 내용을 발행해둔다.
        // 왜 굳이 한 번 더 발행하나? -> "언제 어떤 알림 발송이 시도됐는지"를
        // 별도 로그성 토픽으로 남겨두면, 나중에 모니터링(Prometheus/Grafana)이나
        // 감사(audit) 목적으로 이 토픽만 따로 구독해서 추적하기 편해진다.
        await publish(config.topics.notificationRequested, event);

        // Mock 알림 시스템으로 실제(가짜) 알림을 보낸다 — mockNotification.js 참고.
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