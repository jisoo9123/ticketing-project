// @author gyustar
// @date 2026-09-03

// Kafka 클라이언트 (producer + consumer 팩토리)
//
// Kafka는 "이벤트(사건)를 기록하고 전달하는 우체통" 같은 역할이다.
// booking.js가 "예약 확정됐어요"라는 편지(이벤트)를 우체통(Kafka)에 넣으면,
// consumers/ 폴더의 워커들이 그 편지를 꺼내서 각자 할 일(결제 처리, 알림 발송)을 한다.
//
// 왜 Redis나 직접 함수 호출 대신 Kafka를 쓰나?
//   결제·알림은 예약 확정에 "꼭 필요한" 절차가 아니라 "그 다음에 일어나는" 절차다.
//   Kafka로 분리해두면, 결제 시스템이 잠깐 느려지거나 죽어도 예약 확정 자체(가장
//   중요한 부분)는 영향을 안 받는다. booking.js를 보면 실제로 Kafka 발행이
//   실패해도 예약 확정 응답 자체는 정상적으로 나가도록 try/catch로 감싸져 있다.

const { Kafka, logLevel } = require('kafkajs');
const config = require('../config');

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  // kafkajs 라이브러리 자체의 상세 로그는 끈다 (너무 시끄러워서)
  logLevel: logLevel.NOTHING,
  // Kafka 브로커에 연결을 시도할 때 3초 안에 응답이 없으면 포기한다.
  // 이 값이 없으면 기본값이 더 길어서, Kafka가 아직 준비 안 됐을 때
  // API 요청 하나하나가 오래 걸리게 된다 (개발 중 실제로 이 문제를 겪고 추가한 설정).
  connectionTimeout: 3000,
  // 연결 실패 시 2번까지만 재시도. 너무 많이 재시도하면 그만큼 응답이 늦어진다.
  retry: { retries: 2 },
});

const producer = kafka.producer();
// producer.connect()는 한 번만 하면 되므로, 이미 연결했는지 여부를 기억해둔다.
let producerConnected = false;

// Kafka에 메시지를 보내기 전에, 아직 연결 안 했으면 연결부터 한다.
// (게으른 연결 방식 — 서버가 시작될 때가 아니라, 실제로 메시지를 처음 보낼 때 연결한다)
async function getProducer() {
  if (!producerConnected) {
    await producer.connect();
    producerConnected = true;
    console.log('[kafka] producer connected:', config.kafka.brokers.join(','));
  }
  return producer;
}

// 특정 토픽(채널)에 이벤트(payload)를 발행(publish)하는 함수.
// booking.js, paymentConsumer.js, notificationConsumer.js가 이 함수를 가져다 쓴다.
async function publish(topic, payload) {
  const p = await getProducer();
  await p.send({
    topic,
    messages: [{
      // key를 지정하면 Kafka가 같은 key를 가진 메시지를 항상 같은 파티션으로
      // 보내준다 — 같은 예약(booking_id)에 대한 이벤트 순서가 뒤섞이지 않게 하기 위함.
      key: payload.booking_id || payload.ticket_id || undefined,
      value: JSON.stringify(payload),
    }],
  });
}

// Consumer(메시지를 받아서 처리하는 쪽)를 만드는 함수.
// groupIdSuffix로 각 Consumer마다 다른 그룹 이름을 갖게 해서,
// paymentConsumer와 notificationConsumer가 서로 다른 메시지를 각자 처리하게 한다.
function createConsumer(groupIdSuffix) {
  return kafka.consumer({ groupId: `${config.kafka.consumerGroup}-${groupIdSuffix}` });
}

module.exports = { kafka, getProducer, publish, createConsumer };