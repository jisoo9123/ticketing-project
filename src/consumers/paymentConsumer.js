// @author gyustar
// @date 2026-09-03

// Payment Consumer — booking.created 이벤트를 구독하는 워커
//
// [2026-09-03 재작성] 이벤트 중복 처리 방지를 Redis가 아니라 실제 스키마의
// processed_event 테이블로 하도록 바꿨다.
//
// 왜 이게 필요한가:
//   Kafka는 "적어도 한 번은 전달한다(at-least-once)"는 걸 보장하지,
//   "정확히 한 번만 전달한다"는 걸 보장하지 않는다. 즉 같은 메시지가
//   드물게 두 번 전달될 수 있다 (네트워크 재시도, 컨슈머 재시작 등).
//   이때 결제를 두 번 처리하면 안 되므로, "이 이벤트를 이미 처리했는가"를
//   먼저 확인하는 절차가 필요하다 — processed_event 테이블이 그 역할.
//
// processed_event.event_id는 PK(UNIQUE)이므로, 같은 이벤트로 INSERT를
// 두 번 시도하면 두 번째는 에러가 난다. 이걸 "이미 처리한 이벤트니
// 건너뛰라"는 신호로 활용한다.

const { createConsumer, publish } = require('../clients/kafkaClient');
const pool = require('../clients/pgClient');
const mockPayment = require('../mock/mockPayment');
const config = require('../config');

// TODO(gyustar): payment.amount가 NOT NULL인데 스키마 어디에도 좌석/이벤트
// 가격(price) 컬럼이 없다. 실제 가격 정책이 정해질 때까지 임시 고정값을 쓴다.
// 팀 회의에서 확인해야 하는 스키마 갭이다.
const TEMP_FIXED_AMOUNT = 10000;

async function startPaymentConsumer() {
  const consumer = createConsumer('payment');
  await consumer.connect();
  await consumer.subscribe({ topic: config.topics.bookingCreated, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      // processed_event.event_id는 문자열(varchar)이라, "토픽이름:booking_id" 형태로
      // 이 메시지만의 고유 키를 만든다. booking_id 하나로도 충분히 유일하지만,
      // 나중에 다른 토픽과 섞여도 안 헷갈리게 토픽명을 접두어로 붙여둔다.
      const processedEventId = `${config.topics.bookingCreated}:${event.booking_id}`;

      // === 먼저 "이미 처리한 적 있는지" DB에 기록을 시도한다 ===
      // INSERT가 성공하면 = 처음 보는 이벤트 -> 아래에서 실제 결제 처리 진행
      // INSERT가 실패하면(중복키 에러) = 이미 처리한 이벤트 -> 조용히 건너뛴다
      try {
        await pool.query(
          `INSERT INTO processed_event (event_id, event_type) VALUES ($1, $2)`,
          [processedEventId, config.topics.bookingCreated]
        );
      } catch (err) {
        if (err.code === '23505') {
          // unique_violation — 이미 처리된 이벤트. 정상적인 상황이니 에러로 취급하지 않는다.
          console.log(`[payment-consumer] booking_id=${event.booking_id} 이미 처리된 이벤트, 건너뜀`);
          return;
        }
        throw err;
      }

      try {
        const payment = await mockPayment.charge(event);

        // payment 테이블에 결제 결과를 기록한다 (booking_id는 UNIQUE라서 예약당 결제 1건).
        await pool.query(
          `INSERT INTO payment (booking_id, status, amount, processed_at)
           VALUES ($1, $2, $3, now())`,
          [event.booking_id, 'APPROVED', TEMP_FIXED_AMOUNT]
        );

        await publish(config.topics.paymentCompleted, {
          ...event,
          payment_id: payment.payment_id,
          payment_status: payment.status,
          approved_at: payment.approved_at,
        });

        console.log(`[payment-consumer] booking_id=${event.booking_id} 결제 완료`);
      } catch (err) {
        console.error(`[payment-consumer] booking_id=${event.booking_id} 결제 실패:`, err.message);
      }
    },
  });

  console.log('[payment-consumer] started, listening on', config.topics.bookingCreated);
}

module.exports = { startPaymentConsumer };
