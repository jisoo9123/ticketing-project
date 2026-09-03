// 실제 알림 발송(SMS/이메일/푸시) API 자리에 세워둔 Mock.
// 유저에게 나가는 "예매 확정" 알림을 시뮬레이션한다 — 시스템 내부 로그가 아니라
// 최종 수신자는 유저다.

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function send({ booking_id, event_id, user_id, payment_id }) {
  await delay(80 + Math.floor(Math.random() * 200));

  console.log(
    `[mock-notification] user_id=${user_id} 에게 예매 확정 알림 발송 ` +
    `(booking_id=${booking_id}, event_id=${event_id}, payment_id=${payment_id})`
  );

  return {
    notification_id: `noti_${booking_id}`,
    sent_to: user_id,
    channel: 'mock',
    sent_at: new Date().toISOString(),
  };
}

module.exports = { send };
