// @author gyustar
// @date 2026-09-03

// Mock 알림 시스템
//
// 이것도 mockPayment.js와 같은 성격의 "가짜"다. 실제 서비스라면 여기서
// SMS나 이메일, 앱 푸시 알림을 실제로 발송하는 외부 API를 호출해야 한다.
//
// 중요한 포인트: 이 알림은 "시스템 내부 로그"가 아니라 "최종적으로 유저에게
// 가는 알림"이다 — 예매가 확정됐다는 걸 사용자한테 실제로 알려주는 역할.
// 지금은 Mock이라서 진짜 문자/이메일은 안 나가고, 대신 서버 콘솔에
// "이런 알림이 나갔을 것이다"라고 로그만 남긴다.

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// notificationConsumer.js가 payment.completed 이벤트를 받으면 이 함수를 호출한다.
async function send({ booking_id, event_id, user_id, payment_id }) {
  // 80~280ms 랜덤 대기 — 실제 알림 발송 API 호출 시간을 흉내낸다.
  await delay(80 + Math.floor(Math.random() * 200));

  // 실제로는 여기서 SMS API나 이메일 발송 API를 호출하겠지만,
  // 지금은 콘솔에 로그만 남긴다. 이 로그가 서버 실행 중에 뜨면
  // "이 시점에 이 사용자에게 알림이 나갔다"는 걸 확인할 수 있다.
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