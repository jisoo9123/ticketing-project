// Mock 결제 시스템
//
// "Mock"이란 실제 시스템 대신 세워둔 가짜라는 뜻이다.
// 진짜 결제사(카카오페이, 토스 같은 PG사) API를 연결하려면 계약과 심사가
// 필요하고 이 프로젝트 범위를 벗어나므로, 그 자리에 "그럴듯하게 흉내내는"
// 코드를 대신 세워뒀다.
//
// 이 파일이 흉내내는 것:
//   - 실제 결제는 인터넷을 거쳐 외부 서버와 통신하므로 시간이 걸린다 -> delay()로 흉내
//   - 실제 결제는 가끔 실패한다(카드 한도 초과 등) -> 낮은 확률로 실패하도록 흉내
// 이렇게 해두면, paymentConsumer.js가 "결제 성공/실패 둘 다"에 대응하는
// 코드를 진짜 결제사 없이도 미리 테스트해볼 수 있다.

// 몇 밀리초(ms) 동안 그냥 기다리는 함수. "네트워크 통신에 시간이 걸린다"를 흉내낸다.
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 실제 결제를 요청하는 함수. paymentConsumer.js가 booking.created 이벤트를
// 받으면 이 함수를 호출한다.
async function charge({ booking_id, event_id, seat_id, user_id }) {
  // 150~550ms 사이 랜덤하게 대기 — 실제 결제 API 호출이 즉시 끝나지 않는 걸 흉내낸다.
  await delay(150 + Math.floor(Math.random() * 400));

  // 환경변수로 "일부러 실패시키는 비율"을 조절할 수 있다.
  // 기본값 0(=절대 실패 안 함)이지만, 테스트 중에 "결제 실패 시 어떻게 되는지"
  // 확인하고 싶으면 MOCK_PAYMENT_FAILURE_RATE=0.3 같은 식으로 30% 확률로 실패하게 만들 수 있다.
  const failureRate = parseFloat(process.env.MOCK_PAYMENT_FAILURE_RATE || '0');
  if (Math.random() < failureRate) {
    throw new Error('mock_payment_declined');
  }

  // 진짜 결제사라면 결제 승인 번호 같은 걸 돌려줄 텐데, 여기서는 그걸 흉내낸 값을 만든다.
  return {
    payment_id: `pay_${booking_id}`,
    booking_id,
    event_id,
    seat_id,
    user_id,
    status: 'approved',
    approved_at: new Date().toISOString(),
  };
}

module.exports = { charge };
