// 실제 결제사(PG사) 연동 자리에 세워둔 Mock.
// 외부 결제 API 호출을 흉내내며, 무작위 지연과 낮은 확률의 실패를 섞어
// Payment Consumer의 재시도/에러 처리 로직을 검증할 수 있게 한다.

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function charge({ booking_id, event_id, seat_id, user_id }) {
  await delay(150 + Math.floor(Math.random() * 400));

  const failureRate = parseFloat(process.env.MOCK_PAYMENT_FAILURE_RATE || '0');
  if (Math.random() < failureRate) {
    throw new Error('mock_payment_declined');
  }

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
