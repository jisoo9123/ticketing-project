// F02 — Auth (입장 허용 확인) 라우트
//
// queue.js에서 받은 토큰이 진짜인지, 아직 유효한지 확인하고
// "이 사람 입장시켜도 됩니다"라는 도장을 찍어주는 곳.
//
// 참고: 여기서 말하는 "인증(Auth)"은 아이디/비밀번호 로그인이 아니라,
// "대기열을 정상적으로 통과했는가"를 확인하는 절차다. 실제 로그인 시스템은
// 이 프로젝트 범위 밖이다 (README에도 명시되어 있다).

const express = require('express');
const redis = require('../clients/redisClient');

const router = express.Router();

// POST /auth/verify { event_id, token }
//
// 처리 순서:
//   1) queue.js가 만들어둔 토큰이 Redis에 아직 살아있는지 확인
//   2) 없다면(유효기간 10분 지났거나 애초에 없는 토큰) 401 에러로 거부
//   3) 있다면 "이 사용자는 입장 허용됨"이라는 표시를 Redis에 또 하나 남긴다
//      -> 이 표시는 다음 단계(seat.js의 좌석 Hold)에서 다시 확인한다
router.post('/verify', async (req, res) => {
  const { event_id, token } = req.body || {};

  if (!event_id || !token) {
    return res.status(400).json({ error: 'event_id and token are required' });
  }

  // queue.js에서 저장했던 것과 정확히 같은 키 형식으로 조회해야 값을 찾을 수 있다.
  const tokenKey = `queue:${event_id}:token:${token}`;
  const raw = await redis.get(tokenKey);

  if (!raw) {
    // 토큰이 없다는 건 — 유효기간이 지났거나, 애초에 존재하지 않는(가짜) 토큰이라는 뜻.
    // 401 Unauthorized: "너 누군지 확인이 안 된다"는 상태 코드.
    return res.status(401).json({ error: 'invalid_or_expired_token' });
  }

  // queue.js에서 저장했던 JSON 문자열을 다시 객체로 되돌린다.
  const data = JSON.parse(raw);

  // "이 사용자, 이 이벤트에 입장 허용됨"이라는 표시를 별도로 남긴다.
  // seat.js의 좌석 Hold 요청이 들어왔을 때, 이 표시가 있는 사람만 통과시킨다
  // (대기열 안 거치고 바로 좌석 Hold를 시도하는 걸 막기 위함).
  const admissionKey = `queue:${event_id}:admitted:${data.user_id}`;
  await redis.set(admissionKey, '1', 'EX', 600);

  return res.status(200).json({
    status: 'admitted',
    event_id,
    user_id: data.user_id,
    position: data.position,
  });
});

module.exports = router;
