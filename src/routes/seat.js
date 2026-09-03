// @author gyustar
// @date 2026-09-03

// F03 — Seat (좌석 조회 및 임시 Hold) 라우트
//
// 두 가지 일을 한다:
//   1) 좌석 목록 보여주기 (그냥 조회) — GET /seats/:eventId
//   2) 특정 좌석을 "잠깐 나만 쓸 수 있게" 임시로 잡아두기 — POST /seats/:seatId/hold
//
// "좌석 목록의 진짜 상태"는 PostgreSQL(예약 확정 데이터가 있는 곳)에 있지만,
// "지금 누가 이 좌석을 보고 있는 중이다"라는 임시 상태는 Redis에 둔다.
// 이렇게 나누는 이유: Redis의 SET NX(아래 설명)는 "동시에 여러 명이 같은
// 좌석을 누르면 딱 한 명만 성공시키는" 기능을 아주 간단하고 빠르게 해준다.

const express = require('express');
const pool = require('../clients/pgClient');
const redis = require('../clients/redisClient');
const idempotency = require('../middleware/idempotency');
const config = require('../config');

const router = express.Router();

// GET /seats/:eventId — 특정 이벤트의 좌석 목록을 조회한다.
// 이건 단순 조회라서 Idempotency-Key가 필요 없다 (같은 요청 여러 번 해도 문제 될 게 없음).
router.get('/:eventId', async (req, res) => {
  const { eventId } = req.params;

  // seats 테이블에서 이 이벤트에 속한 좌석들을 좌석 번호 순으로 가져온다.
  // $1은 SQL 인젝션(악의적인 값 주입) 공격을 막기 위한 안전한 파라미터 방식이다 —
  // 절대로 문자열을 그냥 이어붙여서 쿼리를 만들면 안 된다.
  const { rows } = await pool.query(
    `SELECT id, seat_no, status
       FROM seats
      WHERE event_id = $1
      ORDER BY seat_no`,
    [eventId]
  );

  return res.status(200).json({ event_id: eventId, seats: rows });
});

// POST /seats/:seatId/hold { event_id, user_id }
//
// 처리 순서:
//   1) 이 사용자가 auth.js에서 "입장 허용" 도장을 받았는지 확인 (안 받았으면 거부)
//   2) Redis의 SET ... NX(Not eXists — 키가 없을 때만 성공하는 옵션)로 좌석을 잡는다
//      -> 두 사람이 동시에 같은 좌석에 이 요청을 보내도, Redis는 한 명에게만
//         "성공"을 돌려주고 나머지는 "이미 있음(실패)"으로 처리한다.
//         이게 바로 "동시성 제어"의 실제 구현이다.
//   3) 성공하면 이 좌석은 SEAT_HOLD_TTL_SECONDS(기본 5분) 동안만 이 사용자 것으로 묶인다.
router.post('/:seatId/hold', idempotency(), async (req, res) => {
  const { seatId } = req.params;
  const { event_id, user_id } = req.body || {};

  if (!event_id || !user_id) {
    return res.status(400).json({ error: 'event_id and user_id are required' });
  }

  // auth.js에서 남겨둔 "입장 허용" 표시가 있는지 확인한다.
  // 이게 없으면 대기열/인증 절차를 건너뛰고 바로 좌석을 잡으려는 시도이므로 막는다.
  const admissionKey = `queue:${event_id}:admitted:${user_id}`;
  const admitted = await redis.get(admissionKey);
  if (!admitted) {
    // 403 Forbidden: "요청 자체는 이해했지만, 권한이 없어서 거부한다"는 뜻.
    return res.status(403).json({ error: 'not_admitted' });
  }

  const holdKey = `hold:${event_id}:${seatId}`;
  // Redis SET에 NX 옵션을 주면: "이 키가 이미 존재하면 아무것도 안 하고 실패를 반환,
  // 존재하지 않으면 값을 설정하고 성공을 반환"한다.
  // 이 원자적인(atomic) 동작 덕분에, 수십 명이 동시에 같은 좌석을 눌러도
  // 딱 한 명만 acquired 값을 받게 된다 — 이게 이 라우트의 핵심 로직이다.
  const acquired = await redis.set(
    holdKey,
    user_id,
    'EX',
    config.seatHoldTtlSeconds,
    'NX'
  );

  if (!acquired) {
    // 이미 다른 사람이 이 좌석을 Hold 중이라는 뜻.
    // 409 Conflict: "네 요청은 정상이지만, 지금 서버 상태랑 충돌한다"는 의미.
    return res.status(409).json({ error: 'seat_already_held' });
  }

  return res.status(200).json({
    status: 'held',
    event_id,
    seat_id: seatId,
    user_id,
    hold_ttl_seconds: config.seatHoldTtlSeconds,
  });
});

module.exports = router;