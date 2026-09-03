# app namespace — Node.js 구현체

Kubernetes `app namespace`에서 도는 FastAPI 자리를 Node.js(Express)로 구현한 것.
Redis(Sentinel) · PostgreSQL · Kafka와 코드로 직접 연동한다.

## 구성

```
src/
├── server.js              엔트리 포인트 — Express 서버 + Kafka Consumer 기동
├── config.js               환경변수 로더
├── clients/
│   ├── redisClient.js      Redis 연결
│   ├── pgClient.js         PostgreSQL 연결 풀
│   └── kafkaClient.js      Kafka producer/consumer 팩토리
├── middleware/
│   └── idempotency.js      Idempotency-Key 기반 중복 처리 방지
├── routes/
│   ├── queue.js             POST /queue/join      — 대기열 등록
│   ├── auth.js               POST /auth/verify      — 입장 허용 확인
│   ├── seat.js                GET  /seats/:eventId   — 좌석 조회
│   │                          POST /seats/:seatId/hold — 좌석 임시 Hold
│   └── booking.js            POST /bookings          — 예약 확정 (핵심 동시성 제어)
├── consumers/
│   ├── paymentConsumer.js    booking.created 구독 → Mock 결제 → payment.completed 발행
│   └── notificationConsumer.js payment.completed 구독 → Mock 알림 발송
├── mock/
│   ├── mockPayment.js
│   └── mockNotification.js
└── db/
    └── schema.sql           seats / bookings 테이블
```

## 로컬 실행

```bash
cp .env.example .env      # 값 채우기
npm install
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f src/db/schema.sql
npm run dev
```

## 요청 흐름 (E2E)

```bash
# 1. 대기열 등록 — 대기번호 + 토큰 발급
curl -X POST localhost:3000/queue/join \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: q-001' \
  -d '{"event_id":"evt1","user_id":"u1"}'

# 2. 입장 허용 확인
curl -X POST localhost:3000/auth/verify \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt1","token":"<위에서 받은 token>"}'

# 3. 좌석 임시 Hold
curl -X POST localhost:3000/seats/seat-1/hold \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: h-001' \
  -d '{"event_id":"evt1","user_id":"u1"}'

# 4. 예약 확정 — 성공 시 booking.created 이벤트 발행 → 결제/알림 Consumer가 뒤이어 처리
curl -X POST localhost:3000/bookings \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: b-001' \
  -d '{"event_id":"evt1","seat_id":"seat-1","user_id":"u1"}'
```

## 핵심 설계 포인트

- **Idempotency**: 모든 쓰기성 엔드포인트(`queue/join`, `seats/:id/hold`, `bookings`)는
  `Idempotency-Key` 헤더를 필수로 받는다. Worker 장애로 클라이언트가 같은 요청을
  재시도해도 새로 처리하지 않고 캐시된 응답을 그대로 돌려준다.
- **좌석 중복 방지**: `bookings` 라우트의 PostgreSQL `UPDATE ... WHERE status='available'`이
  동시에 들어온 두 요청 중 한쪽만 성공시키는 지점. 이게 이 프로젝트에서 제일 중요한 코드.
- **Redis vs PostgreSQL 역할 분리**: Redis는 휘발성이 괜찮은 임시 데이터(대기 순번,
  토큰, 좌석 임시 Hold)만 담당. 실제로 "이 좌석은 이 사람 것"이라는 최종 사실은
  PostgreSQL의 `bookings` 테이블만 갖고 있음.
- **Kafka는 예약 확정 이후의 부가 처리**: 결제·알림은 예약 성공에 필수 경로가
  아니라 비동기 후속 처리로 분리 — 결제 Mock이 느려도 예약 자체는 이미 끝난 상태.

## 다음 단계

- Kafka consumer 실패 시 재시도/DLQ 정책 추가
- `.env`의 `REDIS_URL`을 Sentinel 구성으로 교체 (`ioredis` sentinel 옵션)
- k6 부하 테스트 스크립트 연결, MetalLB 장애 주입 시나리오 검증
- 인증(Auth)은 현재 토큰 존재 여부만 확인 — 실제 로그인 연동은 범위 밖
