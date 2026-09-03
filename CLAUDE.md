# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

Kubernetes `app namespace`에서 도는 티켓팅 백엔드의 Node.js(Express) 구현체.
`data namespace`의 Redis · PostgreSQL(PPAS) · Kafka와 직접 연동한다.
코드 주석과 커밋 메시지는 한국어로 작성되어 있으니 같은 톤을 유지할 것.

## 스키마 관련 주의사항

- 실제 운영 DB 스키마는 송지원님이 구축한 **7개 테이블** 기준:
  `app_user` / `ticket_event` / `seat` / `booking` / `booking_seat` / `payment` / `processed_event`.
- 우리가 임의로 만들었던 `seats` / `bookings` 2테이블 스키마는 **폐기**됨.
- `src/db/schema-reference.sql`이 실제 스키마 참조 파일 — **절대 운영 DB에 재실행 금지**.

## 명령어

```bash
npm install
npm run dev          # node --watch, 파일 변경 시 자동 재시작
npm start            # 프로덕션 기동

# 로컬 의존 인프라 (Redis/PostgreSQL/Kafka) 기동 — postgres 컨테이너가
# src/db/schema.sql(구 2테이블 스키마)을 initdb 스크립트로 자동 실행한다 —
# 운영 스키마와 다르다, "스키마 관련 주의사항" 참고
docker compose -f docker-compose.local.yml up -d

# 기존 DB에 스키마만 적용할 때
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f src/db/schema.sql

# Kafka 없이 API만 검증
SKIP_KAFKA_CONSUMERS=true npm run dev

# 결제 실패 경로 검증 (Mock 결제를 30% 확률로 실패시킴)
MOCK_PAYMENT_FAILURE_RATE=0.3 npm run dev
```

## 테스트

테스트 러너(jest/mocha)가 없다. 각 테스트는 `node`로 직접 실행하는 독립 스크립트이며,
성공 시 exit 0 / 실패 시 exit 1로 판정한다. `npm test` 스크립트는 존재하지 않는다.

```bash
# PostgreSQL만 필요 — booking.js의 조건부 UPDATE SQL 자체를 검증 (서버 불필요)
PGPASSWORD=localtest CONCURRENT_REQUESTS=20 node test/concurrency-core.test.js

# 실행 중인 서버를 대상으로 전체 요청 경로(Idempotency → Redis Hold → PG 확정) 검증
BASE_URL=http://localhost:3000 PGPASSWORD=localtest node test/concurrency-booking.test.js
```

두 테스트 모두 "동시 요청 N건 중 정확히 1건만 확정"을 PASS 조건으로 삼는다.
`concurrency-core.test.js`는 실제 운영 스키마 기준으로 검증 완료(아래 "검증 완료" 참고).
`concurrency-booking.test.js`는 아직 구 스키마 기준이라 그대로는 통과하지 않는다.

## 아키텍처

### 요청 흐름
`POST /queue/join` (순번+토큰 발급) → `POST /auth/verify` (토큰 검증 후 admitted 마킹)
→ `POST /seats/:seatId/hold` (Redis SET NX로 임시 선점) → `POST /bookings` (PG 트랜잭션으로 확정)
→ Kafka `booking.created` → paymentConsumer → `payment.completed` → notificationConsumer.

각 단계는 앞 단계가 Redis에 남긴 키의 존재를 확인해서 우회를 막는다:
`queue:{event}:token:{token}` → `queue:{event}:admitted:{user}` → `hold:{event}:{seat}`.

### 저장소 역할 분리 (이 프로젝트의 핵심 원칙)
- **Redis** = 휘발성 임시 상태만: 대기 순번, 입장 토큰(10분), admitted 마킹(10분),
  좌석 Hold(`SEAT_HOLD_TTL_SECONDS`, 기본 5분), Idempotency 응답 캐시.
- **PostgreSQL** = "이 좌석은 이 사람 것"이라는 최종 사실. 예약 테이블만이 진실이다
  (운영 스키마에서는 `booking` / `booking_seat`).

### 동시성 제어가 걸리는 두 지점
1. `seat.js`: Redis `SET ... NX` — 동시 Hold 요청 중 한 명만 성공.
2. `booking.js`: 좌석 행을 `status='available'` 조건으로 갱신하는 조건부 UPDATE —
   `rowCount === 0`이면 이미 다른 사람이 선점한 것. **이 프로젝트에서 가장 중요한 코드.**
   DB 레벨 최종 방어선은 예약 테이블의 UNIQUE 제약(운영 스키마에서는
   `booking_idempotency_key_key`). 테이블/컬럼명은 운영 스키마 기준으로 확인할 것.

이 SQL을 수정할 때는 `test/concurrency-core.test.js`에 하드코딩된 동일 SQL도 함께 고쳐야 한다
(테스트가 booking.js의 트랜잭션 블록을 그대로 복제해 두었기 때문).

### 장애 격리 설계 (수정 시 깨뜨리지 말 것)
- `booking.js`의 Kafka publish는 `try/catch`로 감싸져 있다 — 커밋은 이미 끝난 뒤이므로
  발행 실패가 예약 확정 응답을 실패로 만들면 안 된다.
- `server.js`는 consumer 기동을 `await`하지 않고 `.catch()`로만 처리한다 — Kafka가
  안 떠 있어도 API는 응답해야 한다. `process.on('unhandledRejection')`도 같은 목적.
- `redisClient`/`pgClient`의 `on('error')`는 로그만 남긴다 — 개별 요청에서 실패 처리한다.

### Idempotency
`middleware/idempotency.js`가 `res.json`을 감싸서 2xx 응답만 Redis에
`idem:{method}:{url}:{key}`로 캐싱한다(기본 24시간). 쓰기 엔드포인트
(`queue/join`, `seats/:id/hold`, `bookings`)는 `Idempotency-Key` 헤더가 없으면 400.
재생된 응답에는 `Idempotency-Replayed: true` 헤더가 붙는다.
MetalLB VIP 전환 시 클라이언트 자동 재시도로 인한 중복 예매를 막기 위한 장치다.

### 설정
모든 환경변수는 `src/config.js` 한 곳에서만 읽는다. 다른 파일에서 `process.env`를 직접
읽지 말 것(예외: `SKIP_KAFKA_CONSUMERS`, `MOCK_PAYMENT_FAILURE_RATE` 같은 개발용 스위치).
Kafka 토픽 이름도 `config.topics`에 모아 두었다.

## 알려진 범위 밖 / 미완성

- 인증은 대기열 토큰 확인일 뿐, 실제 로그인 연동 아님.
- Kafka consumer 실패 시 재시도/DLQ 정책 없음 — 현재는 로그만 남긴다.
- Redis는 단일 인스턴스 기준. Sentinel 구성으로 가려면 `redisClient.js`의 `new Redis(...)`를
  sentinel 옵션으로 교체해야 한다.
- `config.topics.paymentRequested`는 정의만 되어 있고 아직 사용처가 없다.

## 미해결 이슈

- `payment.amount`가 NOT NULL인데 `seat` / `ticket_event`에 price 컬럼이 없다 →
  `paymentConsumer.js`에 임시 고정값(10000) + TODO를 표시해 둠. **팀 확인 필요.**
- `test/concurrency-booking.test.js`(HTTP 레벨 테스트)는 아직 옛날 스키마 기준 →
  다음 작업 때 수정 필요.

## 검증 완료

- 동시성 테스트(`concurrency-core.test.js`): 실제 운영 스키마 기준으로 30건 x 5회 전부 PASS.
- DB 레벨 `idempotency_key` UNIQUE 제약(`booking_idempotency_key_key`) 동작 확인됨.

## 인프라 연결 미확인

- Redis / PostgreSQL / Kafka 접속 방식이 VM IP 직접인지 K8s Service DNS인지 아직 확인 전.
- Worker 6대로 확장됨 (w4=.110, w5=.111, w6=.112 가 K8s 노드로 편입).
- `.env` 값을 실제 인프라 기준으로 아직 채우지 않았다.

## 다음 할 일 순서

1. 실제 인프라 접속 방식 확인 (Redis / PG / Kafka)
2. `.env` 파일 실제 값으로 채우기
3. E2E 테스트 (curl 4단계: 큐 → 인증 → Hold → 예매)
4. 웹 UI 추가 (`public/` + `express.static`)
5. Docker build + 이미지 레지스트리 push
