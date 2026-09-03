-- data namespace / PostgreSQL — 예약 확정 원본 데이터

CREATE TABLE IF NOT EXISTS seats (
  id          TEXT PRIMARY KEY,
  event_id    TEXT NOT NULL,
  seat_no     TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'available'
              CHECK (status IN ('available', 'booked')),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_id, seat_no)
);

CREATE TABLE IF NOT EXISTS bookings (
  id          TEXT PRIMARY KEY,
  event_id    TEXT NOT NULL,
  seat_id     TEXT NOT NULL REFERENCES seats(id),
  user_id     TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'confirmed'
              CHECK (status IN ('confirmed', 'cancelled')),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 같은 좌석에 대해 confirmed 예약이 두 개 이상 존재할 수 없다 (DB 레벨 최종 방어선)
  UNIQUE (event_id, seat_id)
);

CREATE INDEX IF NOT EXISTS idx_seats_event_status ON seats (event_id, status);
CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings (user_id);
