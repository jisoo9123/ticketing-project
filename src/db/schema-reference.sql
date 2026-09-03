-- @author gyustar
-- @date 2026-09-03
--
-- 이 파일은 우리가 새로 만드는 스키마가 아니다.
-- 송지원님이 이미 PostgreSQL(.111, 이후 data namespace로 재편)에 구축해둔
-- 실제 테이블 구조를 pg_dump 결과 그대로 복사해서 "참조용"으로만 남겨둔 것.
--
-- 절대 이 파일로 실제 운영 DB에 CREATE TABLE을 실행하지 말 것 — 이미 있다.
-- 로컬 개발/테스트 환경에서 같은 구조를 재현하고 싶을 때만 이 파일 그대로 적용한다.
--
-- (기존에 우리가 임의로 설계했던 seats/bookings 2테이블 스키마는 폐기함 —
--  실제 운영 스키마가 idempotency_key UNIQUE 제약, version 컬럼 기반 낙관적 잠금,
--  processed_event 기반 Kafka 중복 처리 방지까지 이미 반영된 더 견고한 설계였음)

CREATE TABLE public.app_user (
    id bigint NOT NULL,
    email character varying(255) NOT NULL,
    password_hash character varying(255) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE public.app_user_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.app_user_id_seq OWNED BY public.app_user.id;
ALTER TABLE ONLY public.app_user ALTER COLUMN id SET DEFAULT nextval('public.app_user_id_seq'::regclass);
ALTER TABLE ONLY public.app_user ADD CONSTRAINT app_user_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.app_user ADD CONSTRAINT app_user_email_key UNIQUE (email);

CREATE TABLE public.ticket_event (
    id bigint NOT NULL,
    title character varying(200) NOT NULL,
    opens_at timestamp with time zone NOT NULL,
    closes_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE public.ticket_event_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.ticket_event_id_seq OWNED BY public.ticket_event.id;
ALTER TABLE ONLY public.ticket_event ALTER COLUMN id SET DEFAULT nextval('public.ticket_event_id_seq'::regclass);
ALTER TABLE ONLY public.ticket_event ADD CONSTRAINT ticket_event_pkey PRIMARY KEY (id);

CREATE TABLE public.seat (
    id bigint NOT NULL,
    event_id bigint NOT NULL,
    seat_code character varying(50) NOT NULL,
    status character varying(20) DEFAULT 'AVAILABLE'::character varying NOT NULL,
    version bigint DEFAULT 0 NOT NULL
);
CREATE SEQUENCE public.seat_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.seat_id_seq OWNED BY public.seat.id;
ALTER TABLE ONLY public.seat ALTER COLUMN id SET DEFAULT nextval('public.seat_id_seq'::regclass);
ALTER TABLE ONLY public.seat ADD CONSTRAINT seat_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.seat ADD CONSTRAINT seat_event_id_seat_code_key UNIQUE (event_id, seat_code);
ALTER TABLE ONLY public.seat ADD CONSTRAINT seat_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.ticket_event(id);

CREATE TABLE public.booking (
    id bigint NOT NULL,
    user_id bigint NOT NULL,
    event_id bigint NOT NULL,
    idempotency_key character varying(100) NOT NULL,
    status character varying(20) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
CREATE SEQUENCE public.booking_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.booking_id_seq OWNED BY public.booking.id;
ALTER TABLE ONLY public.booking ALTER COLUMN id SET DEFAULT nextval('public.booking_id_seq'::regclass);
ALTER TABLE ONLY public.booking ADD CONSTRAINT booking_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.booking ADD CONSTRAINT booking_idempotency_key_key UNIQUE (idempotency_key);
ALTER TABLE ONLY public.booking ADD CONSTRAINT booking_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.app_user(id);
ALTER TABLE ONLY public.booking ADD CONSTRAINT booking_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.ticket_event(id);

CREATE TABLE public.booking_seat (
    booking_id bigint NOT NULL,
    seat_id bigint NOT NULL
);
ALTER TABLE ONLY public.booking_seat ADD CONSTRAINT booking_seat_pkey PRIMARY KEY (booking_id, seat_id);
ALTER TABLE ONLY public.booking_seat ADD CONSTRAINT booking_seat_seat_id_key UNIQUE (seat_id);
ALTER TABLE ONLY public.booking_seat ADD CONSTRAINT booking_seat_booking_id_fkey FOREIGN KEY (booking_id) REFERENCES public.booking(id);
ALTER TABLE ONLY public.booking_seat ADD CONSTRAINT booking_seat_seat_id_fkey FOREIGN KEY (seat_id) REFERENCES public.seat(id);

CREATE TABLE public.payment (
    id bigint NOT NULL,
    booking_id bigint NOT NULL,
    status character varying(20) NOT NULL,
    amount numeric(12,2) NOT NULL,
    processed_at timestamp with time zone,
    CONSTRAINT payment_amount_check CHECK ((amount >= (0)::numeric))
);
CREATE SEQUENCE public.payment_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.payment_id_seq OWNED BY public.payment.id;
ALTER TABLE ONLY public.payment ALTER COLUMN id SET DEFAULT nextval('public.payment_id_seq'::regclass);
ALTER TABLE ONLY public.payment ADD CONSTRAINT payment_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.payment ADD CONSTRAINT payment_booking_id_key UNIQUE (booking_id);
ALTER TABLE ONLY public.payment ADD CONSTRAINT payment_booking_id_fkey FOREIGN KEY (booking_id) REFERENCES public.booking(id);
-- ⚠️ amount가 NOT NULL인데 seat/ticket_event 어디에도 가격(price) 컬럼이 없다.
-- 팀 확인이 필요한 스키마 갭 — paymentConsumer.js에 TODO로 표시해둠.

CREATE TABLE public.processed_event (
    event_id character varying(100) NOT NULL,
    event_type character varying(100) NOT NULL,
    processed_at timestamp with time zone DEFAULT now() NOT NULL
);
ALTER TABLE ONLY public.processed_event ADD CONSTRAINT processed_event_pkey PRIMARY KEY (event_id);
