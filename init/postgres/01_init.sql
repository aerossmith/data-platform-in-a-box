-- ============================================================
-- Data Platform in a Box — PostgreSQL init
-- Выполняется один раз при первом старте контейнера
-- ============================================================

-- Схема для прикладных таблиц
CREATE SCHEMA IF NOT EXISTS analytics;

-- Таблица пользователей (пример OLTP)
CREATE TABLE IF NOT EXISTS analytics.users (
    user_id     BIGSERIAL PRIMARY KEY,
    email       VARCHAR(255) UNIQUE NOT NULL,
    full_name   VARCHAR(255),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE
);

-- Таблица событий (пример transactional)
CREATE TABLE IF NOT EXISTS analytics.events (
    event_id    BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES analytics.users(user_id),
    event_type  VARCHAR(50) NOT NULL,
    payload     JSONB,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_user_id    ON analytics.events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON analytics.events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_type       ON analytics.events(event_type);

-- Тестовые данные
INSERT INTO analytics.users (email, full_name) VALUES
    ('alice@example.com', 'Alice Smith'),
    ('bob@example.com',   'Bob Johnson'),
    ('carol@example.com', 'Carol White')
ON CONFLICT (email) DO NOTHING;

INSERT INTO analytics.events (user_id, event_type, payload) VALUES
    (1, 'login',    '{"ip": "10.0.0.1"}'),
    (1, 'purchase', '{"amount": 1500, "currency": "RUB"}'),
    (2, 'login',    '{"ip": "10.0.0.2"}'),
    (3, 'login',    '{"ip": "10.0.0.3"}'),
    (3, 'logout',   '{}');

-- Служебная схема для будущих агентов (RAG, audit, и т.п.)
CREATE SCHEMA IF NOT EXISTS agents;

-- Проверка
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL init complete. Users: %, Events: %',
        (SELECT count(*) FROM analytics.users),
        (SELECT count(*) FROM analytics.events);
END $$;
