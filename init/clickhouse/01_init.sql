-- ============================================================
-- Data Platform in a Box — ClickHouse init
-- Выполняется один раз при первом старте контейнера
-- ============================================================

-- База уже создана через CLICKHOUSE_DB, используем её
USE dpib;

-- Аналитическая таблица событий (большой объём, быстрые агрегации)
CREATE TABLE IF NOT EXISTS events_analytics
(
    event_id    UInt64,
    user_id     UInt64,
    event_type  LowCardinality(String),
    event_date  Date,
    event_time  DateTime,
    amount      Nullable(Decimal(18, 2)),
    country     LowCardinality(String),
    device      LowCardinality(String),
    payload     String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id)
SETTINGS index_granularity = 8192;

-- Витрина: дневная агрегация событий
CREATE TABLE IF NOT EXISTS events_daily
(
    event_date    Date,
    event_type    LowCardinality(String),
    events_count  UInt64,
    unique_users  UInt64,
    total_amount  Decimal(18, 2)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type);

-- Тестовые данные
INSERT INTO events_analytics
    (event_id, user_id, event_type, event_date, event_time, amount, country, device, payload)
VALUES
    (1, 1, 'login',    today(), now(), NULL,    'RU', 'desktop', '{}'),
    (2, 1, 'purchase', today(), now(), 1500.00, 'RU', 'desktop', '{"item":"book"}'),
    (3, 2, 'login',    today(), now(), NULL,    'RU', 'mobile',  '{}'),
    (4, 2, 'purchase', today(), now(), 2300.50, 'RU', 'mobile',  '{"item":"course"}'),
    (5, 3, 'login',    today(), now(), NULL,    'KZ', 'desktop', '{}'),
    (6, 3, 'logout',   today(), now(), NULL,    'KZ', 'desktop', '{}');
