-- ============================================================
-- Data Platform in a Box — ClickHouse: отдельные БД для слоёв
--
-- Архитектура: три БД на стек.
--   dpib         — сырой слой (bronze, raw_json и приклад)
--   dpib_silver  — очищенные и дедуплицированные витрины (dbt)
--   dpib_gold    — бизнес-витрины и агрегаты (dbt)
--
-- Скрипт выполняется ClickHouse только при первом старте (когда
-- /var/lib/clickhouse пустой). Для существующего тома применить
-- вручную через `make clickhouse-bootstrap`.
-- ============================================================

CREATE DATABASE IF NOT EXISTS dpib_silver;
CREATE DATABASE IF NOT EXISTS dpib_gold;

-- Доступы для основного пользователя приклада (dpib).
-- В нашем окружении CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 и dpib
-- создан как обычный пользователь — выдаём ему права на новые БД.
GRANT ALL ON dpib_silver.* TO dpib;
GRANT ALL ON dpib_gold.*   TO dpib;
