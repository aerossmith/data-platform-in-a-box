-- ============================================================
-- Data Platform in a Box — ClickHouse: медальонные базы
-- ============================================================
-- Медальонная архитектура использует три отдельные базы:
--   dpib         — bronze (исходный слой, создаётся ClickHouse-сервером
--                  автоматически из ENV CLICKHOUSE_DB)
--   dpib_silver  — staging + silver (нормализация и dedup, dbt)
--   dpib_gold    — gold (витрины под потребителей, dbt)
--
-- Этот скрипт выполняется ClickHouse ТОЛЬКО при первом старте
-- (когда папка /var/lib/clickhouse пустая). На существующем
-- томе скрипт игнорируется — используй `make clickhouse-bootstrap`.
-- ============================================================

CREATE DATABASE IF NOT EXISTS dpib_silver;
CREATE DATABASE IF NOT EXISTS dpib_gold;
