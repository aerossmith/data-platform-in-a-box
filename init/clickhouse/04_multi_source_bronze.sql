-- ============================================================
-- Дополнительные источники вакансий в Bronze.
-- Идемпотентно: можно применять повторно к существующему тому.
-- ============================================================

CREATE DATABASE IF NOT EXISTS dpib;
USE dpib;

CREATE TABLE IF NOT EXISTS bronze_trudvsem_vacancies
(
    ingested_at  DateTime DEFAULT now(),
    snapshot_id  UUID,
    source       LowCardinality(String) DEFAULT 'trudvsem.ru',
    search_text  LowCardinality(String),
    search_area  LowCardinality(String),
    page_num     UInt16,
    vacancy_id   String,
    raw_json     String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (snapshot_id, search_text, search_area, vacancy_id);
