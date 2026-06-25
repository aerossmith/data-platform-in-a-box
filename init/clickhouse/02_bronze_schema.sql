-- ============================================================
-- Data Platform in a Box — ClickHouse Bronze layer
-- Сырые данные snapshot-ов из внешних API.
-- Принцип: что пришло, то и легло. Никогда не модифицируем.
-- ============================================================

USE dpib;

-- HH.ru: сырые вакансии (один snapshot = одна страница выдачи API)
CREATE TABLE IF NOT EXISTS bronze_hh_vacancies
(
    ingested_at  DateTime DEFAULT now(),
    snapshot_id  UUID,                                  -- ID запуска DAG (один на весь батч)
    source       LowCardinality(String) DEFAULT 'hh.ru',
    search_text  LowCardinality(String),                -- 'DevOps Engineer' / 'Platform Engineer'
    search_area  LowCardinality(String),                -- '1' (Москва) / '2' (СПб) / 'remote'
    page_num     UInt16,                                -- номер страницы выдачи
    vacancy_id   String,                                -- из raw для быстрого поиска и dedup в silver
    raw_json     String                                 -- весь объект вакансии как пришёл от API
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, vacancy_id)
SETTINGS index_granularity = 8192;

-- HH.ru: сырые ответы по работодателям (опционально, для будущих этапов)
CREATE TABLE IF NOT EXISTS bronze_hh_employers
(
    ingested_at  DateTime DEFAULT now(),
    snapshot_id  UUID,
    source       LowCardinality(String) DEFAULT 'hh.ru',
    employer_id  String,
    raw_json     String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, employer_id);
