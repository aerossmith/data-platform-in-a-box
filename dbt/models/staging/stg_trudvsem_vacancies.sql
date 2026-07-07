{{
  config(
    materialized='view'
  )
}}

-- ================================================================
-- stg_trudvsem_vacancies
-- ================================================================
-- View поверх bronze_trudvsem_vacancies («Работа России»).
-- В отличие от HH тут ровно один формат payload: плоский объект
-- vacancy из открытого API, контакты вырезаны на ingest.
-- Никаких coalesce-каскадов не нужно — источник стабильный.
-- ================================================================

WITH src AS (
    SELECT
        ingested_at,
        snapshot_id,
        source,
        search_text,
        search_area,
        page_num,
        vacancy_id,
        raw_json
    FROM {{ source('bronze', 'bronze_trudvsem_vacancies') }}
)

SELECT
    -- bronze-ключи как есть
    ingested_at,
    snapshot_id,
    source,
    search_text,
    search_area,
    page_num,
    vacancy_id,

    -- основные атрибуты
    nullIf(JSONExtractString(raw_json, 'job-name'), '')                    AS title,
    nullIf(JSONExtractString(raw_json, 'company', 'name'), '')             AS employer_name,
    nullIf(JSONExtractString(raw_json, 'region', 'name'), '')              AS region_name,
    nullIf(JSONExtractString(raw_json, 'vac_url'), '')                     AS vacancy_url,

    -- зарплата: строка "от 120000" + числовые границы
    nullIf(JSONExtractString(raw_json, 'salary'), '')                      AS salary_text,
    JSONExtract(raw_json, 'salary_min', 'Nullable(Float64)')               AS salary_min,
    JSONExtract(raw_json, 'salary_max', 'Nullable(Float64)')               AS salary_max,
    nullIf(JSONExtractString(raw_json, 'currency'), '')                    AS salary_currency,

    -- условия
    nullIf(JSONExtractString(raw_json, 'employment'), '')                  AS employment,
    nullIf(JSONExtractString(raw_json, 'schedule'), '')                    AS schedule_text,
    nullIf(JSONExtractString(raw_json, 'qualification'), '')               AS qualification,

    -- требуемый стаж (лет) из вложенного requirement
    JSONExtract(raw_json, 'requirement', 'experience', 'Nullable(Float64)') AS experience_years,

    -- тексты: требования и обязанности
    nullIf(JSONExtractString(raw_json, 'requirements'), '')                AS requirements,
    nullIf(JSONExtractString(raw_json, 'duty'), '')                        AS duty,

    -- классификация госпортала
    nullIf(JSONExtractString(raw_json, 'category', 'specialisation'), '')  AS specialisation,

    -- даты
    toDateOrNull(JSONExtractString(raw_json, 'creation-date'))             AS creation_date,
    parseDateTimeBestEffortOrNull(
        JSONExtractString(raw_json, 'date_modify')
    )                                                                      AS date_modify,

    -- утилити
    length(coalesce(nullIf(JSONExtractString(raw_json, 'requirements'), ''), '')) AS requirements_length,
    length(coalesce(nullIf(JSONExtractString(raw_json, 'duty'), ''), ''))         AS duty_length,

    raw_json
FROM src
