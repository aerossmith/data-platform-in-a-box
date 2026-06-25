{{
  config(
    materialized='view'
  )
}}

-- ================================================================
-- stg_hh_vacancies
-- ================================================================
-- View поверх bronze_hh_vacancies. Парсит raw_json в типизированные
-- колонки, унифицируя три разных payload-варианта:
--
--   1. HTML fallback + detail_status='ok'
--      Богатый payload: title, description, skills, experience,
--      detail_employer, detail_url. Salary часто null (HH-нюанс).
--
--   2. HTML fallback + detail_status='captcha'/'error'/'http_*'
--      Только card-уровень: title, employer, address, salary, и
--      производные experience_card/work_format_card/card_text.
--
--   3. API (source_mode IS NULL в raw_json — на момент написания
--      api.hh.ru закрыт ddos-guard'ом, payload-ов в bronze нет,
--      но маппинг готов на случай если HH откроет API).
--
-- Принцип coalesce: detail > card > api > NULL.
-- Это view (а не table), чтобы не дублировать данные. Для тяжёлых
-- запросов читатель пойдёт в silver_hh_vacancies.
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
    FROM {{ source('bronze', 'bronze_hh_vacancies') }}
),

parsed AS (
    SELECT
        -- ---- bronze-ключи (без изменений) ----
        ingested_at,
        snapshot_id,
        source,
        search_text,
        search_area,
        page_num,
        vacancy_id,

        -- ---- метаданные о происхождении строки ----
        -- API-payload не содержит source_mode → считаем что это 'api'
        coalesce(
            nullIf(JSONExtractString(raw_json, 'source_mode'), ''),
            'api'
        ) AS source_mode,

        nullIf(JSONExtractString(raw_json, 'detail_status'), '') AS detail_status,
        nullIf(JSONExtractString(raw_json, 'detail_error'), '') AS detail_error,

        -- ---- общие поля: name / employer / url ----
        -- title: detail_title > title > name (API)
        coalesce(
            nullIf(JSONExtractString(raw_json, 'detail_title'), ''),
            nullIf(JSONExtractString(raw_json, 'title'), ''),
            nullIf(JSONExtractString(raw_json, 'name'), '')
        ) AS title,

        -- employer: detail_employer > employer (card/API строка) > employer.name (API nested)
        coalesce(
            nullIf(JSONExtractString(raw_json, 'detail_employer'), ''),
            nullIf(JSONExtractString(raw_json, 'employer'), ''),
            nullIf(JSONExtractString(JSONExtractRaw(raw_json, 'employer'), 'name'), '')
        ) AS employer_name,

        -- vacancy URL: HTML 'url' / API 'alternate_url'
        coalesce(
            nullIf(JSONExtractString(raw_json, 'url'), ''),
            nullIf(JSONExtractString(raw_json, 'alternate_url'), '')
        ) AS vacancy_url,

        -- ---- experience ----
        -- detail > card-derived ("Опыт 3-6 лет"). Оба строковые.
        coalesce(
            nullIf(JSONExtractString(raw_json, 'experience'), ''),
            nullIf(JSONExtractString(raw_json, 'experience_card'), '')
        ) AS experience,

        -- ---- employment / schedule ----
        -- В detail у HH часто пусто, поэтому fallback на work_format_card
        -- ("Можно удалённо, Гибрид, ...")
        coalesce(
            nullIf(JSONExtractString(raw_json, 'employment'), ''),
            nullIf(JSONExtractString(raw_json, 'work_format_card'), '')
        ) AS employment,

        coalesce(
            nullIf(JSONExtractString(raw_json, 'schedule'), ''),
            nullIf(JSONExtractString(raw_json, 'work_format_card'), '')
        ) AS schedule_text,

        nullIf(JSONExtractString(raw_json, 'work_format_card'), '') AS work_format_card,

        -- ---- salary ----
        coalesce(
            nullIf(JSONExtractString(raw_json, 'salary_detail'), ''),
            nullIf(JSONExtractString(raw_json, 'salary'), '')
        ) AS salary_text,

        -- API-числовые поля salary.from/to (на будущее). Сейчас в bronze NULL.
        JSONExtract(JSONExtractRaw(raw_json, 'salary'), 'from', 'Nullable(Int64)')   AS salary_from,
        JSONExtract(JSONExtractRaw(raw_json, 'salary'), 'to',   'Nullable(Int64)')   AS salary_to,
        nullIf(JSONExtractString(JSONExtractRaw(raw_json, 'salary'), 'currency'), '') AS salary_currency,

        -- ---- address ----
        coalesce(
            nullIf(JSONExtractString(raw_json, 'address_detail'), ''),
            nullIf(JSONExtractString(raw_json, 'address'), '')
        ) AS address_text,

        -- ---- description (только из detail HTML) ----
        nullIf(JSONExtractString(raw_json, 'description'), '') AS description,

        -- ---- skills (массив строк, только из detail HTML) ----
        -- При detail_status='ok' иногда приходит [] — теги навыков на странице
        -- HH помечены не везде. См. roadmap: NER/keywords по description.
        JSONExtract(raw_json, 'skills', 'Array(String)') AS skills,

        -- ---- card_text (для silver QA + ad-hoc анализа в gold) ----
        nullIf(JSONExtractString(raw_json, 'card_text'), '') AS card_text,

        -- ---- raw для дебага (на случай если что-то поедет) ----
        raw_json AS raw_json

    FROM src
)

SELECT
    *,
    -- ---- утилити для consumers ----
    length(coalesce(description, '')) AS description_length,
    length(skills)                     AS skills_count,
    -- Признак «есть детальный payload»
    (source_mode = 'html_fallback' AND detail_status = 'ok') AS has_detail,
    -- Качество строки — для выбора лучшей в silver
    multiIf(
        source_mode = 'html_fallback' AND detail_status = 'ok',      4,
        source_mode = 'html_fallback' AND detail_status = 'captcha', 3,
        source_mode = 'html_fallback',                                2,
        source_mode = 'api',                                          1,
        0
    ) AS quality_score
FROM parsed
