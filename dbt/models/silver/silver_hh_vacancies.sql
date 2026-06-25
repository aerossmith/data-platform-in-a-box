{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(snapshot_id, vacancy_id)',
    partition_by='toYYYYMM(ingested_at)'
  )
}}

-- ================================================================
-- silver_hh_vacancies
-- ================================================================
-- Grain: (snapshot_id, vacancy_id) — внутри одного снапшота одна
-- вакансия = одна строка. Между снапшотами повторение — это
-- лайфсайкл вакансии (новая версия данных, статус и т.д.).
--
-- Логика выбора «лучшей» строки из множества bronze-рядов одной
-- вакансии в одном снапшоте:
--   ORDER BY quality_score DESC,
--            description_length DESC,
--            ingested_at DESC
--
-- Т.е. сначала тот, у кого detail_status='ok'; среди равных —
-- у кого более длинное description; среди равных — последний по
-- времени insert.
--
-- Дополнительно агрегируем matched_roles / matched_areas — под
-- какие комбинации поиска вакансия попадала в этом снапшоте.
-- ================================================================

WITH best_row AS (
    SELECT
        snapshot_id,
        vacancy_id,
        source_mode,
        detail_status,
        detail_error,
        title,
        employer_name,
        vacancy_url,
        experience,
        employment,
        schedule_text,
        work_format_card,
        salary_text,
        salary_from,
        salary_to,
        salary_currency,
        address_text,
        description,
        description_length,
        skills,
        skills_count,
        card_text,
        quality_score,
        has_detail,
        ingested_at,
        -- Выбираем лучшую строку для каждой пары (snapshot_id, vacancy_id)
        row_number() OVER (
            PARTITION BY snapshot_id, vacancy_id
            ORDER BY
                quality_score DESC,
                description_length DESC,
                ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_hh_vacancies') }}
),

matched AS (
    -- Под какие search_text / search_area попадала вакансия в этом снапшоте
    SELECT
        snapshot_id,
        vacancy_id,
        groupUniqArray(search_text) AS matched_roles,
        groupUniqArray(search_area) AS matched_areas,
        count() AS matched_rows
    FROM {{ ref('stg_hh_vacancies') }}
    GROUP BY snapshot_id, vacancy_id
)

SELECT
    b.snapshot_id          AS snapshot_id,
    b.vacancy_id           AS vacancy_id,

    -- Лучшая строка
    b.title                AS title,
    b.employer_name        AS employer_name,
    b.vacancy_url          AS vacancy_url,
    b.experience           AS experience,
    b.employment           AS employment,
    b.schedule_text        AS schedule_text,
    b.work_format_card     AS work_format_card,
    b.salary_text          AS salary_text,
    b.salary_from          AS salary_from,
    b.salary_to            AS salary_to,
    b.salary_currency      AS salary_currency,
    b.address_text         AS address_text,
    b.description          AS description,
    b.description_length   AS description_length,
    b.skills               AS skills,
    b.skills_count         AS skills_count,
    b.card_text            AS card_text,
    b.source_mode          AS source_mode,
    b.detail_status        AS detail_status,
    b.detail_error         AS detail_error,
    b.quality_score        AS quality_score,
    b.has_detail           AS has_detail,
    b.ingested_at          AS ingested_at,

    -- Агрегаты «под какие поиски попала»
    m.matched_roles        AS matched_roles,
    m.matched_areas        AS matched_areas,
    m.matched_rows         AS matched_rows
FROM best_row b
LEFT JOIN matched m
    ON  b.snapshot_id = m.snapshot_id
    AND b.vacancy_id  = m.vacancy_id
WHERE b.rn = 1
