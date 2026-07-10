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
-- вакансия = одна строка. Между снапшотами повторение — лайфсайкл.
--
-- ЗАЩИТА ОТ ЧАСТИЧНЫХ СНАПШОТОВ:
-- complete_snapshots фильтрует только снапшоты где присутствуют
-- все 6 комбинаций (2 роли × 3 региона). Частичные снапшоты
-- (например 4/6 из-за упавшего ingest) в silver не попадают.
-- Данные в bronze при этом не удаляются.
--
-- Логика выбора «лучшей» строки из множества bronze-рядов:
--   argMax по (quality_score, description_length, ingested_at).
-- Одновременно собираем matched_* за один проход staging, чтобы не
-- дублировать JSONExtract и не держать оконную сортировку с JOIN в памяти.
-- ================================================================

WITH complete_snapshots AS (
    -- Только снапшоты где все 6 комбинаций присутствуют.
    -- uniqExact конкатенации исключает частичные прогоны.
    SELECT snapshot_id
    FROM dpib.bronze_hh_vacancies
    GROUP BY snapshot_id
    HAVING uniqExact(concat(search_text, '|', search_area)) = 6
),

aggregated AS (
    SELECT
        snapshot_id,
        vacancy_id,
        argMax(
            tuple(
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
                ingested_at
            ),
            tuple(quality_score, description_length, ingested_at)
        ) AS best,
        groupUniqArray(search_text) AS matched_roles,
        groupUniqArray(search_area) AS matched_areas,
        count()                     AS matched_rows
    FROM {{ ref('stg_hh_vacancies') }}
    -- Читаем только полные снапшоты
    WHERE snapshot_id IN (SELECT snapshot_id FROM complete_snapshots)
    GROUP BY snapshot_id, vacancy_id
)

SELECT
    snapshot_id                    AS snapshot_id,
    vacancy_id                     AS vacancy_id,
    tupleElement(best, 4)          AS title,
    tupleElement(best, 5)          AS employer_name,
    tupleElement(best, 6)          AS vacancy_url,
    tupleElement(best, 7)          AS experience,
    tupleElement(best, 8)          AS employment,
    tupleElement(best, 9)          AS schedule_text,
    tupleElement(best, 10)         AS work_format_card,
    tupleElement(best, 11)         AS salary_text,
    tupleElement(best, 12)         AS salary_from,
    tupleElement(best, 13)         AS salary_to,
    tupleElement(best, 14)         AS salary_currency,
    tupleElement(best, 15)         AS address_text,
    tupleElement(best, 16)         AS description,
    tupleElement(best, 17)         AS description_length,
    tupleElement(best, 18)         AS skills,
    tupleElement(best, 19)         AS skills_count,
    tupleElement(best, 20)         AS card_text,
    tupleElement(best, 1)          AS source_mode,
    tupleElement(best, 2)          AS detail_status,
    tupleElement(best, 3)          AS detail_error,
    tupleElement(best, 21)         AS quality_score,
    tupleElement(best, 22)         AS has_detail,
    tupleElement(best, 23)         AS ingested_at,
    matched_roles                  AS matched_roles,
    matched_areas                  AS matched_areas,
    matched_rows                   AS matched_rows
FROM aggregated
