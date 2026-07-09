{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(source, vacancy_id)'
  )
}}

-- ================================================================
-- gold_vacancies_latest
-- ================================================================
-- Актуальная версия каждой вакансии: для каждой (source, vacancy_id)
-- берём запись из самого свежего snapshot по ingested_at.
-- Фильтруем только целевые регионы (msk / spb / remote).
-- ================================================================

WITH latest AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY source, vacancy_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ ref('silver_vacancies_unified') }}
    WHERE region_normalized IS NOT NULL
)

SELECT
    source,
    vacancy_id,
    snapshot_id,
    title,
    employer,
    region_normalized,
    region_raw,
    roles_normalized,
    if(empty(roles_normalized), NULL, roles_normalized[1]) AS primary_role,
    salary_from,
    salary_to,
    salary_currency,
    salary_raw,
    multiIf(
        salary_from IS NOT NULL AND salary_to IS NOT NULL,
            (salary_from + salary_to) / 2,
        salary_from IS NOT NULL, salary_from,
        salary_to   IS NOT NULL, salary_to,
        CAST(NULL AS Nullable(Float64))
    )                                          AS salary_midpoint,
    employment,
    schedule_text,
    experience_min_years,
    experience_max_years,
    experience_raw,
    description,
    length(coalesce(description, ''))          AS description_length,
    url,
    published_at,
    toStartOfMonth(published_at)               AS published_month,
    published_at_is_precise,
    ingested_at,
    hh_source_mode,
    hh_detail_status,
    hh_quality_score,
    hh_matched_roles,
    hh_matched_areas,
    tv_qualification,
    tv_specialisation,
    tv_matched_queries
FROM latest
WHERE rn = 1
