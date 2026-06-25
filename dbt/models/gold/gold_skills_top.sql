{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(vacancies_count, skill)'
  )
}}

-- ================================================================
-- gold_skills_top
-- ================================================================
-- Топ навыков по последнему снапшоту каждой вакансии.
--
-- Источник: silver_hh_vacancies.skills — массив из detail-страницы HH.
-- Внимание: skills часто пустой массив ([]). Эта модель отражает
-- ТОЛЬКО explicit skill-теги. NER/keywords по description — в roadmap.
--
-- Grain: skill (одна строка на навык).
-- ================================================================

WITH latest_per_vacancy AS (
    -- Последний снапшот для каждой вакансии — чтобы не считать один навык
    -- N раз (по числу снапшотов, в которые попала вакансия).
    SELECT
        vacancy_id,
        argMax(skills,          ingested_at) AS skills,
        argMax(matched_roles,   ingested_at) AS matched_roles,
        argMax(matched_areas,   ingested_at) AS matched_areas,
        argMax(has_detail,      ingested_at) AS has_detail
    FROM {{ ref('silver_hh_vacancies') }}
    GROUP BY vacancy_id
),

exploded AS (
    SELECT
        skill,
        vacancy_id,
        matched_roles,
        matched_areas,
        has_detail
    FROM latest_per_vacancy
    ARRAY JOIN skills AS skill
    WHERE skill != ''
)

SELECT
    skill,
    count() AS vacancies_count,
    countIf(has(matched_roles, 'DevOps Engineer')) AS devops_count,
    countIf(has(matched_roles, 'Platform Engineer')) AS platform_count,
    countIf(has(matched_areas, 'msk')) AS msk_count,
    countIf(has(matched_areas, 'spb')) AS spb_count,
    countIf(has(matched_areas, 'remote')) AS remote_count
FROM exploded
GROUP BY skill
ORDER BY vacancies_count DESC
