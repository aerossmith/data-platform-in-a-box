{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(snapshot_id, vacancy_id)',
    partition_by='toYYYYMM(ingested_at)'
  )
}}

-- ================================================================
-- silver_trudvsem_vacancies
-- ================================================================
-- Grain: (snapshot_id, vacancy_id). Одна вакансия может прийти по
-- нескольким поисковым запросам — дедуп внутри снапшота, сами
-- запросы собираются в matched_queries.
--
-- Полный снапшот = все 4 поисковых запроса отработали
-- (uniqExact(search_text) = 4). Частичные снапшоты от упавших
-- ранов в silver не попадают, из bronze не удаляются.
--
-- Выбор лучшей строки: свежая по ingested_at, при равенстве —
-- с более длинными requirements (единственный источник различий
-- между дублями тут — страница выдачи).
-- ================================================================

WITH complete_snapshots AS (
    SELECT snapshot_id
    FROM dpib.bronze_trudvsem_vacancies
    GROUP BY snapshot_id
    HAVING uniqExact(search_text) = 4
),

best_row AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY snapshot_id, vacancy_id
            ORDER BY
                ingested_at DESC,
                requirements_length DESC
        ) AS rn
    FROM {{ ref('stg_trudvsem_vacancies') }}
    WHERE snapshot_id IN (SELECT snapshot_id FROM complete_snapshots)
),

matched AS (
    SELECT
        snapshot_id,
        vacancy_id,
        groupUniqArray(search_text) AS matched_queries,
        count()                     AS matched_rows
    FROM {{ ref('stg_trudvsem_vacancies') }}
    WHERE snapshot_id IN (SELECT snapshot_id FROM complete_snapshots)
    GROUP BY snapshot_id, vacancy_id
)

SELECT
    b.snapshot_id          AS snapshot_id,
    b.vacancy_id           AS vacancy_id,
    b.title                AS title,
    b.employer_name        AS employer_name,
    b.region_name          AS region_name,
    b.vacancy_url          AS vacancy_url,
    b.salary_text          AS salary_text,
    b.salary_min           AS salary_min,
    b.salary_max           AS salary_max,
    b.salary_currency      AS salary_currency,
    b.employment           AS employment,
    b.schedule_text        AS schedule_text,
    b.qualification        AS qualification,
    b.experience_years     AS experience_years,
    b.requirements         AS requirements,
    b.duty                 AS duty,
    b.specialisation       AS specialisation,
    b.creation_date        AS creation_date,
    b.date_modify          AS date_modify,
    b.requirements_length  AS requirements_length,
    b.duty_length          AS duty_length,
    b.ingested_at          AS ingested_at,
    m.matched_queries      AS matched_queries,
    m.matched_rows         AS matched_rows
FROM best_row b
LEFT JOIN matched m
    ON  b.snapshot_id = m.snapshot_id
    AND b.vacancy_id  = m.vacancy_id
WHERE b.rn = 1
