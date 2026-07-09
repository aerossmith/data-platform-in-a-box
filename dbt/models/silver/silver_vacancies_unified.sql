{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(source, snapshot_id, vacancy_id)',
    partition_by='toYYYYMM(ingested_at)'
  )
}}

-- ================================================================
-- silver_vacancies_unified
-- ================================================================
-- Общий контракт вакансий поверх source-specific silver.
-- Читает готовые silver_hh_vacancies и silver_trudvsem_vacancies
-- через ref(), их логику НЕ трогает.
--
-- Grain: (source, snapshot_id, vacancy_id).
--
-- Про типы (важно):
--   silver_hh делает LEFT JOIN → matched_roles/matched_areas
--   имеют тип Nullable(Array(String)). ClickHouse ХРАНИТ такой
--   тип, но не даёт использовать в промежуточных вычислениях.
--   Реально NULL быть не может (matched CTE агрегирует те же
--   строки что и base, LEFT JOIN всегда матчится). Поэтому
--   применяем assumeNotNull — снимает Nullable-обёртку на
--   уровне типа без изменения данных.
--
--   Зарплаты приводим к общему Nullable(Float64):
--   HH salary_from/to — Nullable(Int64), tv — Nullable(Float64).
-- ================================================================

WITH hh AS (
    SELECT
        'hh'                                            AS source,
        snapshot_id                                     AS snapshot_id,
        vacancy_id                                      AS vacancy_id,
        title                                           AS title,
        employer_name                                   AS employer,

        -- Регион: matched_areas содержит msk/spb/remote
        multiIf(
            has(assumeNotNull(matched_areas), 'msk'),    'msk',
            has(assumeNotNull(matched_areas), 'spb'),    'spb',
            has(assumeNotNull(matched_areas), 'remote'), 'remote',
            CAST(NULL AS Nullable(String))
        )                                               AS region_normalized,
        arrayStringConcat(assumeNotNull(matched_areas), ',') AS region_raw,

        -- Роли: matched_roles → devops/platform
        arrayMap(
            r -> multiIf(
                r = 'DevOps Engineer',   'devops',
                r = 'Platform Engineer', 'platform',
                lowerUTF8(r)
            ),
            assumeNotNull(matched_roles)
        )                                               AS roles_normalized,

        -- Зарплаты в общий Nullable(Float64)
        CAST(salary_from AS Nullable(Float64))          AS salary_from,
        CAST(salary_to   AS Nullable(Float64))          AS salary_to,
        coalesce(salary_currency, 'RUB')                AS salary_currency,
        salary_text                                     AS salary_raw,

        employment                                      AS employment,
        schedule_text                                   AS schedule_text,

        -- Опыт HH: парсим строку → числовые границы
        multiIf(
            experience IS NULL OR experience = '',
                CAST(NULL AS Nullable(Int32)),
            match(experience, '(?i)без опыта|не требуется'),
                toNullable(toInt32(0)),
            match(experience, '(?i)более\s*\d+'),
                toInt32OrNull(extract(experience, '(\d+)')),
            length(extractAll(experience, '\d+')) >= 2,
                toInt32OrNull(arrayElement(extractAll(experience, '\d+'), 1)),
            CAST(NULL AS Nullable(Int32))
        )                                               AS experience_min_years,
        multiIf(
            experience IS NULL OR experience = '',
                CAST(NULL AS Nullable(Int32)),
            match(experience, '(?i)без опыта|не требуется'),
                toNullable(toInt32(0)),
            match(experience, '(?i)более\s*\d+'),
                CAST(NULL AS Nullable(Int32)),
            length(extractAll(experience, '\d+')) >= 2,
                toInt32OrNull(arrayElement(extractAll(experience, '\d+'), 2)),
            CAST(NULL AS Nullable(Int32))
        )                                               AS experience_max_years,
        experience                                      AS experience_raw,

        description                                     AS description,
        vacancy_url                                     AS url,

        ingested_at                                     AS published_at,
        false                                           AS published_at_is_precise,
        ingested_at                                     AS ingested_at,

        -- HH-specific: snимаем Nullable-обёртку с массивов через assumeNotNull
        source_mode                                     AS hh_source_mode,
        detail_status                                   AS hh_detail_status,
        quality_score                                   AS hh_quality_score,
        assumeNotNull(matched_roles)                    AS hh_matched_roles,
        assumeNotNull(matched_areas)                    AS hh_matched_areas,

        -- trudvsem-specific → NULL/пусто
        CAST(NULL AS Nullable(String))                  AS tv_qualification,
        CAST(NULL AS Nullable(String))                  AS tv_specialisation,
        CAST([] AS Array(String))                       AS tv_matched_queries

    FROM {{ ref('silver_hh_vacancies') }}
),

tv AS (
    SELECT
        'trudvsem'                                      AS source,
        snapshot_id                                     AS snapshot_id,
        vacancy_id                                      AS vacancy_id,
        title                                           AS title,
        employer_name                                   AS employer,

        -- Регион: точное сопоставление с префиксом «Город»
        multiIf(
            region_name = 'Город Москва',           'msk',
            region_name = 'Город Санкт-Петербург',  'spb',
            match(coalesce(schedule_text, ''), '(?i)удал|дистанц'), 'remote',
            CAST(NULL AS Nullable(String))
        )                                               AS region_normalized,
        region_name                                     AS region_raw,

        -- Роли из matched_queries
        arrayDistinct(
            arrayMap(
                q -> multiIf(
                    q = 'DevOps',                    'devops',
                    q = 'девопс',                    'devops',
                    q = 'SRE',                       'sre',
                    q = 'инженер по инфраструктуре', 'platform',
                    lowerUTF8(q)
                ),
                matched_queries
            )
        )                                               AS roles_normalized,

        -- Зарплаты в общий Nullable(Float64)
        CAST(salary_min AS Nullable(Float64))           AS salary_from,
        CAST(salary_max AS Nullable(Float64))           AS salary_to,
        'RUB'                                           AS salary_currency,
        salary_text                                     AS salary_raw,

        employment                                      AS employment,
        schedule_text                                   AS schedule_text,

        -- Опыт: одно число → минимальный стаж
        CAST(experience_years AS Nullable(Int32))       AS experience_min_years,
        CAST(NULL AS Nullable(Int32))                   AS experience_max_years,
        coalesce(toString(experience_years), '')        AS experience_raw,

        -- Описание: конкатенация requirements + duty
        trim(BOTH ' \n' FROM concat_ws(
            '\n\n',
            coalesce(requirements, ''),
            coalesce(duty, '')
        ))                                              AS description,
        vacancy_url                                     AS url,

        toDateTime(creation_date)                       AS published_at,
        true                                            AS published_at_is_precise,
        ingested_at                                     AS ingested_at,

        -- HH-specific → NULL/пусто
        CAST(NULL AS Nullable(String))                  AS hh_source_mode,
        CAST(NULL AS Nullable(String))                  AS hh_detail_status,
        CAST(NULL AS Nullable(UInt8))                   AS hh_quality_score,
        CAST([] AS Array(String))                       AS hh_matched_roles,
        CAST([] AS Array(String))                       AS hh_matched_areas,

        -- trudvsem-specific
        qualification                                   AS tv_qualification,
        specialisation                                  AS tv_specialisation,
        matched_queries                                 AS tv_matched_queries

    FROM {{ ref('silver_trudvsem_vacancies') }}
)

SELECT * FROM hh
UNION ALL
SELECT * FROM tv
