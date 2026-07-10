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
-- Про типы:
--   assumeNotNull(matched_*) — LEFT JOIN в silver_hh делает эти
--   поля Nullable(Array(String)), а ClickHouse запрещает такой
--   тип в промежуточных вычислениях. NULL там физически быть
--   не может (matched CTE агрегирует те же строки что и base).
--   assumeNotNull(experience) — extractAll от Nullable возвращает
--   Nullable(Array), тоже запрещённый тип. NULL уже отфильтрован
--   первой веткой multiIf.
--
--   Зарплаты HH парсим из salary_text regex-ом (API закрыт, чисел
--   в bronze нет). Перед парсингом нормализуем обычные и неразрывные
--   пробелы; валюту сохраняем без пересчёта. Зарплаты trudvsem уже числа.
-- ================================================================

WITH hh_source AS (
    SELECT
        *,
        replaceAll(
            replaceAll(salary_text, char(194, 160), ' '),
            char(226, 128, 175),
            ' '
        ) AS salary_text_normalized
    FROM {{ ref('silver_hh_vacancies') }}
),

hh AS (
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

        -- Зарплаты HH: числовых полей нет (API закрыт ddos-guard),
        -- парсим salary_text. Форматы:
        --   «от 200 000 ₽ ...»            → from=200000
        --   «до 300 000 ₽ ...»            → to=300000
        --   «от 200 000 до 300 000 ₽ ...» → from=200000, to=300000
        -- Неразрывные пробелы уже нормализованы в hh_source.
        multiIf(
            salary_text_normalized IS NULL OR salary_text_normalized = '',
                CAST(salary_from AS Nullable(Float64)),
            -- Приоритет: числовое из API если появится
            salary_from IS NOT NULL,
                CAST(salary_from AS Nullable(Float64)),
            -- «от N ...» → from
            match(salary_text_normalized, '(?i)^\s*от\s+[\d\s]+'),
                toFloat64OrNull(
                    replaceAll(
                        extract(assumeNotNull(salary_text_normalized), '(?i)^\s*от\s+([\d\s]+)'),
                        ' ',
                        ''
                    )
                ),
            -- «N ₽ ...» без «от/до» — точное значение.
            match(salary_text_normalized, '^\s*[\d\s]+\s*(₽|\$|€|Br|₸|so''m)'),
                toFloat64OrNull(
                    replaceAll(
                        extract(assumeNotNull(salary_text_normalized), '^\s*([\d\s]+)'),
                        ' ',
                        ''
                    )
                ),
            CAST(NULL AS Nullable(Float64))
        )                                               AS salary_from,
        multiIf(
            salary_text_normalized IS NULL OR salary_text_normalized = '',
                CAST(salary_to AS Nullable(Float64)),
            salary_to IS NOT NULL,
                CAST(salary_to AS Nullable(Float64)),
            -- «от X до Y ...» — берём Y
            match(salary_text_normalized, '(?i)^\s*от\s+[\d\s]+\s+до\s+[\d\s]+'),
                toFloat64OrNull(
                    replaceAll(
                        extract(assumeNotNull(salary_text_normalized), '(?i)^\s*от\s+[\d\s]+\s+до\s+([\d\s]+)'),
                        ' ',
                        ''
                    )
                ),
            -- «до N ...» без «от» — только to
            match(salary_text_normalized, '(?i)^\s*до\s+[\d\s]+'),
                toFloat64OrNull(
                    replaceAll(
                        extract(assumeNotNull(salary_text_normalized), '(?i)^\s*до\s+([\d\s]+)'),
                        ' ',
                        ''
                    )
                ),
            match(salary_text_normalized, '^\s*[\d\s]+\s*(₽|\$|€|Br|₸|so''m)'),
                toFloat64OrNull(
                    replaceAll(
                        extract(assumeNotNull(salary_text_normalized), '^\s*([\d\s]+)'),
                        ' ',
                        ''
                    )
                ),
            CAST(NULL AS Nullable(Float64))
        )                                               AS salary_to,
        -- Валюта: детект по символу
        multiIf(
            salary_text_normalized IS NULL OR salary_text_normalized = '', coalesce(salary_currency, 'RUB'),
            position(salary_text_normalized, '$') > 0, 'USD',
            position(salary_text_normalized, '€') > 0, 'EUR',
            position(salary_text_normalized, 'Br') > 0, 'BYN',
            position(salary_text_normalized, '₸') > 0, 'KZT',
            positionCaseInsensitive(salary_text_normalized, 'so''m') > 0, 'UZS',
            position(salary_text_normalized, '₽') > 0, 'RUB',
            coalesce(salary_currency, 'RUB')
        )                                               AS salary_currency,
        salary_text                                     AS salary_raw,

        employment                                      AS employment,
        schedule_text                                   AS schedule_text,

        -- Опыт HH: парсим строку → числовые границы.
        -- assumeNotNull(experience): extractAll от Nullable запрещён.
        multiIf(
            experience IS NULL OR experience = '',
                CAST(NULL AS Nullable(Int32)),
            match(experience, '(?i)без опыта|не требуется'),
                toNullable(toInt32(0)),
            match(experience, '(?i)более\s*\d+'),
                toInt32OrNull(extract(assumeNotNull(experience), '(\d+)')),
            length(extractAll(assumeNotNull(experience), '\d+')) >= 2,
                toInt32OrNull(arrayElement(extractAll(assumeNotNull(experience), '\d+'), 1)),
            CAST(NULL AS Nullable(Int32))
        )                                               AS experience_min_years,
        multiIf(
            experience IS NULL OR experience = '',
                CAST(NULL AS Nullable(Int32)),
            match(experience, '(?i)без опыта|не требуется'),
                toNullable(toInt32(0)),
            match(experience, '(?i)более\s*\d+'),
                CAST(NULL AS Nullable(Int32)),
            length(extractAll(assumeNotNull(experience), '\d+')) >= 2,
                toInt32OrNull(arrayElement(extractAll(assumeNotNull(experience), '\d+'), 2)),
            CAST(NULL AS Nullable(Int32))
        )                                               AS experience_max_years,
        experience                                      AS experience_raw,

        description                                     AS description,
        vacancy_url                                     AS url,

        ingested_at                                     AS published_at,
        false                                           AS published_at_is_precise,
        ingested_at                                     AS ingested_at,

        -- HH-specific
        source_mode                                     AS hh_source_mode,
        detail_status                                   AS hh_detail_status,
        quality_score                                   AS hh_quality_score,
        assumeNotNull(matched_roles)                    AS hh_matched_roles,
        assumeNotNull(matched_areas)                    AS hh_matched_areas,

        -- trudvsem-specific → NULL/пусто
        CAST(NULL AS Nullable(String))                  AS tv_qualification,
        CAST(NULL AS Nullable(String))                  AS tv_specialisation,
        CAST([] AS Array(String))                       AS tv_matched_queries

    FROM hh_source
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

        -- Зарплаты: числа сразу, валюта всегда RUB
        CAST(salary_min AS Nullable(Float64))           AS salary_from,
        CAST(salary_max AS Nullable(Float64))           AS salary_to,
        'RUB'                                           AS salary_currency,
        salary_text                                     AS salary_raw,

        employment                                      AS employment,
        schedule_text                                   AS schedule_text,

        CAST(experience_years AS Nullable(Int32))       AS experience_min_years,
        CAST(NULL AS Nullable(Int32))                   AS experience_max_years,
        coalesce(toString(experience_years), '')        AS experience_raw,

        trim(BOTH ' \n' FROM concat_ws(
            '\n\n',
            coalesce(requirements, ''),
            coalesce(duty, '')
        ))                                              AS description,
        vacancy_url                                     AS url,

        toDateTime(creation_date)                       AS published_at,
        true                                            AS published_at_is_precise,
        ingested_at                                     AS ingested_at,

        CAST(NULL AS Nullable(String))                  AS hh_source_mode,
        CAST(NULL AS Nullable(String))                  AS hh_detail_status,
        CAST(NULL AS Nullable(UInt8))                   AS hh_quality_score,
        CAST([] AS Array(String))                       AS hh_matched_roles,
        CAST([] AS Array(String))                       AS hh_matched_areas,

        qualification                                   AS tv_qualification,
        specialisation                                  AS tv_specialisation,
        matched_queries                                 AS tv_matched_queries

    FROM {{ ref('silver_trudvsem_vacancies') }}
)

SELECT * FROM hh
UNION ALL
SELECT * FROM tv
