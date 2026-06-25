# dbt — трансформации bronze → silver → gold

dbt-проект поверх `dpib.bronze_hh_vacancies` (ClickHouse).
Не трогает ingest, не меняет docker-compose — только читает bronze и пишет staging/silver/gold модели в ту же БД `dpib`.

## Слои

```
bronze_hh_vacancies  ← append-only, raw_json (НЕ ТРОГАЕМ)
        ↓
stg_hh_vacancies     ← view, парсит raw_json, унифицирует API + HTML payload
        ↓
silver_hh_vacancies  ← table, dedup по (snapshot_id, vacancy_id), best-row
        ↓
gold_skills_top      ← топ навыков по latest snapshot of each vacancy
```

## Установка (один раз)

```bash
# Из WSL/Ubuntu внутри $REPO_ROOT
cd dbt

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install dbt-core==1.8.* dbt-clickhouse==1.8.*

# Установить зависимости (dbt_utils)
dbt deps --profiles-dir .
```

## Запуск

```bash
cd dbt
source .venv/bin/activate

# Конфиг по умолчанию: localhost:8123, user=dpib, pass=dpib_pass.
# Если что-то другое — переопредели:
#   export DBT_CH_HOST=...  DBT_CH_PASSWORD=...

# Проверка соединения
dbt debug --profiles-dir .

# Прогон всех моделей (порядок зависимостей dbt разрулит сам)
dbt run --profiles-dir .

# Только staging+silver
dbt run --profiles-dir . --select staging silver

# Прогнать тесты (not_null, accepted_values, unique-combo)
dbt test --profiles-dir .

# Один конкретный select
dbt run --profiles-dir . --select stg_hh_vacancies
```

## Проверка результатов

```bash
make clickhouse-cli
```
```sql
-- Smoke: всё ли поднялось
SELECT count() FROM stg_hh_vacancies;
SELECT count() FROM silver_hh_vacancies;
SELECT count() FROM gold_skills_top;

-- silver: dedup на месте?
SELECT count() AS rows, uniq((snapshot_id, vacancy_id)) AS unique_keys
FROM silver_hh_vacancies;
-- rows == unique_keys → значит grain соблюдён

-- silver: распределение качества payload
SELECT source_mode, detail_status, count()
FROM silver_hh_vacancies
GROUP BY 1, 2 ORDER BY 3 DESC;

-- gold: топ-20 навыков
SELECT skill, vacancies_count, devops_count, platform_count, remote_count
FROM gold_skills_top
ORDER BY vacancies_count DESC
LIMIT 20;
```

## Структура

```
dbt/
├── dbt_project.yml              ← проект, materializations per layer
├── profiles.yml                 ← подключение к ClickHouse через ENV
├── packages.yml                 ← dbt_utils для composite unique
├── models/
│   ├── staging/
│   │   ├── _sources.yml         ← декларация bronze.bronze_hh_vacancies + тесты
│   │   ├── _stg_hh_vacancies.yml
│   │   └── stg_hh_vacancies.sql ← view: JSONExtract → типизированные колонки
│   ├── silver/
│   │   ├── _silver_hh_vacancies.yml
│   │   └── silver_hh_vacancies.sql ← table: dedup + matched_roles/areas
│   └── gold/
│       └── gold_skills_top.sql  ← топ навыков по latest snapshot
└── README.md                    ← вы здесь
```

## Что НЕ делаем на этом этапе

- Не трогаем `orchestration/dags/hh_vacancies_snapshot.py` — он рабочий
- Не меняем `docker-compose.yml` (запускаем dbt из WSL локально)
- Не интегрируем в Airflow — это после стабилизации моделей
- Не парсим skills из description через NER — это в roadmap

## Roadmap

| # | Что | Зачем |
|---|---|---|
| 1 | `gold_vacancies_latest` | Single source of truth — latest snapshot per vacancy_id |
| 2 | `gold_salary_by_role` | Медианы / p25 / p75 зарплат по ролям и регионам |
| 3 | `gold_employer_top` | Топ работодателей, retention вакансий |
| 4 | NER/keywords по description | Skills почти всегда `[]` — нужен fallback |
| 5 | dbt-runner в docker-compose | Воспроизводимость |
| 6 | DAG `dbt_hh_transform` в Airflow | Автоматизация (run после ingest) |
| 7 | dbt-snapshot на vacancy_id | Slow-changing dimension по статусам/зарплате |

## Технические заметки

- ClickHouse `raw_json` хранится как `String` → используем `JSONExtractString`,
  `JSONExtract(..., 'Array(String)')`, `JSONExtract(raw_json_subtree, 'field', 'Nullable(Int64)')`
- Engine для silver: `MergeTree()` + partitioning по месяцам по `ingested_at`
  (бэйз-сетап как в bronze, для retention позже добавим TTL)
- materialized=`view` для staging — не плодим данные, читатели идут в silver
- В `_sources.yml` source-схема указана как `dpib` (= БД), а не `bronze` —
  потому что в нашей лабе всё в одной БД
