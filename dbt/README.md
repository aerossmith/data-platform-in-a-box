# dbt — трансформации bronze → silver → gold

dbt-проект поверх `dpib.bronze_hh_vacancies` (ClickHouse).
Не трогает ingest, не меняет docker-compose — только читает bronze и пишет
модели в отдельные базы `dpib_silver` и `dpib_gold`.

## Раскладка по базам в ClickHouse

| База | Что внутри | Кто пишет |
|---|---|---|
| `dpib`        | `bronze_hh_vacancies`, прикладные таблицы | DAG `hh_vacancies_snapshot` |
| `dpib_silver` | `stg_hh_vacancies` (view), `silver_hh_vacancies` (table) | dbt (этот проект) |
| `dpib_gold`   | `gold_skills_top`, остальные витрины | dbt (этот проект) |

Базы `dpib_silver` и `dpib_gold` создаются:
- автоматически при первом старте ClickHouse через `init/clickhouse/03_create_databases.sql`;
- вручную для уже работающего контейнера: `make clickhouse-bootstrap` (из корня репозитория).

## Поток данных

```
bronze_hh_vacancies          dpib            (append-only, raw_json, не трогаем)
        ↓
stg_hh_vacancies             dpib_silver     (view, парсит raw_json, унифицирует payload)
        ↓
silver_hh_vacancies          dpib_silver     (table, dedup по (snapshot_id, vacancy_id))
        ↓
gold_skills_top              dpib_gold       (топ навыков по latest snapshot)
```

## Установка (один раз)

```bash
# Из WSL внутри корня репозитория
cd dbt

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install dbt-core==1.8.* dbt-clickhouse==1.8.*

# Зависимости (dbt_utils для composite-unique теста)
dbt deps --profiles-dir .
```

## Запуск

```bash
cd dbt
source .venv/bin/activate

# Если базы dpib_silver/dpib_gold ещё не созданы (старый том ClickHouse):
#   из корня репозитория сначала
make clickhouse-bootstrap

# Профиль по умолчанию: localhost:8123, user=dpib, pass=dpib_pass.
# При желании переопредели:
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
-- Smoke: всё ли поднялось в правильных базах
SHOW DATABASES;
SHOW TABLES FROM dpib_silver;
SHOW TABLES FROM dpib_gold;

SELECT count() FROM dpib_silver.stg_hh_vacancies;
SELECT count() FROM dpib_silver.silver_hh_vacancies;
SELECT count() FROM dpib_gold.gold_skills_top;

-- silver: соблюдён ли grain?
SELECT count() AS rows, uniq((snapshot_id, vacancy_id)) AS unique_keys
FROM dpib_silver.silver_hh_vacancies;
-- rows == unique_keys → grain (snapshot_id, vacancy_id) уникален

-- Распределение качества payload
SELECT source_mode, detail_status, count()
FROM dpib_silver.silver_hh_vacancies
GROUP BY 1, 2 ORDER BY 3 DESC;

-- gold: топ-20 навыков
SELECT skill, vacancies_count, devops_count, platform_count, remote_count
FROM dpib_gold.gold_skills_top
ORDER BY vacancies_count DESC
LIMIT 20;
```

## Структура

```
dbt/
├── dbt_project.yml              ← проект, schema per layer (silver/gold)
├── profiles.yml                 ← подключение к ClickHouse через ENV
├── packages.yml                 ← dbt_utils
├── macros/
│   └── generate_schema_name.sql ← маппинг "silver" → dpib_silver, "gold" → dpib_gold
├── models/
│   ├── staging/
│   │   ├── _sources.yml         ← bronze в базе dpib
│   │   ├── _stg_hh_vacancies.yml
│   │   └── stg_hh_vacancies.sql ← view в dpib_silver
│   ├── silver/
│   │   ├── _silver_hh_vacancies.yml
│   │   └── silver_hh_vacancies.sql ← table в dpib_silver
│   └── gold/
│       └── gold_skills_top.sql  ← table в dpib_gold
└── README.md
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

- В dbt-clickhouse `schema` мапится на ClickHouse database. Чтобы получить
  именно `dpib_silver` / `dpib_gold` (а не дефолтный префикс `dpib_*`),
  переопределён макрос `generate_schema_name` — см. `macros/`.
- `raw_json` в bronze хранится как `String`, используем `JSONExtractString`,
  `JSONExtract(..., 'Array(String)')`, и т.д.
- Engine silver: `MergeTree()`, ключ `(snapshot_id, vacancy_id)`,
  партиционирование по месяцам по `ingested_at`.
- `materialized=view` для staging — не дублирует данные, читатели идут в silver.
