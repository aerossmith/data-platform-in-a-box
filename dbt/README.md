# dbt — трансформации bronze → silver → gold

dbt-проект поверх `dpib.bronze_hh_vacancies` (ClickHouse).
Не трогает ingest, не меняет docker-compose. Читает bronze,
пишет модели в отдельные базы `dpib_silver` и `dpib_gold`.

---

## Раскладка по базам ClickHouse

| База | Что внутри | Кто пишет |
|---|---|---|
| `dpib` | `bronze_hh_vacancies` | DAG `hh_vacancies_snapshot` |
| `dpib_silver` | `stg_hh_vacancies` (view), `silver_hh_vacancies` (table) | dbt |
| `dpib_gold` | `gold_skills_top` (table) | dbt |

Базы создаются при первом старте ClickHouse через `init/clickhouse/03_create_databases.sql`.
Для уже работающего контейнера: `make clickhouse-bootstrap`.

---

## Поток данных

```
bronze_hh_vacancies   dpib        append-only, raw_json (не трогаем)
        |
stg_hh_vacancies      dpib_silver view: JSONExtract + quality_score + has_detail
        |
silver_hh_vacancies   dpib_silver table: dedup по (snapshot_id, vacancy_id)
        |
gold_skills_top       dpib_gold   table: топ навыков по latest-снапшоту
```

---

## Локальная разработка

```bash
cd dbt
python3 -m venv .venv && source .venv/bin/activate
pip install dbt-core==1.8.* dbt-clickhouse==1.8.*
dbt deps --profiles-dir .
dbt debug --profiles-dir .   # проверка соединения
dbt build --profiles-dir .   # run + test за один шаг
```

Профиль по умолчанию: `localhost:8123`, user=`dpib`, pass=`dpib_pass`.
При запуске из Airflow переменные `DPIB_CLICKHOUSE_*` уже выставлены.

---

## Запуск через Airflow

DAG `dbt_hh_transform` запускается автоматически после `hh_vacancies_snapshot`.
Можно запустить вручную: `make airflow-trigger DAG=dbt_hh_transform`.

Кеш dbt_packages живёт в `/tmp/dbt-packages-cache` внутри воркер-контейнера.
Первый запуск ~2-3 мин (скачивает dbt_utils), последующие ~5 сек.

---

## Проверка результатов

```bash
make clickhouse-cli
```

```sql
-- Grain silver соблюдён?
SELECT count() AS rows, uniq((snapshot_id, vacancy_id)) AS unique_keys
FROM dpib_silver.silver_hh_vacancies;
-- rows должно == unique_keys

-- Распределение качества payload
SELECT source_mode, detail_status, count()
FROM dpib_silver.silver_hh_vacancies
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Топ-20 навыков
SELECT skill, vacancies_count, devops_count, platform_count, remote_count
FROM dpib_gold.gold_skills_top
ORDER BY vacancies_count DESC LIMIT 20;
```

---

## Структура

```
dbt/
├── dbt_project.yml              schema per layer: silver / gold
├── profiles.yml                 DPIB_CLICKHOUSE_* env vars
├── packages.yml                 dbt_utils (unique_combination_of_columns)
├── macros/
│   └── generate_schema_name.sql silver → dpib_silver, gold → dpib_gold
└── models/
    ├── staging/
    │   ├── _sources.yml         bronze в базе dpib + data_tests
    │   ├── _stg_hh_vacancies.yml
    │   └── stg_hh_vacancies.sql JSONExtract + coalesce по source_mode/detail_status
    ├── silver/
    │   ├── _silver_hh_vacancies.yml
    │   └── silver_hh_vacancies.sql  row_number() + matched_roles/areas
    └── gold/
        └── gold_skills_top.sql  argMax + ARRAY JOIN по skills
```

---

## Ключевые решения

**Grain:**
- bronze: `(snapshot_id, search_text, search_area, vacancy_id, page_num)` — append-only
- silver: `(snapshot_id, vacancy_id)` — dedup внутри снапшота, тест `unique_combination_of_columns`
- gold: `vacancy_id` (latest snapshot per vacancy)

**quality_score в staging:**
```
4 — html_fallback + detail_status=ok     (description, skills доступны)
3 — html_fallback + detail_status=captcha (только карточные поля)
2 — html_fallback + другой статус
1 — api (на момент написания API закрыт ddos-guard)
0 — неизвестно
```

**Выбор лучшей строки в silver:**
`ROW_NUMBER() OVER (PARTITION BY snapshot_id, vacancy_id ORDER BY quality_score DESC, description_length DESC, ingested_at DESC)`

**Маппинг баз через макрос:**
`generate_schema_name.sql` переопределяет стандартный dbt-маппинг.
`+schema: silver` → `dpib_silver`, `+schema: gold` → `dpib_gold`.

---

## Roadmap

| Шаг | Что | Зачем |
|---|---|---|
| 1 | `gold_vacancies_latest` | Single source of truth — latest snapshot per vacancy |
| 2 | `gold_salary_by_role` | Медианы зарплат по ролям и регионам |
| 3 | `gold_employer_top` | Топ работодателей, retention вакансий |
| 4 | NER/keywords по description | skills=[] у большинства вакансий |
| 5 | DAG `dbt_hh_transform` в K8s | Helm + DockerOperator вместо BashOperator |
