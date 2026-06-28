# Data Platform in a Box

![CI](https://github.com/aerossmith/data-platform-in-a-box/actions/workflows/ci.yml/badge.svg)

Локальная DWH/BI платформа в Docker Compose — единый стек для аналитики
и фундамент для AI/DevOps-проектов. Реальные данные (HH.ru вакансии),
медальонная архитектура bronze → silver → gold, полный мониторинг,
автоматический ELT-конвейер.

Подробный план: [PROJECT_PLAN.md](./PROJECT_PLAN.md).

---

## Текущий статус

**Шаг 4 из 10: ELT-конвейер полностью замкнут** ✅

| Слой | Что сделано |
|---|---|
| Инфраструктура | PostgreSQL 16 + ClickHouse 24 + Redis 7 + StatsD-exporter |
| Мониторинг | Prometheus + Grafana + Alertmanager + node-exporter + cAdvisor + postgres-exporter |
| Оркестрация | Airflow 3.x с CeleryExecutor (6 контейнеров) |
| Ingest (E+L) | DAG `hh_vacancies_snapshot`: HH.ru → `bronze_hh_vacancies` каждые 4 часа |
| Transform (T) | DAG `dbt_hh_transform`: bronze → silver → gold (запускается автоматически) |
| Медальонный DWH | Три базы ClickHouse: `dpib` / `dpib_silver` / `dpib_gold` |
| Метрики | Airflow через StatsD → Prometheus; ClickHouse через встроенный `:9363` |
| CI | GitHub Actions: статический lint (~25 сек) |

Следующее: Grafana дашборды с реальными метриками ClickHouse и Airflow.

---

## Как это работает

### ELT-цепочка

```
HH.ru (каждые 4 часа, cron: 0 */4 * * *)
    │
    ▼  hh_vacancies_snapshot
    ├── make_snapshot_id        UUID на весь батч
    ├── search_params           6 комбинаций: 2 роли × 3 региона
    ├── ingest_combo[0..5]      параллельный fetch + HTML fallback + insert в bronze
    ├── report_total            сводка по батчу
    └── trigger_dbt_transform ──► dbt_hh_transform (schedule=None, только триггер)
                                      │
                                      ├── check_bronze     проверка наличия данных
                                      ├── dbt_deps         пакеты из кеша (~5 сек)
                                      ├── dbt_run_staging  stg_hh_vacancies (view)
                                      ├── dbt_test_staging not_null, accepted_values
                                      ├── dbt_run_silver   silver_hh_vacancies (table)
                                      ├── dbt_test_silver  grain уникальность
                                      ├── dbt_run_gold     gold_skills_top (table)
                                      └── check_silver     контрольный SELECT + grain
```

### Медальонная архитектура

```
База ClickHouse   Объект                  Тип    Что внутри
────────────────  ──────────────────────  ─────  ───────────────────────────────────────
dpib              bronze_hh_vacancies     table  raw_json, append-only, не модифицируем
dpib_silver       stg_hh_vacancies        view   JSONExtract + quality_score + has_detail
dpib_silver       silver_hh_vacancies     table  dedup по (snapshot_id, vacancy_id)
dpib_gold         gold_skills_top         table  топ навыков по latest-снапшоту
```

---

## Быстрый старт

### Требования

- Docker Desktop (Windows/macOS) или Docker Engine (Linux)
- Make
- RAM: ~5 ГБ для `core + monitoring`, ~9 ГБ для полного стека

### Запуск

```bash
cp .env.example .env
make up PROFILES="core monitoring orchestration"
make status

# Если ClickHouse уже работал — создать dbt-базы вручную
make clickhouse-bootstrap
```

Первый старт Airflow занимает 3-5 минут (установка пакетов).

### Доступ к сервисам

| Сервис | URL | Учётка |
|---|---|---|
| PostgreSQL | `localhost:5432` | `dpib` / `dpib_pass` |
| ClickHouse | http://localhost:8123 | `dpib` / `dpib_pass` |
| Redis | `localhost:6379` | без пароля |
| StatsD exporter | http://localhost:9102/metrics | без авторизации |
| Prometheus | http://localhost:9090 | без авторизации |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Alertmanager | http://localhost:9093 | без авторизации |
| Airflow UI | http://localhost:8080 | без авторизации (lab-режим) |

### Запуск конвейера

```bash
# Ingest — dbt_hh_transform запустится автоматически следом
make airflow-trigger DAG=hh_vacancies_snapshot

# Только трансформация
make airflow-trigger DAG=dbt_hh_transform

# Логи
make airflow-logs
```

### Проверка данных

```bash
make clickhouse-cli
```

```sql
-- Bronze: количество строк и снапшотов
SELECT count(), uniqExact(snapshot_id) AS snapshots
FROM dpib.bronze_hh_vacancies;

-- Bronze: разбивка по ролям и регионам
SELECT search_text, search_area, count()
FROM dpib.bronze_hh_vacancies
GROUP BY 1, 2 ORDER BY 1, 2;

-- Silver: grain соблюдён?
SELECT count() AS rows, uniq((snapshot_id, vacancy_id)) AS unique_keys
FROM dpib_silver.silver_hh_vacancies;
-- rows должно == unique_keys

-- Silver: качество payload
SELECT source_mode, detail_status, count()
FROM dpib_silver.silver_hh_vacancies
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Gold: топ-20 навыков
SELECT skill, vacancies_count, devops_count, platform_count, remote_count
FROM dpib_gold.gold_skills_top
ORDER BY vacancies_count DESC LIMIT 20;
```

---

## Структура репозитория

```
data-platform-in-a-box/
├── .github/workflows/ci.yml
├── .yamllint
├── docker-compose.yml                  ← профили: core / monitoring / orchestration
├── .env.example
├── Makefile
├── README.md
├── PROJECT_PLAN.md
├── init/
│   ├── postgres/
│   │   ├── 01_init.sql
│   │   └── 02_airflow.sql              ← БД airflow_meta
│   └── clickhouse/
│       ├── 01_init.sql
│       ├── 02_bronze_schema.sql        ← bronze_hh_vacancies
│       └── 03_create_databases.sql     ← dpib_silver, dpib_gold
├── config/
│   ├── clickhouse/{prometheus.xml, network.xml}
│   ├── prometheus/{prometheus.yml, alerts.yml}
│   ├── alertmanager/alertmanager.yml
│   └── grafana/{provisioning/, dashboards/}
├── orchestration/
│   ├── README.md
│   ├── dags/
│   │   ├── hh_vacancies_snapshot.py    ← E+L: HH.ru → bronze
│   │   └── dbt_hh_transform.py        ← T: bronze → silver → gold
│   └── logs/
└── dbt/
    ├── README.md
    ├── dbt_project.yml
    ├── profiles.yml                    ← DPIB_CLICKHOUSE_* env vars
    ├── packages.yml
    ├── macros/generate_schema_name.sql ← silver→dpib_silver, gold→dpib_gold
    └── models/
        ├── staging/{_sources.yml, _stg_hh_vacancies.yml, stg_hh_vacancies.sql}
        ├── silver/{_silver_hh_vacancies.yml, silver_hh_vacancies.sql}
        └── gold/gold_skills_top.sql
```

---

## Мониторинг

| Источник | Порт | Ключевые метрики |
|---|---|---|
| node-exporter | 9100 | CPU, RAM, диск, сеть хоста |
| cadvisor | 8080 | CPU/RAM/IO по каждому контейнеру |
| postgres-exporter | 9187 | подключения, locks, размер БД |
| clickhouse | 9363 | parts, merges, inserted_rows, query_duration |
| statsd-exporter | 9102 | dag_duration, task_duration, operator_failures, celery |

`statsd-exporter` входит в профиль `core` — доступен без `monitoring`.

---

## CI / CD

На каждый push в `main` и каждый PR — статический lint без поднятия контейнеров:
`docker compose config`, `yamllint`, `xmllint`, `jq empty`, `sqlfluff`. Время ~25 сек.

---

## HH.ru ingest: технические детали

`api.hh.ru` закрыт ddos-guard. DAG использует `curl_cffi` с `impersonate=chrome120`
и двухступенчатый warmup (главная → страница поиска → API). При `401/403`
переключается на HTML-парсинг `hh.ru/search/vacancy` через BeautifulSoup
с опциональной загрузкой detail-страниц (описание, навыки, опыт).

### Настройки парсера (через `.env`)

```env
DPIB_HH_IMPERSONATE=chrome120
DPIB_HH_HTML_FALLBACK_MAX_PAGES=5
DPIB_HH_FETCH_DETAILS=true
DPIB_HH_DETAIL_MAX_PER_COMBO=100
DPIB_HH_DETAIL_PAUSE_MIN_SEC=0.2
DPIB_HH_DETAIL_PAUSE_MAX_SEC=0.6
```

### Поля raw_json по source_mode

| `source_mode` | `detail_status` | Доступные поля |
|---|---|---|
| `html_fallback` | `ok` | `description`, `skills`, `experience`, `employment`, `salary_detail` |
| `html_fallback` | `captcha` | `card_text`, `experience_card`, `work_format_card` |
| `html_fallback` | `not_fetched_limit` | только карточные поля |
| `api` | — | `name`, `employer.name`, `salary.from/to`, ... |

Подробнее: [orchestration/README.md](./orchestration/README.md).

---

## Что дальше

**Шаг 5** — Grafana дашборды с реальными метриками ClickHouse и Airflow.
Минимум два дашборда: здоровье ClickHouse и здоровье Airflow.

Полный план: [PROJECT_PLAN.md](./PROJECT_PLAN.md).
