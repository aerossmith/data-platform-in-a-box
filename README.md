# Data Platform in a Box

![CI](https://github.com/aerossmith/data-platform-in-a-box/actions/workflows/ci.yml/badge.svg)

Локальная DWH/BI платформа в Docker Compose — единый стек для аналитики
и фундамент для AI/DevOps-проектов. С реальными данными (HH.ru вакансии),
медальонной архитектурой и one-command demo.

Подробный план: [`PROJECT_PLAN.md`](./PROJECT_PLAN.md).

---

## Текущий статус

**Шаг 3 из 10: добавлен профиль `orchestration` (Airflow 3.x + Celery)** 🔥

Готово:

- `core` — PostgreSQL 16 + ClickHouse 24 + Redis 7
- `monitoring` — Prometheus + Grafana + Alertmanager + node-exporter + cAdvisor + postgres-exporter
- `orchestration` — Airflow 3.x с CeleryExecutor (api-server + scheduler + dag-processor + triggerer + worker)
- Первый DAG: `hh_vacancies_snapshot` (HH.ru → bronze в ClickHouse, каждые 4 часа)
- Bronze-слой в ClickHouse под медальонную архитектуру
- GitHub Actions CI: статический lint (compose / YAML / XML / JSON / SQL)

Дальше: dbt → Superset → `make demo` → AI-слой.

---

## Быстрый старт

### Требования

- Docker Desktop (Windows/macOS) или Docker Engine (Linux)
- Make
- RAM: ~5 GB для `core+monitoring`, ~9 GB для полного стека с Airflow

### Запуск

```bash
cp .env.example .env

# базовый стек (БД + Redis + мониторинг)
make up PROFILES="core monitoring"

# + Airflow
make up PROFILES="core monitoring orchestration"

# проверить
make status
```

Первый старт Airflow занимает 2-3 минуты — нужно проинициализировать БД
и подтянуть `clickhouse-connect` через `_PIP_ADDITIONAL_REQUIREMENTS`.

### Что доступно

| Сервис         | URL / адрес                  | Учётка                          |
|---------------|------------------------------|---------------------------------|
| PostgreSQL    | `localhost:5432`             | `dpib` / `dpib_pass` (БД `dpib`) |
| ClickHouse    | http://localhost:8123        | `dpib` / `dpib_pass`            |
| Redis         | `localhost:6379`             | без пароля                      |
| Prometheus    | http://localhost:9090        | без авторизации                 |
| Grafana       | http://localhost:3000        | `admin` / `admin`               |
| Alertmanager  | http://localhost:9093        | без авторизации                 |
| **Airflow UI**| http://localhost:8080        | `admin` / `admin`               |

### Проверка работы DAG

```bash
make airflow-list-dags                              # должен показать hh_vacancies_snapshot
make airflow-trigger DAG=hh_vacancies_snapshot      # запустить вручную (не дожидаясь расписания)
make airflow-logs                                   # tail логов всех Airflow-сервисов

# проверить, что вакансии пришли в bronze
make clickhouse-cli
# > SELECT count() FROM bronze_hh_vacancies;
# > SELECT search_text, search_area, count() FROM bronze_hh_vacancies GROUP BY 1,2;
```

### HH ingest: API + HTML fallback

`hh_vacancies_snapshot` сначала пробует публичный API HH. Если API возвращает `401/403`, DAG не уходит в пустой
зелёный `skip`, а переключается на HTML fallback по странице поиска и пишет найденные карточки в
`bronze_hh_vacancies.raw_json` с `source_mode="html_fallback"`.

Проектные детали: [`orchestration/README.md`](./orchestration/README.md).
Обезличенный операционный сценарий: [`runbooks/external-api-blocked-html-fallback.md`](./runbooks/external-api-blocked-html-fallback.md).

---

## Полезные команды

```bash
make help                                            # список всех команд
make up PROFILES="core monitoring orchestration"     # полный стек
make down PROFILES="core monitoring orchestration"   # стоп
make status                                          # статусы
make logs SVC=clickhouse                             # логи одного сервиса
make airflow-logs                                    # tail Airflow (scheduler+worker+dag-processor)
make psql / clickhouse-cli / redis-cli               # клиенты БД
make airflow-trigger DAG=...                         # запуск DAG вручную
make reset                                           # ОПАСНО: удалит тома и данные
```

---

## Структура репозитория

```
data-platform-in-a-box/
├── .github/workflows/ci.yml            ← GitHub Actions: статический lint
├── docker-compose.yml
├── .env.example
├── Makefile
├── README.md
├── PROJECT_PLAN.md
├── init/
│   ├── postgres/
│   │   ├── 01_init.sql                 ← analytics-схема + тестовые данные
│   │   └── 02_airflow.sql              ← БД airflow_meta + пользователь
│   └── clickhouse/
│       ├── 01_init.sql                 ← events_analytics (демо)
│       └── 02_bronze_schema.sql        ← bronze_hh_vacancies, bronze_hh_employers
├── config/
│   ├── clickhouse/{prometheus.xml, network.xml}
│   ├── prometheus/{prometheus.yml, alerts.yml}
│   ├── alertmanager/alertmanager.yml
│   └── grafana/{provisioning, dashboards}
├── orchestration/                      ← новый каталог (шаг 3)
│   ├── README.md
│   ├── requirements.txt                ← clickhouse-connect, requests
│   ├── dags/
│   │   └── hh_vacancies_snapshot.py    ← snapshot HH.ru → bronze
│   ├── plugins/
│   └── logs/
└── docs/                               ← TBD: runbooks для будущего RAG
```

---

## Что мониторим

| Источник           | Что отдаёт                                          |
|--------------------|----------------------------------------------------|
| node-exporter      | CPU, RAM, диск, сеть хоста (WSL VM на Windows)      |
| cAdvisor           | CPU, RAM, IO, сеть по каждому контейнеру            |
| postgres-exporter  | подключения, размер БД, репликация, locks           |
| ClickHouse `:9363` | parts, merges, query duration, replication queue    |

Базовые алерты в `config/prometheus/alerts.yml`. В шаге AIOps (этап 8)
подключим webhook в n8n → LLM → Telegram.

---

## CI / CD

На каждый push в `main` и каждый pull request — статический lint:
`docker compose config`, `yamllint`, `xmllint`, `jq empty`, `sqlfluff`.

Без подъёма контейнеров, ~20–30 секунд. Smoke-тест локально через `make up`.

---

## Что дальше

**Шаг 4** — dbt для трансформаций bronze → silver → gold + тесты качества данных.
Будут готовые gold-витрины (медианы зарплат, топ-навыки, активные работодатели)
поверх данных, которые уже льются через Airflow.

Полный план — в [`PROJECT_PLAN.md`](./PROJECT_PLAN.md).
