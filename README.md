# Data Platform in a Box

Локальная DWH/BI платформа в Docker Compose — единый стек для аналитики и фундамент для AI/DevOps-проектов.

Подробный план: [`PROJECT_PLAN.md`](./PROJECT_PLAN.md).

---

## Текущий статус

**Шаг 2 из 10: профили `core` + `monitoring`** ✅

- `core` — PostgreSQL 16, ClickHouse 24
- `monitoring` — Prometheus, Grafana, Alertmanager, node-exporter, cAdvisor, postgres-exporter
- Включены встроенные prometheus-метрики ClickHouse
- Готовый дашборд DPIB Overview + базовые алерты

Дальше по плану: оркестрация (Airflow) → визуализация (Superset) → AI-слой (Qdrant).

---

## Быстрый старт

### Требования

- Docker Desktop с включённой WSL-интеграцией (Windows) или Docker Engine (Linux)
- Make
- ~5–6 GB свободной RAM для `core` + `monitoring`

### Запуск

```bash
cp .env.example .env

# поднять core + monitoring (рекомендуется)
make up PROFILES="core monitoring"

# или только базы
make up PROFILES="core"

# проверить
make status
```

Через 20–30 секунд все контейнеры должны быть в статусе `Up` / `healthy`.

### Что доступно

| Сервис         | URL / адрес                  | Учётка                          |
|---------------|------------------------------|---------------------------------|
| PostgreSQL    | `localhost:5432`             | `dpib` / `dpib_pass`            |
| ClickHouse    | http://localhost:8123        | `dpib` / `dpib_pass`            |
| Prometheus    | http://localhost:9090        | без авторизации                 |
| Grafana       | http://localhost:3000        | `admin` / `admin`               |
| Alertmanager  | http://localhost:9093        | без авторизации                 |
| ClickHouse metrics | http://localhost:9363/metrics | без авторизации            |

### Проверки

```bash
# дашборд DPIB Overview подтянется автоматически в папке "DPIB"
open http://localhost:3000

# Prometheus — посмотреть targets
open http://localhost:9090/targets   # все должны быть UP

# тестовые данные в БД
make psql                # → SELECT * FROM analytics.users;
make clickhouse-cli      # → SELECT * FROM events_analytics;
```

### Импорт готовых дашбордов (рекомендуется)

В Grafana → Dashboards → New → Import → ввести ID:

| ID    | Что                              |
|-------|----------------------------------|
| 1860  | Node Exporter Full               |
| 14282 | cAdvisor (контейнеры)            |
| 9628  | PostgreSQL Database              |
| 14192 | ClickHouse (community)           |

Datasource — `Prometheus` (уже добавлен через provisioning).

---

## Полезные команды

```bash
make help                              # список всех команд
make up PROFILES="core monitoring"     # старт
make up PROFILES="core"                # только базы
make down PROFILES="core monitoring"   # стоп
make status                            # статусы
make logs                              # логи всех
make logs SVC=prometheus               # логи одного сервиса
make restart                           # перезапуск
make psql                              # клиент PostgreSQL
make clickhouse-cli                    # клиент ClickHouse
make reset                             # ОПАСНО: удалит тома и данные
```

---

## Структура репозитория

```
data-platform-in-a-box/
├── docker-compose.yml
├── .env.example
├── Makefile
├── README.md
├── PROJECT_PLAN.md
├── init/
│   ├── postgres/01_init.sql
│   └── clickhouse/01_init.sql
├── config/
│   ├── clickhouse/prometheus.xml        ← включает метрики в ClickHouse
│   ├── prometheus/
│   │   ├── prometheus.yml               ← targets для скрапинга
│   │   └── alerts.yml                   ← правила алертов
│   ├── alertmanager/alertmanager.yml
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/datasources.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/dpib-overview.json
└── docs/                                ← TBD: runbooks для будущего RAG
```

---

## Что мониторим

| Источник           | Что отдаёт                                          |
|--------------------|----------------------------------------------------|
| node-exporter      | CPU, RAM, диск, сеть хоста (WSL VM на Windows)      |
| cAdvisor           | CPU, RAM, IO, сеть по каждому контейнеру            |
| postgres-exporter  | подключения, размер БД, репликация, locks           |
| ClickHouse `:9363` | parts, merges, query duration, replication queue    |

### Базовые алерты (config/prometheus/alerts.yml)

- ServiceDown (любой сервис недоступен > 1 минуты)
- HighCpuUsage / HighMemoryUsage / DiskSpaceLow на хосте
- PostgresDown / PostgresTooManyConnections
- ClickHouseTooManyParts / ClickHouseReplicationLag

Сейчас алерты идут в null-receiver Alertmanager (видны только в UI). В шаге №5 (AIOps) подключим webhook в n8n → LLM → Telegram.

---

## Что дальше

**Шаг 3** — Airflow для оркестрации. Готовый DAG, который перекладывает данные из PostgreSQL в ClickHouse через staging. Это база для будущего dbt-pipeline (проект №2 из общего roadmap).

Полный план — в [`PROJECT_PLAN.md`](./PROJECT_PLAN.md).
# data-platform-in-a-box
