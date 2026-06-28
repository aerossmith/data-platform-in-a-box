# Data Platform in a Box — План проекта

Версия: 3.0
Дата: июнь 2026

---

## Цель

Локальная DWH/BI платформа в Docker Compose с реальной полезной нагрузкой,
поднимаемая одной командой. Используется как:

- рабочий стенд для аналитики реальных данных рынка труда
- фундамент для AI/DevOps-проектов (RAG, Text-to-SQL, AIOps)
- portfolio-демонстрация платформенных навыков (DevOps + Data)

---

## Текущий статус

| # | Этап | Статус |
|---|------|--------|
| 1 | Core — PostgreSQL + ClickHouse + Redis | ✅ готово |
| 2 | Monitoring — Prometheus + Grafana + Alertmanager + exporters | ✅ готово |
| 2.5 | CI — GitHub Actions, статический lint | ✅ готово |
| 3 | Orchestration — Airflow 3.x + ingest HH.ru → bronze | ✅ готово |
| 4 | Transform — dbt bronze → silver → gold, автозапуск из Airflow | ✅ готово |
| 5 | Grafana дашборды с реальными метриками CH + Airflow | 🔜 следующий |
| 6 | Superset — дашборд поверх gold-витрин | ⏳ |
| 7 | `make demo` — one-command deploy с данными | ⏳ |
| 8 | AI-слой — Qdrant + RAG поверх runbooks | ⏳ |
| 9 | AIOps-lite — Alertmanager → n8n → LLM → Telegram | ⏳ |
| 10 | Второй коннектор — HuggingFace | ⏳ |
| 11 | Финальный README, диаграмма, скриншоты | ⏳ |

---

## Что сделано (детали)

### Шаг 1 — Core

- PostgreSQL 16-alpine + ClickHouse 24-alpine + Redis 7-alpine
- Init-скрипты: схемы, тестовые данные, БД airflow_meta
- Healthcheck-и для всех сервисов
- Docker-профиль `core`

### Шаг 2 — Monitoring

- Prometheus 2.54 scrape: node-exporter, cAdvisor, postgres-exporter, ClickHouse :9363
- Grafana 11.2 с provisioning datasource + базовый дашборд
- Alertmanager 0.27 (пока null-receiver, вебхуки — шаг 9)
- StatsD-exporter в профиле `core` (принимает UDP от Airflow, отдаёт Prometheus)
- Docker-профиль `monitoring`

### Шаг 2.5 — CI

- GitHub Actions: `docker compose config`, `yamllint`, `xmllint`, `jq empty`, `sqlfluff`
- Без поднятия контейнеров, ~25 сек на каждый push/PR

### Шаг 3 — Orchestration (E + L)

- Airflow 3.0.3-python3.12, CeleryExecutor, 6 контейнеров
- DAG `hh_vacancies_snapshot` — cron `0 */4 * * *`
- 6 срезов параллельно: DevOps Engineer + Platform Engineer × msk + spb + remote
- curl_cffi с `impersonate=chrome120` — TLS fingerprint Chrome (обход ddos-guard)
- Warmup-flow: hh.ru/ → hh.ru/search/vacancy → API/HTML
- При 401/403 — HTML fallback через BeautifulSoup с detail-страницами
- Retry-safe вставка (SELECT существующих vacancy_id перед INSERT)
- Поля `source_mode` / `detail_status` / `quality_score` в raw_json
- Медальонная bronze-схема: `dpib.bronze_hh_vacancies`
- StatsD-метрики Airflow → Prometheus через statsd-exporter
- Docker-профиль `orchestration`

### Шаг 4 — Transform (T)

- dbt 1.8.9 + dbt-clickhouse 1.8.9
- Три базы ClickHouse: `dpib` (bronze) / `dpib_silver` / `dpib_gold`
- `stg_hh_vacancies` — view, JSONExtract + coalesce по source_mode/detail_status, quality_score
- `silver_hh_vacancies` — table, dedup по (snapshot_id, vacancy_id), matched_roles/areas
- `gold_skills_top` — table, топ навыков по latest-снапшоту вакансии
- Тесты: 25 проверок, PASS=25 WARN=0 ERROR=0
- DAG `dbt_hh_transform` — 8 задач: check_bronze → deps(кеш) → staging → test → silver → test → gold → check_silver
- Автозапуск через `TriggerDagRunOperator` после `hh_vacancies_snapshot`
- Кеш dbt_packages в `/tmp/dbt-packages-cache` (~5 сек вместо 2-3 мин)
- dbt монтируется `:ro`, проект копируется в `/tmp` без .venv (~145 МБ)

---

## Что дальше

### Шаг 5 — Grafana дашборды (следующий)

Данные уже идут в Prometheus — нужны дашборды:

**ClickHouse health:**
- `ClickHouseMetrics_Query` — активные запросы
- `ClickHouseProfileEvents_InsertedRows` — скорость вставки
- `ClickHouseAsyncMetrics_TotalPartsOfMergeTreeTables` — количество parts
- размер таблиц по слоям (bronze/silver/gold) — через SQL-datasource

**Airflow health:**
- `airflow_dag_*_duration` — время выполнения DAG-ов
- `airflow_operator_failures_*` — падения по типу оператора
- `airflow_scheduler_heartbeat` — живость планировщика
- `airflow_celery_*` — состояние очереди

### Шаг 6 — Superset

- Docker-профиль `viz`
- Подключение к ClickHouse (dpib_gold)
- Дашборд: топ навыков, динамика по снапшотам, разбивка по ролям/регионам

### Шаг 7 — `make demo`

- One-command: `make demo`
- Поднимает стек → ждёт healthcheck → первый snapshot → dbt build → открывает Superset
- Время: 3-5 минут с нуля

### Шаг 8 — AI-слой (Qdrant + RAG)

- Qdrant в профиле `ai`
- Индексация: `runbooks/`, `dbt/README.md`, `orchestration/README.md`
- Первый RAG-агент для ответов на вопросы об инфраструктуре
- Связь с проектом `devops-agent-from-chatgpt`

### Шаг 9 — AIOps-lite

- Alertmanager → webhook → n8n (уже в облаке)
- n8n → LLM → анализ алерта → Telegram
- Первый реальный сигнал: CPU spike или упавший DAG

### Шаг 10 — HuggingFace коннектор

- Второй источник: `huggingface.co/api/models`
- Тот же bronze/silver/gold принцип
- Multi-source DWH для портфолио

### Шаг 11 — Финальный README

- Диаграмма архитектуры (Mermaid или draw.io)
- Скриншоты: Grafana, Superset, Airflow UI, Qdrant
- Описание для резюме

---

## Домены данных

### HH.ru (основной)

API: `api.hh.ru` — публичный, без авторизации.
Snapshot каждые 4 часа через Airflow + HTML fallback при блокировке.

Что собираем: вакансии DevOps/Platform Engineer по Москве, СПб, удалёнке.
Почему: доступно из РФ, релевантно лично, объём >10k вакансий, естественный лайфсайкл.

### HuggingFace (шаг 10)

API: `huggingface.co/api/models` — публичный.
Топ-N моделей с метаданными (downloads, likes, теги, тип задачи).
Тренды популярности LLM, динамика релизов.

---

## Стек

| Слой | Инструмент | Профиль |
|---|---|---|
| OLTP | PostgreSQL 16 | core |
| OLAP | ClickHouse 24 | core |
| Broker | Redis 7 | core |
| Metrics relay | StatsD-exporter | core |
| Orchestration | Airflow 3.x + Celery | orchestration |
| Transform | dbt 1.8 + dbt-clickhouse | (из Airflow) |
| Monitoring | Prometheus + Grafana + Alertmanager | monitoring |
| Exporters | node-exporter + cAdvisor + pg-exporter | monitoring |
| Visualization | Superset | viz (шаг 6) |
| Vector DB | Qdrant | ai (шаг 8) |

---

## Связь с другими проектами

- **GitLab CI + dbt** — dbt-модели отсюда переносятся в корпоративный GitLab с полным CI/CD
- **Runbook-агент (RAG)** — `runbooks/` индексируются в Qdrant, агент отвечает на вопросы по инфраструктуре
- **Text-to-SQL бот** — бот делает запросы к gold-витринам ClickHouse, schema отсюда — контекст для LLM
- **AIOps-lite** — Alertmanager шлёт webhook в n8n, LLM анализирует с RAG из runbooks
