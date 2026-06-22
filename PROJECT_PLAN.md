# Data Platform in a Box — План проекта

Версия: 2.0
Дата: июнь 2026
Изменения от v1.0: зафиксированы домены данных (HH.ru, HuggingFace),
введена медальонная архитектура bronze/silver/gold, переформулированы шаги 3–4
с акцентом на полезной нагрузке и one-command demo.

---

## Цель

Локальная DWH/BI платформа в Docker Compose с **реальной полезной нагрузкой**,
поднимаемая одной командой. Используется как:

- рабочий стенд для аналитики реальных данных
- фундамент для AI/DevOps-проектов (RAG, Text-to-SQL, AIOps)
- portfolio-демонстрация платформенных навыков

---

## Домены данных

### Домен №1 (основной): HH.ru — рынок труда в IT

API: `api.hh.ru` — публичный, без авторизации, без VPN. Метод `/vacancies`
с фильтрами по специализации, региону, опыту, плюс справочники `/dictionaries`
и работодатели через `/employers`.

**Что собираем:**

- Открытые вакансии по DevOps, SRE, Platform Engineer, Data Engineer, ML Engineer
- Регионы: Москва, СПб, удалёнка, плюс топ-городов
- Фильтры опыта, занятости, графика работы
- Работодатели с метаинформацией

**Подход — snapshot.** Каждые 30 минут Airflow тянет свежую страницу выдачи,
складывает raw JSON в bronze. История строится естественно: со временем
видно, какие вакансии висят долго, какие быстро закрываются, как меняется
медиана по неделям, какие навыки растут в спросе.

**Почему именно этот домен:**

- доступ напрямую из РФ, никаких прокси
- релевантен лично — параллельный мониторинг своего же рынка
- на собеседовании естественный ответ «зачем именно это»
- объём приличный: ~10k+ DevOps-вакансий в любой момент

### Домен №2 (вторичный, после стабилизации №1): HuggingFace — каталог моделей

API: `huggingface.co/api/models`. Открытый, без VPN. Та же snapshot-схема —
раз в час слепок топ-N моделей с метаданными (downloads, likes, теги, тип задачи,
автор, базовая модель-родитель). История даёт тренды популярности LLM,
динамику новых релизов, активность авторов.

**Почему вторым:**

- демонстрирует, что платформа multi-source (важно для портфолио)
- релевантно LLMOps-позиционированию (второй пет-проект)
- та же архитектура — те же bronze/silver/gold принципы

---

## Медальонная архитектура

Стандарт индустрии — три слоя с явным разделением ответственности.

### Bronze — сырые данные «как пришли»

Принцип неизменности: что API отдал, то и легло. Никогда не удаляем,
никогда не модифицируем. Источник правды.

```sql
CREATE TABLE bronze_hh_vacancies (
    ingested_at  DateTime DEFAULT now(),
    snapshot_id  UUID,                     -- ID запуска DAG
    source       LowCardinality(String),   -- 'hh.ru'
    raw_json     String,                   -- весь объект как пришёл
    vacancy_id   String                    -- из raw для быстрого поиска
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, vacancy_id);
```

### Silver — нормализованные данные

Парсинг JSON, типизация, ETL. Идемпотентность через ReplacingMergeTree —
если запустить пайплайн дважды, дублей не появится.

```sql
CREATE TABLE silver_vacancies (
    vacancy_id      String,
    snapshot_at     DateTime,
    title           String,
    company_id      String,
    company_name    String,
    salary_from     Nullable(Decimal(12,2)),
    salary_to       Nullable(Decimal(12,2)),
    salary_currency LowCardinality(String),
    experience      LowCardinality(String),
    employment      LowCardinality(String),
    area_name       LowCardinality(String),
    published_at    DateTime,
    skills          Array(String),         -- распарсенные из описания
    description     String,
    raw_ingested_at DateTime                -- для версионирования
) ENGINE = ReplacingMergeTree(raw_ingested_at)
ORDER BY (vacancy_id, snapshot_at);
```

### Gold — аналитические витрины (через dbt)

Агрегаты, тренды, ranking. Готово к подключению в Superset.

Примеры моделей:

- `mart_salary_by_role_week` — медиана зарплат по ролям и неделям
- `mart_skills_demand` — ранжирование навыков по упоминаниям в описаниях
- `mart_employers_activity` — топ-работодатели по активности найма
- `mart_vacancy_lifetime` — сколько живёт вакансия до закрытия
- `mart_remote_vs_office` — динамика удалёнки vs офис по неделям

---

## Состав стека

| Слой | Инструмент | Назначение |
|------|-----------|-----------|
| Core OLTP | PostgreSQL 16 | Метаданные Airflow, состояние агентов |
| Core OLAP | ClickHouse 24 | Все три слоя bronze/silver/gold |
| Оркестрация | Airflow 2.9+ | Ingest, нормализация, dbt run |
| Трансформации | dbt + ClickHouse | Gold-витрины с тестами качества |
| Визуализация | Superset | Дашборд из коробки |
| Мониторинг | Prometheus + Grafana + Alertmanager | Observability стека (наш USP) |
| Экспортёры | node-exporter, cAdvisor, postgres-exporter | Метрики хоста, контейнеров, БД |
| AI-слой | Qdrant | Векторная база для RAG (этап AI) |
| Прокси | Traefik | Единая точка входа |

---

## Что НЕ берём (отличия от эталонной статьи)

| Инструмент | Почему пропускаем |
|-----------|-------------------|
| Apache Kafka | Snapshot-подход через REST API не требует стриминга. Kafka вернётся на этапе AIOps если будет нужен event-bus для алертов |
| Apache Spark | Объёмы лабораторные, ClickHouse сам считает rolling window через оконные функции на порядки быстрее. Spark оправдан при горизонтальном масштабировании, которого здесь не будет |
| MinIO | Промежуточное S3-хранилище избыточно для REST-данных. ClickHouse напрямую складывает в bronze |

### Что добавляем своего (чего нет в эталонной статье)

- **Полноценный мониторинг и алертинг стека** — Prometheus, Grafana, Alertmanager, экспортёры. Это наш USP для DevOps/Platform-позиционирования
- **AI-слой** — Qdrant и RAG-агент поверх runbook-документации
- **CI/CD на GitHub Actions** — статические проверки (compose syntax, yamllint, xmllint, jq, sqlfluff)

---

## Структура запуска

```bash
# базовая инфраструктура (то что уже работает)
make up PROFILES="core monitoring"

# полный стек с данными (one-command demo)
make demo
```

`make demo` поднимает стек, ждёт healthcheck-и, накатывает первый snapshot
вакансий, прогоняет dbt build, регистрирует datasource в Superset и
загружает готовый дашборд. Через 5 минут после команды — рабочая платформа
с реальными данными. Это критичный аспект для презентации в портфолио.

---

## Этапы (актуальная очерёдность)

| # | Этап | Статус |
|---|------|--------|
| 1 | Core (PostgreSQL + ClickHouse) | ✅ |
| 2 | Monitoring (Prometheus + Grafana + экспортёры) | ✅ |
| 2.5 | CI на GitHub Actions (статический lint) | ✅ |
| 3 | **Orchestration** — Airflow + ingest HH.ru + bronze/silver | 🔜 следующий |
| 4 | **dbt** — gold-витрины + тесты качества | ⏳ |
| 5 | **Viz** — Superset с автонастроенным дашбордом | ⏳ |
| 6 | `make demo` — one-command deploy с данными | ⏳ |
| 7 | **AI-слой** — Qdrant + первые runbook'и в RAG | ⏳ |
| 8 | Alertmanager → n8n webhook (AIOps-lite) | ⏳ |
| 9 | Второй коннектор: HuggingFace | ⏳ |
| 10 | Финальный README, диаграмма архитектуры, screenshots | ⏳ |

---

## Системные требования

### Локальный запуск (целевая машина: i5 12-gen, 32 GB RAM, RTX 3050, SSD)

| Профиль | RAM | Время первого `make demo` |
|---------|-----|---------------------------|
| core | ~3 GB | 20 сек |
| + monitoring | ~5 GB | 30 сек |
| + orchestration | ~7 GB | 1-2 мин |
| + viz | ~8 GB | 1 мин |
| + ai (Qdrant) | ~9 GB | 30 сек |
| **итого полный demo** | **~9 GB** | **3-5 мин** |

Запас огромный, GPU не задействован (нужен только для LM Studio за пределами стека).

### Вырост: Kubernetes + Yandex Cloud

| Конфигурация | Ноды | RAM | Диск | Стоимость (YC, ≈) |
|--------------|------|-----|------|-------------------|
| Минимальный K8s | 3× s3-c2-m8 (2 vCPU, 8 GB) | 24 GB | 90 GB SSD | ~15 000 ₽/мес |
| Рабочий K8s | 3× s3-c4-m16 (4 vCPU, 16 GB) | 48 GB | 200 GB SSD | ~30 000 ₽/мес |
| С GPU (для LLM) | + 1× g2-c8-m64-g1 | +64 GB | +100 GB | +60 000 ₽/мес (T4) |

Плюс managed-сервисы по выбору:

- Managed PostgreSQL — от 5 000 ₽/мес (s3-c2-m8, 50 GB)
- Managed ClickHouse — от 8 000 ₽/мес
- Object Storage (S3) для бэкапов — копейки на малых объёмах

**Стратегия миграции:** на этапе K8s держать PostgreSQL и ClickHouse как
managed-сервисы YC, а в кластере крутить только stateless-компоненты
(Airflow, Superset, Grafana, агенты). Ближе к продакшн-паттернам, проще
в эксплуатации.

---

## Структура репозитория (целевая, к концу всех этапов)

```
data-platform-in-a-box/
├── .github/workflows/ci.yml
├── docker-compose.yml
├── .env.example
├── Makefile
├── README.md
├── PROJECT_PLAN.md
├── init/
│   ├── postgres/01_init.sql           ← + создание БД для Airflow
│   └── clickhouse/
│       ├── 01_schema.sql              ← bronze/silver схемы
│       └── 02_test_data.sql           ← опционально
├── config/
│   ├── clickhouse/
│   ├── prometheus/
│   ├── alertmanager/
│   ├── grafana/
│   ├── airflow/                       ← (этап 3)
│   └── superset/                      ← (этап 5)
├── dags/                              ← Airflow DAG-и (этап 3)
│   ├── hh_snapshot.py
│   └── hh_silver_normalize.py
├── dbt/                               ← dbt-проект (этап 4)
│   ├── models/silver/
│   ├── models/gold/
│   └── tests/
├── docs/
│   ├── architecture.md
│   ├── decisions/                     ← ADR
│   └── runbooks/                      ← регламенты (потом в RAG)
└── scripts/
    ├── demo_seed.sh                   ← заливка первого snapshot
    └── superset_bootstrap.py          ← регистрация datasource и дашборда
```

---

## Связь с остальными пет-проектами

- **Проект №2 (GitLab CI + dbt)** → dbt-модели из этого проекта переносятся
  в корпоративный GitLab, обвязываются полным CI/CD pipeline (lint → test →
  staging → prod). Здесь у нас прообраз, там — полноценная реализация.
- **Проект №3 (Runbook-агент с RAG)** → регламенты из `docs/runbooks/`
  индексируются в Qdrant. Агент отвечает на «что делать если X сломалось».
- **Проект №4 (Text-to-SQL бот)** → бот делает запросы к Gold-витринам
  ClickHouse. Schema из этого проекта — обучающий контекст для LLM.
- **Проект №5 (AIOps-lite)** → Alertmanager шлёт webhook в n8n, LLM
  анализирует с использованием RAG из проекта №3 и метрик из этого стека.
