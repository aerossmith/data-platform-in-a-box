# Data Platform in a Box — План проекта

Версия: 1.0
Дата: май 2026

---

## Цель

Единая DWH/BI платформа в Docker Compose — рабочий стенд для аналитики и фундамент для AI/DevOps-проектов (RAG-агент, Text-to-SQL, AIOps).

---

## Состав стека

### Ядро (core)
- **PostgreSQL 15/16** — OLTP, метаданные Airflow/Superset, хранилище состояний агентов
- **ClickHouse** — аналитический движок, целевая база для Text-to-SQL

### Оркестрация (orchestration)
- **Airflow 2.9+** — ETL/ELT пайплайны, интеграция с dbt

### Визуализация (viz)
- **Superset** — дашборды поверх PostgreSQL и ClickHouse
- **Grafana** — мониторинг и операционные дашборды

### Мониторинг (monitoring)
- **Prometheus** — сбор метрик со всех сервисов
- **Alertmanager** — правила алертинга, webhook в n8n/Telegram
- **node-exporter** — метрики хоста
- **cAdvisor** — метрики контейнеров

### AI-слой (ai)
- **Qdrant** — векторная база для RAG (runbook-агент)

### Прокси (proxy)
- **Traefik** или **Nginx** — единая точка входа, проксирование UI

---

## Архитектура запуска

Docker Compose с профилями — можно поднять весь стек или отдельные части:

```bash
# всё
docker compose --profile core --profile orchestration --profile viz --profile monitoring --profile ai up -d

# только базы
docker compose --profile core up -d

# базы + мониторинг
docker compose --profile core --profile monitoring up -d
```

---

## Инфраструктурные принципы

- Отдельная docker-сеть с предсказуемыми именами хостов (postgres, clickhouse, airflow и т.д.)
- Persist-тома для данных — `make down && make up` не убивает данные
- Healthcheck на каждом сервисе
- `.env.example` с параметрами (версии образов, порты, пароли)
- `init/` — SQL-скрипты для начальных схем и тестовых данных
- Grafana provisioning — дашборды и datasources из файлов, без ручной настройки

### Makefile

```makefile
up:        docker compose --profile core --profile orchestration --profile viz --profile monitoring up -d
down:      docker compose down
status:    docker compose ps
logs:      docker compose logs -f $(svc)
reset:     docker compose down -v && docker compose --profile core up -d
```

---

## Документация

```
README.md                    — архитектура, quick start, скриншоты
docs/architecture.md         — обоснование выбора компонентов, диаграмма
docs/runbooks/               — регламенты эксплуатации (→ потом в RAG)
docs/runbooks/postgresql.md  — VACUUM, репликация, бэкапы
docs/runbooks/clickhouse.md  — мержи, партиции, реплики
docs/runbooks/airflow.md     — DAG-операции, zombie tasks, очистка
```

---

## Что можно совместить из «базы» и «выроста»

Некоторые элементы из «выроста» дешёво добавить сразу, без дополнительных сложностей:

### Брать сразу (затраты минимальные, польза большая)

1. **Alertmanager** — это буквально ещё один контейнер + файл с правилами. Без него Prometheus собирает метрики в пустоту. Правила на старте: контейнер упал, диск > 80%, replication lag. Webhook в Telegram через n8n.

2. **cAdvisor** — один контейнер, zero-config. Даёт метрики всех контейнеров в Prometheus из коробки.

3. **Grafana provisioning** — дашборды из JSON-файлов при старте. Трудозатраты те же, что при ручной настройке, но результат воспроизводим и лежит в git.

4. **GitLab CI: lint + smoke test** — простой `.gitlab-ci.yml`: проверяет docker-compose.yml на валидность и делает `docker compose up → curl healthcheck → docker compose down`. 30 минут на настройку, зато при каждом коммите уверен что ничего не сломал.

5. **docs/runbooks/** — писать регламенты параллельно с настройкой. Настроил PostgreSQL → записал как делал. Эти же файлы потом идут в Qdrant для RAG-агента.

### Оставить на вырост (требуют отдельного погружения)

- Kubernetes + Helm — другая парадигма, другие знания
- Terraform для Yandex Cloud — нужен аккаунт, биллинг, сеть
- Ansible — имеет смысл только при нескольких хостах
- ArgoCD / GitOps — нужен работающий кластер K8s
- Vault — оверкилл для локального стенда

---

## Системные требования

### Базовый стек (Docker Compose на локальной машине)

Твоё железо: i5 12-gen, 32 GB RAM, RTX 3050 8GB VRAM, SSD.

| Компонент       | RAM        | CPU       | Диск       | Примечание                              |
|----------------|------------|-----------|------------|-----------------------------------------|
| PostgreSQL     | 512 MB–1 GB | 0.5 ядра | 1–5 GB     | shared_buffers = 256 MB для лабы        |
| ClickHouse     | 1–2 GB     | 1 ядро   | 2–10 GB    | жрёт при больших запросах, но для лабы ок |
| Airflow        | 1–1.5 GB   | 1 ядро   | 500 MB     | webserver + scheduler + triggerer       |
| Superset       | 512 MB–1 GB | 0.5 ядра | 500 MB     | тяжёлый на старте, потом отпускает      |
| Prometheus     | 256–512 MB | 0.25 ядра | 1–5 GB    | зависит от retention (14 дней по умолч.) |
| Grafana        | 256 MB     | 0.25 ядра | 100 MB     | лёгкий                                  |
| Alertmanager   | 64 MB      | минимум   | 50 MB      | почти ничего не ест                     |
| cAdvisor       | 128 MB     | 0.25 ядра | —          | read-only, без персиста                 |
| node-exporter  | 32 MB      | минимум   | —          | совсем лёгкий                           |
| Qdrant         | 512 MB–1 GB | 0.5 ядра | 500 MB–2 GB| зависит от объёма эмбеддингов           |
| Traefik/Nginx  | 64–128 MB  | минимум   | 50 MB      | —                                       |
| **ИТОГО**      | **5–9 GB** | **4–5 ядер** | **6–25 GB** | —                                   |

**Вердикт**: на 32 GB RAM влезает комфортно. Остаётся ~23 GB на систему, WSL, LM Studio и браузер. SSD обязателен — ClickHouse и PostgreSQL чувствительны к IOPS. GPU тут не нужен (нужен только для LLM-инференса в LM Studio).

**Совет**: в docker-compose ставь `mem_limit` на тяжёлые сервисы (ClickHouse, Airflow, Superset), чтобы один контейнер не сожрал всё.

### Вырост: Kubernetes + Yandex Cloud

| Конфигурация      | Ноды                   | RAM       | Диск     | Стоимость (YC, ≈)     |
|-------------------|------------------------|-----------|----------|------------------------|
| Минимальный K8s   | 3× s3-c2-m8 (2 vCPU, 8 GB) | 24 GB     | 90 GB SSD | ~15 000 ₽/мес         |
| Рабочий K8s       | 3× s3-c4-m16 (4 vCPU, 16 GB)| 48 GB    | 200 GB SSD| ~30 000 ₽/мес         |
| С GPU (для LLM)   | + 1× g2-c8-m64-g1     | +64 GB    | +100 GB  | +60 000 ₽/мес (T4)    |

Плюс managed-сервисы (если заменять контейнеры):
- Managed PostgreSQL — от 5 000 ₽/мес (s3-c2-m8, 50 GB)
- Managed ClickHouse — от 8 000 ₽/мес
- Object Storage (S3) для бэкапов — копейки на малых объёмах

**Совет**: на этапе K8s можно держать PostgreSQL и ClickHouse как managed-сервисы в YC, а в кластере крутить только stateless-компоненты (Airflow, Superset, Grafana, агенты). Это проще в эксплуатации и ближе к продакшн-паттернам.

---

## Структура репозитория

```
data-platform-in-a-box/
├── docker-compose.yml
├── .env.example
├── Makefile
├── README.md
├── docs/
│   ├── architecture.md
│   └── runbooks/
│       ├── postgresql.md
│       ├── clickhouse.md
│       └── airflow.md
├── config/
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alerts.yml
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources.yml
│   │   │   └── dashboards.yml
│   │   └── dashboards/
│   │       ├── postgres.json
│   │       ├── clickhouse.json
│   │       └── containers.json
│   ├── alertmanager/
│   │   └── alertmanager.yml
│   ├── traefik/
│   │   └── traefik.yml
│   └── airflow/
│       └── airflow.cfg (или через env vars)
├── init/
│   ├── postgres/
│   │   └── 01_init.sql
│   └── clickhouse/
│       └── 01_init.sql
├── dags/
│   └── example_dag.py
└── .gitlab-ci.yml
```

---

## Порядок реализации

1. **docker-compose.yml** — профиль `core` (PostgreSQL + ClickHouse) + healthchecks + volumes
2. Профиль `monitoring` (Prometheus + Grafana + cAdvisor + node-exporter) + provisioning дашбордов
3. Профиль `orchestration` (Airflow) + пример DAG
4. Профиль `viz` (Superset) + подключение к обеим базам
5. Профиль `ai` (Qdrant)
6. Alertmanager + базовые правила + webhook
7. Traefik/Nginx как единый вход
8. Makefile, .env.example, README, архитектурная диаграмма
9. GitLab CI: lint + smoke test
10. Первые runbooks в docs/

---

## Связь с остальными проектами

- **Проект №2 (GitLab CI + dbt)** → dbt-модели работают поверх ClickHouse/PostgreSQL из этого стека, pipeline в `.gitlab-ci.yml`
- **Проект №3 (Runbook-агент)** → регламенты из `docs/runbooks/` индексируются в Qdrant из профиля `ai`
- **Проект №4 (Text-to-SQL)** → запросы к ClickHouse/PostgreSQL, схемы из `init/`
- **Проект №5 (AIOps)** → Alertmanager шлёт webhook, Prometheus даёт метрики, Qdrant даёт контекст из runbooks
