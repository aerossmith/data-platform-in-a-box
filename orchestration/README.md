# Orchestration (Airflow 3.x)

Каталог монтируется в контейнеры Airflow:

| Путь на хосте | Путь в контейнере | Назначение |
|---|---|---|
| `dags/` | `/opt/airflow/dags` | DAG-и |
| `plugins/` | `/opt/airflow/plugins` | Кастомные плагины |
| `logs/` | `/opt/airflow/logs` | Логи task instances |
| `../dbt/` | `/opt/airflow/dbt:ro` | dbt-проект (read-only) |

---

## Архитектура Airflow

```
airflow-api-server    UI + REST API (FastAPI, :8080)
airflow-scheduler     планирует задачи, ставит в очередь Celery
airflow-dag-processor парсит DAG-файлы (отдельный процесс в 3.x)
airflow-triggerer     async sensors и deferrable operators
airflow-worker        Celery-воркер, исполняет задачи
airflow-init          разовый: db migrate

Redis    Celery broker (профиль core)
Postgres метаданные Airflow, БД airflow_meta (профиль core)
StatsD   принимает UDP-метрики от Airflow (профиль core)
```

---

## DAG-и

| DAG | Schedule | Тип | Описание |
|---|---|---|---|
| `hh_vacancies_snapshot` | `0 */4 * * *` | E+L | HH.ru → bronze_hh_vacancies |
| `dbt_hh_transform` | `None` | T | bronze → silver → gold через dbt |

ELT-связка: последняя задача `hh_vacancies_snapshot` — `trigger_dbt_transform` —
вызывает `TriggerDagRunOperator` с `conf={"snapshot_id": ...}`. Это передаёт
**конкретный** snapshot_id завершённого ingest в downstream-трансформацию,
а не "последнюю запись по времени" (которая может быть частичной от упавшего рана).

---

## hh_vacancies_snapshot: как работает

6 срезов параллельно: 2 роли × 3 региона.

**Warmup-flow:** `curl_cffi Session(impersonate=chrome120)` → GET hh.ru/ → GET hh.ru/search/vacancy → API-запрос.

**HTML fallback при 401/403:** парсинг `hh.ru/search/vacancy` через BeautifulSoup.
По каждой вакансии опционально загружается detail-страница (description, skills, experience).

**Retry-safe:** перед INSERT делает SELECT существующих vacancy_id, фильтрует дубли.

**Управление памятью** (важно — решает SIGKILL воркера):
- Один `curl_cffi.Session` на весь `ingest_combo` — переиспользуется во всех
  fetch_detail вызовах. Раньше создавался новый Session на каждую вакансию
  без `.close()`, что приводило к утечке libcurl-handle.
- `soup.decompose()` после парсинга каждой detail-страницы — освобождает DOM.
- `session.close()` в `try/finally` в конце `ingest_combo` — гарантирует
  освобождение ресурсов даже при exception.
- `AIRFLOW__CELERY__WORKER_CONCURRENCY=1` — mapped-задачи выполняются
  последовательно, избегаем concurrent INSERT в ClickHouse.

### Поля raw_json

| `source_mode` | `detail_status` | Ключевые поля |
|---|---|---|
| `html_fallback` | `ok` | description, skills, experience, salary_detail |
| `html_fallback` | `captcha` | card_text, experience_card, work_format_card |
| `html_fallback` | `not_fetched_limit` | только карточные поля |
| `api` | — | name, employer.name, salary.from/to |

### Настройки через .env

```env
DPIB_HH_IMPERSONATE=chrome120
DPIB_HH_HTML_FALLBACK_MAX_PAGES=5
DPIB_HH_FETCH_DETAILS=true
DPIB_HH_DETAIL_MAX_PER_COMBO=100
DPIB_HH_DETAIL_PAUSE_MIN_SEC=0.2
DPIB_HH_DETAIL_PAUSE_MAX_SEC=0.6
```

---

## dbt_hh_transform: задачи

| Задача | Тип | Что делает |
|---|---|---|
| `check_bronze` | Python | Читает snapshot_id из dag_run.conf, проверяет 6 комбинаций |
| `dbt_deps` | Bash | Копирует проект в /tmp, восстанавливает пакеты из кеша |
| `dbt_run_staging` | Bash | dbt run staging → stg_hh_vacancies (view) |
| `dbt_test_staging` | Bash | dbt test staging → not_null, accepted_values |
| `dbt_run_silver` | Bash | dbt run silver → silver_hh_vacancies (table) |
| `dbt_test_silver` | Bash | dbt test silver → unique_combination_of_columns |
| `dbt_run_gold` | Bash | dbt run gold → gold_skills_top (table) |
| `check_silver` | Python | SELECT snapshot_id из silver, проверка grain |

**`check_bronze` логика выбора snapshot_id:**
1. Если в `dag_run.conf["snapshot_id"]` есть UUID — используем его
   (это путь через `TriggerDagRunOperator` из ingest, гарантированно полный)
2. Иначе fallback: последний снапшот с `uniqExact(concat(search_text,'|',search_area)) = 6`
   (явно ПОЛНЫЙ, не просто последняя запись по `ingested_at`)
3. Падает с понятной ошибкой если выбранный snapshot < 6 комбинаций

**Защита silver от частичных снапшотов:** silver-модель содержит CTE
`complete_snapshots` с `HAVING uniqExact(...) = 6`. Даже при ручном dbt run
silver не подхватит частичные данные.

**Кеш dbt deps:** пакеты ставятся один раз в `/tmp/dbt-packages-cache`.
При следующих запусках восстанавливаются `cp -r` (~5 сек вместо 2-3 мин).

**Параллелизм dbt:** `threads: 1` задан в `dbt/profiles.yml`, а не флагом
в командах. Причина: `dbt deps` не поддерживает `--threads`, а параллельные
тесты `accepted_values` на staging view с `JSONExtract` ловят
`MEMORY_LIMIT_EXCEEDED`. Профильная настройка применяется к `run`/`test`,
но не ломает `deps`.

**read-only монтирование:** `/opt/airflow/dbt:ro`. Задача `dbt_deps` копирует
только `models/`, `macros/`, `*.yml` — без `.venv` (~145 МБ) и артефактов.

---

## Метрики Airflow через StatsD

Airflow → UDP → `statsd-exporter:9125` → Prometheus `/metrics` на `:9102`.

```
airflow_dag_*_duration          время выполнения DAG-а
airflow_operator_failures_*     счётчик падений по типу оператора
airflow_scheduler_heartbeat     heartbeat планировщика
airflow_celery_*                состояние очереди Celery
```

Проверить:

```bash
curl -s http://localhost:9102/metrics | grep "^airflow" | head -10
```

---

## Лимиты контейнеров (по итогам отладки)

| Контейнер | mem_limit | Комментарий |
|---|---|---|
| `clickhouse` | 3g | На `2g` упирался при concurrent analytical-запросах |
| `airflow-worker` | 1536m | На `1g` SIGKILL при detail-fetch + INSERT |
| `airflow-scheduler` | 768m | Стабильно |
| `airflow-api-server` | 1g | Стабильно |

Применить изменения `mem_limit` через `up -d --force-recreate <service>`
(обычный `restart` не пересоздаёт контейнер).

Проверка фактического лимита:
```bash
docker inspect <container> --format '{{.HostConfig.Memory}}'
# 3221225472 = 3 GiB, 1610612736 = 1.5 GiB
```

ClickHouse видит cgroup-лимит:
```sql
SELECT formatReadableSize(value) FROM system.asynchronous_metrics
WHERE metric = 'CGroupMemoryTotal';
```

---

## Полезные команды

```bash
make airflow-trigger DAG=hh_vacancies_snapshot
make airflow-trigger DAG=dbt_hh_transform
make airflow-logs
make airflow-shell

# Проверить кеш dbt
docker exec dpib-airflow-worker ls -lah /tmp/dbt-packages-cache 2>/dev/null || echo "кеш пуст"

# Список DAG-ов
docker exec dpib-airflow-scheduler airflow dags list

# Проверить import errors
docker exec dpib-airflow-scheduler airflow dags list-import-errors
```

---

## См. также

- [`runbooks/elt-memory-and-sigkill-troubleshooting.md`](../runbooks/elt-memory-and-sigkill-troubleshooting.md)
  — диагностика SIGKILL и MEMORY_LIMIT_EXCEEDED
- [`runbooks/external-api-blocked-html-fallback.md`](../runbooks/external-api-blocked-html-fallback.md)
  — обход ddos-guard через TLS impersonation
