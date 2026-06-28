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
вызывает `TriggerDagRunOperator` и запускает `dbt_hh_transform` автоматически.

---

## hh_vacancies_snapshot: как работает

6 срезов параллельно: 2 роли × 3 региона.

**Warmup-flow:** `curl_cffi Session(impersonate=chrome120)` → GET hh.ru/ → GET hh.ru/search/vacancy → API-запрос.

**HTML fallback при 401/403:** парсинг `hh.ru/search/vacancy` через BeautifulSoup.
По каждой вакансии опционально загружается detail-страница (description, skills, experience).

**Retry-safe:** перед INSERT делает SELECT существующих vacancy_id, фильтрует дубли.

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
| `check_bronze` | Python | Проверяет наличие данных в последнем снапшоте |
| `dbt_deps` | Bash | Копирует проект в /tmp, восстанавливает пакеты из кеша |
| `dbt_run_staging` | Bash | dbt run staging → stg_hh_vacancies (view) |
| `dbt_test_staging` | Bash | dbt test staging → not_null, accepted_values |
| `dbt_run_silver` | Bash | dbt run silver → silver_hh_vacancies (table) |
| `dbt_test_silver` | Bash | dbt test silver → unique_combination_of_columns |
| `dbt_run_gold` | Bash | dbt run gold → gold_skills_top (table) |
| `check_silver` | Python | SELECT grain + качество, падает если rows != unique_keys |

**Кеш dbt deps:** пакеты ставятся один раз в `/tmp/dbt-packages-cache`.
При следующих запусках восстанавливаются cp -r (~5 сек вместо 2-3 мин).

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
```
