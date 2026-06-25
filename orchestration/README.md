# Orchestration (Airflow 3.x)

Этот каталог монтируется в контейнеры Airflow:

| Путь в хосте | Путь в контейнере | Назначение |
|--------------|-------------------|-----------|
| `dags/`      | `/opt/airflow/dags`    | DAG-и (Python) |
| `plugins/`   | `/opt/airflow/plugins` | Кастомные плагины (если будут) |
| `logs/`      | `/opt/airflow/logs`    | Логи task instances |
| `requirements.txt` | через `_PIP_ADDITIONAL_REQUIREMENTS` | доп. Python-пакеты |

## Архитектура

```
                    ┌──────────────────────┐
                    │   airflow-api-server │  ← UI и REST API (FastAPI)
                    └──────────────────────┘
                              │
                    ┌──────────────────────┐
                    │   airflow-scheduler  │  ← планирует и ставит задачи в очередь
                    └──────────────────────┘
                              │
                    ┌──────────────────────┐
                    │ airflow-dag-processor│  ← парсит DAG-файлы (отдельный сервис в 3.x)
                    └──────────────────────┘
                              │
                    ┌──────────────────────┐
                    │   airflow-triggerer  │  ← async sensors
                    └──────────────────────┘
                              │
                    ┌──────────────────────┐
                    │    airflow-worker    │  ← Celery worker, исполняет задачи
                    └──────────────────────┘
                         │           │
                    ┌────┴───┐   ┌───┴────┐
                    │ Redis  │   │Postgres│
                    │(broker)│   │ (meta) │
                    └────────┘   └────────┘
```

PostgreSQL (БД `airflow_meta`) и Redis уже есть в профиле `core`.

## Как добавить новый DAG

1. Создай `dags/мой_dag.py`
2. Airflow подхватит через `dag-processor` (~10-30 секунд)
3. Проверь: `make airflow-list-dags`
4. Запусти вручную: `make airflow-trigger DAG=мой_dag`

## Текущие DAG-и

| DAG ID | Schedule | Описание |
|--------|----------|----------|
| `hh_vacancies_snapshot` | `0 */4 * * *` | Snapshot вакансий с HH.ru → bronze (ClickHouse) |

### `hh_vacancies_snapshot`: как сейчас забираются данные

DAG собирает 6 срезов: две роли (`DevOps Engineer`, `Platform Engineer`) × три региона (`msk`, `spb`, `remote`).
Основной путь — публичный endpoint `https://api.hh.ru/vacancies`.

На практике `api.hh.ru` может вернуть `401/403 forbidden` из-за внешней защиты HH/ddos-guard. Чтобы DAG не был
просто "зелёным без данных", в `ingest_combo` добавлен fallback:

1. создаётся `curl_cffi.Session` с `impersonate=chrome120`;
2. выполняется warmup: `https://hh.ru/` → `https://hh.ru/search/vacancy?...`;
3. если API вернул `401/403`, задача переключается на HTML-поиск `https://hh.ru/search/vacancy`;
4. из карточек выдачи парсятся `vacancy_id`, `title`, `url`, `employer`, `salary`, `address`, `card_text`,
   `experience_card`, `work_format_card`;
5. по `url` опционально догружается detail-страница вакансии: `description`, `experience`, `employment`, `schedule`,
   `skills`, `salary_detail`, `address_detail`;
6. результат пишется в `bronze_hh_vacancies.raw_json` с `source_mode="html_fallback"`.

Ограничитель fallback задаётся через `.env`:

```env
DPIB_HH_HTML_FALLBACK_MAX_PAGES=5
DPIB_HH_FETCH_DETAILS=true
DPIB_HH_DETAIL_MAX_PER_COMBO=100
DPIB_HH_DETAIL_PAUSE_MIN_SEC=0.2
DPIB_HH_DETAIL_PAUSE_MAX_SEC=0.6
```

Текущая проверка после фикса: manual run `2026-06-25 18:52:44` завершился `success`, все 6 mapped-задач
`ingest_combo` завершились `success`, в `bronze_hh_vacancies` попала 421 строка.

Проверить факт загрузки:

```sql
SELECT count(), uniqExact(snapshot_id), max(ingested_at)
FROM bronze_hh_vacancies;

SELECT search_text, search_area, count()
FROM bronze_hh_vacancies
GROUP BY search_text, search_area
ORDER BY search_text, search_area;
```

Важно: HTML fallback всё равно менее контрактный, чем API: CSS/HTML HH может меняться. Detail-страницы могут увести на
captcha, поэтому detail-догрузка не валит DAG при ошибке одной вакансии: в `raw_json.detail_status` будет `ok`,
`captcha`, `http_*`, `error`, `not_fetched_limit` или `disabled`. Для silver/gold нужно читать поля с учётом
`source_mode` и `detail_status`; если `detail_status != "ok"`, использовать поля из карточки (`experience_card`,
`work_format_card`, `card_text`).

## Доступ к UI

http://localhost:8080 — логин/пароль из `.env` (по умолчанию `admin / admin`).

## Полезные команды

```bash
make airflow-logs                                    # tail логов scheduler+worker+dag-processor
make airflow-shell                                   # bash внутри scheduler
make airflow-list-dags                               # список DAG-ов
make airflow-trigger DAG=hh_vacancies_snapshot       # запустить DAG вручную

# проверить состояние Celery worker-а
docker exec dpib-airflow-worker airflow celery inspect active
```

## Про `_PIP_ADDITIONAL_REQUIREMENTS`

Это **анти-паттерн для прода** — пакеты ставятся при каждом старте контейнера, что
замедляет запуск и небезопасно. Для лабы удобно: меняешь `requirements.txt` —
перезапуск контейнера, всё подтянулось.

Когда дойдём до выроста в Kubernetes — соберём кастомный образ:

```dockerfile
FROM apache/airflow:3.0.3-python3.12
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```
