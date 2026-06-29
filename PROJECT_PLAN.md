# Data Platform in a Box — План проекта

Версия: 3.1
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

## Контрольный успешный прогон шага 4

- ingest run: `manual__2026-06-29T10:01:46+00:00` — success
- dbt run: `manual__2026-06-29T10:10:37+00:00` — success
- snapshot_id: `a87ffbd8-cac0-4ac5-a81a-aa1131437cf3`
- bronze: 432 строки, 6 комбинаций (полный снапшот)
- silver: 380 строк, grain `(snapshot_id, vacancy_id)` уникален
- staging tests: 17/17 PASS
- silver tests: 5/5 PASS
- partial_rows_in_silver: 0 (фильтр complete_snapshots работает)
- gold_skills_top: 422 строки
- ни одного `SIGKILL` / `MEMORY_LIMIT_EXCEEDED` в логах

---

## Что сделано (детали)

### Шаг 1 — Core
PostgreSQL 16-alpine + ClickHouse 24-alpine + Redis 7-alpine. Init-скрипты,
healthcheck-и, сетевая изоляция. Профиль `core`.

### Шаг 2 — Monitoring
Prometheus 2.54, Grafana 11.2, Alertmanager 0.27, node-exporter, cAdvisor,
postgres-exporter. ClickHouse отдаёт метрики на `:9363` через `<prometheus>`
конфиг. StatsD-exporter в профиле `core` (обслуживает Airflow всегда).

### Шаг 2.5 — CI
GitHub Actions: `docker compose config`, `yamllint`, `xmllint`, `jq empty`,
`sqlfluff`. Без контейнеров, ~25 сек.

### Шаг 3 — Orchestration (E + L)
Airflow 3.0.3-python3.12, CeleryExecutor, 6 контейнеров. DAG
`hh_vacancies_snapshot` — cron `0 */4 * * *`. 6 срезов: DevOps Engineer +
Platform Engineer × msk + spb + remote. curl_cffi `impersonate=chrome120`
+ warmup-flow + HTML fallback. Retry-safe вставка.

### Шаг 4 — Transform (T) + защита целостности

**dbt-проект:**
- три базы: `dpib` (bronze) / `dpib_silver` (staging + silver) / `dpib_gold`
- `stg_hh_vacancies` view с JSONExtract + quality_score
- `silver_hh_vacancies` table с dedup по `(snapshot_id, vacancy_id)`
- `gold_skills_top` table с argMax по latest-снапшоту
- 22 теста (`data_tests:` синтаксис), все зелёные

**Airflow DAG `dbt_hh_transform`:**
- 8 задач: `check_bronze → dbt_deps → run/test staging → run/test silver → run gold → check_silver`
- Запускается автоматически через `TriggerDagRunOperator` из ingest
- Кеш `dbt_packages` в `/tmp/dbt-packages-cache` (~5 сек вместо 2-3 мин)

**Архитектурные решения по итогам отладки (важно для портфолио):**

1. **ingest переиспользует один `curl_cffi.Session`** — раньше на каждую
   detail-страницу создавался новый Session, который не закрывался. Это
   приводило к утечке libcurl-handle и `SIGKILL` воркера. Сейчас warmup
   возвращает session, его передают во все downstream-функции.

2. **`BeautifulSoup` освобождается через `decompose()`** — после извлечения
   данных явно вызывается в `try/finally`. Без этого парсеры держали
   деревья DOM в памяти.

3. **`session.close()` в `finally`** в `ingest_combo` — гарантирует
   освобождение даже при exception.

4. **Лимиты контейнеров:**
   - `airflow-worker` `mem_limit=1536m` (было `1g` — не хватало на
     detail-fetch + insert одновременно)
   - `clickhouse` `mem_limit=3g` (было `2g` — упирался при параллельных
     analytical-запросах)
   - `AIRFLOW__CELERY__WORKER_CONCURRENCY=1` — последовательное выполнение
     mapped-задач, избегаем concurrent INSERT в ClickHouse

5. **`snapshot_id` передаётся через `TriggerDagRunOperator.conf`** —
   `dbt_hh_transform` трансформирует именно завершённый снапшот, не
   "последнюю запись по `ingested_at`" (которая может быть частичной от
   упавшего предыдущего рана).

6. **`check_bronze` требует 6 комбинаций** — явно падает с понятной ошибкой
   если переданный snapshot_id частичный (`combos < 6`). Защищает от
   пропуска проблемы при ручных перезапусках.

7. **Silver исключает частичные снапшоты через `complete_snapshots` CTE** —
   `HAVING uniqExact(concat(search_text, '|', search_area)) = 6`. Это
   защита на уровне модели: даже если кто-то запустит dbt вручную при
   частичных данных в bronze, silver-таблица останется чистой.

8. **`threads: 1` задан в `dbt/profiles.yml`** — не флагом в командах.
   Причина: `dbt deps` не поддерживает `--threads`, а параллельные
   тесты `accepted_values` на staging view с `JSONExtract` упираются в
   `MEMORY_LIMIT_EXCEEDED`. Профильная настройка применяется к `run/test`
   но не ломает `deps`.

9. **`dbt deps` запускается без `--threads`** — общий `PF` в DAG-е не
   содержит флаг, threads берётся из профиля только где поддерживается.

10. **`config/clickhouse/system-logs.xml`** — отключены `text_log`,
    `metric_log`, `asynchronous_metric_log`, `trace_log`,
    `processors_profile_log`. Без TTL они росли до сотен миллионов строк
    (text_log дошёл до 57M, asynchronous_metric_log до 209M) и создавали
    постоянные фоновые merge с memory pressure. Prometheus endpoint
    `:9363` продолжает работать — он читает текущие значения через
    `system.metrics` / `system.events` / `system.asynchronous_metrics`
    (real-time, не таблицы). Query log оставлен для диагностики SQL.

**Тесты dbt:**
- staging тесты с `where: snapshot_id = (последний полный снапшот)` —
  не сканируют всю историю view
- silver: `unique_combination_of_columns(snapshot_id, vacancy_id)` через
  `dbt_utils`

**Исторические частичные снапшоты не удалены** — они в bronze как
свидетельство пройденной отладки. Silver их игнорирует через фильтр.

---

## Что дальше

### Шаг 5 — Grafana дашборды (следующий)

Данные уже идут в Prometheus, нужны дашборды.

**ClickHouse health:**
- ProfileEvent_Query
- ProfileEvent_InsertedRows
- AsynchronousMetric_TotalPartsOfMergeTreeTables
- размер таблиц по слоям через SQL-datasource

**Airflow health:**
- airflow_dag_*_duration
- airflow_operator_failures_*
- airflow_scheduler_heartbeat
- airflow_celery_*

### Шаг 6 — Superset
Профиль `viz`, подключение к ClickHouse, дашборд поверх dpib_gold.

### Шаг 7 — `make demo`
One-command deploy с данными за 3-5 минут.

### Шаг 8 — AI-слой (Qdrant + RAG)
Индексация `runbooks/`, dbt-документации. Первый RAG-агент.

### Шаг 9 — AIOps-lite
Alertmanager → n8n → LLM → Telegram.

### Шаг 10 — HuggingFace коннектор
Второй источник, multi-source DWH.

### Шаг 11 — Финальный README
Диаграмма, скриншоты, описание для резюме.

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

## Runbooks

- [`runbooks/external-api-blocked-html-fallback.md`](./runbooks/external-api-blocked-html-fallback.md)
  — обход ddos-guard через TLS impersonation + HTML fallback
- [`runbooks/elt-memory-and-sigkill-troubleshooting.md`](./runbooks/elt-memory-and-sigkill-troubleshooting.md)
  — SIGKILL и MEMORY_LIMIT_EXCEEDED в ELT-конвейере: диагностика и фиксы
