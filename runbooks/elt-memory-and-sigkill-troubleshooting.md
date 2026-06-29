# Runbook: SIGKILL и MEMORY_LIMIT_EXCEEDED при ELT-конвейере с внешним HTTP-источником

## Симптомы

- Airflow-задачи `ingest_*` периодически убиваются `SIGKILL (-9)` посреди работы
- ClickHouse отклоняет `INSERT` с `Code: 241 MEMORY_LIMIT_EXCEEDED`
- В `system.asynchronous_metrics` видно: `CGroupMemoryUsed` подходит к `CGroupMemoryTotal`
- dbt-тесты падают `MEMORY_LIMIT_EXCEEDED` при параллельном запуске
- Появляются частичные снапшоты — данные есть, но не по всем срезам (например 4 из 6)
- Симптомы накапливаются — после рестарта контейнеров работает, через несколько часов снова падает

---

## Базовая диагностика

### 1. Реальные лимиты контейнеров

```bash
# Memory limit в bytes
docker inspect <container> --format '{{.HostConfig.Memory}}'

# 3221225472 = 3 GiB, 1610612736 = 1.5 GiB, 0 = без лимита
```

Если в `docker-compose.yml` подняли `mem_limit`, но `inspect` показывает старое значение — нужен `up -d --force-recreate <service>`, обычный `restart` не пересоздаёт контейнер.

### 2. Cgroup-память изнутри ClickHouse

```sql
SELECT metric, formatReadableSize(value) AS v
FROM system.asynchronous_metrics
WHERE metric IN ('CGroupMemoryTotal', 'CGroupMemoryUsed', 'MemoryTracking')
ORDER BY metric;
```

`CGroupMemoryTotal` — то что ClickHouse видит как доступную память. Если меньше `mem_limit` из compose — пересоздание не применилось.

### 3. Что съедает память внутри ClickHouse

```sql
-- Размер системных таблиц
SELECT
    database, table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS rows
FROM system.parts
WHERE active AND database = 'system'
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC;

-- Активные слияния
SELECT database, table, elapsed, progress, memory_usage
FROM system.merges;
```

Опасные имена: `text_log`, `asynchronous_metric_log`, `metric_log`, `trace_log`, `processors_profile_log` — без TTL они растут бесконечно.

### 4. Логи Airflow-задачи

Смотри последние строки task instance log:

- `Process killed (signal 9)` — OOM-killer воркера
- `MEMORY_LIMIT_EXCEEDED while executing` — ClickHouse отдал ошибку на INSERT
- `Killed` без stacktrace — почти всегда SIGKILL

Параллельно проверь `dmesg` хоста на `Out of memory: Killed process` если есть доступ.

---

## Типичные причины

### Причина А: утечка HTTP-сессий в ingest

**Симптом:** воркер падает по SIGKILL во время цикла обработки сотен URL-ов.

**Что обычно неправильно:**
- На каждый запрос детальной страницы создаётся новый `requests.Session()` / `curl_cffi.Session()`
- Сессия не закрывается — `.close()` не вызывается
- Парсеры (`BeautifulSoup`, `lxml.html.fromstring`) не освобождаются после извлечения данных
- Объекты response не освобождают тело при ошибке

**Что должно быть:**
- Одна сессия на одну задачу (одна вкладка warmup → её же передаём в downstream функции)
- Сессия закрывается в `try / finally` в конце задачи
- BeautifulSoup → `soup.decompose()` после парсинга
- Большие тексты ограничиваются по длине (`text[:N]`) перед сохранением

```python
def ingest_combo(...):
    session, referer = warmup_session(...)
    try:
        for url in urls:
            payload = fetch_detail(session, url, referer)  # один session, не новый!
            ...
    finally:
        session.close()
```

### Причина B: параллельные тяжёлые тесты на view

**Симптом:** dbt-тесты падают `MEMORY_LIMIT_EXCEEDED` при `dbt test`, хотя `dbt run` проходит.

**Что обычно неправильно:**
- staging-модель = view над сырым слоем с `JSONExtract` по всей истории
- В `profiles.yml` `threads: 4` (или больше)
- Тесты `accepted_values` / `unique` делают полный скан view — 4 потока одновременно = 4× память

**Что должно быть:**
- В `profiles.yml`: `threads: 1` — последовательное выполнение для analytical БД с view-материализацией
- В тестах `where:` фильтр на конкретный логический ключ (последний полный снапшот / последний день)

```yaml
- accepted_values:
    values: ['a', 'b', 'c']
    where: >
      snapshot_id = (
        SELECT snapshot_id FROM source_table
        GROUP BY snapshot_id
        HAVING completeness_check
        ORDER BY max(ts) DESC LIMIT 1
      )
```

### Причина C: подробные системные логи без TTL

**Симптом:** база регулярно упирается в лимит памяти, фоновые merge не останавливаются, рестарт помогает на часы, потом всё возвращается.

**Что обычно неправильно:**
- Включены детальные системные таблицы (`text_log`, `asynchronous_metric_log`, `trace_log`)
- TTL не задан — растут до сотен миллионов строк
- Background merge на этих таблицах конкурирует с пользовательскими INSERT за память

**Что должно быть:**
- Решение 1: TTL на системные логи через `<ttl>event_date + INTERVAL N DAY DELETE</ttl>`
- Решение 2: отключить ненужные подсистемы через `<text_log remove="1"/>` в config.d
- Real-time метрики через Prometheus endpoint (`<prometheus>` блок) продолжают работать — они идут из `system.metrics` / `system.events` / `system.asynchronous_metrics`, а не из таблиц-логов

```xml
<clickhouse>
    <text_log remove="1"/>
    <metric_log remove="1"/>
    <asynchronous_metric_log remove="1"/>
    <trace_log remove="1"/>
    <processors_profile_log remove="1"/>
</clickhouse>
```

Query log оставлять — нужен для диагностики реальных запросов.

### Причина D: параллельные воркеры на одной БД

**Симптом:** одиночный запуск проходит, параллельный мапинг — падает.

**Что обычно неправильно:**
- `WORKER_CONCURRENCY` > 1 в Celery
- 6 mapped задач Airflow одновременно делают INSERT в одну таблицу
- ClickHouse при concurrent INSERT держит несколько активных частей в памяти

**Что должно быть для лабы:**
- `AIRFLOW__CELERY__WORKER_CONCURRENCY=1` — последовательное выполнение mapped-задач
- Или увеличить `airflow-worker mem_limit` пропорционально параллелизму

---

## Защита от частичных снапшотов

Когда ingest падает на N из M комбинаций — в сырое хранилище попадают неполные данные. dbt-модель silver, если читает «последний снапшот по времени», подхватит частичный набор и испортит downstream.

**Защита на уровне dbt-модели silver:**

```sql
WITH complete_snapshots AS (
    SELECT snapshot_id
    FROM source_table
    GROUP BY snapshot_id
    HAVING uniqExact(combination_key) = expected_combinations
)

SELECT ... FROM staging
WHERE snapshot_id IN (SELECT snapshot_id FROM complete_snapshots)
```

**Защита на уровне Airflow check-задачи** (перед dbt run):

```python
@task
def check_source(snapshot_id):
    combos = client.query(
        "SELECT uniqExact(combo_key) FROM source WHERE snapshot_id = %s",
        (snapshot_id,)
    ).result_rows[0][0]
    if combos < EXPECTED_COMBOS:
        raise RuntimeError(f"Partial snapshot: {combos}/{EXPECTED_COMBOS}")
```

**Передача snapshot_id между DAG:** через `TriggerDagRunOperator.conf`, не через "взять последнюю запись по времени". Это исключает race-condition между упавшим ingest и следующим запуском transform.

```python
TriggerDagRunOperator(
    trigger_dag_id="transform_dag",
    conf={"snapshot_id": "{{ ti.xcom_pull(task_ids='make_snapshot_id') }}"},
)
```

---

## Чек-лист валидации после фикса

После применения изменений и `up -d --force-recreate`:

```bash
# 1. Фактические лимиты применились
docker inspect <ch-container> --format '{{.HostConfig.Memory}}'
docker inspect <worker-container> --format '{{.HostConfig.Memory}}'

# 2. БД видит новый лимит
docker exec <ch-container> <client> --query \
  "SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryTotal'"

# 3. Worker concurrency
docker exec <scheduler> airflow config get-value celery worker_concurrency

# 4. DAG-и компилируются и импортируются
python3 -m py_compile path/to/dag.py
docker exec <scheduler> airflow dags list-import-errors  # No data found

# 5. dbt parse
cd dbt && dbt parse --profiles-dir .

# 6. compose валиден
docker compose --profile all-needed config --quiet
```

После запуска end-to-end:

```sql
-- Снапшот полный?
SELECT snapshot_id, uniqExact(combo_key) AS combos
FROM source_table
GROUP BY snapshot_id ORDER BY max(ts) DESC LIMIT 3;

-- Тот же snapshot_id в downstream?
SELECT count(), uniq((snapshot_id, business_key)) AS unique_grain
FROM downstream_table WHERE snapshot_id = '<new_snapshot>';
-- count() должно быть равно unique_grain
```

Без OOM/SIGKILL в логах нового рана = фикс рабочий.

---

## Что НЕ делать в продакшен-подобной среде

- `OPTIMIZE TABLE ... FINAL` на больших таблицах под нагрузкой — заберёт всю память
- `DROP / TRUNCATE` системных таблиц с историей — пропадёт диагностика
- `DROP` частичных снапшотов в bronze — они исторический факт, что когда-то ingest упал. Фильтровать в downstream, не удалять в источнике
- `KILL QUERY` без understanding — может оставить inconsistent состояние при INSERT
- Увеличивать `mem_limit` контейнеру без перезапуска через `--force-recreate` — изменение в compose-файле не применится
