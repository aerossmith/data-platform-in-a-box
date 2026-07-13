# dbt — трансформации bronze → silver → gold

dbt-проект поверх Bronze-таблиц вакансий в ClickHouse:
`dpib.bronze_hh_vacancies` и `dpib.bronze_trudvsem_vacancies`.
Не трогает ingest, не меняет docker-compose. Читает bronze,
пишет модели в отдельные базы `dpib_silver` и `dpib_gold`.

---

## Раскладка по базам ClickHouse

| База | Что внутри | Кто пишет |
|---|---|---|
| `dpib` | `bronze_hh_vacancies`, `bronze_trudvsem_vacancies` | ingest DAG-и |
| `dpib_silver` | `stg_hh_vacancies`, `stg_trudvsem_vacancies` (view), `silver_hh_vacancies`, `silver_trudvsem_vacancies`, `silver_vacancies_unified` (table) | dbt |
| `dpib_gold` | `gold_skills_top`, `gold_vacancies_latest` (table) | dbt |

Базы создаются при первом старте ClickHouse через `init/clickhouse/03_create_databases.sql`.
Для уже работающего контейнера: `make clickhouse-bootstrap`.

---

## Поток данных

```
bronze_hh_vacancies   dpib        append-only, raw_json (не трогаем)
        |
stg_hh_vacancies      dpib_silver view: JSONExtract + quality_score + has_detail
        |
silver_hh_vacancies   dpib_silver table: dedup + фильтр complete_snapshots
        ├─> gold_skills_top          dpib_gold   table: топ HH-навыков по latest-снапшоту
        └─> silver_vacancies_unified dpib_silver table: общий контракт HH + «Работа России»

bronze_trudvsem_vacancies   dpib        append-only, raw_json (не трогаем)
        |
stg_trudvsem_vacancies      dpib_silver view: JSONExtract открытого API
        |
silver_trudvsem_vacancies   dpib_silver table: dedup + фильтр complete_snapshots по 4 запросам
        |
silver_vacancies_unified    dpib_silver table: общий контракт HH + «Работа России»
        |
gold_vacancies_latest       dpib_gold   table: latest-вакансии двух источников
```

---

## Локальная разработка

```bash
cd dbt
python3 -m venv .venv && source .venv/bin/activate
pip install dbt-core==1.8.* dbt-clickhouse==1.8.*
dbt deps --profiles-dir .
dbt debug --profiles-dir .   # проверка соединения
dbt build --profiles-dir .   # run + test за один шаг
```

Профиль по умолчанию: `localhost:8123`, user=`dpib`, pass=`dpib_pass`.
При запуске из Airflow переменные `DPIB_CLICKHOUSE_*` уже выставлены.

---

## Запуск через Airflow

DAG `dbt_hh_transform` запускается автоматически после `hh_vacancies_snapshot`
через `TriggerDagRunOperator` с `conf={"snapshot_id": ...}`. Ручной запуск:
`make airflow-trigger DAG=dbt_hh_transform`.

Кеш dbt_packages живёт в `/tmp/dbt-packages-cache` внутри воркер-контейнера.
Первый запуск ~2-3 мин (скачивает dbt_utils), последующие ~5 сек.

---

## Защита от частичных снапшотов

Если ingest упал на N из 6 комбинаций — bronze получит частичный снапшот.
В этом случае `silver` НЕ должен подхватывать неполные данные. Защита на трёх уровнях:

**1. Уровень silver-модели:** `silver_hh_vacancies.sql` содержит CTE
```sql
WITH complete_snapshots AS (
    SELECT snapshot_id FROM dpib.bronze_hh_vacancies
    GROUP BY snapshot_id
    HAVING uniqExact(concat(search_text, '|', search_area)) = 6
)
```
и фильтрует `WHERE snapshot_id IN (SELECT snapshot_id FROM complete_snapshots)`.

**2. Уровень staging-тестов:** в `_stg_hh_vacancies.yml` тесты
`accepted_values` / `not_null` имеют `where:` фильтр на последний полный снапшот.
Это и защищает от шума, и предотвращает `MEMORY_LIMIT_EXCEEDED` (тесты не сканируют
всю историю view с `JSONExtract`).

**3. Уровень Airflow:** `check_bronze` в `dbt_hh_transform` падает с понятной
ошибкой если переданный `snapshot_id` имеет `combos < 6`. Это срабатывает
до запуска dbt — экономит время.

**Старые частичные снапшоты в bronze не удалены** — они исторический факт.
Все три механизма их просто игнорируют.

---

## Производительность

**`threads: 1` в `dbt/profiles.yml`** (не флагом в командах).

Причина: параллельные `accepted_values` / `not_null` тесты на staging-view с
`JSONExtract` упирались в `MEMORY_LIMIT_EXCEEDED` ClickHouse. `--threads`
не поддерживается в `dbt deps`, поэтому держим настройку в профиле — она
применяется только к `run` / `test`.

**Query settings в `dbt_project.yml`:** для локального ClickHouse с лимитом
3 GiB у silver/gold моделей выставлены `max_threads=1` и spill-пороги
`max_bytes_before_external_group_by/sort=64MiB`. Это не меняет результат, но
не даёт одному запросу съесть весь cgroup-лимит.

**HH Silver без window sort:** `silver_hh_vacancies` выбирает лучшую строку
через `argMax(tuple(...), tuple(quality_score, description_length, ingested_at))`
и собирает `matched_queries` в той же агрегации. Это заменяет старую схему
`row_number() + LEFT JOIN`, которая лишний раз сканировала staging и давила
память на локальном стенде.

**Зарплаты HH из HTML:** в `silver_vacancies_unified` salary-парсер
нормализует обычные пробелы, NBSP и narrow NBSP перед regex. Без этого строки
вида `350 000 ₽` могли парситься как `350`. Валюта сохраняется как исходная
`RUB/USD/EUR/BYN/KZT/UZS`, без пересчёта курсов.

**Кеш dbt_packages:** при запуске через Airflow первый `dbt_deps` устанавливает
пакеты в `/tmp/dbt-packages-cache`, последующие просто `cp -r` из кеша.
Сэкономили ~2-3 минуты на каждый ран.

---

## Проверка результатов

```bash
make clickhouse-cli
```

```sql
-- Какой последний полный снапшот?
SELECT snapshot_id, uniqExact(concat(search_text,'|',search_area)) AS combos
FROM dpib.bronze_hh_vacancies
GROUP BY snapshot_id
ORDER BY max(ingested_at) DESC LIMIT 3;

-- Grain silver соблюдён?
SELECT count() AS rows, uniq((snapshot_id, vacancy_id)) AS unique_keys
FROM dpib_silver.silver_hh_vacancies;
-- rows должно == unique_keys

-- Партиал в silver попал? (должно быть 0)
SELECT count() AS partial_rows_in_silver
FROM dpib_silver.silver_hh_vacancies s
WHERE NOT EXISTS (
    SELECT 1 FROM dpib.bronze_hh_vacancies b
    WHERE b.snapshot_id = s.snapshot_id
    GROUP BY b.snapshot_id
    HAVING uniqExact(concat(b.search_text, '|', b.search_area)) = 6
);

-- Распределение качества payload
SELECT source_mode, detail_status, count()
FROM dpib_silver.silver_hh_vacancies
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Топ-20 навыков
SELECT skill, vacancies_count, devops_count, platform_count, remote_count
FROM dpib_gold.gold_skills_top
ORDER BY vacancies_count DESC LIMIT 20;
```

---

## Структура

```
dbt/
├── dbt_project.yml              schema per layer: silver / gold
├── profiles.yml                 DPIB_CLICKHOUSE_* env vars + threads=1
├── packages.yml                 dbt_utils (unique_combination_of_columns)
├── macros/
│   └── generate_schema_name.sql silver → dpib_silver, gold → dpib_gold
└── models/
    ├── staging/
    │   ├── _sources.yml         bronze в базе dpib + data_tests
    │   ├── _stg_hh_vacancies.yml тесты с where = последний полный snapshot
    │   ├── stg_hh_vacancies.sql  JSONExtract + coalesce по source_mode
    │   ├── _stg_trudvsem_vacancies.yml
    │   └── stg_trudvsem_vacancies.sql JSONExtract payload «Работы России»
    ├── silver/
    │   ├── _silver_hh_vacancies.yml
    │   ├── silver_hh_vacancies.sql  CTE complete_snapshots + argMax best-row
    │   ├── _silver_trudvsem_vacancies.yml
    │   ├── silver_trudvsem_vacancies.sql dedup по (snapshot_id, vacancy_id)
    │   └── silver_vacancies_unified.sql общий контракт источников
    └── gold/
        ├── gold_vacancies_latest.sql latest-вакансии двух источников
        └── gold_skills_top.sql       argMax + ARRAY JOIN по skills
```

---

## Ключевые решения

**Grain:**
- bronze: `(snapshot_id, search_text, search_area, vacancy_id, page_num)` — append-only
- silver: `(snapshot_id, vacancy_id)` — dedup внутри снапшота, тест `unique_combination_of_columns`
- gold: `vacancy_id` (latest snapshot per vacancy)

**quality_score в staging:**
```
4 — html_fallback + detail_status=ok     (description, skills доступны)
3 — html_fallback + detail_status=captcha (только карточные поля)
2 — html_fallback + другой статус
1 — api (на момент написания API закрыт ddos-guard)
0 — неизвестно
```

**Выбор лучшей строки в HH silver:**
`argMax(tuple(...), tuple(quality_score, description_length, ingested_at))`
на grain `(snapshot_id, vacancy_id)`. Это даёт тот же best-row контракт без
дорогого window sort.

**Маппинг баз через макрос:**
`generate_schema_name.sql` переопределяет стандартный dbt-маппинг.
`+schema: silver` → `dpib_silver`, `+schema: gold` → `dpib_gold`.

---

## Контрольный прогон (для справки)

После фикса unified/gold и памяти ClickHouse (10 июля 2026):
- `dbt run/test staging` — 2 модели, 26 тестов PASS
- `dbt run/test silver` — 3 модели, 20 тестов PASS
- `dbt run/test gold` — 2 модели, 8 тестов PASS
- Airflow `dbt_hh_transform` manual run — success
- `silver_hh_vacancies`: 19180 строк, grain уникален
- `silver_trudvsem_vacancies`: 2302 строки, grain уникален
- `silver_vacancies_unified`: 21482 строки, grain уникален
- `gold_vacancies_latest`: 1481 строка, grain `(source, vacancy_id)` уникален

---

## Roadmap

### Будущие Gold-витрины для аналитики в Superset

Ниже — план моделей для будущего аналитического дашборда Superset по образцу
[исследования рынка Python-вакансий](https://habr.com/ru/articles/1036500/).
У нас узкий срез (DevOps + Platform), continuous ingest и HTML detail-payload
с реальным `description` — это даёт точнее результат чем "теги HH = шум",
на которые жалуется автор статьи.

| # | Модель | Grain | Что показывает |
|---|---|---|---|
| 1 | `gold_vacancies_latest` | `vacancy_id` | Single source of truth — latest snapshot per vacancy (база для остальных) |
| 2 | `gold_salary_by_role_city` | `(role, city, period)` | Медиана / p25 / p75 ЗП в разрезе роль × город. Источник для heatmap |
| 3 | `gold_salary_dynamics_monthly` | `(role, month)` | Динамика медиан по месяцам. Растёт ли рынок DevOps |
| 4 | `gold_salary_disclosure_rate` | `(role, period)` | % вакансий с указанной зарплатой. Кто молчит чаще |
| 5 | `gold_salary_range_width` | `(role, city)` | Ширина вилки `(to-from)/midpoint`. Зрелость рынка |
| 6 | `gold_skills_growth_trend` | `(skill, month)` | Динамика упоминаний навыков. Что растёт, что падает |
| 7 | `gold_skills_cooccurrence` | `(skill_a, skill_b)` | Матрица сочетаемости. Источник для networkx-графа |
| 8 | `gold_market_centralization` | `(city, period)` | Доля города в общем числе вакансий. Москво-центричность |

**Антипаттерны от которых уходим** (из ошибок автора статьи):
- Фильтр `n < 30` в gold — иначе "Тбилиси DS 450К (n=4)" попадёт в выводы как шум.
- Использовать `description` + `skills` из detail-payload, не теги HH —
  у нас `quality_score >= 3` гарантирует наличие реального текста.
- Использовать `matched_roles` / `matched_areas` из silver — одна вакансия может
  попасть в разные срезы, не дублировать в подсчётах.

Эти модели не относятся к Grafana: Grafana в DPIB оставлена для технического
мониторинга ClickHouse и Airflow. Перед их реализацией нужно накопить несколько
полных плановых снапшотов и утвердить метрики аналитики.

### Технические шаги

| # | Что | Зачем |
|---|---|---|
| 9 | NER/keywords по `description` | Когда `skills` массив пустой, восстановить навыки из текста |
| 10 | dbt snapshots для history | Отслеживать изменения salary/title в одной вакансии между снапшотами |
| 11 | dbt incremental на bronze→silver | Сейчас silver = full table. На длинной истории станет медленным |
| 12 | DAG `dbt_hh_transform` в K8s | Helm + DockerOperator вместо BashOperator |


---

## См. также

- [`../runbooks/elt-memory-and-sigkill-troubleshooting.md`](../runbooks/elt-memory-and-sigkill-troubleshooting.md)
  — диагностика SIGKILL / MEMORY_LIMIT_EXCEEDED
- [`../orchestration/README.md`](../orchestration/README.md)
  — Airflow DAG-и, лимиты, проверки
