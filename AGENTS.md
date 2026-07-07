# Контекст проекта для агентов

## Назначение

Локальная учебная data-платформа в Docker Compose: Airflow загружает вакансии
в ClickHouse, dbt преобразует Bronze → Silver → Gold, Prometheus/Grafana
наблюдают за сервисами и ELT.

## Стек и структура

- `docker-compose.yml`, `Makefile` — источник истины для локального стенда.
- `init/clickhouse/` — идемпотентные DDL для ClickHouse.
- `orchestration/dags/` — Airflow 3 DAG-и загрузки и трансформации.
- `dbt/` — staging, silver и gold модели.
- `config/prometheus/`, `config/grafana/` — мониторинг.
- `runbooks/` — обезличенные эксплуатационные инструкции.

## Источники вакансий

Каждый источник пишет исходный JSON в отдельную таблицу Bronze:

- HH → `bronze_hh_vacancies`;
- «Работа России» → `bronze_trudvsem_vacancies`.

Не объединять разные payload в одной Bronze-таблице. Общий контракт строить
в staging/silver только после получения реального payload «Работы России»
и сверки зарплаты, региона, графика, описания и дат с HH.

HH API для соискательских/исследовательских приложений недоступен; рабочий путь
проекта — ограниченный HTML fallback. Не усиливать обход антибот-защиты.
SuperJob исключён из проекта: ключ приложения недоступен, DAG удалён. «Работа
России» использует открытый API без ключа. Контактные данные людей в новые
Bronze-таблицы не сохранять.

Новый DAG `trudvsem_vacancies_snapshot` пока реализует только Bronze. До
расширения Silver требуется runtime-проверка после запуска контейнеров.

## Рабочие команды

Запускать из WSL:

```bash
make up PROFILES="core orchestration"
make status
make clickhouse-bootstrap
make clickhouse-multi-source-bootstrap
make airflow-list-dags
make airflow-trigger DAG=trudvsem_vacancies_snapshot
```

Статические проверки без запуска сервисов:

```bash
python -m py_compile orchestration/dags/*.py
docker compose config --quiet
git diff --check
```

После запуска обязательно проверить:

```bash
docker exec dpib-airflow-scheduler airflow dags list-import-errors
```

## Критерии готовности ingestion

Зелёный DAG сам по себе не доказывает корректность:

- новый `snapshot_id` присутствует в нужной Bronze-таблице;
- `count() > 0`;
- идентификаторы вакансий заполнены;
- повтор задачи не создаёт дублей внутри одного snapshot/search;
- в логах нет `401/403/429`, OOM, SIGKILL и скрытого `skip`;
- секреты и контактные данные не попали в Git или логи.

Для HH дополнительно требуется полный snapshot из шести поисковых комбинаций.

## Мониторинг

Пороговые алерты памяти проверять только на поднятом профиле `monitoring`.
Не угадывать имена Airflow-метрик: сначала посмотреть фактический вывод
StatsD exporter, затем добавлять алерт длительности по нескольким плановым
запускам. Стандартный endpoint ClickHouse не показывает рост каждой системной
таблицы отдельно; для этого нужен ограниченный SQL-check или отдельный exporter.

## Правила изменений

- Перед работой проверить `git status` и не перезаписывать пользовательские правки.
- `dbt/README.md` может содержать незавершённые пользовательские изменения.
- Не отслеживать `orchestration/logs/`; это runtime-данные Airflow.
- Не запускать контейнеры, если пользователь сообщил, что стенд остановлен,
  без отдельного согласования запуска.
- Не выполнять `git commit`, `git push`, удаление данных/томов или reset без
  явной команды пользователя.
- Секреты хранить только в локальном `.env` или защищённом хранилище.
- Изменения DDL делать через идемпотентные файлы в `init/clickhouse/`.
- Сохранять рабочий HH ingestion; новые источники добавлять изолированно.
