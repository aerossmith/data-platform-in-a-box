# Выбор Docker Engine при работе из WSL

## Назначение

Исключить одновременный запуск одного Compose-проекта в Docker Engine внутри
WSL и Docker Desktop на хосте. Это два независимых движка: у них разные
контейнеры, сети и именованные тома, даже когда имена Compose-проекта совпадают.

## Признаки проблемы

- `make status` показывает пустой список, хотя сервисы доступны в браузере;
- `docker compose up -d` сообщает, что порт уже занят;
- один клиент показывает контейнеры в состоянии `Up`, другой — `Created`;
- одновременно существуют две группы одноимённых контейнеров в разных
  Docker-контекстах.

## Диагностика

Выполнять из WSL до запуска или перезапуска:

```bash
docker context ls
docker compose ls
docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

Если используется Docker Desktop, дополнительно проверить его Windows-клиент:

```bash
docker.exe compose ls
docker.exe ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
```

Нужно выбрать ровно один движок, в котором находятся рабочие данные.
Не делать вывод только по имени контейнера: проверить и статус сервисов, и
наличие ожидаемых данных в хранилище.

## Рабочий режим

Для проекта, который развёрнут в Docker Engine WSL, использовать обычные
команды без `sudo`:

```bash
make up PROFILES="core orchestration monitoring"
make status PROFILES="core orchestration monitoring"
```

Если осознанно выбран Docker Desktop, передавать клиент в Makefile явно:

```bash
make up PROFILES="core orchestration monitoring" DOCKER=docker.exe
make status PROFILES="core orchestration monitoring" DOCKER=docker.exe
```

Не смешивать режимы в одной сессии и не использовать `sudo make ...`: у root
может быть другой Docker-контекст или сокет.

## Безопасное устранение дубликата

1. В обоих движках зафиксировать `docker compose ps -a` и определить, где
   находится нужный набор данных.
2. В ошибочном движке остановить и удалить только контейнеры и сеть проекта:

   ```bash
   docker compose --profile core --profile orchestration --profile monitoring down
   ```

   Не добавлять `-v`: именованные тома с данными должны сохраниться.
3. В выбранном движке поднять стек один раз и проверить статусы.
4. Убедиться, что во втором движке не осталось контейнеров данного проекта.

## Критерии завершения

- порты сервисов опубликованы только одним Docker Engine;
- Airflow API и scheduler healthy;
- ClickHouse, PostgreSQL и Redis healthy;
- `airflow dags list-import-errors` не возвращает ошибок;
- в таблицах хранилища присутствуют ожидаемые данные;
- не удалены именованные тома и не затронуты другие Compose-проекты.
