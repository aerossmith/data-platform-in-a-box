# ============================================================
# Data Platform in a Box — Makefile
# Запускать из WSL или из git-bash (Make должен быть в PATH)
# ============================================================

# Профили по умолчанию. Переопределяй: make up PROFILES="core monitoring"
PROFILES ?= core

# Собираем флаги --profile для docker compose
PROFILE_FLAGS := $(foreach p,$(PROFILES),--profile $(p))

.PHONY: help up down restart status logs ps clean reset psql clickhouse-cli redis-cli \
        airflow-shell airflow-logs airflow-trigger airflow-list-dags airflow-bootstrap \
        clickhouse-bootstrap

help:  ## показать список команд
	@echo "Data Platform in a Box"
	@echo ""
	@echo "Команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Профили (PROFILES=...):"
	@echo "  core          — PostgreSQL + ClickHouse + Redis"
	@echo "  monitoring    — Prometheus + Grafana + экспортёры"
	@echo "  orchestration — Airflow 3.x (CeleryExecutor)"
	@echo "  viz           — Superset (TBD)"
	@echo "  ai            — Qdrant (TBD)"
	@echo ""
	@echo "Пример: make up PROFILES=\"core orchestration\""

up:  ## поднять стек (по умолчанию профиль core)
	docker compose $(PROFILE_FLAGS) up -d
	@echo ""
	@echo "Готово. Проверь: make status"

down:  ## остановить стек (тома сохраняются)
	docker compose $(PROFILE_FLAGS) down

restart:  ## перезапустить стек
	$(MAKE) down PROFILES="$(PROFILES)"
	$(MAKE) up PROFILES="$(PROFILES)"

status:  ## показать статус контейнеров
	docker compose $(PROFILE_FLAGS) ps

ps: status  ## алиас для status

logs:  ## показать логи (make logs SVC=postgres)
	@if [ -z "$(SVC)" ]; then \
		docker compose $(PROFILE_FLAGS) logs -f --tail=100; \
	else \
		docker compose $(PROFILE_FLAGS) logs -f --tail=100 $(SVC); \
	fi

clean:  ## остановить и удалить контейнеры (тома сохраняются)
	docker compose $(PROFILE_FLAGS) down --remove-orphans

reset:  ## ОПАСНО: удалить контейнеры И тома (потеря данных)
	@echo "ВНИМАНИЕ: удалятся все тома и данные!"
	@read -p "Продолжить? [y/N] " ans; [ "$$ans" = "y" ] || exit 1
	docker compose $(PROFILE_FLAGS) down -v --remove-orphans

# ---- Подключения к сервисам ----
psql:  ## подключиться к PostgreSQL psql-клиентом
	docker exec -it dpib-postgres psql -U $${POSTGRES_USER:-dpib} -d $${POSTGRES_DB:-dpib}

clickhouse-cli:  ## подключиться к ClickHouse client
	docker exec -it dpib-clickhouse clickhouse-client \
		--user $${CLICKHOUSE_USER:-dpib} \
		--password $${CLICKHOUSE_PASSWORD:-dpib_pass} \
		--database $${CLICKHOUSE_DB:-dpib}

redis-cli:  ## подключиться к Redis
	docker exec -it dpib-redis redis-cli

# ---- Airflow ----
airflow-shell:  ## bash внутри airflow-scheduler
	docker exec -it dpib-airflow-scheduler bash

airflow-logs:  ## tail логов scheduler+worker одновременно
	docker compose --profile orchestration logs -f --tail=100 airflow-scheduler airflow-worker airflow-dag-processor

airflow-trigger:  ## запустить DAG вручную: make airflow-trigger DAG=hh_vacancies_snapshot
	docker exec dpib-airflow-scheduler airflow dags trigger $(DAG)

airflow-list-dags:  ## показать все зарегистрированные DAG-и
	docker exec dpib-airflow-scheduler airflow dags list

airflow-bootstrap:  ## создать user+db airflow в существующем PostgreSQL (если том уже был)
	@echo "Применяю init/postgres/02_airflow.sql к работающему PostgreSQL..."
	docker exec -i dpib-postgres psql -U $${POSTGRES_USER:-dpib} -d postgres < init/postgres/02_airflow.sql
	@echo ""
	@echo "Готово. Теперь пересоздай Airflow контейнеры:"
	@echo "  docker compose --profile orchestration up -d --force-recreate"

clickhouse-bootstrap:  ## создать БД dpib_silver+dpib_gold в работающем ClickHouse (если том уже был)
	@echo "Применяю init/clickhouse/03_create_databases.sql к работающему ClickHouse..."
	docker exec -i dpib-clickhouse clickhouse-client \
		--user $${CLICKHOUSE_USER:-dpib} \
		--password $${CLICKHOUSE_PASSWORD:-dpib_pass} \
		--multiquery < init/clickhouse/03_create_databases.sql
	@echo ""
	@echo "Готово. Проверь: docker exec dpib-clickhouse clickhouse-client --query 'SHOW DATABASES'"
