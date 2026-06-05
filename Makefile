# ============================================================
# Data Platform in a Box — Makefile
# Запускать из WSL или из git-bash (Make должен быть в PATH)
# ============================================================

# Профили по умолчанию. Переопределяй: make up PROFILES="core monitoring"
PROFILES ?= core

# Собираем флаги --profile для docker compose
PROFILE_FLAGS := $(foreach p,$(PROFILES),--profile $(p))

.PHONY: help up down restart status logs ps clean reset psql clickhouse-cli

help:  ## показать список команд
	@echo "Data Platform in a Box"
	@echo ""
	@echo "Команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Профили (PROFILES=...):"
	@echo "  core          — PostgreSQL + ClickHouse"
	@echo "  monitoring    — Prometheus + Grafana (TBD)"
	@echo "  orchestration — Airflow (TBD)"
	@echo "  viz           — Superset (TBD)"
	@echo "  ai            — Qdrant (TBD)"
	@echo ""
	@echo "Пример: make up PROFILES=\"core monitoring\""

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

# ---- Подключения к базам для быстрой проверки ----
psql:  ## подключиться к PostgreSQL psql-клиентом
	docker exec -it dpib-postgres psql -U $${POSTGRES_USER:-dpib} -d $${POSTGRES_DB:-dpib}

clickhouse-cli:  ## подключиться к ClickHouse client
	docker exec -it dpib-clickhouse clickhouse-client \
		--user $${CLICKHOUSE_USER:-dpib} \
		--password $${CLICKHOUSE_PASSWORD:-dpib_pass} \
		--database $${CLICKHOUSE_DB:-dpib}
