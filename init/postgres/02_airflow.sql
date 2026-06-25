-- ============================================================
-- Data Platform in a Box — PostgreSQL: Airflow metadata
-- Отдельная БД и пользователь для Airflow.
--
-- ВАЖНО: этот скрипт выполняется PostgreSQL ТОЛЬКО при первом старте
-- (когда папка /var/lib/postgresql/data пустая). На существующем томе
-- скрипт игнорируется. Если нужно подтянуть его задним числом —
-- используй `make airflow-bootstrap`.
--
-- Скрипт сделан идемпотентным (можно запускать повторно вручную).
-- ============================================================

-- Пользователь airflow (idempotent через DO-блок)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow') THEN
        CREATE USER airflow WITH PASSWORD 'airflow_pass';
        RAISE NOTICE 'User airflow created';
    ELSE
        RAISE NOTICE 'User airflow already exists, skip';
    END IF;
END $$;

-- БД airflow_meta (CREATE DATABASE не поддерживает IF NOT EXISTS в DO-блоке,
-- но идемпотентность достигается тем, что повторный запуск просто упадёт
-- с понятной ошибкой "database already exists" — для лабы достаточно)
SELECT 'CREATE DATABASE airflow_meta OWNER airflow ENCODING ''UTF8'''
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_meta')
\gexec

GRANT ALL PRIVILEGES ON DATABASE airflow_meta TO airflow;

DO $$
BEGIN
    RAISE NOTICE 'Airflow metadata ready: db=airflow_meta, user=airflow';
END $$;
