# Этот каталог монтируется в Airflow-контейнеры как /opt/airflow/auth (read-only)
# и используется SimpleAuthManager для статических паролей пользователей.

Файл `simple_auth_manager_passwords.json`:
- Формат: `{"username": "password"}`
- По умолчанию: `admin / admin`
- В проде — не использовать SimpleAuthManager; переключиться на FabAuthManager или KeycloakAuthManager.
