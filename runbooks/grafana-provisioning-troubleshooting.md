# Runbook: проверка provisioning Grafana

## Назначение

Проверка локального provisioning технических Grafana-дашбордов и источников
данных после изменения файлов в `config/grafana/` или пересоздания Grafana.

## Ожидаемая конфигурация

- JSON-дашборды лежат в `config/grafana/dashboards/`;
- провайдер дашбордов читает `/var/lib/grafana/dashboards/` и создаёт папку
  `DPIB`;
- Prometheus и ClickHouse описаны отдельными YAML-файлами в
  `config/grafana/provisioning/datasources/`;
- Grafana используется только для технического мониторинга. Аналитические
  дашборды не добавляются в этот контур.

## Безопасный порядок проверки

```bash
# Проверить итоговую Compose-конфигурацию и пересоздать только Grafana.
docker compose --profile core --profile monitoring config --quiet
docker compose --profile core --profile monitoring up -d --force-recreate grafana

# Убедиться, что все файлы смонтированы в контейнер.
docker exec dpib-grafana ls -la /var/lib/grafana/dashboards/
docker exec dpib-grafana ls -la /etc/grafana/provisioning/datasources/

# Посмотреть последние сообщения provisioning.
docker logs dpib-grafana 2>&1 | grep -iE 'provision|dashboard|datasource' | tail -20
```

Готовность подтверждается не только статусом контейнера: в логах должны быть
успешное provisioning datasource и завершение provisioning дашбордов, а в UI
должны отображаться ожидаемые дашборды в папке `DPIB`.

## Известные неопасные сообщения

Grafana может записывать сообщения об отсутствующих каталогах
`/etc/grafana/provisioning/plugins` и
`/etc/grafana/provisioning/alerting`. В текущем локальном контуре эти каталоги
не используются: плагины устанавливаются из Compose, а alerting не
провижинится.

Эти сообщения можно считать шумом только если provisioning datasource и
дашбордов завершился успешно. Не создавать пустые каталоги и не добавлять
alerting-конфигурацию лишь для подавления логов: это изменит контур без
функциональной необходимости.

## Когда эскалировать

Проверять и исправлять конфигурацию нужно, если:

- отсутствует ожидаемый файл в контейнере;
- datasource не появляется в Grafana;
- дашборд не появляется в папке `DPIB`;
- в логах есть ошибка чтения конкретного datasource или JSON-дашборда.
