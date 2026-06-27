"""
dbt_hh_transform — T в паттерне ELT.
Запускается через TriggerDagRunOperator из hh_vacancies_snapshot.
Цепочка: check_bronze → dbt_deps → staging → test → silver → test → gold → check_silver

Решение read-only монтирования /opt/airflow/dbt:
  Копируем проект в /tmp/dbt-project (туда dbt пишет logs/target/dbt_packages).

Кеш dbt deps:
  Пакеты хранятся в /tmp/dbt-packages-cache — устанавливаются один раз
  при первом запуске, потом просто копируются из кеша (~секунды вместо 2-3 минут).
  Кеш сбрасывается только при рестарте контейнера.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

import clickhouse_connect
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_SRC       = "/opt/airflow/dbt"
DBT_TMP       = "/tmp/dbt-project"
DBT_PKG_CACHE = "/tmp/dbt-packages-cache"
PF            = f"--profiles-dir {DBT_TMP} --no-version-check"

CH_HOST     = os.environ.get("DPIB_CLICKHOUSE_HOST",     "clickhouse")
CH_PORT     = int(os.environ.get("DPIB_CLICKHOUSE_PORT", "8123"))
CH_USER     = os.environ.get("DPIB_CLICKHOUSE_USER",     "dpib")
CH_PASSWORD = os.environ.get("DPIB_CLICKHOUSE_PASSWORD", "dpib_pass")
CH_DATABASE = os.environ.get("DPIB_CLICKHOUSE_DB",       "dpib")


def _ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
    )


@dag(
    dag_id="dbt_hh_transform",
    description="bronze → staging → silver → gold через dbt.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dpib", "retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["dbt", "silver", "gold", "transform"],
)
def dbt_hh_transform():

    @task
    def check_bronze() -> dict[str, Any]:
        """Проверяем что в bronze есть свежие данные перед запуском dbt."""
        client = _ch()
        result = client.query("""
            SELECT
                snapshot_id,
                count()          AS rows,
                uniq(vacancy_id) AS unique_vacancies,
                min(ingested_at) AS first_row,
                max(ingested_at) AS last_row
            FROM dpib.bronze_hh_vacancies
            WHERE snapshot_id = (
                SELECT snapshot_id FROM dpib.bronze_hh_vacancies
                ORDER BY ingested_at DESC LIMIT 1
            )
            GROUP BY snapshot_id
        """)
        if not result.result_rows:
            raise RuntimeError("bronze_hh_vacancies пустой — нечего трансформировать")
        row = result.result_rows[0]
        stats = {
            "snapshot_id":        str(row[0]),
            "bronze_rows":        row[1],
            "bronze_unique_vacs": row[2],
            "first_row":          str(row[3]),
            "last_row":           str(row[4]),
        }
        print("\n[check_bronze] Последний снапшот:")
        for k, v in stats.items():
            print(f"  {k}: {v}")
        return stats

    # Кешируем dbt deps — устанавливаем один раз в /tmp/dbt-packages-cache,
    # при последующих запусках просто копируем из кеша (~секунды).
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
set -e

# Свежая копия проекта (без dbt_packages)
rm -rf {DBT_TMP}
cp -r {DBT_SRC} {DBT_TMP}

if [ -d {DBT_PKG_CACHE} ]; then
    echo "[dbt_deps] Кеш найден, восстанавливаем пакеты из {DBT_PKG_CACHE}"
    cp -r {DBT_PKG_CACHE} {DBT_TMP}/dbt_packages
    echo "[dbt_deps] Пакеты восстановлены из кеша"
else
    echo "[dbt_deps] Кеша нет, запускаем dbt deps (первый раз)"
    cd {DBT_TMP} && dbt deps {PF} 2>&1
    echo "[dbt_deps] Кешируем установленные пакеты в {DBT_PKG_CACHE}"
    cp -r {DBT_TMP}/dbt_packages {DBT_PKG_CACHE}
    echo "[dbt_deps] Кеш сохранён"
fi
""",
    )

    run_staging  = BashOperator(task_id="dbt_run_staging",
        bash_command=f"cd {DBT_TMP} && dbt run  {PF} --select staging 2>&1")
    test_staging = BashOperator(task_id="dbt_test_staging",
        bash_command=f"cd {DBT_TMP} && dbt test {PF} --select staging 2>&1")
    run_silver   = BashOperator(task_id="dbt_run_silver",
        bash_command=f"cd {DBT_TMP} && dbt run  {PF} --select silver 2>&1")
    test_silver  = BashOperator(task_id="dbt_test_silver",
        bash_command=f"cd {DBT_TMP} && dbt test {PF} --select silver 2>&1")
    run_gold     = BashOperator(task_id="dbt_run_gold",
        bash_command=f"cd {DBT_TMP} && dbt run  {PF} --select gold 2>&1")

    @task
    def check_silver(bronze_stats: dict[str, Any]) -> dict[str, Any]:
        """Контрольный SELECT: grain, качество payload, описания, навыки."""
        client = _ch()
        r = client.query("""
            SELECT
                count()                            AS silver_rows,
                uniq((snapshot_id, vacancy_id))    AS unique_keys,
                countIf(detail_status = 'ok')      AS detail_ok,
                countIf(detail_status = 'captcha') AS detail_captcha,
                countIf(description_length > 0)    AS has_description,
                countIf(skills_count > 0)          AS has_skills,
                round(avg(description_length))     AS avg_desc_len
            FROM dpib_silver.silver_hh_vacancies
            WHERE snapshot_id = {snapshot_id:UUID}
        """, parameters={"snapshot_id": bronze_stats["snapshot_id"]})

        row = r.result_rows[0] if r.result_rows else (0,) * 7
        silver_rows, unique_keys = row[0], row[1]
        grain_ok = silver_rows == unique_keys

        def pct(n):
            return f"{round(n / silver_rows * 100, 1)}%" if silver_rows else "0%"

        stats = {
            "silver_rows":   silver_rows,
            "unique_keys":   unique_keys,
            "grain_ok":      grain_ok,
            "detail_ok_pct": pct(row[2]),
            "desc_pct":      pct(row[4]),
            "skills_pct":    pct(row[5]),
            "avg_desc_len":  row[6],
        }
        print(f"\n[check_silver] snapshot={bronze_stats['snapshot_id']}:")
        print(f"  bronze → silver: {bronze_stats['bronze_rows']} → {silver_rows}")
        print(f"  grain уникален:  {grain_ok}")
        print(f"  detail_status=ok: {row[2]} ({pct(row[2])})")
        print(f"  есть description: {row[4]} ({pct(row[4])})")
        print(f"  есть skills:      {row[5]} ({pct(row[5])})")
        print(f"  средняя длина описания: {row[6]} символов")

        if not grain_ok:
            raise RuntimeError(
                f"Нарушен grain silver: rows={silver_rows} != unique_keys={unique_keys}"
            )
        return stats

    bronze = check_bronze()
    silver_check = check_silver(bronze_stats=bronze)

    (
        bronze
        >> dbt_deps
        >> run_staging >> test_staging
        >> run_silver  >> test_silver
        >> run_gold    >> silver_check
    )


dbt_hh_transform()
