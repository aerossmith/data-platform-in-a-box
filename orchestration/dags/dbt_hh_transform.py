"""
dbt_hh_transform — T в паттерне ELT.
Запускается через TriggerDagRunOperator из hh_vacancies_snapshot.
Цепочка: check_bronze → dbt_deps → staging → test → silver → test → gold → check_silver

snapshot_id:
  Читается из dag_run.conf["snapshot_id"] (передан из ingest через TriggerDagRunOperator).
  При ручном запуске без conf — fallback на последний ПОЛНЫЙ снапшот (6 комбинаций).
  check_bronze явно проваливается если snapshot_id частичный (< 6 комбинаций).

Память:
  threads=1 задан в dbt/profiles.yml. Staging-view читает bronze с JSONExtract.
  Тесты ограничены последним полным snapshot через where в _stg_hh_vacancies.yml.
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

EXPECTED_COMBOS = 6  # DevOps + Platform × msk + spb + remote

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
    def check_bronze(**context) -> dict[str, Any]:
        """
        1. Читает snapshot_id из dag_run.conf (TriggerDagRunOperator путь).
        2. При ручном запуске без conf — ищет последний ПОЛНЫЙ снапшот
           (uniqExact(search_text||'|'||search_area) = 6).
        3. Явно падает если snapshot частичный (< 6 комбинаций).
           Это защита silver от неполных данных.
        """
        client = _ch()

        dag_run = context.get("dag_run")
        conf_snapshot_id = None
        if dag_run and dag_run.conf:
            conf_snapshot_id = dag_run.conf.get("snapshot_id")

        if conf_snapshot_id:
            print(f"[check_bronze] snapshot_id из dag_run.conf: {conf_snapshot_id}")
            snapshot_id = conf_snapshot_id
        else:
            print(f"[check_bronze] conf пустой — ищем последний полный снапшот ({EXPECTED_COMBOS} комбинаций)")
            latest = client.query(f"""
                SELECT snapshot_id
                FROM dpib.bronze_hh_vacancies
                GROUP BY snapshot_id
                HAVING uniqExact(concat(search_text, '|', search_area)) = {EXPECTED_COMBOS}
                ORDER BY max(ingested_at) DESC
                LIMIT 1
            """)
            if not latest.result_rows:
                raise RuntimeError(
                    f"Не найден ни один полный снапшот с {EXPECTED_COMBOS} комбинациями в bronze. "
                    f"Запустите ingest и дождитесь success всех {EXPECTED_COMBOS} ingest_combo."
                )
            snapshot_id = str(latest.result_rows[0][0])
            print(f"[check_bronze] Fallback snapshot_id: {snapshot_id}")

        # Проверяем количество комбинаций в выбранном снапшоте
        combo_check = client.query(f"""
            SELECT
                uniqExact(concat(search_text, '|', search_area)) AS combos,
                count()          AS rows,
                uniq(vacancy_id) AS unique_vacancies,
                min(ingested_at) AS first_row,
                max(ingested_at) AS last_row
            FROM dpib.bronze_hh_vacancies
            WHERE snapshot_id = {{snapshot_id:UUID}}
        """, parameters={"snapshot_id": snapshot_id})

        if not combo_check.result_rows or combo_check.result_rows[0][1] == 0:
            raise RuntimeError(
                f"snapshot_id={snapshot_id} не найден в bronze_hh_vacancies."
            )

        row = combo_check.result_rows[0]
        combos = row[0]

        if combos < EXPECTED_COMBOS:
            raise RuntimeError(
                f"snapshot_id={snapshot_id} ЧАСТИЧНЫЙ: {combos}/{EXPECTED_COMBOS} комбинаций. "
                f"Трансформацию не запускаем — silver получит неполные данные. "
                f"Дождитесь успешного ingest со всеми {EXPECTED_COMBOS} комбинациями."
            )

        stats = {
            "snapshot_id":        snapshot_id,
            "combos":             combos,
            "bronze_rows":        row[1],
            "bronze_unique_vacs": row[2],
            "first_row":          str(row[3]),
            "last_row":           str(row[4]),
        }

        print(f"\n[check_bronze] Снапшот для трансформации:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

        return stats

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
set -e

echo "[dbt_deps] Копируем проект (без .venv, logs, target, dbt_packages)"
rm -rf {DBT_TMP}
mkdir -p {DBT_TMP}

for item in models macros tests seeds analyses snapshots \
            dbt_project.yml profiles.yml packages.yml; do
    src="{DBT_SRC}/$item"
    if [ -e "$src" ]; then
        cp -r "$src" "{DBT_TMP}/$item"
    fi
done

echo "[dbt_deps] Проект скопирован, размер: $(du -sh {DBT_TMP} | cut -f1)"

if [ -d {DBT_PKG_CACHE} ]; then
    echo "[dbt_deps] Кеш найден — восстанавливаем пакеты"
    cp -r {DBT_PKG_CACHE} {DBT_TMP}/dbt_packages
else
    echo "[dbt_deps] Кеша нет — запускаем dbt deps"
    cd {DBT_TMP} && dbt deps {PF} 2>&1
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
        """
        Финальная верификация:
        - snapshot из bronze присутствует в silver
        - grain (snapshot_id, vacancy_id) уникален
        - логируем bronze → silver статистику
        """
        client = _ch()
        snapshot_id = bronze_stats["snapshot_id"]

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
        """, parameters={"snapshot_id": snapshot_id})

        row = r.result_rows[0] if r.result_rows else (0,) * 7
        silver_rows, unique_keys = row[0], row[1]
        grain_ok          = silver_rows == unique_keys
        snapshot_in_silver = silver_rows > 0

        def pct(n):
            return f"{round(n / silver_rows * 100, 1)}%" if silver_rows else "0%"

        stats = {
            "snapshot_id":       snapshot_id,
            "bronze_rows":       bronze_stats["bronze_rows"],
            "silver_rows":       silver_rows,
            "grain_ok":          grain_ok,
            "snapshot_in_silver": snapshot_in_silver,
            "detail_ok_pct":     pct(row[2]),
            "desc_pct":          pct(row[4]),
            "skills_pct":        pct(row[5]),
            "avg_desc_len":      row[6],
        }

        print(f"\n[check_silver] snapshot_id={snapshot_id}:")
        print(f"  bronze_rows → silver_rows: {bronze_stats['bronze_rows']} → {silver_rows}")
        print(f"  snapshot присутствует в silver: {snapshot_in_silver}")
        print(f"  grain уникален: {grain_ok}")
        print(f"  detail_status=ok:  {row[2]} ({pct(row[2])})")
        print(f"  есть description:  {row[4]} ({pct(row[4])})")
        print(f"  есть skills:       {row[5]} ({pct(row[5])})")
        print(f"  средняя длина описания: {row[6]} символов")

        if not snapshot_in_silver:
            raise RuntimeError(
                f"snapshot_id={snapshot_id} есть в bronze, но отсутствует в silver"
            )
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
