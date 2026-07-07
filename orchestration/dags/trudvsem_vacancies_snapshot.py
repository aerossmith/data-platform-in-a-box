"""Снимок открытых вакансий «Работа России» в ClickHouse Bronze."""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

import clickhouse_connect
import requests
from airflow.decorators import dag, task

API_URL = "https://opendata.trudvsem.ru/api/v1/vacancies"
SEARCH_TEXTS = [
    "DevOps",  # ядро, накрывает "DevOps инженер"
    "девопс",  # кириллическая форма, не пересекается с латиницей
    "SRE",
    "инженер по инфраструктуре",  # ближайший аналог Platform Engineer
]
SEARCH_AREA = "all"
PAGE_SIZE = 100
MAX_PAGES = int(os.environ.get("DPIB_TRUDVSEM_MAX_PAGES", "5"))
REQUEST_TIMEOUT = 30

CH_HOST = os.environ.get("DPIB_CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("DPIB_CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("DPIB_CLICKHOUSE_USER", "dpib")
CH_PASSWORD = os.environ.get("DPIB_CLICKHOUSE_PASSWORD", "dpib_pass")
CH_DATABASE = os.environ.get("DPIB_CLICKHOUSE_DB", "dpib")

CONTACT_KEYS = {
    "contact",
    "contacts",
    "contact-list",
    "contact_list",
    "contact-person",
    "contact_person",
    "email",
    "fax",
    "phone",
    "phones",
}


def _without_contacts(value: Any) -> Any:
    """Удаляет ненужные для аналитики контактные поля до записи в Bronze."""
    if isinstance(value, dict):
        return {
            key: _without_contacts(item)
            for key, item in value.items()
            if key.lower() not in CONTACT_KEYS
        }
    if isinstance(value, list):
        return [_without_contacts(item) for item in value]
    return value


def _insert_rows(rows: list[dict[str, Any]], snapshot_id: str, search_text: str) -> int:
    if not rows:
        return 0

    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )
    columns = ["snapshot_id", "search_text", "search_area", "page_num", "vacancy_id", "raw_json"]
    try:
        existing = client.query(
            """
            SELECT DISTINCT vacancy_id
            FROM bronze_trudvsem_vacancies
            WHERE snapshot_id = {snapshot_id:UUID}
              AND search_text = {search_text:String}
              AND search_area = {search_area:String}
            """,
            parameters={
                "snapshot_id": snapshot_id,
                "search_text": search_text,
                "search_area": SEARCH_AREA,
            },
        )
        existing_ids = {str(row[0]) for row in existing.result_rows}
        new_rows = [row for row in rows if row["vacancy_id"] not in existing_ids]
        if not new_rows:
            return 0

        client.insert(
            table="bronze_trudvsem_vacancies",
            data=[[row[column] for column in columns] for row in new_rows],
            column_names=columns,
        )
        return len(new_rows)
    finally:
        client.close()


@dag(
    dag_id="trudvsem_vacancies_snapshot",
    description="Открытые вакансии «Работа России» → ClickHouse Bronze.",
    schedule="40 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dpib", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["trudvsem", "bronze", "vacancies"],
)
def trudvsem_vacancies_snapshot():
    @task
    def make_snapshot_id() -> str:
        return str(uuid.uuid4())

    @task
    def ingest_search(search_text: str, snapshot_id: str) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        with requests.Session() as session:
            session.headers.update({"User-Agent": "data-platform-in-a-box/1.0"})
            for page in range(MAX_PAGES):
                response = session.get(
                    API_URL,
                    params={
                        "text": search_text,
                        "limit": PAGE_SIZE,
                        "offset": page,
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
                if str(payload.get("status")) != "200":
                    raise RuntimeError(
                        f"«Работа России» вернула status={payload.get('status')} для page={page}"
                    )

                items = payload.get("results", {}).get("vacancies", [])
                if not items:
                    break

                for item in items:
                    vacancy = _without_contacts(item.get("vacancy", item))
                    vacancy_id = str(vacancy.get("id", "")).strip()
                    if not vacancy_id or vacancy_id in seen_ids:
                        continue
                    rows.append(
                        {
                            "snapshot_id": snapshot_id,
                            "search_text": search_text,
                            "search_area": SEARCH_AREA,
                            "page_num": page,
                            "vacancy_id": vacancy_id,
                            "raw_json": json.dumps(vacancy, ensure_ascii=False),
                        }
                    )
                    seen_ids.add(vacancy_id)

                total = int(payload.get("meta", {}).get("total", 0) or 0)
                if len(items) < PAGE_SIZE or (page + 1) * PAGE_SIZE >= total:
                    break
                time.sleep(0.5)

        if not rows:
            raise RuntimeError(f"«Работа России» не вернула вакансии для запроса {search_text!r}")

        inserted = _insert_rows(rows, snapshot_id, search_text)
        print(f"[trudvsem/{search_text}] fetched={len(rows)}, inserted={inserted}")
        return {"search_text": search_text, "fetched": len(rows), "inserted": inserted}

    @task
    def report_total(results: list[dict[str, Any]]) -> dict[str, int]:
        fetched = sum(result["fetched"] for result in results)
        inserted = sum(result["inserted"] for result in results)
        print(f"[trudvsem] fetched={fetched}, inserted={inserted}")
        return {"fetched": fetched, "inserted": inserted}

    snapshot_id = make_snapshot_id()
    results = ingest_search.partial(snapshot_id=snapshot_id).expand(search_text=SEARCH_TEXTS)
    report_total(results)


trudvsem_vacancies_snapshot()
