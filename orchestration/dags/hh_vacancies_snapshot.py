"""
============================================================
hh_vacancies_snapshot — обход ddos-guard через 2-step warmup
============================================================
- curl_cffi с impersonate=chrome120 (TLS-fingerprint Chrome)
- warmup: главная hh.ru → страница поиска → API
- HTML fallback при 401/403 от API
- После успешного ingest — триггер dbt_hh_transform (T в ELT)
- snapshot_id передаётся в TriggerDagRunOperator.conf

Фиксы памяти:
- fetch_detail_payload использует переданный session (не создаёт новый на каждую вакансию)
- soup.decompose() после парсинга каждой detail-страницы
- ingest_combo закрывает session через try/finally
============================================================
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote_plus, urljoin

import clickhouse_connect
from curl_cffi import requests as curl_requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from bs4 import BeautifulSoup

HH_HOME_URL = "https://hh.ru/"
HH_SEARCH_URL_TEMPLATE = "https://hh.ru/search/vacancy?text={text}&area={area}"
HH_SEARCH_URL = "https://hh.ru/search/vacancy"
HH_API_URL = "https://api.hh.ru/vacancies"

IMPERSONATE_PROFILE = os.environ.get("DPIB_HH_IMPERSONATE", "chrome120")

PER_PAGE = 100
MAX_PAGES = 20
HTML_FALLBACK_MAX_PAGES = int(os.environ.get("DPIB_HH_HTML_FALLBACK_MAX_PAGES", "5"))
FETCH_DETAILS = os.environ.get("DPIB_HH_FETCH_DETAILS", "true").lower() in {"1", "true", "yes", "on"}
DETAIL_MAX_PER_COMBO = int(os.environ.get("DPIB_HH_DETAIL_MAX_PER_COMBO", "100"))
DETAIL_PAUSE_MIN_SEC = float(os.environ.get("DPIB_HH_DETAIL_PAUSE_MIN_SEC", "0.2"))
DETAIL_PAUSE_MAX_SEC = float(os.environ.get("DPIB_HH_DETAIL_PAUSE_MAX_SEC", "0.6"))
REQUEST_TIMEOUT = 30
INSERT_BATCH_SIZE = 500

CH_HOST = os.environ.get("DPIB_CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.environ.get("DPIB_CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("DPIB_CLICKHOUSE_USER", "dpib")
CH_PASSWORD = os.environ.get("DPIB_CLICKHOUSE_PASSWORD", "dpib_pass")
CH_DATABASE = os.environ.get("DPIB_CLICKHOUSE_DB", "dpib")

log = logging.getLogger(__name__)


def build_search_params() -> list[dict[str, Any]]:
    roles = ["DevOps Engineer", "Platform Engineer"]
    regions = [
        {"label": "msk",    "area": "1"},
        {"label": "spb",    "area": "2"},
        {"label": "remote", "schedule": "remote"},
    ]
    combos = []
    for role in roles:
        for region in regions:
            params = {"text": role}
            params.update({k: v for k, v in region.items() if k != "label"})
            combos.append({
                "search_text": role,
                "search_area": region["label"],
                "api_params": params,
            })
    return combos


def warmup_session(combo, prefix):
    session = curl_requests.Session(impersonate=IMPERSONATE_PROFILE)

    print(f"{prefix} warmup 1/2: GET {HH_HOME_URL} (impersonate={IMPERSONATE_PROFILE})")
    r1 = session.get(HH_HOME_URL, timeout=REQUEST_TIMEOUT)
    ddg = [c.name for c in session.cookies.jar if c.name.startswith("__ddg")]
    print(f"{prefix}   → status={r1.status_code}, body_size={len(r1.content)}, ddg_cookies={ddg}")
    if r1.status_code != 200:
        print(f"{prefix}   WARN: home returned {r1.status_code}, body={r1.text[:300]}")

    pause = random.uniform(1.5, 3.0)
    print(f"{prefix}   sleep {pause:.1f}s")
    time.sleep(pause)

    role = combo["search_text"]
    area = combo["api_params"].get("area", "")
    search_url = HH_SEARCH_URL_TEMPLATE.format(text=quote_plus(role), area=area)
    print(f"{prefix} warmup 2/2: GET {search_url}")
    r2 = session.get(search_url, headers={"Referer": HH_HOME_URL}, timeout=REQUEST_TIMEOUT)
    print(f"{prefix}   → status={r2.status_code}, body_size={len(r2.content)}")
    if r2.status_code != 200:
        print(f"{prefix}   WARN: search returned {r2.status_code}, body={r2.text[:300]}")

    pause = random.uniform(1.0, 2.5)
    print(f"{prefix}   sleep {pause:.1f}s")
    time.sleep(pause)

    return session, search_url


def first_text(root, selector: str) -> str | None:
    node = root.select_one(selector) if root else None
    if not node:
        return None
    return node.get_text(" ", strip=True) or None


def unique_texts(root, selectors: list[str]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    if not root:
        return values
    for selector in selectors:
        for node in root.select(selector):
            text = node.get_text(" ", strip=True)
            if text and text not in seen:
                values.append(text)
                seen.add(text)
    return values


def limited_text(root, selector: str, limit: int = 20000) -> str | None:
    node = root.select_one(selector) if root else None
    if not node:
        return None
    text = node.get_text("\n", strip=True)
    return text[:limit] if text else None


def find_card_root(title_link):
    node = title_link
    for _ in range(8):
        if node is None:
            break
        if node.select_one('a[data-qa="vacancy-serp__vacancy-employer"]'):
            return node
        text = node.get_text(" ", strip=True)
        if "Откликнуться" in text or "Вакансии" in text:
            return node
        node = node.parent
    return title_link.parent


def vacancy_id_from_url(url: str) -> str:
    match = re.search(r"/vacancy/(\d+)", url)
    return match.group(1) if match else url


def canonical_vacancy_url(url: str) -> str:
    vacancy_id = vacancy_id_from_url(url)
    if vacancy_id and vacancy_id != url:
        return f"{HH_HOME_URL.rstrip('/')}/vacancy/{vacancy_id}"
    return url.split("?", 1)[0]


def extract_experience_from_card(card_text: str | None) -> str | None:
    if not card_text:
        return None
    patterns = [
        r"Опыт\s+\d+\s*[–-]\s*\d+\s+года?",
        r"Опыт\s+\d+\s*[–-]\s*\d+\s+лет",
        r"Опыт\s+более\s+\d+\s+лет",
        r"Без опыта",
    ]
    for pattern in patterns:
        match = re.search(pattern, card_text)
        if match:
            return match.group(0)
    return None


def extract_work_format_from_card(card_text: str | None) -> str | None:
    if not card_text:
        return None
    markers = ["Можно удалённо", "Можно удаленно", "Гибрид", "Полный день", "Сменный график"]
    found = [m for m in markers if m in card_text]
    return ", ".join(found) if found else None


def fetch_detail_payload(session, vacancy_url: str, referer: str, prefix: str) -> dict[str, Any]:
    """
    Загружает detail-страницу вакансии.
    ВАЖНО: использует переданный session (прогретый, с cookies).
    НЕ создаёт новый Session на каждую вакансию — это была утечка памяти
    которая приводила к SIGKILL воркера.
    soup.decompose() вызывается в finally для освобождения памяти.
    """
    detail_url = canonical_vacancy_url(vacancy_url)
    soup = None
    try:
        r = session.get(detail_url, headers={"Referer": referer}, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        return {"detail_status": "error", "detail_error": f"{type(e).__name__}: {e}"[:500]}

    payload: dict[str, Any] = {
        "detail_status": "ok" if r.status_code == 200 else f"http_{r.status_code}",
        "detail_url": r.url,
        "detail_requested_url": detail_url,
    }
    if r.status_code != 200:
        payload["detail_error"] = r.text[:500]
        return payload

    try:
        soup = BeautifulSoup(r.text, "html.parser")
        page_title = first_text(soup, "h1") or ""
        if "/account/captcha" in r.url or "не робот" in page_title.lower() or "captcha" in r.url:
            payload.update({"detail_status": "captcha", "detail_error": page_title[:500] or r.text[:500]})
            return payload

        description = (
            limited_text(soup, '[data-qa="vacancy-description"]')
            or limited_text(soup, '[data-qa="vacancy-description-text"]')
        )
        payload.update({
            "detail_title":    first_text(soup, '[data-qa="vacancy-title"]') or first_text(soup, "h1"),
            "detail_employer": first_text(soup, '[data-qa="vacancy-company-name"]'),
            "experience":      first_text(soup, '[data-qa="vacancy-experience"]'),
            "employment": (
                first_text(soup, '[data-qa="vacancy-view-employment-mode"]')
                or first_text(soup, '[data-qa="vacancy-view-employment"]')
            ),
            "schedule":        first_text(soup, '[data-qa="vacancy-view-schedule"]'),
            "salary_detail":   first_text(soup, '[data-qa="vacancy-salary"]'),
            "address_detail": (
                first_text(soup, '[data-qa="vacancy-view-raw-address"]')
                or first_text(soup, '[data-qa="vacancy-view-location"]')
            ),
            "description": description,
            "skills": unique_texts(soup, [
                '[data-qa="skills-element"]',
                '[data-qa="bloko-tag__text"]',
                '[data-qa="vacancy-skill"]',
            ]),
        })
        print(
            f"{prefix} detail: status={payload['detail_status']}, "
            f"skills={len(payload.get('skills', []))}, desc_len={len(description or '')}"
        )
    finally:
        # Освобождаем память BS4 дерева — важно при обработке сотен страниц
        if soup is not None:
            soup.decompose()

    return payload


def fetch_html_fallback(session, combo: dict[str, Any], snapshot_id: str,
                        prefix: str, referer: str) -> list[dict[str, Any]]:
    role = combo["search_text"]
    area = combo["search_area"]
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    details_fetched = 0
    search_params = dict(combo["api_params"])

    for page in range(HTML_FALLBACK_MAX_PAGES):
        params = dict(search_params)
        params["page"] = page
        print(f"{prefix} HTML fallback page={page} params={params}")

        r = session.get(HH_SEARCH_URL, params=params, headers={"Referer": referer}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(
                f"{prefix} HTML fallback returned {r.status_code} on page={page}. "
                f"URL={r.url} | body={r.text[:500]}"
            )

        soup = None
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            title_links = soup.select('a[data-qa="serp-item__title"]')
            print(f"{prefix} HTML fallback page={page} got {len(title_links)} cards")
            if not title_links:
                break

            page_new_rows = 0
            for title_link in title_links:
                vacancy_url = urljoin(HH_HOME_URL, title_link.get("href", ""))
                vacancy_id = vacancy_id_from_url(vacancy_url)
                if vacancy_id in seen_ids:
                    continue

                card = find_card_root(title_link)
                card_text = card.get_text(" ", strip=True)[:2000] if card else None
                raw = {
                    "source_mode":      "html_fallback",
                    "vacancy_id":       vacancy_id,
                    "title":            title_link.get_text(" ", strip=True),
                    "url":              vacancy_url,
                    "employer":         first_text(card, 'a[data-qa="vacancy-serp__vacancy-employer"]'),
                    "salary":           first_text(card, 'span[data-qa="vacancy-serp__vacancy-compensation"]'),
                    "address":          first_text(card, 'span[data-qa="vacancy-serp__vacancy-address"]'),
                    "search_text":      role,
                    "search_area":      area,
                    "page_num":         page,
                    "search_url":       r.url,
                    "card_text":        card_text,
                    "experience_card":  extract_experience_from_card(card_text),
                    "work_format_card": extract_work_format_from_card(card_text),
                }

                if FETCH_DETAILS and details_fetched < DETAIL_MAX_PER_COMBO:
                    # session передаётся в fetch_detail_payload — не создаём новый
                    raw.update(fetch_detail_payload(session, vacancy_url, r.url, prefix))
                    details_fetched += 1
                    time.sleep(random.uniform(DETAIL_PAUSE_MIN_SEC, DETAIL_PAUSE_MAX_SEC))
                elif FETCH_DETAILS:
                    raw["detail_status"] = "not_fetched_limit"
                else:
                    raw["detail_status"] = "disabled"

                rows.append({
                    "snapshot_id": snapshot_id,
                    "search_text": role,
                    "search_area": area,
                    "page_num":    page,
                    "vacancy_id":  vacancy_id,
                    "raw_json":    json.dumps(raw, ensure_ascii=False),
                })
                seen_ids.add(vacancy_id)
                page_new_rows += 1
        finally:
            if soup is not None:
                soup.decompose()

        if page_new_rows == 0:
            break
        time.sleep(random.uniform(0.8, 1.6))

    if not rows:
        raise RuntimeError(f"{prefix} HH API заблокирован и HTML fallback вернул 0 строк")

    print(f"{prefix} HTML fallback: итого {len(rows)} вакансий, details={details_fetched}")
    return rows


@dag(
    dag_id="hh_vacancies_snapshot",
    description="Snapshot вакансий с HH.ru → bronze (ClickHouse). Раз в 4 часа.",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "dpib", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["hh", "bronze", "snapshot", "ingest"],
)
def hh_vacancies_snapshot():

    @task
    def make_snapshot_id() -> str:
        sid = str(uuid.uuid4())
        print(f"[snapshot] snapshot_id={sid}")
        return sid

    @task
    def search_params() -> list[dict[str, Any]]:
        combos = build_search_params()
        print(f"[search_params] generated {len(combos)} combos")
        for c in combos:
            print(f"  - {c['search_text']} / {c['search_area']}: {c['api_params']}")
        return combos

    @task(retries=3, retry_delay=timedelta(seconds=45))
    def ingest_combo(combo: dict[str, Any], snapshot_id: str) -> dict[str, Any]:
        role = combo["search_text"]
        area = combo["search_area"]
        prefix = f"[{role}/{area}]"
        print(f"{prefix} START")

        session, referer = warmup_session(combo, prefix)
        all_rows = []
        total_pages = None

        try:
            base_params = dict(combo["api_params"])
            base_params["per_page"] = PER_PAGE

            api_headers = {
                "Referer": referer,
                "Origin": "https://hh.ru",
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
            }

            for page in range(MAX_PAGES):
                base_params["page"] = page
                print(f"{prefix} fetching page={page} params={base_params}")

                try:
                    r = session.get(HH_API_URL, params=base_params, headers=api_headers, timeout=REQUEST_TIMEOUT)
                except Exception as e:
                    raise RuntimeError(f"{prefix} HH request failed on page={page}: {e}") from e

                if r.status_code != 200:
                    if r.status_code in (401, 403):
                        print(f"{prefix} API вернул {r.status_code}, переключаемся на HTML fallback")
                        # session передаётся в fallback — не создаём новый
                        all_rows.extend(fetch_html_fallback(session, combo, snapshot_id, prefix, referer))
                        break
                    raise RuntimeError(
                        f"{prefix} HH вернул {r.status_code} на page={page}. "
                        f"URL={r.url} | body={r.text[:500]}"
                    )

                data = r.json()
                items = data.get("items", [])
                total_pages = data.get("pages", 0)
                found = data.get("found", 0)
                print(f"{prefix} page={page} got {len(items)} items (total found={found}, pages={total_pages})")

                if not items:
                    break

                for vac in items:
                    all_rows.append({
                        "snapshot_id": snapshot_id,
                        "search_text": role,
                        "search_area": area,
                        "page_num":    page,
                        "vacancy_id":  str(vac.get("id", "")),
                        "raw_json":    json.dumps(vac, ensure_ascii=False),
                    })

                if page >= total_pages - 1:
                    print(f"{prefix} последняя страница ({total_pages-1}), стоп")
                    break
                time.sleep(random.uniform(0.4, 0.9))

        finally:
            # Закрываем session в любом случае — освобождаем libcurl-handle
            try:
                session.close()
                print(f"{prefix} session closed")
            except Exception:
                pass

        print(f"{prefix} fetched total {len(all_rows)} vacancies")

        if not all_rows:
            return {"role": role, "area": area, "inserted": 0}

        client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT,
            username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE,
        )
        columns = ["snapshot_id", "search_text", "search_area", "page_num", "vacancy_id", "raw_json"]

        existing_result = client.query(
            """
            SELECT DISTINCT vacancy_id FROM bronze_hh_vacancies
            WHERE snapshot_id = {snapshot_id:UUID}
              AND search_text  = {search_text:String}
              AND search_area  = {search_area:String}
            """,
            parameters={"snapshot_id": snapshot_id, "search_text": role, "search_area": area},
        )
        existing_ids = {row[0] for row in existing_result.result_rows}
        if existing_ids:
            before = len(all_rows)
            all_rows = [row for row in all_rows if row["vacancy_id"] not in existing_ids]
            print(f"{prefix} retry-safe: пропущено {before - len(all_rows)} уже вставленных строк")

        if not all_rows:
            print(f"{prefix} ничего нового для вставки")
            return {"role": role, "area": area, "inserted": 0}

        inserted = 0
        for i in range(0, len(all_rows), INSERT_BATCH_SIZE):
            batch = all_rows[i:i + INSERT_BATCH_SIZE]
            client.insert(
                table="bronze_hh_vacancies",
                data=[[r[c] for c in columns] for r in batch],
                column_names=columns,
            )
            inserted += len(batch)
            print(f"{prefix} inserted batch {i // INSERT_BATCH_SIZE + 1}, total: {inserted}")

        print(f"{prefix} DONE — inserted {inserted} rows into bronze_hh_vacancies")
        return {"role": role, "area": area, "inserted": inserted}

    @task
    def report_total(results: list[dict[str, Any]]) -> dict[str, int]:
        total = sum(r.get("inserted", 0) for r in results)
        print(f"\n{'=' * 60}\nSNAPSHOT TOTAL: {total} vacancies inserted\n{'=' * 60}")
        for r in results:
            print(f"  {r['role']:25s} / {r['area']:8s}  →  {r['inserted']:5d} rows")
        return {"total_inserted": total}

    snap_id = make_snapshot_id()
    combos  = search_params()
    results = ingest_combo.partial(snapshot_id=snap_id).expand(combo=combos)
    total   = report_total(results)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="dbt_hh_transform",
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=30,
        conf={"snapshot_id": "{{ ti.xcom_pull(task_ids='make_snapshot_id') }}"},
    )
    total >> trigger_dbt


hh_vacancies_snapshot()
