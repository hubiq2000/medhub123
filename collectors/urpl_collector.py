import json
import os
import time

import requests

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from config import QUEUE_FILE, RATE_LIMIT_DELAY, REQUEST_TIMEOUT, SOURCES


LIST_URL = "https://rejestry.ezdrowie.gov.pl/api/v1/rpl/medicinal-products/public/"
DETAIL_URL_TEMPLATE = "https://rejestry.ezdrowie.gov.pl/api/v1/rpl/medicinal-products/{id}"
MIN_REQUEST_DELAY = 0.5


def _effective_delay():
    return max(RATE_LIMIT_DELAY, MIN_REQUEST_DELAY)


def _extract_products(payload):
    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        return []

    for key in ("content", "items", "results", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return value

    return []


def _ensure_urpl_dir():
    try:
        os.makedirs(SOURCES["urpl"], exist_ok=True)
        return True
    except OSError as error:
        print(f"[URPL] Failed to create source directory {SOURCES['urpl']}: {error}")
        return False


def fetch_drug_list(page, size=100):
    params = {
        "page": page,
        "size": size,
        "sortField": "nazwaProduktu",
    }

    try:
        response = requests.get(LIST_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, json.JSONDecodeError) as error:
        print(f"[URPL] Failed to fetch page {page}: {error}")
        return None


def fetch_all_drugs():
    if not _ensure_urpl_dir():
        return

    page = 1
    collected = 0
    retries_for_page = 0
    consecutive_failed_pages = 0
    max_retries_for_page = 3
    max_consecutive_failed_pages = 20

    while True:
        page_file = os.path.join(SOURCES["urpl"], f"page_{page:04d}.json")

        if os.path.exists(page_file):
            try:
                with open(page_file, "r", encoding="utf-8") as source_file:
                    page_data = json.load(source_file)
                page_items = _extract_products(page_data)
            except (OSError, json.JSONDecodeError) as error:
                print(f"[URPL] Failed to read existing page {page}: {error}")
                page_items = []

            collected += len(page_items)
            print(f"Page {page}/? — {collected} drugs collected")

            if not page_items:
                break

            page += 1
            continue

        page_data = fetch_drug_list(page)
        if page_data is None:
            retries_for_page += 1
            if retries_for_page >= max_retries_for_page:
                print(f"[URPL] Skipping page {page} after {max_retries_for_page} failed attempts")
                page += 1
                retries_for_page = 0
                consecutive_failed_pages += 1
                if consecutive_failed_pages >= max_consecutive_failed_pages:
                    print("[URPL] Stopping download after too many consecutive failed pages")
                    break
            time.sleep(_effective_delay())
            continue

        retries_for_page = 0
        consecutive_failed_pages = 0

        page_items = _extract_products(page_data)

        try:
            with open(page_file, "w", encoding="utf-8") as destination_file:
                json.dump(page_data, destination_file, ensure_ascii=False, indent=2)
        except OSError as error:
            print(f"[URPL] Failed to save page {page}: {error}")

        collected += len(page_items)
        print(f"Page {page}/? — {collected} drugs collected")

        if not page_items:
            break

        page += 1
        time.sleep(_effective_delay())


def build_drug_queue():
    if not _ensure_urpl_dir():
        return

    page_files = [
        file_name
        for file_name in os.listdir(SOURCES["urpl"])
        if file_name.startswith("page_") and file_name.endswith(".json")
    ]
    page_files.sort()

    substances = set()

    for file_name in page_files:
        file_path = os.path.join(SOURCES["urpl"], file_name)
        try:
            with open(file_path, "r", encoding="utf-8") as source_file:
                page_data = json.load(source_file)
        except (OSError, json.JSONDecodeError) as error:
            print(f"[URPL] Failed to read {file_name}: {error}")
            continue

        for product in _extract_products(page_data):
            if not isinstance(product, dict):
                continue

            active_substance = product.get("substancjaCzynna")
            if not active_substance:
                continue

            normalized = str(active_substance).strip()
            if normalized:
                substances.add(normalized)

    sorted_substances = sorted(substances, key=lambda value: value.lower())

    if os.path.exists(QUEUE_FILE):
        print(f"[URPL] Queue file already exists, skipping: {QUEUE_FILE}")
        return

    queue_dir = os.path.dirname(QUEUE_FILE)
    if queue_dir:
        try:
            os.makedirs(queue_dir, exist_ok=True)
        except OSError as error:
            print(f"[URPL] Failed to create queue directory {queue_dir}: {error}")
            return

    try:
        with open(QUEUE_FILE, "w", encoding="utf-8") as queue_file:
            queue_file.write("\n".join(sorted_substances))
            if sorted_substances:
                queue_file.write("\n")
    except OSError as error:
        print(f"[URPL] Failed to write queue file: {error}")
        return

    print(f"Queue built: {len(sorted_substances)} unique active substances")


def get_drug_details(product_id):
    details_url = DETAIL_URL_TEMPLATE.format(id=product_id)

    try:
        response = requests.get(details_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, json.JSONDecodeError) as error:
        print(f"[URPL] Failed to fetch product details {product_id}: {error}")
        return None


if __name__ == "__main__":
    fetch_all_drugs()
    build_drug_queue()