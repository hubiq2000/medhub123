import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from config import LOG_FILE, QUEUE_FILE, RATE_LIMIT_DELAY, REQUEST_TIMEOUT, SOURCES


BASE_URL = "https://rejestrymedyczne.ezdrowie.gov.pl"
SEARCH_API_URL = f"{BASE_URL}/api/rpl/medicinal-products/public/"
CHARACTERISTIC_URL_TEMPLATE = f"{BASE_URL}/api/rpl/medicinal-products/{{product_id}}/characteristic"

USER_AGENT = "pharmavault-agent/1.0 (educational)"
MIN_REQUEST_DELAY = 1.0

# EMA QRD section number → Polish heading used in output JSON
SECTION_MAP = [
    ("4.1", "Wskazania do stosowania"),
    ("4.2", "Dawkowanie i sposób podawania"),
    ("4.3", "Przeciwwskazania"),
    ("4.4", "Specjalne ostrzeżenia"),
    ("4.5", "Interakcje z innymi lekami"),
    ("4.8", "Działania niepożądane"),
    ("5.1", "Właściwości farmakodynamiczne"),
    ("5.2", "Właściwości farmakokinetyczne"),
]
SECTION_HEADINGS = [heading for _, heading in SECTION_MAP]
ALL_SECTION_NUMBERS = [
    "4.1", "4.2", "4.3", "4.4", "4.5", "4.6", "4.7", "4.8", "4.9",
    "5.1", "5.2", "5.3",
    "6.1", "6.2", "6.3", "6.4", "6.5", "6.6",
]

LAST_REQUEST_TS = 0.0


def _effective_delay():
    try:
        configured = float(RATE_LIMIT_DELAY)
    except (TypeError, ValueError):
        configured = 0.0
    return max(configured, MIN_REQUEST_DELAY)


def _log(message):
    print(message)
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as log_file:
            log_file.write(f"{message}\n")
    except OSError:
        pass


def _throttled_get(url, params=None):
    global LAST_REQUEST_TS

    elapsed = time.time() - LAST_REQUEST_TS
    delay = _effective_delay()
    if elapsed < delay:
        time.sleep(delay - elapsed)

    try:
        response = requests.get(
            url,
            params=params,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "pl",
            },
            timeout=REQUEST_TIMEOUT,
        )
        LAST_REQUEST_TS = time.time()
        response.raise_for_status()
        return response
    except requests.RequestException as error:
        LAST_REQUEST_TS = time.time()
        _log(f"[CHPL] Request failed for {url}: {error}")
        return None


def _clean_text(text):
    if not text:
        return ""
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    return cleaned


def _slugify(drug_name):
    return str(drug_name).strip().lower().replace(" ", "_")


def _output_path_for_drug(drug_name):
    return os.path.join(SOURCES["chpl"], f"{_slugify(drug_name)}.json")


def search_ezdrowie(drug_name):
    """Search official Polish drug registry by name."""
    query = _clean_text(drug_name)
    if not query:
        return []

    response = _throttled_get(
        SEARCH_API_URL,
        params={"name": query, "page": 0, "size": 5},
    )
    if response is None:
        _log(f"[CHPL] search '{query}' → 0 results")
        return []

    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError) as error:
        _log(f"[CHPL] Failed to parse search JSON for '{query}': {error}")
        return []

    items = []
    if isinstance(payload, dict):
        candidate = payload.get("content")
        if isinstance(candidate, list):
            items = candidate
        else:
            for key in ("results", "items", "data", "hits"):
                value = payload.get(key)
                if isinstance(value, list):
                    items = value
                    break
    elif isinstance(payload, list):
        items = payload

    results = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item_id = item.get("id") or item.get("productId") or item.get("uuid")
        name = (
            item.get("medicinalProductName")
            or item.get("productName")
            or item.get("name")
            or item.get("nazwaProduktu")
            or item.get("nazwa")
        )
        if item_id is None:
            continue
        results.append({
            "id": str(item_id).strip(),
            "name": _clean_text(name),
        })

    _log(f"[CHPL] search '{query}' → {len(results)} results")
    return results


def fetch_chpl_text(product_id):
    """Fetch raw CHPL document (HTML or text) for a given product id."""
    pid = _clean_text(product_id)
    if not pid:
        return ""

    url = CHARACTERISTIC_URL_TEMPLATE.format(product_id=pid)
    response = _throttled_get(url)
    if response is None:
        return ""

    try:
        if not response.encoding:
            response.encoding = "utf-8"
        return response.text or ""
    except Exception as error:
        _log(f"[CHPL] Failed to decode characteristic for {pid}: {error}")
        return ""


def parse_chpl_sections(raw_text):
    """Parse EMA QRD numbered sections (4.1, 4.2, ... 5.2) from CHPL raw text/HTML."""
    result = {heading: "" for heading in SECTION_HEADINGS}
    if not raw_text:
        return result

    # Strip HTML to plain text while preserving line breaks between blocks.
    try:
        soup = BeautifulSoup(raw_text, "html.parser")
        for tag_name in ("script", "style", "noscript"):
            for tag in soup.find_all(tag_name):
                tag.decompose()
        plain = soup.get_text("\n")
    except Exception:
        plain = re.sub(r"<[^>]+>", "\n", raw_text)

    # Normalize whitespace per line, drop empty lines.
    lines = [re.sub(r"\s+", " ", line).strip() for line in plain.splitlines()]
    plain = "\n".join(line for line in lines if line)

    # Find every section-number header occurrence.
    pattern = re.compile(
        r"(?m)^\s*(?P<num>\d+\.\d+)(?:\.|\s|\)|:)\s*(?P<rest>.*)$"
    )

    matches = []
    for m in pattern.finditer(plain):
        num = m.group("num")
        if num in ALL_SECTION_NUMBERS:
            matches.append((num, m.start(), m.end()))

    if not matches:
        return result

    wanted = {num: heading for num, heading in SECTION_MAP}

    for index, (num, _start, end) in enumerate(matches):
        if num not in wanted:
            continue
        next_start = matches[index + 1][1] if index + 1 < len(matches) else len(plain)
        body = plain[end:next_start]
        result[wanted[num]] = _clean_text(body)

    return result


def save_chpl(drug_name, data):
    slug = _slugify(drug_name)
    destination_dir = SOURCES["chpl"]
    destination_path = os.path.join(destination_dir, f"{slug}.json")

    if os.path.exists(destination_path):
        _log(f"[CHPL] File already exists, skipping: {destination_path}")
        return destination_path

    try:
        os.makedirs(destination_dir, exist_ok=True)
    except OSError as error:
        _log(f"[CHPL] Failed to create destination directory {destination_dir}: {error}")
        return None

    source = data or {}
    payload = {heading: _clean_text(source.get(heading, "")) for heading in SECTION_HEADINGS}

    try:
        with open(destination_path, "w", encoding="utf-8") as output_file:
            json.dump(payload, output_file, ensure_ascii=False, indent=2)
    except OSError as error:
        _log(f"[CHPL] Failed to save CHPL for '{drug_name}': {error}")
        return None

    return destination_path


def process_drug(drug_name):
    if not _clean_text(drug_name):
        _log("[CHPL] Empty drug name provided")
        return None

    existing_path = _output_path_for_drug(drug_name)
    if os.path.exists(existing_path):
        _log(f"[CHPL] Already downloaded, skipping: {existing_path}")
        return existing_path

    results = search_ezdrowie(drug_name)
    if not results:
        _log(f"[CHPL] {drug_name} → not found in URPL")
        save_chpl(drug_name, {})
        return None

    first = results[0]
    product_id = first.get("id", "")
    raw_text = fetch_chpl_text(product_id)
    sections = parse_chpl_sections(raw_text)

    saved_path = save_chpl(drug_name, sections)
    filled = sum(1 for heading in SECTION_HEADINGS if sections.get(heading))
    if saved_path:
        _log(f"[CHPL] {drug_name} → saved {filled} sections (product_id: {product_id})")

    return saved_path


def process_from_queue(limit=10):
    try:
        limit_value = int(limit)
    except (TypeError, ValueError):
        _log(f"[CHPL] Invalid batch limit '{limit}', using 10")
        limit_value = 10

    limit_value = max(0, limit_value)

    if limit_value == 0:
        _log("[CHPL] Batch limit is 0, nothing to process")
        return 0

    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as queue_file:
            queue_items = [_clean_text(line) for line in queue_file.readlines()]
    except OSError as error:
        _log(f"[CHPL] Failed to read queue file {QUEUE_FILE}: {error}")
        return 0

    queue_items = [item for item in queue_items if item]

    processed = 0
    saved = 0

    for drug_name in queue_items:
        if processed >= limit_value:
            break

        destination_path = _output_path_for_drug(drug_name)
        if os.path.exists(destination_path):
            continue

        result = process_drug(drug_name)
        processed += 1
        if result:
            saved += 1

        _log(f"[CHPL] Batch progress: {processed}/{limit_value} processed, {saved} saved")

    _log(f"[CHPL] Batch complete: {processed} processed, {saved} saved")
    return saved


if __name__ == "__main__":
    args = os.sys.argv[1:]

    if not args:
        process_from_queue(limit=10)
    elif args[0] == "--batch":
        batch_limit = 10
        if len(args) > 1:
            batch_limit = args[1]
        process_from_queue(limit=batch_limit)
    else:
        drug_query = " ".join(args)
        process_drug(drug_query)
