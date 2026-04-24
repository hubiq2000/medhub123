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


SEARCH_API_URL = "https://www.chpl.com.pl/api/search"
SEARCH_WEB_URL = "https://www.chpl.com.pl/search"
BASE_URL = "https://www.chpl.com.pl"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
MIN_REQUEST_DELAY = 2.0

SECTION_HEADINGS = [
    "Skład jakościowy i ilościowy",
    "Postać farmaceutyczna",
    "Wskazania do stosowania",
    "Dawkowanie i sposób podawania",
    "Przeciwwskazania",
    "Specjalne ostrzeżenia",
    "Interakcje z innymi lekami",
    "Działania niepożądane",
    "Właściwości farmakodynamiczne",
    "Właściwości farmakokinetyczne",
]

LAST_REQUEST_TS = 0.0


def _effective_delay():
    return max(float(RATE_LIMIT_DELAY), MIN_REQUEST_DELAY)


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
            headers={"User-Agent": USER_AGENT},
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


def _normalize_heading(text):
    normalized = _clean_text(text).lower()
    normalized = re.sub(r"^\d+(?:\.\d+)*\.?\s*", "", normalized)
    normalized = re.sub(r"\s*:\s*$", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _heading_match(line_text):
    normalized_line = _normalize_heading(line_text)
    if not normalized_line or len(normalized_line) > 200:
        return None

    for heading in SECTION_HEADINGS:
        normalized_heading = _normalize_heading(heading)
        if normalized_line == normalized_heading:
            return heading
        if normalized_line.startswith(normalized_heading + " "):
            return heading
        if normalized_heading in normalized_line and len(normalized_line) <= len(normalized_heading) + 35:
            return heading

    return None


def _slugify(drug_name):
    return str(drug_name).strip().lower().replace(" ", "_")


def _output_path_for_drug(drug_name):
    return os.path.join(SOURCES["chpl"], f"{_slugify(drug_name)}.json")


def _normalize_url(url):
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return f"{BASE_URL}{url}"
    return f"{BASE_URL}/{url}"


def _extract_id(value):
    if value is None:
        return ""
    as_text = str(value).strip()
    if as_text:
        return as_text
    return ""


def _normalize_search_item(item):
    if not isinstance(item, dict):
        return None

    name = item.get("name") or item.get("nazwa") or item.get("title") or item.get("productName")
    url = item.get("url") or item.get("link") or item.get("href")
    item_id = item.get("id") or item.get("productId") or item.get("uuid")

    if not url and item.get("slug"):
        url = item.get("slug")

    normalized_name = _clean_text(name)
    normalized_url = _normalize_url(_clean_text(url)) if url else ""
    normalized_id = _extract_id(item_id)

    if not normalized_url:
        return None

    if not normalized_name:
        normalized_name = normalized_url.rstrip("/").split("/")[-1].replace("-", " ").strip()

    if not normalized_id:
        id_match = re.search(r"(\d+)", normalized_url)
        if id_match:
            normalized_id = id_match.group(1)

    return {
        "name": normalized_name,
        "url": normalized_url,
        "id": normalized_id,
    }


def _extract_search_items_from_payload(payload):
    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        return []

    for key in ("results", "items", "content", "data", "hits"):
        value = payload.get(key)
        if isinstance(value, list):
            return value

    return []


def search_drug(drug_name):
    query = _clean_text(drug_name)
    if not query:
        return []

    api_response = _throttled_get(SEARCH_API_URL, params={"query": query, "limit": 5})
    if api_response is not None:
        try:
            payload = api_response.json()
            parsed = []
            seen_urls = set()
            for item in _extract_search_items_from_payload(payload):
                normalized = _normalize_search_item(item)
                if not normalized:
                    continue
                if normalized["url"] in seen_urls:
                    continue
                seen_urls.add(normalized["url"])
                parsed.append(normalized)
                if len(parsed) >= 5:
                    break

            if parsed:
                return parsed
        except json.JSONDecodeError as error:
            _log(f"[CHPL] Failed to parse API search response for '{query}': {error}")

    web_response = _throttled_get(SEARCH_WEB_URL, params={"q": query})
    if web_response is None:
        return []

    try:
        soup = BeautifulSoup(web_response.text, "html.parser")
    except Exception as error:
        _log(f"[CHPL] Failed to parse HTML search response for '{query}': {error}")
        return []

    results = []
    seen_urls = set()

    for link in soup.find_all("a", href=True):
        href = _clean_text(link.get("href"))
        if not href:
            continue
        if "search" in href and "q=" in href:
            continue

        normalized_url = _normalize_url(href)
        if BASE_URL not in normalized_url:
            continue
        if normalized_url in seen_urls:
            continue

        link_text = _clean_text(link.get_text(" ", strip=True))
        if not link_text:
            continue

        if query.lower() not in link_text.lower() and query.lower() not in normalized_url.lower():
            continue

        seen_urls.add(normalized_url)

        id_match = re.search(r"(\d+)", normalized_url)
        item_id = id_match.group(1) if id_match else ""

        results.append(
            {
                "name": link_text,
                "url": normalized_url,
                "id": item_id,
            }
        )

        if len(results) >= 5:
            break

    return results


def scrape_chpl(drug_url):
    data = {heading: "" for heading in SECTION_HEADINGS}

    response = _throttled_get(drug_url)
    if response is None:
        return data

    try:
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as error:
        _log(f"[CHPL] Failed to parse CHPL page {drug_url}: {error}")
        return data

    for tag_name in ("script", "style", "noscript"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    lines = []
    for raw_line in soup.get_text("\n").splitlines():
        cleaned_line = _clean_text(raw_line)
        if cleaned_line:
            lines.append(cleaned_line)

    heading_positions = []
    seen = set()
    for index, line in enumerate(lines):
        heading = _heading_match(line)
        if heading and (heading, index) not in seen:
            heading_positions.append((heading, index))
            seen.add((heading, index))

    if not heading_positions:
        return data

    heading_positions.sort(key=lambda pair: pair[1])

    for position, (heading, start_index) in enumerate(heading_positions):
        if heading not in data:
            continue

        end_index = len(lines)
        if position + 1 < len(heading_positions):
            end_index = heading_positions[position + 1][1]

        content_lines = lines[start_index + 1 : end_index]
        data[heading] = _clean_text(" ".join(content_lines))

    return data


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

    payload = {heading: _clean_text(data.get(heading, "")) for heading in SECTION_HEADINGS}

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

    results = search_drug(drug_name)
    if not results:
        _log(f"[CHPL] No search results for '{drug_name}'")
        return None

    first_result = results[0]
    drug_url = first_result.get("url")
    if not drug_url:
        _log(f"[CHPL] First search result has no URL for '{drug_name}'")
        return None

    scraped_data = scrape_chpl(drug_url)
    saved_path = save_chpl(drug_name, scraped_data)
    if saved_path:
        _log(f"[CHPL] Saved '{drug_name}' to {saved_path}")

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