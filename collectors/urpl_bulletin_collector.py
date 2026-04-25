import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from config import LOG_FILE, REQUEST_TIMEOUT, SOURCES


BASE_URL = "https://archiwum.urpl.gov.pl"
INDEX_URL = f"{BASE_URL}/pl/aktualnosci/biuletyny-i-wykazy"
USER_AGENT = "pharmavault-agent/1.0 (regulatory-research)"
MIN_REQUEST_DELAY = 1.5
INDEX_TTL_SECONDS = 7 * 24 * 3600

LAST_REQUEST_TS = 0.0


def _log(message):
    print(message)
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{message}\n")
    except OSError:
        pass


def _slugify(name):
    slug = str(name).strip().lower()
    slug = re.sub(r"\s+", "_", slug)
    slug = re.sub(r"[^a-z0-9_\-]", "", slug)
    return slug or "drug"


def _clean(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _throttled_get(url):
    global LAST_REQUEST_TS
    elapsed = time.time() - LAST_REQUEST_TS
    if elapsed < MIN_REQUEST_DELAY:
        time.sleep(MIN_REQUEST_DELAY - elapsed)
    try:
        response = requests.get(
            url,
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
        _log(f"[URPL-BULL] Request failed for {url}: {error}")
        return None


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _index_cache_path():
    return os.path.join(SOURCES["urpl"], "bulletin_index.json")


def _output_path(drug_name):
    return os.path.join(SOURCES["urpl"], f"{_slugify(drug_name)}_bulletins.json")


def _extract_date(text):
    if not text:
        return ""
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b", text)
    if m:
        return m.group(1)
    return ""


def _parse_index_page(soup):
    """Parse a single archive listing page. Returns (items, next_url_or_None)."""
    items = []
    seen = set()

    # Find anchors that look like bulletin entries.
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        title = _clean(a.get_text(" "))
        if not title or len(title) < 3:
            continue
        # Heuristic: bulletin titles typically contain "biuletyn" or "wykaz" or year.
        low = title.lower()
        if not (
            "biuletyn" in low
            or "wykaz" in low
            or re.search(r"\b(19|20)\d{2}\b", low)
            or href.lower().endswith(".pdf")
        ):
            continue

        absolute = urljoin(BASE_URL, href)
        if absolute in seen:
            continue
        seen.add(absolute)

        # Try to find a sibling/parent date string.
        date_str = ""
        parent = a.find_parent(["li", "article", "div", "tr", "p"])
        if parent is not None:
            date_str = _extract_date(parent.get_text(" "))
        if not date_str:
            date_str = _extract_date(title)

        items.append({
            "title": title,
            "url": absolute,
            "date": date_str,
        })

    # Find pagination "next" link.
    next_url = None
    for a in soup.find_all("a", href=True):
        label = _clean(a.get_text(" ")).lower()
        if label in ("następna", "nastepna", "next", "»", "›"):
            next_url = urljoin(BASE_URL, a["href"].strip())
            break
    if not next_url:
        # rel=next
        link = soup.find("a", attrs={"rel": "next"})
        if link and link.get("href"):
            next_url = urljoin(BASE_URL, link["href"].strip())

    return items, next_url


def fetch_bulletin_index():
    """Fetch full bulletin index, following pagination."""
    results = []
    seen_urls = set()
    visited_pages = set()
    url = INDEX_URL
    max_pages = 25  # safety cap

    while url and len(visited_pages) < max_pages:
        if url in visited_pages:
            break
        visited_pages.add(url)

        response = _throttled_get(url)
        if response is None:
            break

        try:
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as error:
            _log(f"[URPL-BULL] Failed to parse index page {url}: {error}")
            break

        items, next_url = _parse_index_page(soup)
        for item in items:
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            results.append(item)

        url = next_url

    _log(f"[URPL-BULL] index fetched: {len(results)} bulletins from {len(visited_pages)} page(s)")
    return results


def _load_cached_index():
    path = _index_cache_path()
    if not os.path.exists(path):
        return None
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        return None
    if age > INDEX_TTL_SECONDS:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        _log(f"[URPL-BULL] Failed to read cached index: {error}")
        return None
    if isinstance(data, dict) and isinstance(data.get("bulletins"), list):
        return data["bulletins"]
    if isinstance(data, list):
        return data
    return None


def _save_index_cache(bulletins):
    path = _index_cache_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"fetched_at": _now_iso(), "bulletins": bulletins},
                f,
                ensure_ascii=False,
                indent=2,
            )
    except OSError as error:
        _log(f"[URPL-BULL] Failed to save index cache: {error}")


def fetch_bulletin_content(bulletin_url):
    """Fetch one bulletin (HTML or PDF link) and return enriched dict."""
    url = (bulletin_url or "").strip()
    result = {"title": "", "date": "", "url": url, "summary": ""}
    if not url:
        return result

    if url.lower().endswith(".pdf"):
        result["summary"] = "PDF — requires manual review"
        return result

    response = _throttled_get(url)
    if response is None:
        return result

    content_type = (response.headers.get("Content-Type") or "").lower()
    if "pdf" in content_type:
        result["summary"] = "PDF — requires manual review"
        return result

    try:
        soup = BeautifulSoup(response.text, "html.parser")
        for tag_name in ("script", "style", "noscript"):
            for tag in soup.find_all(tag_name):
                tag.decompose()
        title_tag = soup.find(["h1", "h2", "title"])
        if title_tag:
            result["title"] = _clean(title_tag.get_text(" "))
        body_text = _clean(soup.get_text(" "))
        result["summary"] = body_text[:2000]
        if not result["date"]:
            result["date"] = _extract_date(body_text)
    except Exception as error:
        _log(f"[URPL-BULL] Failed to parse bulletin {url}: {error}")

    return result


def search_bulletins_for_drug(drug_name, bulletins_index):
    query = _clean(drug_name).lower()
    if not query or not bulletins_index:
        return []

    matches = []
    for entry in bulletins_index:
        if not isinstance(entry, dict):
            continue
        haystack = " ".join(
            str(entry.get(field, "")) for field in ("title", "summary", "url")
        ).lower()
        if query in haystack:
            matches.append(entry)
    return matches


def process_drug(drug_name):
    name = _clean(drug_name)
    if not name:
        _log("[URPL-BULL] Empty drug name provided")
        return None

    bulletins_index = _load_cached_index()
    if bulletins_index is None:
        bulletins_index = fetch_bulletin_index()
        _save_index_cache(bulletins_index)

    matches = search_bulletins_for_drug(name, bulletins_index)

    enriched = []
    for entry in matches:
        item = {
            "title": _clean(entry.get("title", "")),
            "date": _clean(entry.get("date", "")),
            "url": _clean(entry.get("url", "")),
            "summary": _clean(entry.get("summary", "")),
        }
        if not item["summary"]:
            details = fetch_bulletin_content(item["url"])
            if details.get("title") and not item["title"]:
                item["title"] = details["title"]
            if details.get("date") and not item["date"]:
                item["date"] = details["date"]
            item["summary"] = details.get("summary", "")
        enriched.append(item)

    payload = {
        "drug_name": name,
        "bulletins": enriched,
        "bulletin_count": len(enriched),
        "checked_at": _now_iso(),
    }

    out_path = _output_path(name)
    try:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as error:
        _log(f"[URPL-BULL] Failed to save {out_path}: {error}")
        return None

    if enriched:
        first_title = enriched[0]["title"] or "untitled"
        _log(f"[URPL-BULL] {name} → {len(enriched)} bulletin found ({first_title})")
    else:
        _log(f"[URPL-BULL] {name} → 0 bulletins found")

    return out_path


if __name__ == "__main__":
    args = os.sys.argv[1:]
    if not args:
        print("Usage: python -m collectors.urpl_bulletin_collector <drug_name>")
    else:
        process_drug(" ".join(args))
