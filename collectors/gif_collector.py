import json
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from config import LOG_FILE, REQUEST_TIMEOUT, SOURCES


BASE_URL = "https://www.gif.gov.pl"
ALERTS_URL = "https://www.gif.gov.pl/pl/bezpieczenstwo-lekow/"
RECALLS_URL = "https://www.gif.gov.pl/pl/wycofania-z-obrotu/"

USER_AGENT = "pharmavault-agent/1.0 (safety-monitoring)"
MIN_REQUEST_DELAY = 2.0

LAST_REQUEST_TS = 0.0


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


def _slugify(drug_name):
    return str(drug_name).strip().lower().replace(" ", "_")


def _clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def fetch_page(url):
    """GET URL respecting 2.0s rate limit. Returns BeautifulSoup or None on error."""
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
        if not response.encoding:
            response.encoding = "utf-8"
        return BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as error:
        LAST_REQUEST_TS = time.time()
        _log(f"[GIF] Request failed for {url}: {error}")
        return None
    except Exception as error:
        _log(f"[GIF] Failed to parse page {url}: {error}")
        return None


_DATE_RE = re.compile(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{2}-\d{2})\b")


def _extract_date(text, fallback_node=None):
    if text:
        match = _DATE_RE.search(text)
        if match:
            return match.group(1)
    if fallback_node is not None:
        for tag in fallback_node.find_all(["time", "span", "small", "em"]):
            candidate = tag.get("datetime") or tag.get_text(" ", strip=True)
            if candidate:
                m = _DATE_RE.search(candidate)
                if m:
                    return m.group(1)
    return ""


def _parse_listing(soup, alert_type):
    """Generic GIF listing parser. Looks for anchors that are likely article entries."""
    if soup is None:
        return []

    items = []
    seen_urls = set()

    # Strategy: find content area, then iterate over list items / article wrappers.
    candidate_containers = []
    for selector in ["main", "article", "div.content", "div#content", "div.entry-content", "body"]:
        for node in soup.select(selector):
            candidate_containers.append(node)
            break  # only first per selector

    if not candidate_containers:
        candidate_containers = [soup]

    container = candidate_containers[0]

    # Try to find list items / wrappers carrying anchors with non-trivial text.
    wrappers = container.find_all(["li", "article"]) or container.find_all("div")

    for wrapper in wrappers:
        anchor = wrapper.find("a", href=True)
        if anchor is None:
            continue
        title = _clean_text(anchor.get_text(" ", strip=True))
        if not title or len(title) < 5:
            continue
        href = anchor.get("href", "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        url = urljoin(BASE_URL, href)
        if url in seen_urls:
            continue

        wrapper_text = wrapper.get_text(" ", strip=True)
        date_str = _extract_date(wrapper_text, wrapper)

        seen_urls.add(url)
        items.append({
            "title": title,
            "date": date_str,
            "url": url,
            "type": alert_type,
        })

    # Fallback: scan all anchors directly if wrappers gave nothing.
    if not items:
        for anchor in container.find_all("a", href=True):
            title = _clean_text(anchor.get_text(" ", strip=True))
            if not title or len(title) < 8:
                continue
            href = anchor.get("href", "").strip()
            if not href or href.startswith("#") or href.lower().startswith("javascript:"):
                continue
            url = urljoin(BASE_URL, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)
            items.append({
                "title": title,
                "date": "",
                "url": url,
                "type": alert_type,
            })

    return items


def parse_alert_list(soup):
    try:
        return _parse_listing(soup, "alert")
    except Exception as error:
        _log(f"[GIF] Failed to parse alert list: {error}")
        return []


def parse_recall_list(soup):
    try:
        return _parse_listing(soup, "recall")
    except Exception as error:
        _log(f"[GIF] Failed to parse recall list: {error}")
        return []


def match_drug(alerts, drug_name):
    name = _clean_text(drug_name).lower()
    if not name or not alerts:
        return []

    needles = {name}
    if len(name) >= 5:
        needles.add(name[:5])
        needles.add(name[: max(5, len(name) - 2)])
    if len(name) > 2:
        needles.add(name[:-2])

    needles = {n for n in needles if n and len(n) >= 3}

    matched = []
    for item in alerts:
        title = (item.get("title") or "").lower()
        if not title:
            continue
        if any(n in title for n in needles):
            matched.append(item)
    return matched


def _merge_dedup(*lists):
    seen = set()
    merged = []
    for lst in lists:
        for item in lst or []:
            url = item.get("url", "")
            if not url or url in seen:
                continue
            seen.add(url)
            merged.append(item)
    return merged


def get_all_recent_alerts():
    """Returns merged list from both pages without drug filter."""
    alerts_soup = fetch_page(ALERTS_URL)
    recalls_soup = fetch_page(RECALLS_URL)
    alerts = parse_alert_list(alerts_soup)
    recalls = parse_recall_list(recalls_soup)
    return _merge_dedup(alerts, recalls)


def process_drug(drug_name):
    name = _clean_text(drug_name)
    if not name:
        _log("[GIF] Empty drug name provided")
        return False

    slug = _slugify(name)
    destination_dir = SOURCES["gif"]
    destination_path = os.path.join(destination_dir, f"{slug}.json")

    alerts_soup = fetch_page(ALERTS_URL)
    recalls_soup = fetch_page(RECALLS_URL)
    alert_items = parse_alert_list(alerts_soup)
    recall_items = parse_recall_list(recalls_soup)
    all_items = _merge_dedup(alert_items, recall_items)

    matched = match_drug(all_items, name)
    has_alert = len(matched) > 0

    payload = {
        "drug_name": name,
        "has_alert": has_alert,
        "alerts": matched,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }

    try:
        os.makedirs(destination_dir, exist_ok=True)
        with open(destination_path, "w", encoding="utf-8") as output_file:
            json.dump(payload, output_file, ensure_ascii=False, indent=2)
    except OSError as error:
        _log(f"[GIF] Failed to save GIF data for '{name}': {error}")
        return has_alert

    if has_alert:
        _log(f"[GIF] ⚠ {name} → {len(matched)} alerts found")
    else:
        _log(f"[GIF] {name} → OK (0 alerts)")

    return has_alert


if __name__ == "__main__":
    args = os.sys.argv[1:]
    if not args:
        print("Usage: python -m collectors.gif_collector <drug_name>")
    else:
        process_drug(" ".join(args))
