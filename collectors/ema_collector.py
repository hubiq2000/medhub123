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


BASE_URL = "https://www.ema.europa.eu"
SEARCH_URL = f"{BASE_URL}/en/medicines/find-medicine/human-medicines/search"
EPAR_PREFIX = "/en/medicines/human/EPAR/"
USER_AGENT = "pharmavault-agent/1.0 (regulatory-research)"
MIN_REQUEST_DELAY = 1.5

LAST_REQUEST_TS = 0.0


def _log(msg):
    print(msg)
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{msg}\n")
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


def _throttled_get(url, params=None):
    global LAST_REQUEST_TS
    elapsed = time.time() - LAST_REQUEST_TS
    if elapsed < MIN_REQUEST_DELAY:
        time.sleep(MIN_REQUEST_DELAY - elapsed)
    try:
        resp = requests.get(
            url,
            params=params,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "en",
            },
            timeout=REQUEST_TIMEOUT,
        )
        LAST_REQUEST_TS = time.time()
        resp.raise_for_status()
        if not resp.encoding:
            resp.encoding = "utf-8"
        return resp
    except requests.RequestException as e:
        LAST_REQUEST_TS = time.time()
        _log(f"[EMA] Request failed for {url}: {e}")
        return None


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _output_path(drug_name):
    return os.path.join(SOURCES["ema"], f"{_slugify(drug_name)}.json")


def search_ema(drug_name):
    """Search EMA and return first matching product metadata or None."""
    q = _clean(drug_name)
    if not q:
        return None

    params = {"search_api_views_fulltext": q}
    resp = _throttled_get(SEARCH_URL, params=params)
    if resp is None:
        return None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        _log(f"[EMA] Failed to parse search results: {e}")
        return None

    # Find first anchor that links to an EPAR product
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if EPAR_PREFIX in href:
            url = urljoin(BASE_URL, href)
            name = _clean(a.get_text(" "))
            # Try to find active substance near this anchor
            substance = ""
            parent = a.find_parent()
            if parent:
                txt = parent.get_text(" ")
                # common label 'Active substance' or 'Active ingredient'
                m = re.search(r"Active (substance|ingredient)[:\s]*([^;\n\r]+)", txt, re.I)
                if m:
                    substance = _clean(m.group(2))
                else:
                    # try a short heuristic: look for parentheses content
                    m2 = re.search(r"\(([^)]+)\)", name)
                    if m2:
                        substance = _clean(m2.group(1))

            return {"name": name, "url": url, "substance": substance}

    # No EPAR results found
    return None


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{1,2}[./-]\d{1,2}[./-]\d{2,4})")


def _extract_first_paragraph_after_heading(soup, headings):
    for h in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
        txt = _clean(h.get_text(" ")).lower()
        if any(keyword in txt for keyword in headings):
            # look for next sibling paragraphs
            node = h.find_next_sibling()
            while node is not None:
                if node.name == "p":
                    return _clean(node.get_text(" "))
                # sometimes content is wrapped in div
                if node.name in ("div", "section"):
                    p = node.find("p")
                    if p:
                        return _clean(p.get_text(" "))
                node = node.find_next_sibling()
    return ""


def fetch_epar_page(epar_url):
    """Fetch EPAR page and extract requested metadata."""
    result = {
        "authorisation_status": "",
        "therapeutic_area": "",
        "indication_summary": "",
        "opinion_date": "",
    }

    if not epar_url:
        return result

    resp = _throttled_get(epar_url)
    if resp is None:
        return result

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        _log(f"[EMA] Failed to parse EPAR page {epar_url}: {e}")
        return result

    full_text = _clean(soup.get_text(" "))
    # Authorisation status
    if re.search(r"\bwithdrawn\b", full_text, re.I):
        result["authorisation_status"] = "withdrawn"
    elif re.search(r"\brefused\b", full_text, re.I):
        result["authorisation_status"] = "refused"
    elif re.search(r"\bauthoris(ed|ation)\b|\bmarketing authorisation\b", full_text, re.I):
        # prefer 'authorised'
        result["authorisation_status"] = "authorised"

    # Therapeutic area: look for label or product details list
    # Try table-like pairs: strong/em elements with label
    for label in ("therapeutic area", "therapeutic indication", "therapeutic area:"):
        el = soup.find(text=re.compile(label, re.I))
        if el:
            # get nearby text
            parent = el.parent
            txt = _clean(parent.get_text(" "))
            # strip the label
            txt = re.sub(re.compile(label, re.I), "", txt).strip(" :-\n")
            if txt:
                result["therapeutic_area"] = txt
                break

    # Indication summary: search for headings containing 'indication' or 'therapeutic indications'
    indication = _extract_first_paragraph_after_heading(soup, ["indication", "therapeutic indication"])
    if indication:
        result["indication_summary"] = indication[:600]

    # Opinion / authorisation date: find first date near 'opinion' or 'authorisation'
    # Search nearby phrases
    m = re.search(r"(opinion|authorisation|marketing authorisation|authorised)[^\n]{0,120}?(" + _DATE_RE.pattern + r")", full_text, re.I)
    if m:
        # last group contains date
        date_match = _DATE_RE.search(m.group(0))
        if date_match:
            result["opinion_date"] = date_match.group(1)
    else:
        # fallback: any date on the page
        dm = _DATE_RE.search(full_text)
        if dm:
            result["opinion_date"] = dm.group(1)

    return result


def save_ema(drug_name, data):
    path = _output_path(drug_name)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path
    except OSError as e:
        _log(f"[EMA] Failed to save data for {drug_name}: {e}")
        return None


def process_drug(drug_name):
    name = _clean(drug_name)
    if not name:
        _log("[EMA] Empty drug name provided")
        return None

    out_path = _output_path(name)
    # Do not re-fetch if file exists
    if os.path.exists(out_path):
        try:
            with open(out_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            status = data.get("authorisation_status", "cached")
            _log(f"[EMA] {name} → cached ({status})")
            return out_path
        except Exception:
            # fall through and re-fetch if cache invalid
            pass

    search = search_ema(name)
    if search is None:
        payload = {"drug_name": name, "authorisation_status": "not_found", "fetched_at": _now_iso()}
        save_ema(name, payload)
        _log(f"[EMA] {name} → not found in EMA database")
        return out_path

    epar_url = search.get("url")
    epar_meta = fetch_epar_page(epar_url)

    payload = {
        "drug_name": name,
        "ema_product_name": search.get("name", ""),
        "active_substance": search.get("substance", ""),
        "authorisation_status": epar_meta.get("authorisation_status", ""),
        "therapeutic_area": epar_meta.get("therapeutic_area", ""),
        "indication_summary": epar_meta.get("indication_summary", ""),
        "epar_url": epar_url,
        "opinion_date": epar_meta.get("opinion_date", ""),
        "fetched_at": _now_iso(),
    }

    save_ema(name, payload)
    status = payload.get("authorisation_status", "unknown")
    prod = payload.get("ema_product_name", "")
    ta = payload.get("therapeutic_area", "")
    _log(f"[EMA] {name} → {status.capitalize()} ({prod}, {ta})")
    return out_path


if __name__ == "__main__":
    args = os.sys.argv[1:]
    if not args:
        print("Usage: python -m collectors.ema_collector <drug_name>")
    else:
        process_drug(" ".join(args))

