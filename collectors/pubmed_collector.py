import json
import os
import re
import time
from datetime import datetime, timezone
from typing import List, Tuple

import requests
import xml.etree.ElementTree as ET

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from config import LOG_FILE, REQUEST_TIMEOUT, SOURCES


# NCBI E-utilities base
BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
ESEARCH = f"{BASE}/esearch.fcgi"
EFETCH = f"{BASE}/efetch.fcgi"

USER_AGENT = "pharmavault-agent/1.0 (research)"
# NCBI free tier: max 3 requests/sec -> min delay ~0.34s
MIN_REQUEST_DELAY = 0.34
LAST_NCBI_TS = 0.0


def _log(msg: str):
    print(msg)
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{msg}\n")
    except OSError:
        pass


def _slugify(name: str) -> str:
    slug = str(name).strip().lower()
    slug = re.sub(r"\s+", "_", slug)
    slug = re.sub(r"[^a-z0-9_\-]", "", slug)
    return slug or "drug"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _throttled_post(url: str, data: dict) -> requests.Response | None:
    global LAST_NCBI_TS
    elapsed = time.time() - LAST_NCBI_TS
    if elapsed < MIN_REQUEST_DELAY:
        time.sleep(MIN_REQUEST_DELAY - elapsed)
    try:
        resp = requests.post(
            url,
            data=data,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        LAST_NCBI_TS = time.time()
        resp.raise_for_status()
        if not resp.encoding:
            resp.encoding = "utf-8"
        return resp
    except requests.RequestException as e:
        LAST_NCBI_TS = time.time()
        _log(f"[PUBMED] Request failed for {url}: {e}")
        return None


def _output_path(drug_name: str) -> str:
    return os.path.join(SOURCES["pubmed"], f"{_slugify(drug_name)}.json")


def _build_query(drug_name: str) -> str:
    name = str(drug_name).strip()
    # Quality filter: Clinical Trial OR Review OR Systematic Review
    quality = "(Clinical Trial[pt] OR Review[pt] OR Systematic Review[pt])"
    query = f"{name}[Title/Abstract] AND {quality} AND hasabstract[text] AND humans[mh]"
    return query


def search_pubmed(drug_name: str, max_results: int = 5) -> Tuple[List[str], int]:
    """Search PubMed via esearch. Returns (pmid_list, total_found).
    Respects NCBI rate limits and includes tool/email params.
    """
    query = _build_query(drug_name)
    data = {
        "db": "pubmed",
        "term": query,
        "retmax": str(max_results),
        "sort": "relevance",
        "usehistory": "y",
        "retmode": "json",
        "tool": "pharmavault-agent",
        "email": "research@pharmavault.local",
    }
    resp = _throttled_post(ESEARCH, data)
    if resp is None:
        return [], 0
    try:
        payload = resp.json()
        es = payload.get("esearchresult", {})
        idlist = es.get("idlist", [])
        count = int(es.get("count", 0))
        return [str(x) for x in idlist], count
    except Exception as e:
        _log(f"[PUBMED] Failed to parse esearch response: {e}")
        return [], 0


def fetch_abstracts(pmids: List[str]) -> List[dict]:
    """Fetch abstracts via efetch and parse XML. Returns list of dicts.
    Never raises; returns [] on errors.
    """
    if not pmids:
        return []

    ids = ",".join(pmids)
    data = {
        "db": "pubmed",
        "id": ids,
        "rettype": "abstract",
        "retmode": "xml",
        "tool": "pharmavault-agent",
        "email": "research@pharmavault.local",
    }
    resp = _throttled_post(EFETCH, data)
    if resp is None:
        return []

    try:
        root = ET.fromstring(resp.text)
    except Exception as e:
        _log(f"[PUBMED] Failed to parse efetch XML: {e}")
        return []

    articles = []
    for article in root.findall("./PubmedArticle"):
        try:
            pmid_el = article.find("./MedlineCitation/PMID")
            pmid = pmid_el.text.strip() if pmid_el is not None and pmid_el.text else ""

            title_el = article.find("./MedlineCitation/Article/ArticleTitle")
            title = ET.tostring(title_el, encoding="unicode", method="text").strip() if title_el is not None else ""

            # AbstractText may contain multiple nodes
            abstract_texts = []
            for at in article.findall("./MedlineCitation/Article/Abstract/AbstractText"):
                text = ET.tostring(at, encoding="unicode", method="text").strip()
                if text:
                    abstract_texts.append(text)
            abstract = " ".join(abstract_texts).strip()
            if len(abstract) > 1000:
                abstract = abstract[:997].rstrip() + "..."

            journal_el = article.find("./MedlineCitation/Article/Journal/Title")
            journal = journal_el.text.strip() if journal_el is not None and journal_el.text else ""

            # pub date: try Journal/JournalIssue/PubDate/Year, else MedlineDate (first 4 digits)
            pub_date = ""
            py = article.find("./MedlineCitation/Article/Journal/JournalIssue/PubDate/Year")
            if py is not None and py.text:
                pub_date = py.text.strip()
            else:
                md = article.find("./MedlineCitation/Article/Journal/JournalIssue/PubDate/MedlineDate")
                if md is not None and md.text:
                    m = re.search(r"(\d{4})", md.text)
                    if m:
                        pub_date = m.group(1)

            # Publication type: first PublicationType
            pub_type = ""
            pt = article.find("./MedlineCitation/Article/PublicationTypeList/PublicationType")
            if pt is not None and pt.text:
                pub_type = pt.text.strip()

            articles.append({
                "pmid": pmid,
                "title": title,
                "abstract": abstract,
                "journal": journal,
                "pub_date": pub_date,
                "pub_type": pub_type,
            })
        except Exception:
            # skip problematic article but continue
            continue

    return articles


def save_pubmed(drug_name: str, data: dict) -> str | None:
    path = _output_path(drug_name)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return path
    except OSError as e:
        _log(f"[PUBMED] Failed to save data for {drug_name}: {e}")
        return None


def process_drug(drug_name: str, max_results: int = 5) -> str | None:
    name = str(drug_name).strip()
    if not name:
        _log("[PUBMED] Empty drug name provided")
        return None

    query = _build_query(name)
    pmids, total = search_pubmed(name, max_results=max_results)

    if not pmids:
        payload = {
            "drug_name": name,
            "query": query,
            "total_found": total,
            "abstracts": [],
            "fetched_at": _now_iso(),
        }
        save_pubmed(name, payload)
        _log(f"[PUBMED] {name} → 0 results (no clinical literature)")
        return _output_path(name)

    abstracts = fetch_abstracts(pmids)

    payload = {
        "drug_name": name,
        "query": query,
        "total_found": total,
        "abstracts": abstracts,
        "fetched_at": _now_iso(),
    }

    save_pubmed(name, payload)
    _log(f"[PUBMED] {name} → {len(abstracts)} abstracts ({total} total found)")
    return _output_path(name)


if __name__ == "__main__":
    args = os.sys.argv[1:]
    if not args:
        print("Usage: python -m collectors.pubmed_collector <drug_name>")
    else:
        process_drug(" ".join(args))

