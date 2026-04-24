import datetime
import json
import os
import re
import time

import requests

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from config import LOG_FILE, MODEL, OLLAMA_URL, SOURCES, VAULT_PATH


MAX_PROMPT_LENGTH = 2000
OLLAMA_TIMEOUT = 120
OLLAMA_NUM_PREDICT = 400

SECTION_TEMPLATE = [
    "## Mechanizm działania",
    "## Wskazania",
    "## Dawkowanie dorosli",
    "## Dawkowanie dzieci",
    "## Przeciwwskazania",
    "## Działania niepożądane",
    "## Interakcje",
    "## Właściwości farmakokinetyczne",
]


def _slugify(drug_name):
    slug = str(drug_name or "").strip().lower()
    slug = re.sub(r"\s+", "_", slug)
    slug = re.sub(r"[^a-z0-9_\-ąćęłńóśźż]", "", slug)
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown_drug"


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


def _read_json_if_exists(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as source_file:
            data = json.load(source_file)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _to_text(data):
    if not data:
        return ""
    if isinstance(data, str):
        return data.strip()
    if isinstance(data, (int, float, bool)):
        return str(data)
    if isinstance(data, list):
        parts = [_to_text(item) for item in data]
        return "\n".join(part for part in parts if part)
    if isinstance(data, dict):
        parts = []
        for key, value in data.items():
            value_text = _to_text(value)
            if value_text:
                parts.append(f"{key}: {value_text}")
        return "\n".join(parts)
    return str(data)


def _available_sources(sources):
    available = []
    if sources.get("chpl"):
        available.append("CHPL")
    if sources.get("mp"):
        available.append("mp.pl")
    if sources.get("pubmed"):
        available.append("PubMed")
    return available


def load_sources(drug_name):
    slug = _slugify(drug_name)
    chpl_path = os.path.join(SOURCES["chpl"], f"{slug}.json")
    pubmed_path = os.path.join(SOURCES["pubmed"], f"{slug}.json")
    mp_path = os.path.join(SOURCES["mp"], f"{slug}.json")

    return {
        "chpl": _read_json_if_exists(chpl_path),
        "pubmed": _read_json_if_exists(pubmed_path),
        "mp": _read_json_if_exists(mp_path),
    }


def _truncate_text(text, max_len):
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[: max_len - 3].rstrip() + "..."


def build_prompt(drug_name, sources):
    header = (
        f"Przygotuj notatkę medyczną o leku: {drug_name}. "
        "Nie wolno wymyślać danych. "
        "Jeśli brak informacji dla sekcji, wpisz dokładnie: Brak danych. "
        "Każde twierdzenie faktograficzne zakończ tagiem źródła: [CHPL], [mp.pl] lub [PubMed]. "
        "Użyj dokładnie tego szablonu nagłówków i kolejności:\n\n"
        + "\n".join(SECTION_TEMPLATE)
        + "\n\n"
    )

    remaining = MAX_PROMPT_LENGTH - len(header)
    if remaining <= 0:
        return _truncate_text(header, MAX_PROMPT_LENGTH)

    parts = []

    chpl_text = _to_text(sources.get("chpl", {}))
    if chpl_text and remaining > 0:
        chpl_block = f"[CHPL]\n{_truncate_text(chpl_text, remaining)}"
        parts.append(chpl_block)
        remaining = MAX_PROMPT_LENGTH - len(header) - len("\n\n".join(parts))

    mp_text = _to_text(sources.get("mp", {}))
    if mp_text and remaining > 0:
        mp_block = f"[mp.pl]\n{_truncate_text(mp_text, remaining)}"
        tentative = "\n\n".join(parts + [mp_block])
        if len(header) + len(tentative) <= MAX_PROMPT_LENGTH:
            parts.append(mp_block)
            remaining = MAX_PROMPT_LENGTH - len(header) - len(tentative)

    pubmed_text = _to_text(sources.get("pubmed", {}))
    if pubmed_text and remaining > 0:
        pubmed_block = f"[PubMed]\n{_truncate_text(pubmed_text, remaining)}"
        tentative = "\n\n".join(parts + [pubmed_block])
        if len(header) + len(tentative) <= MAX_PROMPT_LENGTH:
            parts.append(pubmed_block)
        else:
            still_left = MAX_PROMPT_LENGTH - len(header) - len("\n\n".join(parts))
            if still_left > len("[PubMed]\n"):
                trimmed = f"[PubMed]\n{_truncate_text(pubmed_text, still_left - len('[PubMed]\\n'))}"
                parts.append(trimmed)

    prompt = header + "\n\n".join(parts)
    if len(prompt) > MAX_PROMPT_LENGTH:
        prompt = _truncate_text(prompt, MAX_PROMPT_LENGTH)
    return prompt


def call_ollama(prompt):
    started_at = time.time()
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": OLLAMA_NUM_PREDICT,
            "temperature": 0.2,
        },
    }

    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=OLLAMA_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        text = data.get("response", "") if isinstance(data, dict) else ""
        result = str(text or "").strip()
        if not result:
            result = "Błąd Ollama: pusta odpowiedź"
        return result
    except (requests.RequestException, json.JSONDecodeError) as error:
        return f"Błąd Ollama: {error}"
    finally:
        elapsed = time.time() - started_at
        _log(f"[NOTE] Ollama duration: {elapsed:.2f}s")


def _normalize_generated_body(ollama_response):
    sections = {heading: [] for heading in SECTION_TEMPLATE}
    current_heading = None

    for raw_line in str(ollama_response or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line in SECTION_TEMPLATE:
            current_heading = line
            continue
        if current_heading:
            sections[current_heading].append(line)

    body_parts = []
    for heading in SECTION_TEMPLATE:
        lines = sections.get(heading, [])
        if not lines:
            section_text = "Brak danych"
        else:
            valid_lines = []
            for line in lines:
                if line == "Brak danych":
                    valid_lines.append(line)
                    continue
                if re.search(r"\[(CHPL|mp\.pl|PubMed)\]$", line):
                    valid_lines.append(line)
                else:
                    valid_lines = []
                    break
            section_text = "\n".join(valid_lines) if valid_lines else "Brak danych"

        body_parts.append(f"{heading}\n{section_text}")

    return "\n\n".join(body_parts)


def build_note(drug_name, ollama_response, sources):
    available = _available_sources(sources)
    sources_yaml = ", ".join(available)
    today = datetime.date.today().isoformat()

    frontmatter = [
        "---",
        f"lek: {drug_name}",
        f"data_aktualizacji: {today}",
        f"zrodla: [{sources_yaml}]" if sources_yaml else "zrodla: []",
        "chpl_dostepny: tak" if sources.get("chpl") else "chpl_dostepny: nie",
        "gif_alert: nie",
        "---",
        "",
    ]

    body = _normalize_generated_body(ollama_response)

    footer = "\n\n*Weryfikuj z oficjalnymi źródłami CHPL/EMA*\n"
    return "\n".join(frontmatter) + body + footer


def save_note(drug_name, content):
    slug = _slugify(drug_name)
    notes_dir = os.path.join(VAULT_PATH, "Leki")
    os.makedirs(notes_dir, exist_ok=True)
    output_path = os.path.join(notes_dir, f"{slug}.md")

    with open(output_path, "w", encoding="utf-8") as note_file:
        note_file.write(content)

    return output_path


def generate(drug_name):
    sources = load_sources(drug_name)
    prompt = build_prompt(drug_name, sources)
    ollama_response = call_ollama(prompt)
    note_content = build_note(drug_name, ollama_response, sources)
    saved_path = save_note(drug_name, note_content)
    print(f"✅ {drug_name} → {saved_path}")
    return saved_path


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python agent/note_generator.py <drug_name>")
    else:
        generate(" ".join(sys.argv[1:]).strip())