import argparse
import datetime
import importlib
import json
import os
import threading
import time

import psutil

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from agent import note_generator as agent
from config import BATCH_SIZE, LOG_FILE, QUEUE_FILE, RAM_LIMIT_PERCENT, SOURCES, VAULT_PATH, SOURCE_PRIORITY, SOURCE_LABELS


PROCESSED_FILE = os.path.join(VAULT_PATH, "processed.txt")
CRON_LINE = "0 */4 * * * cd ~/pharmacy-agent && .venv/bin/python scheduler/cron_runner.py"
SOURCES_COLLECTED_FILE = os.path.join(VAULT_PATH, "sources_collected.json")


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


def _read_text_lines(path):
    try:
        with open(path, "r", encoding="utf-8") as input_file:
            return [line.strip() for line in input_file if line.strip()]
    except OSError:
        return []


def _processed_drugs():
    names = set()
    for line in _read_text_lines(PROCESSED_FILE):
        if " " in line:
            _, drug_name = line.split(" ", 1)
            names.add(drug_name.strip())
        else:
            names.add(line)
    return names


def _format_duration(seconds):
    total_seconds = int(max(0, seconds))
    minutes, secs = divmod(total_seconds, 60)
    return f"{minutes} min {secs} sec"


def _atomic_write_json(path, data):
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except OSError:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


def check_ram():
    usage_percent = float(psutil.virtual_memory().percent)
    if usage_percent > float(RAM_LIMIT_PERCENT):
        _log(f"[CRON] RAM usage too high: {usage_percent:.1f}% > {RAM_LIMIT_PERCENT}%")
        return False
    return True


def get_next_batch(batch_size):
    queue = _read_text_lines(QUEUE_FILE)
    processed = _processed_drugs()

    total_count = len(queue)
    done_count = sum(1 for drug in queue if drug in processed)
    remaining = [drug for drug in queue if drug not in processed]

    print(f"Queue: {total_count} total | {done_count} done | {len(remaining)} remaining")
    limit = max(0, int(batch_size))
    return remaining[:limit]


def mark_processed(drug_name):
    timestamp = datetime.datetime.now().replace(microsecond=0).isoformat()
    try:
        os.makedirs(VAULT_PATH, exist_ok=True)
        with open(PROCESSED_FILE, "a", encoding="utf-8") as output_file:
            output_file.write(f"{timestamp} {drug_name}\n")
    except OSError as error:
        _log(f"[CRON] Failed to write processed marker for '{drug_name}': {error}")


def collect_sources_for_drug(drug_name, per_source_timeout=15, total_timeout=90):
    """Iterate SOURCE_PRIORITY and call each collector's process_drug(drug_name).
    Each source gets `per_source_timeout` seconds (thread join). The entire
    collection phase is limited to `total_timeout` seconds.
    Returns dict mapping source->bool (True=success).
    """
    results = {}
    start = time.time()

    for src in SOURCE_PRIORITY:
        # stop if total timeout exceeded
        elapsed = time.time() - start
        if elapsed >= total_timeout:
            _log(f"[COLLECT] {drug_name}: total collection timeout reached ({elapsed:.1f}s)")
            results[src] = False
            continue

        label = SOURCE_LABELS.get(src, src)
        mod = None
        candidates = [f"collectors.{src}_collector", f"collectors.{src}_scraper", f"collectors.{src}"]
        for cand in candidates:
            try:
                mod = importlib.import_module(cand)
                break
            except Exception:
                continue
        if mod is None:
            _log(f"[COLLECT] {drug_name}: {label} importer missing — skipping")
            results[src] = False
            continue

        success_flag = False

        def _worker():
            nonlocal success_flag
            try:
                res = None
                if hasattr(mod, "process_drug"):
                    res = mod.process_drug(drug_name)
                elif hasattr(mod, "process"):
                    res = mod.process(drug_name)
                success_flag = bool(res)
            except Exception as e:
                _log(f"[COLLECT] {drug_name}: exception in {label}: {e}")
                success_flag = False

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        thread.join(per_source_timeout)
        if thread.is_alive():
            _log(f"[COLLECT] {drug_name}: {label} timed out after {per_source_timeout}s")
            results[src] = False
            # thread will continue in background but we move on
        else:
            results[src] = bool(success_flag)

    # Log summary
    ok_count = sum(1 for v in results.values() if v)
    parts = []
    for s in SOURCE_PRIORITY:
        ok = results.get(s, False)
        mark = "✓" if ok else "✗"
        parts.append(f"{s}{mark}")
    _log(f"[COLLECT] {drug_name}: {' '.join(parts)} ({ok_count}/{len(SOURCE_PRIORITY)})")
    return results


def run_batch(batch_size=None, sources_only=False):
    selected_batch_size = BATCH_SIZE if batch_size is None else int(batch_size)

    # initial ram check
    if not check_ram():
        _log("[CRON] Batch aborted due to RAM limit")
        return 0

    batch = get_next_batch(selected_batch_size)
    total_requested = len(batch)

    run_started = time.time()
    processed_now = 0

    for index, drug_name in enumerate(batch):
        # check RAM before each drug
        if not check_ram():
            _log("[CRON] Stopping batch due to RAM limit")
            break

        item_started = time.time()
        start_stamp = datetime.datetime.now().replace(microsecond=0).isoformat()
        _log(f"[CRON] START {start_stamp} {drug_name}")

        # collection phase
        collect_start = time.time()
        collect_results = collect_sources_for_drug(drug_name)
        collect_elapsed = time.time() - collect_start

        generate_elapsed = 0.0
        generation_error = None
        if sources_only:
            # record sources_collected
            try:
                existing = {}
                if os.path.exists(SOURCES_COLLECTED_FILE):
                    with open(SOURCES_COLLECTED_FILE, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                existing[drug_name] = {"collected_at": datetime.datetime.now().replace(microsecond=0).isoformat(), "sources": collect_results}
                _atomic_write_json(SOURCES_COLLECTED_FILE, existing)
                _log(f"[SOURCES-ONLY] {drug_name} collected and recorded")
            except Exception as e:
                _log(f"[SOURCES-ONLY] Failed to record for {drug_name}: {e}")
        else:
            try:
                gen_start = time.time()
                agent.generate(drug_name)
                generate_elapsed = time.time() - gen_start
                mark_processed(drug_name)
                processed_now += 1
            except Exception as error:
                generation_error = error
                _log(f"[CRON] ERROR generate {drug_name}: {error}")

        item_elapsed = time.time() - item_started
        end_stamp = datetime.datetime.now().replace(microsecond=0).isoformat()
        _log(f"[CRON] END {end_stamp} {drug_name} | duration: {item_elapsed:.2f}s")

        # Log batch-level timings
        _log(f"[BATCH] {drug_name}: collect={collect_elapsed:.2f}s generate={generate_elapsed:.2f}s total={item_elapsed:.2f}s")

        if index < len(batch) - 1:
            time.sleep(5)

    total_elapsed = time.time() - run_started
    summary = f"Batch done: {processed_now}/{total_requested} | {_format_duration(total_elapsed)} total"
    print(summary)
    _log(f"[CRON] {summary}")
    return processed_now


def setup_vault_dirs():
    os.makedirs(VAULT_PATH, exist_ok=True)
    for source_dir in SOURCES.values():
        os.makedirs(source_dir, exist_ok=True)


def cron_install():
    print(CRON_LINE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pharmacy note generation batch")
    parser.add_argument("--batch", type=int, default=None, help="Number of drugs to process in this run")
    parser.add_argument("--sources-only", action="store_true", help="Only collect sources, skip note generation")
    args = parser.parse_args()

    setup_vault_dirs()
    run_batch(args.batch, sources_only=args.sources_only)
