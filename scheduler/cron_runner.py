import argparse
import datetime
import os
import time

import psutil

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in os.sys.path:
        os.sys.path.insert(0, project_root)

from agent import note_generator as agent
from config import BATCH_SIZE, LOG_FILE, QUEUE_FILE, RAM_LIMIT_PERCENT, SOURCES, VAULT_PATH


PROCESSED_FILE = os.path.join(VAULT_PATH, "processed.txt")
CRON_LINE = "0 */4 * * * cd ~/pharmacy-agent && .venv/bin/python scheduler/cron_runner.py"


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


def run_batch(batch_size=None):
    selected_batch_size = BATCH_SIZE if batch_size is None else int(batch_size)

    if not check_ram():
        _log("[CRON] Batch aborted due to RAM limit")
        return 0

    batch = get_next_batch(selected_batch_size)
    total_requested = len(batch)

    run_started = time.time()
    processed_now = 0

    for index, drug_name in enumerate(batch):
        item_started = time.time()
        start_stamp = datetime.datetime.now().replace(microsecond=0).isoformat()
        _log(f"[CRON] START {start_stamp} {drug_name}")
        try:
            agent.generate(drug_name)
            mark_processed(drug_name)
            processed_now += 1
        except Exception as error:
            _log(f"[CRON] ERROR {drug_name}: {error}")

        item_elapsed = time.time() - item_started
        end_stamp = datetime.datetime.now().replace(microsecond=0).isoformat()
        _log(f"[CRON] END {end_stamp} {drug_name} | duration: {item_elapsed:.2f}s")

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
    args = parser.parse_args()

    setup_vault_dirs()
    run_batch(args.batch)