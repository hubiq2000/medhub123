# medhub

`medhub` is a local pharmacy research agent scaffold. It collects drug data from multiple sources,
processes it with a local Ollama model, and saves structured Markdown notes for an Obsidian vault.

## Requirements

- Python 3.12
- Local Ollama instance (default endpoint in `config.py`)

## Setup

1. Create and activate a virtual environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. (Optional) Validate config import:

```bash
python -c "import config; print(config.VAULT_PATH)"
```

## Running collectors

Run collectors directly from the project root, for example:

```bash
python collectors/urpl_collector.py
python collectors/chpl_scraper.py paracetamol
```

## Running the scheduler

Run batch note generation with:

```bash
python scheduler/cron_runner.py --batch 5
```

You can omit `--batch` to use `BATCH_SIZE` from `config.py`.