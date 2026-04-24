# pharmavault-agent

`pharmavault-agent` is a local pharmacy research agent scaffold. It is designed to collect drug data from Polish and
international sources, process it with a local Ollama LLM, and save structured Markdown notes to a vault on disk.

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

Collector packages are scaffolded under `collectors/` and are intended to expose runnable modules (for example
`collectors.urpl`, `collectors.chpl`, etc.).

Run a collector module with:

```bash
python -m collectors.<collector_name>
```

Example:

```bash
python -m collectors.urpl
```

## Running the scheduler

Scheduler package is scaffolded under `scheduler/`.

Run the scheduler module with:

```bash
python -m scheduler
```

If you create a dedicated entry module (for example `scheduler.main`), run:

```bash
python -m scheduler.main
```