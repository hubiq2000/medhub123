VAULT_PATH = "/mnt/hdd/pharmacy-vault"
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:7b-instruct"
MAX_TOKENS = 400
TEMPERATURE = 0.2
RATE_LIMIT_DELAY = 0.4
REQUEST_TIMEOUT = 15
RAM_LIMIT_PERCENT = 80
BATCH_SIZE = 5
LOG_FILE = f"{VAULT_PATH}/agent.log"
QUEUE_FILE = f"{VAULT_PATH}/queue.txt"
SOURCES = {
    "urpl": f"{VAULT_PATH}/sources/urpl",
    "chpl": f"{VAULT_PATH}/sources/chpl",
    "mp": f"{VAULT_PATH}/sources/mp",
    "pubmed": f"{VAULT_PATH}/sources/pubmed",
    "pmc": f"{VAULT_PATH}/sources/pmc",
    "ema": f"{VAULT_PATH}/sources/ema",
    "fda": f"{VAULT_PATH}/sources/fda",
    "gif": f"{VAULT_PATH}/sources/gif",
}