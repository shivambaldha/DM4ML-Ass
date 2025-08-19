import os
import pandas as pd
import requests
import logging
from datetime import datetime
from pathlib import Path

# ----------------- CONFIG -----------------
RAW_DATA_DIR = Path("data/raw")
LOG_FILE = "logs/ingestion.log"
TRANSACTIONS_CSV = "sample_data/transactions.csv"
WEB_API_URL = "https://jsonplaceholder.typicode.com/posts"  # Mock API

# Create necessary directories
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(parents=True, exist_ok=True)

# ----------------- LOGGING -----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log_message(message):
    print(message)
    logging.info(message)

# ----------------- INGESTION FUNCTIONS -----------------
def ingest_transactions():
    """Fetch customer transactions from CSV."""
    try:
        df = pd.read_csv(TRANSACTIONS_CSV)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = RAW_DATA_DIR / f"transactions_{timestamp}.csv"
        df.to_csv(output_path, index=False)
        log_message(f"Transactions data ingested: {output_path}")
    except Exception as e:
        logging.error(f"Failed to ingest transactions: {e}")

def ingest_web_logs():
    """Fetch web logs from mock API."""
    try:
        response = requests.get(WEB_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = RAW_DATA_DIR / f"web_logs_{timestamp}.csv"
        df.to_csv(output_path, index=False)
        log_message(f"Web logs data ingested: {output_path}")
    except Exception as e:
        logging.error(f"Failed to ingest web logs: {e}")

# ----------------- MAIN PIPELINE -----------------
def run_ingestion():
    log_message("Starting data ingestion...")
    ingest_transactions()
    ingest_web_logs()
    log_message("Data ingestion completed successfully.")

if __name__ == "__main__":
    run_ingestion()
