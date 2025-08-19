import re
import shutil
import logging
from pathlib import Path
from datetime import datetime

# -------- CONFIG --------
RAW_DROP_DIR = Path("data/raw")           # where Step 2 wrote raw files
DATA_LAKE_RAW = Path("data_lake/raw")     # lake root (local)
LOG_FILE = "logs/storage_upload.log"

# Create dirs
DATA_LAKE_RAW.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(parents=True, exist_ok=True)

# -------- LOGGING --------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
def log(msg): 
    print(msg); logging.info(msg)

# -------- HELPERS --------
FILENAME_PATTERNS = {
    "transactions": re.compile(r"^transactions_(\d{8}_\d{6})\.csv$"),
    "web_logs": re.compile(r"^web_logs_(\d{8}_\d{6})\.csv$"),
}

def detect_source_and_runid(file_name: str):
    for source, pattern in FILENAME_PATTERNS.items():
        m = pattern.match(file_name)
        if m:
            run_id = m.group(1)  # YYYYMMDD_HHMMSS from Step 2
            return source, run_id
    return None, None

def run():
    files = [p for p in RAW_DROP_DIR.glob("*.csv")]
    if not files:
        log("No raw files found to upload.")
        return

    for fpath in files:
        source, run_id = detect_source_and_runid(fpath.name)
        if not source:
            log(f"Skipping unrecognized file: {fpath.name}")
            continue

        # derive partition values
        date_str = run_id.split("_")[0]  # YYYYMMDD
        ingestion_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

        # build lake destination path
        dest_dir = DATA_LAKE_RAW / f"source={source}" / f"ingestion_date={ingestion_date}" / f"run_id={run_id}"
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_file = dest_dir / "part-00001.csv"
        shutil.move(str(fpath), dest_file)
        log(f"Uploaded to lake: {dest_file}")

    log("Raw storage upload completed successfully.")

if __name__ == "__main__":
    run()
