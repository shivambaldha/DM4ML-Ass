import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# ---------------- CONFIG ----------------
DATA_LAKE_RAW = Path("data_lake/raw")
LOG_FILE = "logs/data_validation.log"
REPORT_FILE = Path("reports/data_quality_report.csv")

# Expected schema definitions for validation
EXPECTED_SCHEMAS = {
    "transactions": {
        "customer_id": "int64",
        "transaction_id": "object",
        "transaction_date": "object",
        "amount": "float64",
        "payment_method": "object",
        "transaction_status": "object"
    },
    "web_logs": {
        "userId": "int64",
        "id": "int64",
        "title": "object",
        "body": "object"
    }
}

# Create necessary directories
Path("logs").mkdir(parents=True, exist_ok=True)
REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

# Logging setup
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log(msg):
    print(msg)
    logging.info(msg)

def validate_file(file_path: Path, source: str):
    df = pd.read_csv(file_path)
    issues = []

    # 1. Missing values
    missing_counts = df.isnull().sum()
    for col, count in missing_counts.items():
        if count > 0:
            issues.append(f"Missing {count} values in column {col}")

    # 2. Data type mismatches
    expected_types = EXPECTED_SCHEMAS.get(source, {})
    for col, expected_type in expected_types.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if actual_type != expected_type:
                issues.append(f"Column {col} expected {expected_type}, got {actual_type}")

    # 3. Range checks (transactions only)
    if source == "transactions" and "amount" in df.columns:
        if (df["amount"] < 0).any():
            issues.append("Negative transaction amounts found")

    # 4. Duplicate check
    if df.duplicated().any():
        dup_count = df.duplicated().sum()
        issues.append(f"{dup_count} duplicate rows found")

    # 5. Date anomalies (transactions only)
    if source == "transactions" and "transaction_date" in df.columns:
        try:
            df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
            future_dates = df[df["transaction_date"] > datetime.now()]
            if not future_dates.empty:
                issues.append(f"{len(future_dates)} future transaction dates found")
        except Exception as e:
            issues.append(f"Date parsing error: {e}")

    return issues

def run_validation():
    log("Starting data validation...")
    report_rows = []

    for file_path in DATA_LAKE_RAW.rglob("*.csv"):
        source = None
        if "source=transactions" in str(file_path):
            source = "transactions"
        elif "source=web_logs" in str(file_path):
            source = "web_logs"

        if not source:
            log(f"Skipping unrecognized file: {file_path}")
            continue

        issues = validate_file(file_path, source)
        if not issues:
            log(f"{file_path} passed all checks")
            issues_str = "No issues"
        else:
            log(f"Issues found in {file_path}: {issues}")
            issues_str = "; ".join(issues)

        report_rows.append({
            "file_path": str(file_path),
            "source": source,
            "issues": issues_str
        })

    pd.DataFrame(report_rows).to_csv(REPORT_FILE, index=False)
    log(f"Data quality report saved to {REPORT_FILE}")

if __name__ == "__main__":
    run_validation()
