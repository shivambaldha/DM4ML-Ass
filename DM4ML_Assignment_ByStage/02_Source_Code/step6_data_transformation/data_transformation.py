import pandas as pd
import sqlite3
from pathlib import Path
import logging
from datetime import datetime

# ---------------- CONFIG ----------------
CLEAN_DATA_FILE = Path("data/clean/transactions_clean.csv")
DB_FILE = Path("data/feature_store.db")
LOG_FILE = "logs/data_transformation.log"

# Create dirs
Path("logs").mkdir(parents=True, exist_ok=True)
Path("data").mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
def log(msg):
    print(msg)
    logging.info(msg)

def load_clean_data():
    if not CLEAN_DATA_FILE.exists():
        raise FileNotFoundError("Clean data file not found. Run Step 5 first.")
    return pd.read_csv(CLEAN_DATA_FILE)

def engineer_features(df):
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    agg_df = df.groupby("customer_id").agg(
        total_spend=("amount", "sum"),
        transaction_count=("transaction_id", "count"),
        avg_transaction_value=("amount", "mean"),
        first_transaction=("transaction_date", "min"),
        last_transaction=("transaction_date", "max")
    ).reset_index()

    now = datetime.now()
    agg_df["tenure_days"] = (now - agg_df["first_transaction"]).dt.days
    agg_df["recency_days"] = (now - agg_df["last_transaction"]).dt.days

    agg_df.drop(columns=["first_transaction", "last_transaction"], inplace=True)
    return agg_df

def save_to_db(df):
    conn = sqlite3.connect(DB_FILE)
    df.to_sql("customer_features", conn, if_exists="replace", index=False)
    conn.close()
    log(f"Saved transformed features to database: {DB_FILE}")

def run():
    log("Starting data transformation...")
    df = load_clean_data()
    features_df = engineer_features(df)
    save_to_db(features_df)
    log("Data transformation completed successfully.")

if __name__ == "__main__":
    run()
