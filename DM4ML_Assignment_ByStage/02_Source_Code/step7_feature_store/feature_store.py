import sqlite3
from datetime import datetime
import pandas as pd
from pathlib import Path
import logging

# ---------------- CONFIG ----------------
DB_FILE = Path("data/feature_store.db")
LOG_FILE = "logs/feature_store.log"

Path("logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
def log(msg):
    print(msg)
    logging.info(msg)

def init_metadata():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feature_metadata (
            feature_name TEXT PRIMARY KEY,
            description TEXT,
            source TEXT,
            version INTEGER,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()
    log("Feature metadata table initialized.")

def register_features(metadata):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    for feature_name, details in metadata.items():
        cursor.execute("""
            INSERT INTO feature_metadata (feature_name, description, source, version, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(feature_name) DO UPDATE SET
                description=excluded.description,
                source=excluded.source,
                version=excluded.version,
                created_at=excluded.created_at
        """, (feature_name, details["description"], details["source"],
              details["version"], details["created_at"]))
    conn.commit()
    conn.close()
    log("Features registered in metadata.")

def get_features_for_training(feature_list=None):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM customer_features", conn)
    conn.close()

    if feature_list:
        df = df[feature_list]
    return df

if __name__ == "__main__":
    init_metadata()

    feature_meta = {
        "total_spend": {
            "description": "Total spend by customer over all transactions",
            "source": "transactions",
            "version": 1,
            "created_at": datetime.now().isoformat()
        },
        "transaction_count": {
            "description": "Number of transactions by customer",
            "source": "transactions",
            "version": 1,
            "created_at": datetime.now().isoformat()
        },
        "avg_transaction_value": {
            "description": "Average transaction amount",
            "source": "transactions",
            "version": 1,
            "created_at": datetime.now().isoformat()
        },
        "tenure_days": {
            "description": "Number of days since first transaction",
            "source": "transactions",
            "version": 1,
            "created_at": datetime.now().isoformat()
        },
        "recency_days": {
            "description": "Number of days since last transaction",
            "source": "transactions",
            "version": 1,
            "created_at": datetime.now().isoformat()
        }
    }

    register_features(feature_meta)
    df_train = get_features_for_training()
    log(f"Retrieved {len(df_train)} rows for training.")
