import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import logging
import os
import pandas as pd

# ---------------- CONFIG ----------------
DATA_LAKE_RAW = Path("data_lake/raw/source=transactions")  # We'll use transactions as example
CLEAN_DATA_DIR = Path("data/clean")
LOG_FILE = "logs/data_preparation.log"

# Create dirs
CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(parents=True, exist_ok=True)
Path("reports").mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
def log(msg):
    print(msg)
    logging.info(msg)

def load_latest_transactions():
    data_dir = "/opt/airflow/03_Data/sample_data"
    files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("No transaction files found.")

    latest_file = max(
        [os.path.join(data_dir, f) for f in files],
        key=os.path.getctime
    )
    print(f"Loading file: {latest_file}")
    return pd.read_csv(latest_file)

def clean_data(df):
    # Handle missing values
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("Unknown")
        else:
            df[col] = df[col].fillna(df[col].median())

    # Convert date columns
    if "transaction_date" in df.columns:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Encode categorical variables (label encoding for demo)
    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        df[col] = df[col].astype("category").cat.codes

    # Normalize numeric columns (min-max scaling)
    num_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in num_cols:
        if col != "customer_id":  # avoid scaling IDs
            min_val = df[col].min()
            max_val = df[col].max()
            if max_val != min_val:
                df[col] = (df[col] - min_val) / (max_val - min_val)
            else:
                df[col] = 0.0

    return df

def run_eda(df):
    summary_stats = df.describe()
    summary_stats.to_csv("reports/summary_statistics.csv")
    log("Saved summary statistics.")

    num_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in num_cols:
        plt.figure()
        plt.hist(df[col].dropna(), bins=20)
        plt.title(f"Distribution of {col}")
        plt.xlabel(col)
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(f"reports/hist_{col}.png")
        plt.close()

    for col in num_cols:
        plt.figure()
        plt.boxplot(df[col].dropna(), vert=False)
        plt.title(f"Boxplot of {col}")
        plt.xlabel(col)
        plt.tight_layout()
        plt.savefig(f"reports/box_{col}.png")
        plt.close()

    log("EDA plots saved to reports/.")

def run():
    log("Starting data preparation...")
    df = load_latest_transactions()
    df_clean = clean_data(df)

    clean_file = CLEAN_DATA_DIR / "transactions_clean.csv"
    df_clean.to_csv(clean_file, index=False)
    log(f"Clean data saved to {clean_file}")

    run_eda(df_clean)
    log("Data preparation completed successfully.")

if __name__ == "__main__":
    run()
