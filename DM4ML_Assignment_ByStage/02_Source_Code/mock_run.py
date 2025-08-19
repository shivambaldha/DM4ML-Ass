import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import logging
from datetime import datetime

# ---------------- PATHS ----------------
base = Path(".")
data_lake = base / "03_Data" / "data_lake" / "source=mock" / f"ingestion_date={datetime.today().date()}"
reports_dir = base / "05_Reports"
models_dir = base / "models"
logs_dir = base / "04_Logs"

for d in [data_lake, reports_dir, models_dir, logs_dir]:
    d.mkdir(parents=True, exist_ok=True)

# ---------------- LOGGING ----------------
log_file = logs_dir / "mock_run.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
def log(msg):
    print(msg)
    logging.info(msg)

# ---------------- STEP 1: CREATE RAW DATA ----------------
log("Generating mock raw data...")
raw_df = pd.DataFrame({
    "customer_id": range(1, 51),
    "transactions": np.random.randint(1, 100, 50),
    "spend": np.random.uniform(100, 500, 50).round(2),
    "last_login_days": np.random.randint(1, 60, 50)
})
raw_file = data_lake / "mock_transactions.csv"
raw_df.to_csv(raw_file, index=False)
log(f"Raw data saved: {raw_file}")

# ---------------- STEP 2: DATA QUALITY REPORT ----------------
log("Generating mock data quality report...")
quality_report = pd.DataFrame({
    "column": raw_df.columns,
    "missing_values": [raw_df[c].isna().sum() for c in raw_df.columns],
    "dtype": [str(raw_df[c].dtype) for c in raw_df.columns]
})
quality_file = reports_dir / "data_quality_report.csv"
quality_report.to_csv(quality_file, index=False)
log(f"Quality report saved: {quality_file}")

# ---------------- STEP 3: EDA PLOTS ----------------
log("Generating EDA plots...")
for col in ["transactions", "spend", "last_login_days"]:
    plt.figure()
    raw_df[col].hist(bins=10)
    plt.title(f"Histogram of {col}")
    plt.xlabel(col)
    plt.ylabel("Frequency")
    plot_file = reports_dir / f"hist_{col}.png"
    plt.savefig(plot_file)
    plt.close()
    log(f"Saved: {plot_file}")

# ---------------- STEP 4: MODEL TRAINING MOCK ----------------
log("Simulating model training...")
# Fake labels
raw_df["churn"] = np.random.choice([0,1], size=len(raw_df), p=[0.7,0.3])
X = raw_df[["transactions", "spend", "last_login_days"]]
y = raw_df["churn"]

# Fake model (just store mean churn rate)
mock_model = {"churn_rate": y.mean(), "trained_on": str(datetime.now())}
model_file = models_dir / "mock_churn_model.pkl"
joblib.dump(mock_model, model_file)
log(f"Mock model saved: {model_file}")

# Fake metrics
metrics = {
    "accuracy": round(np.random.uniform(0.7, 0.9), 2),
    "precision": round(np.random.uniform(0.6, 0.8), 2),
    "recall": round(np.random.uniform(0.5, 0.7), 2),
    "f1_score": round(np.random.uniform(0.55, 0.75), 2),
    "roc_auc": round(np.random.uniform(0.75, 0.9), 2)
}
log(f"Mock evaluation metrics: {metrics}")

log("Mock pipeline run completed successfully.")
