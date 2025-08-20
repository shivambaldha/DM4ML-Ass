import sqlite3
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
)
import joblib
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

# ---------------- CONFIG ----------------
DB_FILE = Path("data/feature_store.db")
MODEL_DIR = Path("models")
LOG_FILE = "logs/model_building.log"
EXPERIMENT_NAME = "churn_prediction"

MODEL_DIR.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
def log(msg):
    print(msg)
    logging.info(msg)

def load_features():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM customer_features", conn)
    conn.close()
    return df

def simulate_labels(df):
    np.random.seed(42)
    df["churn"] = np.random.choice([0, 1], size=len(df), p=[0.7, 0.3])
    return df

def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1_score": f1_score(y_test, y_pred, zero_division=0),
        "roc_auc": roc_auc_score(y_test, y_proba) if y_proba is not None else None
    }
    return metrics

def run():
    log("Loading features...")
    df = load_features()
    if df.empty:
        log("No features found in database. Run Step 6 first.")
        return

    df = simulate_labels(df)

    X = df.drop(columns=["customer_id", "churn"])
    y = df["churn"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # MLflow experiment setup
    mlflow.set_experiment(EXPERIMENT_NAME)

    # ---------------- Logistic Regression ----------------
    with mlflow.start_run(run_name="logistic_regression"):
        log("Training Logistic Regression...")
        lr_model = LogisticRegression(max_iter=500)
        lr_model.fit(X_train, y_train)
        lr_metrics = evaluate_model(lr_model, X_test, y_test)

        mlflow.log_params({"model_type": "logistic_regression", "max_iter": 500})
        mlflow.log_metrics(lr_metrics)

        signature = infer_signature(X_test, lr_model.predict(X_test))
        mlflow.sklearn.log_model(
            lr_model,
            "logistic_regression_model",
            input_example=X_test.iloc[:5],
            signature=signature
        )

        log(f"Logistic Regression metrics: {lr_metrics}")

    # ---------------- Random Forest ----------------
    with mlflow.start_run(run_name="random_forest"):
        log("Training Random Forest...")
        rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
        rf_model.fit(X_train, y_train)
        rf_metrics = evaluate_model(rf_model, X_test, y_test)

        mlflow.log_params({"model_type": "random_forest", "n_estimators": 100})
        mlflow.log_metrics(rf_metrics)

        signature = infer_signature(X_test, rf_model.predict(X_test))
        mlflow.sklearn.log_model(
            rf_model,
            "random_forest_model",
            input_example=X_test.iloc[:5],
            signature=signature
        )

        log(f"Random Forest metrics: {rf_metrics}")

    # ---------------- Select Best ----------------
    best_model, best_name, best_metrics = (
        (lr_model, "logistic_regression", lr_metrics)
        if lr_metrics["f1_score"] > rf_metrics["f1_score"]
        else (rf_model, "random_forest", rf_metrics)
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = MODEL_DIR / f"{best_name}_{timestamp}.pkl"
    joblib.dump(best_model, model_path)

    log(f"Saved best model locally: {model_path}")
    log(f"Best model: {best_name}, metrics: {best_metrics}")

    # Register best model in MLflow separately
    with mlflow.start_run(run_name="best_model"):
        mlflow.log_params({"selected_model": best_name})
        mlflow.log_metrics(best_metrics)
        signature = infer_signature(X_test, best_model.predict(X_test))
        mlflow.sklearn.log_model(
            best_model,
            best_name,
            registered_model_name="ChurnModel",
            input_example=X_test.iloc[:5],
            signature=signature
        )

if __name__ == "__main__":
    run()
