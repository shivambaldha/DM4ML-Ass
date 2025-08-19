from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "dm4ml_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="churn_end_to_end_pipeline",
    default_args=default_args,
    description="End-to-end data management pipeline for churn prediction",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["dm4ml", "churn", "etl", "mlops"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_data",
        bash_command="python {{ dag_run.conf.get('ingest_script', 'data_ingestion.py') }}",
    )

    raw_store = BashOperator(
        task_id="upload_to_raw_store",
        bash_command="python storage_raw_upload.py",
    )

    validate = BashOperator(
        task_id="validate_raw_data",
        bash_command="python data_validation.py",
    )

    prepare = BashOperator(
        task_id="prepare_clean_data",
        bash_command="python data_preparation.py",
    )

    transform = BashOperator(
        task_id="transform_and_store_features",
        bash_command="python data_transformation.py",
    )

    feature_store = BashOperator(
        task_id="register_feature_metadata",
        bash_command="python feature_store.py",
    )

    train = BashOperator(
        task_id="train_and_evaluate_model",
        bash_command="python model_building.py",
    )

    ingest >> raw_store >> validate >> prepare >> transform >> feature_store >> train
