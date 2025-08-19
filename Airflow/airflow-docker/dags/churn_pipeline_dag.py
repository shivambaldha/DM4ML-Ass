from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os
import sys
import subprocess


# 🔹 Helper function to run a Python script from 02_Source_Code
def run_script(script_path):
    result = subprocess.run(
        [sys.executable, script_path],   # 👈 use same Python env as Airflow
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise Exception(f"Script failed: {script_path}\n{result.stderr}")
    return result.stdout



# 🔹 Absolute path inside the container
BASE_DIR = "/opt/airflow/02_Source_Code"


# 🔹 DAG definition
with DAG(
    dag_id="churn_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # Run manually
    catchup=False,
    tags=["churn", "assignment"],
) as dag:

    t1 = PythonOperator(
        task_id="data_ingestion",
        python_callable=run_script,
        op_args=[os.path.join(BASE_DIR, "step2_data_ingestion", "data_ingestion.py")]
    )

    t2 = PythonOperator(
        task_id="raw_data_storage",
        python_callable=run_script,
        op_args=[os.path.join(BASE_DIR, "step3_raw_data_storage", "storage_raw_upload.py")]
    )

    t3 = PythonOperator(
        task_id="data_validation",
        python_callable=run_script,
        op_args=[os.path.join(BASE_DIR, "step4_data_validation", "data_validation.py")]
    )

    t4 = PythonOperator(
        task_id="data_preparation",
        python_callable=run_script,
        op_args=[os.path.join(BASE_DIR, "step5_data_preparation", "data_preparation.py")]
    )

    t5 = PythonOperator(
        task_id="data_transformation",
        python_callable=run_script,
        op_args=[os.path.join(BASE_DIR, "step6_data_transformation", "data_transformation.py")]
    )

    t6 = PythonOperator(
        task_id="feature_store",
        python_callable=run_script,
        op_args=[os.path.join(BASE_DIR, "step7_feature_store", "feature_store.py")]
    )

    t7 = PythonOperator(
        task_id='model_training',
        python_callable=run_script,
        op_args=['/opt/airflow/02_Source_Code/step9_model_building/model_building.py'],
        dag=dag,
    )

    # 🔹 Define DAG dependencies
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
