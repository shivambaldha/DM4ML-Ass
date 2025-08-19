from prefect import flow, task
import subprocess

def run(cmd: str):
    print(f"RUN: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

@task
def ingest(): run("python data_ingestion.py")

@task
def raw_store(): run("python storage_raw_upload.py")

@task
def validate(): run("python data_validation.py")

@task
def prepare(): run("python data_preparation.py")

@task
def transform(): run("python data_transformation.py")

@task
def feature_store_task(): run("python feature_store.py")

@task
def train(): run("python model_building.py")

@flow(name="churn_end_to_end_pipeline")
def main_flow():
    ingest()
    raw_store()
    validate()
    prepare()
    transform()
    feature_store_task()
    train()

if __name__ == "__main__":
    main_flow()
