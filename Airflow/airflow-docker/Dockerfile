FROM apache/airflow:2.7.3-python3.8

# Install system dependencies as root
USER root
RUN apt-get update && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for Python package installation
USER airflow

# Install Python dependencies (including dvc) as airflow user
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
