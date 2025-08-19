#!/usr/bin/env bash
set -euo pipefail
python data_ingestion.py
python storage_raw_upload.py
python data_validation.py
python data_preparation.py
python data_transformation.py
python feature_store.py
python model_building.py
echo "All steps completed successfully."
