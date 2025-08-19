
.PHONY: all step2 step3 step4 step5 step6 step7 step8 step9 clean

all: step2 step3 step4 step5 step6 step7 step9

step2:
	python data_ingestion.py

step3:
	python storage_raw_upload.py

step4:
	python data_validation.py

step5:
	python data_preparation.py

step6:
	python data_transformation.py

step7:
	python feature_store.py

# Optional: initialize DVC tracking (manual steps in README)
step8:
	@echo "Run DVC commands as per README (manual for safety)."

step9:
	python model_building.py

clean:
	rm -rf data/raw/* data_lake/raw/* reports/* logs/* models/* data/feature_store.db || true
