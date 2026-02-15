# ============================================================
# PIPELINE ORCHESTRATOR
# ============================================================

"""
Local Batch Pipeline Orchestrator

Runs the full data pipeline in order:

1. Bronze Layer  -> Ingestion
2. Silver Layer  -> Transformation
3. Gold Layer    -> Warehouse Load

Acts as a lightweight replacement for Airflow.
"""

import sys
import os

# --------------------------------------------------
# FIX IMPORT PATH (IMPORTANT)
# --------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import time
import logging

from Ingestion.Ingestion import run_ingestion
from Transformation.transformation import run_transformation
from gold_layer.load_to_postgre import run_gold_load

import time
import logging
import os

# ------------------------------------------------------------
# IMPORT PIPELINE STEPS
# ------------------------------------------------------------
from Ingestion.Ingestion import run_ingestion
from Transformation.transformation import run_transformation
from gold_layer.load_to_postgre import run_gold_load


# ------------------------------------------------------------
# LOGGING CONFIG
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(BASE_DIR, "logs", "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


# ------------------------------------------------------------
# PIPELINE EXECUTION
# ------------------------------------------------------------
def run_pipeline():

    start_time = time.time()

    logging.info("===================================")
    logging.info("PIPELINE STARTED")
    logging.info("===================================")

    try:
        # -------------------------
        # STEP 1 — INGESTION
        # -------------------------
        logging.info("Running Ingestion Layer")
        run_ingestion()

        # -------------------------
        # STEP 2 — TRANSFORMATION
        # -------------------------
        logging.info("Running Transformation Layer")
        run_transformation()

        # -------------------------
        # STEP 3 — GOLD LOAD
        # -------------------------
        logging.info("Running Gold Layer")
        run_gold_load()

        logging.info("PIPELINE COMPLETED SUCCESSFULLY")

    except Exception as e:
        logging.error(f"PIPELINE FAILED: {e}")
        raise

    finally:
        total_time = round(time.time() - start_time, 2)
        logging.info(f"Total Pipeline Runtime: {total_time} seconds")
        logging.info("===================================")


# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
