"""
Clinical Trial Data Lake Pipeline — Airflow DAG
================================================
Orchestrates the full pipeline:
  1. ingest     → pick up raw CSV files
  2. validate   → check schema, nulls, data quality
  3. transform  → trigger PySpark job
  4. load       → push Parquet output to S3 (or local in dev)
  5. notify     → log pipeline completion summary
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import csv
import json
import logging

# ── Config ────────────────────────────────────────────────────────────────────
RAW_DATA_PATH = "/opt/airflow/data/sample_trials.csv"
VALIDATED_PATH = "/opt/airflow/data/validated_trials.csv"
SUMMARY_PATH = "/opt/airflow/data/pipeline_summary.json"

REQUIRED_COLUMNS = {
    "patient_id", "trial_id", "drug", "site",
    "dosage_mg", "age", "weight_kg", "enrollment_date",
    "status", "outcome_score"
}

# ── Default DAG args ──────────────────────────────────────────────────────────
default_args = {
    "owner": "shubham",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Task functions ────────────────────────────────────────────────────────────

def ingest(**context):
    """
    Task 1: Ingest
    Checks that the raw data file exists and is not empty.
    In production this would pull from S3 / an SFTP / a database.
    """
    logging.info(f"[INGEST] Looking for raw data at: {RAW_DATA_PATH}")

    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError(f"Raw data not found: {RAW_DATA_PATH}")

    with open(RAW_DATA_PATH) as f:
        rows = list(csv.reader(f))

    record_count = len(rows) - 1  # subtract header
    logging.info(f"[INGEST] Found {record_count} records.")

    if record_count == 0:
        raise ValueError("Raw file is empty — aborting pipeline.")

    # Push metadata to XCom so downstream tasks can use it
    context["ti"].xcom_push(key="record_count", value=record_count)
    logging.info("[INGEST] ✅ Ingestion complete.")


def validate(**context):
    """
    Task 2: Validate
    - Checks all required columns are present
    - Flags rows with missing critical fields
    - Writes clean rows to validated file
    """
    logging.info("[VALIDATE] Starting data quality checks...")

    valid_rows = []
    invalid_rows = []

    with open(RAW_DATA_PATH) as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])

        # Column check
        missing_cols = REQUIRED_COLUMNS - headers
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")

        for row in reader:
            # Flag rows missing critical fields
            if not row["patient_id"] or not row["drug"] or not row["site"]:
                invalid_rows.append(row)
            else:
                valid_rows.append(row)

    logging.info(f"[VALIDATE] Valid rows: {len(valid_rows)} | Invalid rows: {len(invalid_rows)}")

    # Write validated data
    with open(VALIDATED_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(REQUIRED_COLUMNS))
        writer.writeheader()
        writer.writerows(valid_rows)

    context["ti"].xcom_push(key="valid_count", value=len(valid_rows))
    context["ti"].xcom_push(key="invalid_count", value=len(invalid_rows))
    logging.info("[VALIDATE] ✅ Validation complete.")


def transform(**context):
    """
    Task 3: Transform
    Triggers the PySpark job for distributed transformation.
    In dev mode we run it directly; in production this submits to a Spark cluster.
    """
    logging.info("[TRANSFORM] Triggering PySpark transformation job...")

    # In a real setup you'd use SparkSubmitOperator or SSHOperator
    # Here we invoke the spark job as a subprocess for local dev
    import subprocess
    result = subprocess.run(
        ["python", "/opt/airflow/spark_jobs/transform_trials.py"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError("PySpark job failed.")

    logging.info(result.stdout)
    logging.info("[TRANSFORM] ✅ Transformation complete.")


def load(**context):
    """
    Task 4: Load
    Pushes the transformed Parquet output to S3.
    In dev mode, logs the file path as a placeholder.
    """
    logging.info("[LOAD] Preparing to load Parquet files to S3...")

    parquet_path = "/opt/airflow/data/output_parquet/"

    if os.path.exists(parquet_path):
        files = os.listdir(parquet_path)
        logging.info(f"[LOAD] Found {len(files)} output files to upload.")
    else:
        logging.warning("[LOAD] No parquet output found — skipping S3 upload (dev mode).")

    # In production — replace with:
    # s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
    logging.info("[LOAD] ✅ Load step complete (dev mode — S3 upload skipped).")


def notify(**context):
    """
    Task 5: Notify
    Writes a summary JSON — acts as a lightweight pipeline report.
    In production this could send a Slack/email alert.
    """
    ti = context["ti"]
    record_count = ti.xcom_pull(key="record_count", task_ids="ingest") or 0
    valid_count = ti.xcom_pull(key="valid_count", task_ids="validate") or 0
    invalid_count = ti.xcom_pull(key="invalid_count", task_ids="validate") or 0

    summary = {
        "pipeline": "clinical_trial_data_lake",
        "run_date": str(context["ds"]),
        "total_records_ingested": record_count,
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "status": "SUCCESS",
    }

    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2)

    logging.info(f"[NOTIFY] Pipeline summary:\n{json.dumps(summary, indent=2)}")
    logging.info("[NOTIFY] ✅ Pipeline complete.")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="clinical_trial_data_lake",
    default_args=default_args,
    description="End-to-end pipeline: ingest → validate → transform → load → notify",
    schedule_interval="@daily",        # runs every day automatically
    start_date=days_ago(1),
    catchup=False,
    tags=["biopharmaceutical", "data-lake", "spark", "airflow"],
) as dag:

    t1_ingest = PythonOperator(task_id="ingest", python_callable=ingest)
    t2_validate = PythonOperator(task_id="validate", python_callable=validate)
    t3_transform = PythonOperator(task_id="transform", python_callable=transform)
    t4_load = PythonOperator(task_id="load", python_callable=load)
    t5_notify = PythonOperator(task_id="notify", python_callable=notify)

    # Task dependency chain: ingest → validate → transform → load → notify
    t1_ingest >> t2_validate >> t3_transform >> t4_load >> t5_notify
