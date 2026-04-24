from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestration.pipeline import (
    enrich_support_tickets,
    ensure_bigquery_dataset,
    generate_source_data,
    load_raw_tables,
    run_dbt_build,
    validate_raw_files,
)


GCP_PROJECT_ID = os.environ.get("SUPPORT_INTEL_GCP_PROJECT", "your-gcp-project")
RAW_DATASET = os.environ.get("SUPPORT_INTEL_RAW_DATASET", "support_raw")
BQ_LOCATION = os.environ.get("SUPPORT_INTEL_BQ_LOCATION", "US")


with DAG(
    dag_id="support_ticket_intelligence_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "llm", "support"],
) as dag:
    generate_data = PythonOperator(task_id="generate_support_tickets", python_callable=generate_source_data)
    validate_files = PythonOperator(task_id="validate_raw_files", python_callable=validate_raw_files)
    enrich_tickets = PythonOperator(task_id="enrich_support_tickets", python_callable=enrich_support_tickets)
    ensure_dataset = PythonOperator(
        task_id="ensure_bigquery_raw_dataset",
        python_callable=ensure_bigquery_dataset,
        op_kwargs={"project_id": GCP_PROJECT_ID, "dataset_name": RAW_DATASET, "location": BQ_LOCATION},
    )
    load_raw = PythonOperator(
        task_id="load_support_tables_to_bigquery",
        python_callable=load_raw_tables,
        op_kwargs={"project_id": GCP_PROJECT_ID, "dataset_name": RAW_DATASET, "location": BQ_LOCATION},
    )
    build_dbt = PythonOperator(task_id="build_dbt_models", python_callable=run_dbt_build)

    generate_data >> validate_files >> enrich_tickets >> ensure_dataset >> load_raw >> build_dbt
