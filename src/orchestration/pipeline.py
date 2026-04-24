from __future__ import annotations

from pathlib import Path
import os
import shutil
import subprocess
import sys

import pandas as pd
from google.cloud import bigquery

from src.enrichment.ticket_enricher import enrichment_to_dict


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def generate_source_data() -> None:
    subprocess.run([sys.executable, "-m", "src.data.generate_sample_tickets"], cwd=PROJECT_ROOT, check=True)


def validate_raw_files() -> None:
    required = [RAW_DIR / "support_tickets.csv"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required raw files: {missing}")


def enrich_support_tickets() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    tickets_df = pd.read_csv(RAW_DIR / "support_tickets.csv")
    enriched_rows = []
    for row in tickets_df.to_dict(orient="records"):
        enrichment = enrichment_to_dict(row["message_text"])
        enriched_rows.append(
            {
                "ticket_id": row["ticket_id"],
                "thread_id": row["thread_id"],
                "slack_channel": row["slack_channel"],
                "customer_name": row["customer_name"],
                "created_at": row["created_at"],
                "message_text": row["message_text"],
                **enrichment,
            }
        )
    pd.DataFrame(enriched_rows).to_csv(PROCESSED_DIR / "ticket_enrichments.csv", index=False)


def ensure_bigquery_dataset(project_id: str, dataset_name: str, location: str = "US") -> None:
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def load_csv_to_bigquery(project_id: str, dataset_name: str, table_name: str, csv_path: Path, location: str = "US") -> None:
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(csv_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config, location=location)
    load_job.result()


def load_raw_tables(project_id: str, dataset_name: str, location: str = "US") -> None:
    load_csv_to_bigquery(project_id, dataset_name, "support_tickets", RAW_DIR / "support_tickets.csv", location)
    load_csv_to_bigquery(project_id, dataset_name, "ticket_enrichments", PROCESSED_DIR / "ticket_enrichments.csv", location)


def run_dbt_build(project_dir: Path | None = None, profiles_dir: Path | None = None) -> None:
    dbt_project_dir = project_dir or PROJECT_ROOT / "dbt" / "support_intelligence"
    dbt_profiles_dir = profiles_dir or Path(os.environ.get("DBT_PROFILES_DIR", PROJECT_ROOT / "dbt_profiles"))
    dbt_executable = shutil.which("dbt")
    if dbt_executable:
        command = [dbt_executable, "build", "--project-dir", str(dbt_project_dir), "--profiles-dir", str(dbt_profiles_dir)]
    else:
        command = [sys.executable, "-m", "dbt.cli.main", "build", "--project-dir", str(dbt_project_dir), "--profiles-dir", str(dbt_profiles_dir)]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
