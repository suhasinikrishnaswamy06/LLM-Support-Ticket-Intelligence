from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import shutil
import subprocess
import sys

import pandas as pd
from pandas.errors import EmptyDataError
from google.cloud import bigquery

from src.enrichment.ticket_enricher import TicketEnrichmentError, enrichment_to_dict


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
ENRICHMENTS_PATH = PROCESSED_DIR / "ticket_enrichments.csv"
FAILURES_PATH = PROCESSED_DIR / "ticket_enrichment_failures.csv"
RUN_SUMMARY_PATH = PROCESSED_DIR / "ticket_enrichment_run_summary.json"
REPLAY_SUMMARY_PATH = PROCESSED_DIR / "ticket_enrichment_replay_summary.json"

ENRICHMENT_COLUMNS = [
    "ticket_id",
    "thread_id",
    "slack_channel",
    "customer_name",
    "created_at",
    "message_text",
    "issue_category",
    "sentiment",
    "urgency",
    "product_area",
    "summary",
    "confidence",
    "model_name",
    "prompt_version",
    "enrichment_method",
    "processed_at",
    "attempt_count",
    "raw_response",
]

FAILURE_COLUMNS = [
    "ticket_id",
    "thread_id",
    "slack_channel",
    "customer_name",
    "created_at",
    "message_text",
    "failure_type",
    "failure_reason",
    "model_name",
    "prompt_version",
    "failed_at",
]


def generate_source_data() -> None:
    subprocess.run([sys.executable, "-m", "src.data.generate_sample_tickets"], cwd=PROJECT_ROOT, check=True)


def validate_raw_files() -> None:
    required = [RAW_DIR / "support_tickets.csv"]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required raw files: {missing}")


def _read_csv_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame(columns=columns)
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame(columns=columns)


def _write_dataframe(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    dataframe = pd.DataFrame(rows)
    if dataframe.empty:
        dataframe = pd.DataFrame(columns=columns)
    else:
        dataframe = dataframe.reindex(columns=columns)
    dataframe.to_csv(path, index=False)


def _build_success_row(row: dict[str, object], prompt_version: str) -> dict[str, object]:
    enrichment = enrichment_to_dict(str(row["message_text"]), prompt_version=prompt_version)
    return {
        "ticket_id": row["ticket_id"],
        "thread_id": row["thread_id"],
        "slack_channel": row["slack_channel"],
        "customer_name": row["customer_name"],
        "created_at": row["created_at"],
        "message_text": row["message_text"],
        **enrichment,
    }


def _build_failure_row(
    row: dict[str, object],
    exc: Exception,
    model_name: str,
    prompt_version: str,
) -> dict[str, object]:
    return {
        "ticket_id": row["ticket_id"],
        "thread_id": row["thread_id"],
        "slack_channel": row["slack_channel"],
        "customer_name": row["customer_name"],
        "created_at": row["created_at"],
        "message_text": row["message_text"],
        "failure_type": type(exc).__name__,
        "failure_reason": str(exc),
        "model_name": model_name,
        "prompt_version": prompt_version,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }


def enrich_support_tickets() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    tickets_df = pd.read_csv(RAW_DIR / "support_tickets.csv")
    enriched_rows: list[dict[str, object]] = []
    failed_rows: list[dict[str, object]] = []
    prompt_version = os.environ.get("SUPPORT_INTEL_PROMPT_VERSION", "v1")
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

    for row in tickets_df.to_dict(orient="records"):
        try:
            enriched_rows.append(_build_success_row(row, prompt_version=prompt_version))
        except TicketEnrichmentError as exc:
            failed_rows.append(_build_failure_row(row, exc, model_name=model_name, prompt_version=prompt_version))

    _write_dataframe(ENRICHMENTS_PATH, enriched_rows, ENRICHMENT_COLUMNS)
    _write_dataframe(FAILURES_PATH, failed_rows, FAILURE_COLUMNS)

    run_summary = {
        "run_completed_at": datetime.now(timezone.utc).isoformat(),
        "source_ticket_count": int(len(tickets_df)),
        "successful_enrichment_count": int(len(enriched_rows)),
        "failed_enrichment_count": int(len(failed_rows)),
        "success_rate": round(len(enriched_rows) / len(tickets_df), 4) if len(tickets_df) else 0,
        "model_name": model_name,
        "prompt_version": prompt_version,
    }
    RUN_SUMMARY_PATH.write_text(json.dumps(run_summary, indent=2), encoding="utf-8")


def replay_failed_enrichments(limit: int | None = None) -> dict[str, object]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    failures_df = _read_csv_or_empty(FAILURES_PATH, FAILURE_COLUMNS)
    enrichments_df = _read_csv_or_empty(ENRICHMENTS_PATH, ENRICHMENT_COLUMNS)
    failure_records = failures_df.to_dict(orient="records")
    if limit is not None:
        failure_records = failure_records[:limit]

    prompt_version = os.environ.get("SUPPORT_INTEL_PROMPT_VERSION", "v1")
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    replayed_successes: list[dict[str, object]] = []
    remaining_failures: list[dict[str, object]] = []

    replay_ticket_ids = {str(row["ticket_id"]) for row in failure_records}
    untouched_failures = [
        row for row in failures_df.to_dict(orient="records") if str(row["ticket_id"]) not in replay_ticket_ids
    ]

    for row in failure_records:
        try:
            replayed_successes.append(_build_success_row(row, prompt_version=prompt_version))
        except TicketEnrichmentError as exc:
            remaining_failures.append(_build_failure_row(row, exc, model_name=model_name, prompt_version=prompt_version))

    existing_successes = enrichments_df.to_dict(orient="records")
    successes_by_ticket_id = {str(row["ticket_id"]): row for row in existing_successes}
    for row in replayed_successes:
        successes_by_ticket_id[str(row["ticket_id"])] = row

    final_successes = list(successes_by_ticket_id.values())
    final_failures = untouched_failures + remaining_failures

    _write_dataframe(ENRICHMENTS_PATH, final_successes, ENRICHMENT_COLUMNS)
    _write_dataframe(FAILURES_PATH, final_failures, FAILURE_COLUMNS)

    replay_summary = {
        "replay_completed_at": datetime.now(timezone.utc).isoformat(),
        "attempted_replays": int(len(failure_records)),
        "replayed_success_count": int(len(replayed_successes)),
        "remaining_failure_count": int(len(final_failures)),
        "model_name": model_name,
        "prompt_version": prompt_version,
    }
    REPLAY_SUMMARY_PATH.write_text(json.dumps(replay_summary, indent=2), encoding="utf-8")
    return replay_summary


def ensure_bigquery_dataset(project_id: str, dataset_name: str, location: str = "US") -> None:
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)


def load_csv_to_bigquery(
    project_id: str,
    dataset_name: str,
    table_name: str,
    csv_path: Path,
    location: str = "US",
) -> None:
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
    load_csv_to_bigquery(project_id, dataset_name, "ticket_enrichments", ENRICHMENTS_PATH, location)
    if FAILURES_PATH.exists() and FAILURES_PATH.stat().st_size > 0:
        load_csv_to_bigquery(project_id, dataset_name, "ticket_enrichment_failures", FAILURES_PATH, location)


def run_dbt_build(project_dir: Path | None = None, profiles_dir: Path | None = None) -> None:
    dbt_project_dir = project_dir or PROJECT_ROOT / "dbt" / "support_intelligence"
    dbt_profiles_dir = profiles_dir or Path(os.environ.get("DBT_PROFILES_DIR", PROJECT_ROOT / "dbt_profiles"))
    dbt_executable = shutil.which("dbt")
    if dbt_executable:
        command = [dbt_executable, "build", "--project-dir", str(dbt_project_dir), "--profiles-dir", str(dbt_profiles_dir)]
    else:
        command = [sys.executable, "-m", "dbt.cli.main", "build", "--project-dir", str(dbt_project_dir), "--profiles-dir", str(dbt_profiles_dir)]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
