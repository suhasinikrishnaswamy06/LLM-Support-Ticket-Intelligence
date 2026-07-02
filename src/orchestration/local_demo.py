from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd

from src.data.generate_sample_tickets import build_ticket_rows
from src.orchestration.pipeline import (
    ENRICHMENTS_PATH,
    FAILURES_PATH,
    PROCESSED_DIR,
    RAW_DIR,
    RUN_SUMMARY_PATH,
    enrich_support_tickets,
)


def _write_mock_tickets(ticket_count: int) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / "support_tickets.csv"
    dataframe = pd.DataFrame(build_ticket_rows(ticket_count))
    dataframe.to_csv(output_path, index=False)
    return output_path


def run_local_demo(ticket_count: int = 80, force_fallback: bool = True) -> dict[str, object]:
    """Run the credential-free local demo and return a compact verification summary."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if force_fallback:
        os.environ.pop("OPENAI_API_KEY", None)

    raw_path = _write_mock_tickets(ticket_count)
    enrich_support_tickets()

    enrichments_df = pd.read_csv(ENRICHMENTS_PATH)
    failures_df = (
        pd.read_csv(FAILURES_PATH)
        if FAILURES_PATH.exists() and FAILURES_PATH.stat().st_size > 2
        else pd.DataFrame()
    )
    run_summary = json.loads(RUN_SUMMARY_PATH.read_text(encoding="utf-8"))

    verification = {
        "raw_tickets_path": str(raw_path),
        "enrichments_path": str(ENRICHMENTS_PATH),
        "failures_path": str(FAILURES_PATH),
        "run_summary_path": str(RUN_SUMMARY_PATH),
        "source_ticket_count": int(run_summary["source_ticket_count"]),
        "successful_enrichment_count": int(run_summary["successful_enrichment_count"]),
        "failed_enrichment_count": int(run_summary["failed_enrichment_count"]),
        "success_rate": float(run_summary["success_rate"]),
        "enrichment_methods": sorted(enrichments_df["enrichment_method"].dropna().unique().tolist()),
        "failure_rows": int(len(failures_df)),
    }

    return verification


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the credential-free local support-ticket demo.")
    parser.add_argument("--ticket-count", type=int, default=80, help="Number of mock tickets to generate.")
    parser.add_argument(
        "--use-current-openai",
        action="store_true",
        help="Use the current OPENAI_API_KEY instead of forcing fallback enrichment.",
    )
    args = parser.parse_args()

    verification = run_local_demo(ticket_count=args.ticket_count, force_fallback=not args.use_current_openai)
    print("Local demo completed successfully.")
    print(json.dumps(verification, indent=2))


if __name__ == "__main__":
    main()
