from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.orchestration import local_demo, pipeline


def _redirect_pipeline_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw"
    processed_dir = tmp_path / "data" / "processed"
    enrichments_path = processed_dir / "ticket_enrichments.csv"
    failures_path = processed_dir / "ticket_enrichment_failures.csv"
    run_summary_path = processed_dir / "ticket_enrichment_run_summary.json"
    replay_summary_path = processed_dir / "ticket_enrichment_replay_summary.json"

    for module in (pipeline, local_demo):
        monkeypatch.setattr(module, "RAW_DIR", raw_dir)
        monkeypatch.setattr(module, "PROCESSED_DIR", processed_dir)
        monkeypatch.setattr(module, "ENRICHMENTS_PATH", enrichments_path)
        monkeypatch.setattr(module, "FAILURES_PATH", failures_path)
        monkeypatch.setattr(module, "RUN_SUMMARY_PATH", run_summary_path)

    monkeypatch.setattr(pipeline, "REPLAY_SUMMARY_PATH", replay_summary_path)


def test_local_demo_generates_outputs_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "should-be-cleared")
    _redirect_pipeline_paths(monkeypatch, tmp_path)

    result = local_demo.run_local_demo(ticket_count=5)

    assert result["source_ticket_count"] == 5
    assert result["successful_enrichment_count"] == 5
    assert result["failed_enrichment_count"] == 0
    assert result["success_rate"] == 1.0
    assert result["enrichment_methods"] == ["fallback"]
    assert "OPENAI_API_KEY" not in result
    assert not result["failure_rows"]

    enrichments = pd.read_csv(result["enrichments_path"])
    assert len(enrichments) == 5
    assert set(enrichments["enrichment_method"]) == {"fallback"}


def test_replay_failed_enrichments_moves_recovered_rows_to_successes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _redirect_pipeline_paths(monkeypatch, tmp_path)
    pipeline.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(columns=pipeline.ENRICHMENT_COLUMNS).to_csv(pipeline.ENRICHMENTS_PATH, index=False)
    pd.DataFrame(
        [
            {
                "ticket_id": "TCKT-9999",
                "thread_id": "thread-99",
                "slack_channel": "support-api",
                "customer_name": "Acme Health",
                "created_at": "2026-01-01T00:00:00+00:00",
                "message_text": "[Acme Health] Production webhook deliveries are failing across multiple tenants.",
                "failure_type": "TicketEnrichmentError",
                "failure_reason": "temporary failure",
                "model_name": "gpt-4.1-mini",
                "prompt_version": "v1",
                "failed_at": "2026-01-01T00:01:00+00:00",
            }
        ]
    ).to_csv(pipeline.FAILURES_PATH, index=False)

    result = pipeline.replay_failed_enrichments()

    assert result["attempted_replays"] == 1
    assert result["replayed_success_count"] == 1
    assert result["remaining_failure_count"] == 0

    enrichments = pd.read_csv(pipeline.ENRICHMENTS_PATH)
    failures = pd.read_csv(pipeline.FAILURES_PATH)
    assert len(enrichments) == 1
    assert enrichments.loc[0, "ticket_id"] == "TCKT-9999"
    assert failures.empty
