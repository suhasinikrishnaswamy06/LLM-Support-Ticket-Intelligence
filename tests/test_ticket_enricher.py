from __future__ import annotations

import pytest

from src.enrichment.ticket_enricher import (
    TicketEnrichmentError,
    _validate_llm_payload,
    enrich_ticket,
)


def test_fallback_enrichment_classifies_critical_api_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    enrichment = enrich_ticket("Production webhook deliveries are failing across multiple tenants.")

    assert enrichment.enrichment_method == "fallback"
    assert enrichment.issue_category == "api_issue"
    assert enrichment.product_area == "api"
    assert enrichment.urgency == "critical"
    assert enrichment.sentiment == "negative"
    assert enrichment.confidence == 0.72


def test_validate_llm_payload_accepts_valid_payload() -> None:
    _validate_llm_payload(
        {
            "issue_category": "billing_issue",
            "sentiment": "negative",
            "urgency": "high",
            "product_area": "billing",
            "summary": "Customer was double charged after a plan upgrade.",
            "confidence": 0.91,
        }
    )


@pytest.mark.parametrize(
    ("field", "value", "error_fragment"),
    [
        ("urgency", "urgent", "Invalid urgency"),
        ("sentiment", "mixed", "Invalid sentiment"),
        ("product_area", "mobile", "Invalid product_area"),
        ("summary", "", "summary must be non-empty"),
        ("confidence", 1.4, "confidence must be between 0 and 1"),
    ],
)
def test_validate_llm_payload_rejects_invalid_values(
    field: str,
    value: object,
    error_fragment: str,
) -> None:
    payload = {
        "issue_category": "billing_issue",
        "sentiment": "negative",
        "urgency": "high",
        "product_area": "billing",
        "summary": "Customer was double charged after a plan upgrade.",
        "confidence": 0.91,
    }
    payload[field] = value

    with pytest.raises(TicketEnrichmentError, match=error_fragment):
        _validate_llm_payload(payload)


def test_validate_llm_payload_rejects_missing_keys() -> None:
    with pytest.raises(TicketEnrichmentError, match="Missing required keys"):
        _validate_llm_payload({"issue_category": "billing_issue"})
