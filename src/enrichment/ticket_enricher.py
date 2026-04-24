from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from typing import Any

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None


ALLOWED_URGENCY = {"low", "medium", "high", "critical"}
ALLOWED_SENTIMENT = {"positive", "neutral", "negative"}
ALLOWED_PRODUCT_AREAS = {"billing", "authentication", "api", "dashboard", "integrations"}


@dataclass
class TicketEnrichment:
    issue_category: str
    sentiment: str
    urgency: str
    product_area: str
    summary: str
    confidence: float
    model_name: str
    prompt_version: str
    enrichment_method: str
    processed_at: str
    raw_response: str


def _fallback_enrichment(message_text: str) -> TicketEnrichment:
    message_lower = message_text.lower()
    issue_category = "general_support"
    product_area = "dashboard"
    urgency = "medium"
    sentiment = "neutral"

    if "charged" in message_lower or "refund" in message_lower or "billing" in message_lower:
        issue_category = "billing_issue"
        product_area = "billing"
    elif "sso" in message_lower or "login" in message_lower or "password" in message_lower:
        issue_category = "authentication_issue"
        product_area = "authentication"
    elif "api" in message_lower or "webhook" in message_lower:
        issue_category = "api_issue"
        product_area = "api"
    elif "slack" in message_lower or "integration" in message_lower:
        issue_category = "integration_issue"
        product_area = "integrations"

    if "critical" in message_lower or "failing across multiple tenants" in message_lower:
        urgency = "critical"
    elif "all" in message_lower or "double charged" in message_lower or "failing" in message_lower:
        urgency = "high"
    elif "slow" in message_lower or "delayed" in message_lower:
        urgency = "medium"
    else:
        urgency = "low"

    if "thanked" in message_lower or "confirmed" in message_lower:
        sentiment = "positive"
    elif "failing" in message_lower or "double charged" in message_lower or "stopped" in message_lower:
        sentiment = "negative"

    summary = message_text[:120]
    raw_response = json.dumps(
        {
            "issue_category": issue_category,
            "sentiment": sentiment,
            "urgency": urgency,
            "product_area": product_area,
            "summary": summary,
            "confidence": 0.72,
        }
    )
    return TicketEnrichment(
        issue_category=issue_category,
        sentiment=sentiment,
        urgency=urgency,
        product_area=product_area,
        summary=summary,
        confidence=0.72,
        model_name="fallback-keyword-classifier",
        prompt_version="v1",
        enrichment_method="fallback",
        processed_at=datetime.now(timezone.utc).isoformat(),
        raw_response=raw_response,
    )


def _validate_llm_payload(payload: dict[str, Any]) -> None:
    if payload["urgency"] not in ALLOWED_URGENCY:
        raise ValueError(f"Invalid urgency: {payload['urgency']}")
    if payload["sentiment"] not in ALLOWED_SENTIMENT:
        raise ValueError(f"Invalid sentiment: {payload['sentiment']}")
    if payload["product_area"] not in ALLOWED_PRODUCT_AREAS:
        raise ValueError(f"Invalid product_area: {payload['product_area']}")


def enrich_ticket(message_text: str, prompt_version: str = "v1") -> TicketEnrichment:
    api_key = os.environ.get("OPENAI_API_KEY")
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    if not api_key or OpenAI is None:
        return _fallback_enrichment(message_text)

    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model=model_name,
        input=[
            {
                "role": "system",
                "content": (
                    "You classify support tickets into structured JSON. "
                    "Return keys: issue_category, sentiment, urgency, product_area, summary, confidence."
                ),
            },
            {"role": "user", "content": message_text},
        ],
    )
    content = response.output_text
    payload = json.loads(content)
    _validate_llm_payload(payload)
    return TicketEnrichment(
        issue_category=payload["issue_category"],
        sentiment=payload["sentiment"],
        urgency=payload["urgency"],
        product_area=payload["product_area"],
        summary=payload["summary"],
        confidence=float(payload["confidence"]),
        model_name=model_name,
        prompt_version=prompt_version,
        enrichment_method="llm_api",
        processed_at=datetime.now(timezone.utc).isoformat(),
        raw_response=content,
    )


def enrichment_to_dict(message_text: str) -> dict[str, Any]:
    return asdict(enrich_ticket(message_text=message_text))
