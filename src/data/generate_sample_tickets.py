from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import random

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"

CHANNELS = ["support-billing", "support-platform", "support-auth", "support-enterprise"]
PRODUCT_AREAS = ["billing", "authentication", "api", "dashboard", "integrations"]
CUSTOMERS = ["Acme Health", "Northstar Retail", "BrightPath AI", "Orbit Logistics", "Maple Bank"]
MESSAGES = [
    ("billing", "high", "negative", "Customer says they were double charged after upgrading seats."),
    ("authentication", "high", "negative", "SSO login is failing for all Okta users since this morning."),
    ("api", "medium", "neutral", "API requests are timing out intermittently on the create-ticket endpoint."),
    ("dashboard", "low", "neutral", "The dashboard export button is slow but eventually completes."),
    ("integrations", "medium", "negative", "Slack alerts stopped posting to the incident channel after reconnect."),
    ("billing", "low", "positive", "Customer confirmed the refund appeared and thanked the support team."),
    ("api", "critical", "negative", "Production webhook deliveries are failing across multiple tenants."),
    ("authentication", "medium", "negative", "Password reset emails are delayed by more than thirty minutes."),
]


def build_ticket_rows(n: int = 80) -> list[dict[str, object]]:
    random.seed(17)
    now = datetime.now(timezone.utc)
    rows: list[dict[str, object]] = []
    for idx in range(1, n + 1):
        product_area, expected_urgency, expected_sentiment, message = random.choice(MESSAGES)
        created_at = now - timedelta(hours=random.randint(2, 240), minutes=random.randint(0, 59))
        customer = random.choice(CUSTOMERS)
        ticket_id = f"TCKT-{idx:04d}"
        thread_id = f"thread-{random.randint(10, 40)}"
        rows.append(
            {
                "ticket_id": ticket_id,
                "thread_id": thread_id,
                "slack_channel": random.choice(CHANNELS),
                "customer_name": customer,
                "created_at": created_at.isoformat(),
                "message_text": f"[{customer}] {message}",
                "expected_product_area": product_area,
                "expected_urgency": expected_urgency,
                "expected_sentiment": expected_sentiment,
            }
        )
    return rows


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dataframe = pd.DataFrame(build_ticket_rows())
    output_path = RAW_DIR / "support_tickets.csv"
    dataframe.to_csv(output_path, index=False)
    print(f"Wrote {output_path} with {len(dataframe)} tickets")


if __name__ == "__main__":
    main()
