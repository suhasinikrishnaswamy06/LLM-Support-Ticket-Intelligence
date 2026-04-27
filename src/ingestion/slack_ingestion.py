from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
SLACK_API_BASE = "https://slack.com/api"


class SlackIngestionError(Exception):
    """Raised when Slack ticket ingestion cannot complete safely."""


def _env_list(name: str) -> list[str]:
    raw_value = os.environ.get(name, "")
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _slack_get(endpoint: str, token: str, params: dict[str, Any]) -> dict[str, Any]:
    query = urlencode(params)
    request = Request(f"{SLACK_API_BASE}/{endpoint}?{query}")
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:  # pragma: no cover - depends on network/Slack
        raise SlackIngestionError(f"Slack API HTTP error {exc.code} for {endpoint}") from exc
    except URLError as exc:  # pragma: no cover - depends on network/Slack
        raise SlackIngestionError(f"Slack API network error for {endpoint}: {exc.reason}") from exc

    if not payload.get("ok"):
        raise SlackIngestionError(f"Slack API error for {endpoint}: {payload.get('error', 'unknown_error')}")
    return payload


def _fetch_channel_name(channel_id: str, token: str) -> str:
    payload = _slack_get("conversations.info", token, {"channel": channel_id})
    return payload["channel"].get("name", channel_id)


def _extract_customer_name(message_text: str) -> str:
    if message_text.startswith("[") and "]" in message_text:
        return message_text[1 : message_text.index("]")]
    return "unknown_customer"


def fetch_slack_support_tickets(
    token: str,
    channel_ids: list[str],
    lookback_days: int = 7,
    max_messages_per_channel: int = 200,
) -> pd.DataFrame:
    if not token:
        raise SlackIngestionError("SLACK_BOT_TOKEN is required for Slack ingestion")
    if not channel_ids:
        raise SlackIngestionError("SLACK_CHANNEL_IDS must contain at least one channel id")

    oldest_ts = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp()
    rows: list[dict[str, object]] = []

    for channel_id in channel_ids:
        channel_name = _fetch_channel_name(channel_id, token)
        cursor: str | None = None
        fetched = 0

        while fetched < max_messages_per_channel:
            batch_limit = min(200, max_messages_per_channel - fetched)
            params: dict[str, Any] = {
                "channel": channel_id,
                "oldest": oldest_ts,
                "limit": batch_limit,
            }
            if cursor:
                params["cursor"] = cursor

            payload = _slack_get("conversations.history", token, params)
            messages = payload.get("messages", [])
            for message in messages:
                message_text = str(message.get("text", "")).strip()
                if not message_text:
                    continue

                message_ts = str(message.get("ts", ""))
                thread_ts = str(message.get("thread_ts") or message_ts)
                created_at = datetime.fromtimestamp(float(message_ts), tz=timezone.utc).isoformat()
                rows.append(
                    {
                        "ticket_id": f"SLACK-{channel_id}-{message_ts.replace('.', '')}",
                        "thread_id": thread_ts.replace(".", ""),
                        "slack_channel": channel_name,
                        "slack_channel_id": channel_id,
                        "slack_user_id": str(message.get("user", "unknown_user")),
                        "customer_name": _extract_customer_name(message_text),
                        "created_at": created_at,
                        "message_text": message_text,
                        "source_system": "slack_api",
                    }
                )

            fetched += len(messages)
            cursor = payload.get("response_metadata", {}).get("next_cursor") or None
            if not cursor or not messages:
                break

    if not rows:
        raise SlackIngestionError("Slack ingestion returned no messages for the configured channels")

    dataframe = pd.DataFrame(rows)
    dataframe = dataframe.sort_values(by="created_at").reset_index(drop=True)
    return dataframe


def write_slack_support_tickets(dataframe: pd.DataFrame) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / "support_tickets.csv"
    dataframe.to_csv(output_path, index=False)
    return output_path


def ingest_slack_support_tickets_from_env() -> Path:
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel_ids = _env_list("SLACK_CHANNEL_IDS")
    lookback_days = int(os.environ.get("SLACK_LOOKBACK_DAYS", "7"))
    max_messages_per_channel = int(os.environ.get("SLACK_MAX_MESSAGES_PER_CHANNEL", "200"))

    dataframe = fetch_slack_support_tickets(
        token=token,
        channel_ids=channel_ids,
        lookback_days=lookback_days,
        max_messages_per_channel=max_messages_per_channel,
    )
    return write_slack_support_tickets(dataframe)
