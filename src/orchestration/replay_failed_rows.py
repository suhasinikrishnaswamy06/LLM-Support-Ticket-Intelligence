from __future__ import annotations

import argparse
import json

from src.orchestration.pipeline import replay_failed_enrichments


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay failed support ticket enrichments.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of failed rows to replay in this run.",
    )
    args = parser.parse_args()
    result = replay_failed_enrichments(limit=args.limit)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
