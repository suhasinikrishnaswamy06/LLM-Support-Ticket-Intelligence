# Local Demo

The local demo proves the project can run without Slack, OpenAI, BigQuery, Airflow, or Docker credentials. It generates mock support tickets, enriches them with the deterministic fallback classifier, and writes the same processed files that the warehouse workflow expects.

## Run On Windows

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Then run:

```powershell
.\scripts\run_local_demo.ps1
```

If PowerShell blocks local scripts on your machine:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_local_demo.ps1
```

The script uses `.venv\Scripts\python.exe` when a local virtual environment exists. Otherwise, it falls back to `python`.

Generate a smaller demo set:

```powershell
.\scripts\run_local_demo.ps1 -TicketCount 25
```

## Run With Python Directly

```powershell
python -m src.orchestration.local_demo
```

Or choose the ticket count directly:

```powershell
python -m src.orchestration.local_demo --ticket-count 25
```

## Expected Output Files

| File | Purpose |
| --- | --- |
| `data/raw/support_tickets.csv` | Generated mock ticket source data |
| `data/processed/ticket_enrichments.csv` | Structured support-ticket enrichment output |
| `data/processed/ticket_enrichment_failures.csv` | Failed enrichment rows, if any |
| `data/processed/ticket_enrichment_run_summary.json` | Row counts, success rate, model name, and prompt version |

`data/processed/` is gitignored because it is generated output.

## Expected Terminal Summary

A successful no-credentials run prints a JSON summary similar to:

```json
{
  "source_ticket_count": 80,
  "successful_enrichment_count": 80,
  "failed_enrichment_count": 0,
  "success_rate": 1.0,
  "enrichment_methods": ["fallback"],
  "failure_rows": 0
}
```

## Use Real OpenAI Credentials

The demo intentionally clears `OPENAI_API_KEY` by default so it never makes network calls during a simple portfolio review.

To test the currently configured OpenAI environment instead, first install the cloud dependency set:

```powershell
pip install -r requirements-cloud.txt
```

Then run:

```powershell
.\scripts\run_local_demo.ps1 -UseCurrentOpenAI
```
