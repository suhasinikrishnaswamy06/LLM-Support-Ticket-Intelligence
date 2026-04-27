# LLM-Driven Support Ticket Intelligence Pipeline

This project is a portfolio-ready data pipeline that ingests support tickets, enriches unstructured messages into structured support signals, and models analytics-ready outputs for support trend analysis.

## Stack

- Airflow
- Python
- BigQuery
- dbt
- LLM API

## MVP Scope

- generate mock Slack-style support tickets
- optionally ingest real support messages from Slack channels
- enrich ticket text with issue category, sentiment, urgency, product area, summary, and confidence
- persist raw and enriched records for warehouse loading
- capture failed enrichments for auditability and replay
- model analytics-ready support marts in dbt
- orchestrate the workflow in Airflow

## Project Layout

- `airflow/dags/`: orchestration DAG
- `data/raw/`: generated raw support tickets
- `data/processed/`: enrichment outputs, failure outputs, and run summaries
- `dbt/support_intelligence/`: staging and mart models
- `src/data/`: mock ticket generation
- `src/enrichment/`: LLM and fallback enrichment logic
- `src/orchestration/`: pipeline helper functions

## First Steps

1. Create a Python 3.11 virtual environment
2. Copy `.env.example` to `.env` and fill in your BigQuery and OpenAI settings
3. Generate or ingest tickets:

```powershell
python -m src.data.generate_sample_tickets
```

4. Run enrichment locally:

```powershell
python -c "from src.orchestration.pipeline import enrich_support_tickets; enrich_support_tickets()"
```

5. Configure a dbt profile and run:

```powershell
dbt build --project-dir dbt/support_intelligence --profiles-dir dbt_profiles
```

## Slack Ingestion

The pipeline supports three source modes through `SUPPORT_INTEL_SOURCE_MODE`:

- `mock`: always generate synthetic support tickets
- `slack`: require Slack API ingestion
- `auto`: try Slack first, then fall back to mock data if credentials are missing or ingestion fails

To use Slack ingestion, set these environment variables:

- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_IDS` as a comma-separated list like `C12345678,C87654321`
- optional `SLACK_LOOKBACK_DAYS`
- optional `SLACK_MAX_MESSAGES_PER_CHANNEL`

Then run the normal pipeline entry point:

```powershell
python -c "from src.orchestration.pipeline import generate_source_data; generate_source_data()"
```

## Reliability Features

- deterministic fallback enrichment when no API key is available
- validation for required keys, accepted values, non-empty summary, and confidence range
- automatic retry handling for LLM API enrichment attempts
- failed-record capture in `data/processed/ticket_enrichment_failures.csv`
- replay utility for failed rows with summary output in `data/processed/ticket_enrichment_replay_summary.json`
- run-level audit summary in `data/processed/ticket_enrichment_run_summary.json`
- model and prompt version tracking in enrichment outputs
- dbt failure-monitoring mart for failure counts by day, type, channel, model, and prompt version

## Replay Failed Rows

Replay all currently failed rows:

```powershell
python -m src.orchestration.replay_failed_rows
```

Replay only the first 25 failed rows:

```powershell
python -m src.orchestration.replay_failed_rows --limit 25
```

## Airflow Install

Install Airflow separately with the official constraints file on Python 3.11:

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install "apache-airflow==2.10.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.11.txt"
```

## Next Steps

- add replay automation for failed rows
- add downstream marts for support SLA and incident trend reporting
- optionally switch from mock data to Slack API ingestion
