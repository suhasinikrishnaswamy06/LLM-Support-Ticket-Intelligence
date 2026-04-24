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
3. Generate mock tickets:

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

## Reliability Features

- deterministic fallback enrichment when no API key is available
- validation for required keys, accepted values, non-empty summary, and confidence range
- automatic retry handling for LLM API enrichment attempts
- failed-record capture in `data/processed/ticket_enrichment_failures.csv`
- run-level audit summary in `data/processed/ticket_enrichment_run_summary.json`
- model and prompt version tracking in enrichment outputs

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
