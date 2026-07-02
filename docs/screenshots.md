# Screenshot Guide

Use this guide to capture portfolio-ready evidence after running the pipeline. Keep screenshots free of API keys, customer data, service-account paths, and billing details.

Save final images in `docs/images/` with these filenames so the README gallery can reference them consistently.

## Required Screenshots

| File | Capture | Why it matters |
| --- | --- | --- |
| `airflow-dag-success.png` | Airflow Graph or Grid view for `support_ticket_intelligence_pipeline` after a successful run | Shows orchestration and task dependencies |
| `bigquery-raw-tables.png` | BigQuery dataset containing `support_tickets`, `ticket_enrichments`, and `ticket_enrichment_failures` | Shows raw and enriched records landed in the warehouse |
| `dbt-build-success.png` | Terminal output for `dbt build --project-dir dbt/support_intelligence --profiles-dir dbt_profiles` | Shows transformations and tests completed successfully |
| `dbt-lineage.png` | dbt docs lineage for staging models, `fct_support_tickets`, and marts | Shows analytics engineering structure |
| `support-ticket-trends-mart.png` | Query preview for `mart_support_ticket_trends` | Shows the final business-facing analytics output |

## Optional Screenshots

| File | Capture | Why it matters |
| --- | --- | --- |
| `enrichment-run-summary.png` | `data/processed/ticket_enrichment_run_summary.json` or a terminal view of its key fields | Shows success rate, model name, and prompt version tracking |
| `failure-replay-summary.png` | Replay command output or `ticket_enrichment_replay_summary.json` | Shows recovery workflow for failed enrichments |
| `sample-enriched-records.png` | A filtered view of enriched tickets with category, urgency, sentiment, confidence, and method | Shows the LLM/fallback value in one glance |

## Capture Commands

Generate mock tickets:

```powershell
python -m src.data.generate_sample_tickets
```

Run enrichment locally:

```powershell
python -c "from src.orchestration.pipeline import enrich_support_tickets; enrich_support_tickets()"
```

Run dbt:

```powershell
dbt build --project-dir dbt/support_intelligence --profiles-dir dbt_profiles
```

Start Airflow with Docker:

```powershell
docker compose -f docker-compose.airflow.yml --env-file .env.airflow up --build
```

## README Gallery Snippet

After screenshots are captured, add or uncomment this gallery in `README.md`:

```markdown
## Screenshots

### Airflow DAG Success

![Airflow DAG Success](docs/images/airflow-dag-success.png)

### BigQuery Raw And Enriched Tables

![BigQuery Raw Tables](docs/images/bigquery-raw-tables.png)

### dbt Build Success

![dbt Build Success](docs/images/dbt-build-success.png)

### dbt Lineage

![dbt Lineage](docs/images/dbt-lineage.png)

### Support Ticket Trends Mart

![Support Ticket Trends Mart](docs/images/support-ticket-trends-mart.png)
```
