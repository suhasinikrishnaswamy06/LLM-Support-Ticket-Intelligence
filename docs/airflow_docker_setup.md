# Airflow Docker Setup

This project includes a Docker-based Airflow runtime for Windows-friendly verification.

## Files

- `docker-compose.airflow.yml`
- `docker/airflow/Dockerfile`
- `.env.airflow.example`

## Setup

1. Copy `.env.airflow.example` to `.env.airflow`
2. Set a real GCP project id and service-account key path
3. Optionally set Slack credentials if you want live Slack ingestion

## Run

```powershell
docker compose -f docker-compose.airflow.yml --env-file .env.airflow up --build
```

## Notes

- The project code is mounted into the containers at `/opt/project`
- dbt profiles are mounted at `/opt/airflow/dbt_profiles`
- service-account keys are mounted from `HOST_GCP_KEY_DIR` to `/opt/airflow/keys`
- `LocalExecutor` is used for a lightweight local runtime
