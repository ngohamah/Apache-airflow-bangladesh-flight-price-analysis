# Flight Price Analysis Pipeline

An Apache Airflow pipeline that ingests the Flight Price Dataset of Bangladesh (Kaggle, 57,000 bookings), validates and cleans it, computes four KPIs, and loads the results into PostgreSQL for analysis.

See [`docs/report.md`](docs/report.md) for the full write-up: architecture, KPI definitions, data quality findings, and the actual numbers produced by the pipeline.

## Architecture

```
Flight_Price_Dataset_of_Bangladesh.csv
        │
        ▼
   MySQL staging (flights_raw / flights_rejected)
        │
        ▼
   validate & quarantine bad rows
        │
        ▼
   recompute inconsistent Total Fare values
        │
        ├──► avg fare by airline ─┐
        ├──► seasonal variation ──┤
        ├──► bookings by airline ─┤
        └──► top 10 routes ───────┘
                                  ▼
                     PostgreSQL analytics DB
                (flights_clean + 4 KPI tables)
```

Orchestrated by the `flight_price_pipeline` DAG, running on Docker Compose: Airflow (webserver + scheduler), MySQL (staging), and a separate PostgreSQL instance (analytics).

## Prerequisites

- Docker + Docker Compose
- Python 3.11 or 3.12 (for local development/testing outside Docker)

## Setup

1. Place the dataset at `include/data/Flight_Price_Dataset_of_Bangladesh.csv` (gitignored — not committed; see `include/data/README.md`).
2. Copy the environment template and adjust if needed (the defaults work out of the box for local dev):
   ```
   cp .env.example .env
   ```
3. Build and start everything:
   ```
   docker compose up -d --build
   ```
4. Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (login: `admin` / `admin`) and trigger the `flight_price_pipeline` DAG — or run it from the CLI:
   ```
   docker compose exec airflow-scheduler airflow dags test flight_price_pipeline 2026-01-01
   ```

## Local Development

```
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pre-commit install
```

Run the test suite:
```
.venv/bin/pytest tests/
```

Lint and format:
```
.venv/bin/ruff check .
.venv/bin/black .
```

## Project Structure

```
dags/                   Airflow DAG definition
plugins/common/         Config, logging, DB connection helpers
plugins/tasks/          Pure, unit-testable task logic (ingest, validate, transform, load, report_plots)
include/sql/            DDL for the MySQL staging and Postgres analytics schemas
include/data/           Raw dataset (gitignored)
tests/                  Unit tests (pytest)
docs/                   Stakeholder report + generated KPI charts
.github/workflows/      CI (lint + test on push/PR)
```

## CI

Every push and pull request runs `ruff`, `black --check`, and the full `pytest` suite via `.github/workflows/ci.yml`.
