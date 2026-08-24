"""Central configuration: paths, table names, and fixed pipeline constants.

Nothing in this module changes across pipeline runs — per-run values (batch_id,
execution date) are passed through Airflow's context instead.
"""

from __future__ import annotations

from pathlib import Path

# --- Filesystem paths --------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "include" / "data"
SQL_DIR = BASE_DIR / "include" / "sql"
LOG_DIR = BASE_DIR / "logs"
FIGURES_DIR = BASE_DIR / "docs" / "figures"

CSV_FILE = DATA_DIR / "Flight_Price_Dataset_of_Bangladesh.csv"
LOG_FILE = LOG_DIR / "pipeline.log"

# --- Airflow connection IDs ---------------------------------------------------
MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

# --- MySQL staging tables ------------------------------------------------------
STAGING_RAW_TABLE = "flights_raw"
STAGING_REJECTED_TABLE = "flights_rejected"

# --- PostgreSQL analytics tables -----------------------------------------------
ANALYTICS_CLEAN_TABLE = "flights_clean"
KPI_AVG_FARE_BY_AIRLINE_TABLE = "kpi_avg_fare_by_airline"
KPI_SEASONAL_VARIATION_TABLE = "kpi_seasonal_fare_variation"
KPI_BOOKINGS_BY_AIRLINE_TABLE = "kpi_bookings_by_airline"
KPI_TOP_ROUTES_TABLE = "kpi_top_routes"

# --- Source schema (validation) -------------------------------------------------
REQUIRED_COLUMNS = (
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
    "Seasonality",
)

REGULAR_SEASON_LABEL = "Regular"

# --- KPI / transform settings ----------------------------------------------------
TOP_ROUTES_LIMIT = 10
FARE_TOLERANCE_BDT = 0.01  # allowed rounding drift when checking Total == Base + Tax
