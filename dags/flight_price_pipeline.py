"""Flight Price Analysis pipeline: CSV -> MySQL staging -> validate/transform -> Postgres."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from plugins.common.config import (
    ANALYTICS_CLEAN_TABLE,
    CSV_FILE,
    KPI_AVG_FARE_BY_AIRLINE_TABLE,
    KPI_BOOKINGS_BY_AIRLINE_TABLE,
    KPI_SEASONAL_VARIATION_TABLE,
    KPI_TOP_ROUTES_TABLE,
    SQL_DIR,
    STAGING_RAW_TABLE,
    STAGING_REJECTED_TABLE,
)
from plugins.common.db import get_mysql_connection, get_postgres_connection
from plugins.common.logger import get_logger
from plugins.common.sql_io import delete_rows_by_id, fetch_batch
from plugins.tasks.ingest import STAGING_COLUMNS, bulk_insert, prepare_for_staging, read_csv
from plugins.tasks.load import delete_then_insert, tag_batch
from plugins.tasks.schema import apply_ddl
from plugins.tasks.transform import (
    compute_avg_fare_by_airline,
    compute_bookings_by_airline,
    compute_seasonal_fare_variation,
    compute_top_routes,
    fill_total_fare,
    update_total_fare,
)
from plugins.tasks.validate import insert_rejected, split_valid_invalid

logger = get_logger(__name__)

# Transient DB-connection hiccups are worth retrying; a real validation/transform
# bug should surface immediately instead of being retried away silently.
DB_IO_RETRY_KWARGS = {"retries": 2, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="flight_price_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["flight-price"],
)
def flight_price_pipeline():
    @task()
    def create_staging_tables() -> None:
        sql_text = (SQL_DIR / "ddl_staging.sql").read_text()
        connection = get_mysql_connection()
        try:
            statement_count = apply_ddl(connection, sql_text)
            logger.info("Applied %d DDL statement(s) to MySQL staging", statement_count)
        except Exception:
            logger.exception("Failed applying staging DDL")
            raise
        finally:
            connection.close()

    @task(**DB_IO_RETRY_KWARGS)
    def ingest_csv_to_mysql(run_id: str) -> None:
        connection = get_mysql_connection()
        try:
            df = read_csv(CSV_FILE)
            logger.info("Read %d rows from %s", len(df), CSV_FILE)
            prepared = prepare_for_staging(df, batch_id=run_id)
            inserted = bulk_insert(connection, prepared, table_name=STAGING_RAW_TABLE)
            logger.info(
                "Inserted %d rows into %s (batch_id=%s)", inserted, STAGING_RAW_TABLE, run_id
            )
        except Exception:
            logger.exception("Failed ingesting CSV into MySQL staging")
            raise
        finally:
            connection.close()

    @task()
    def validate_and_quarantine(run_id: str) -> None:
        connection = get_mysql_connection()
        try:
            df = fetch_batch(connection, STAGING_RAW_TABLE, batch_id=run_id)
            clean, rejected = split_valid_invalid(df)
            logger.info(
                "Validated %d rows for batch %s: %d clean, %d rejected",
                len(df),
                run_id,
                len(clean),
                len(rejected),
            )
            if not rejected.empty:
                insert_rejected(connection, rejected, table_name=STAGING_REJECTED_TABLE)
                delete_rows_by_id(connection, STAGING_RAW_TABLE, rejected["id"].tolist())
                logger.info(
                    "Quarantined %d row(s) into %s (reasons: %s)",
                    len(rejected),
                    STAGING_REJECTED_TABLE,
                    rejected["rejection_reason"].value_counts().to_dict(),
                )
        except Exception:
            logger.exception("Failed validating/quarantining batch %s", run_id)
            raise
        finally:
            connection.close()

    @task()
    def transform_add_total_fare(run_id: str) -> None:
        connection = get_mysql_connection()
        try:
            df = fetch_batch(connection, STAGING_RAW_TABLE, batch_id=run_id)
            transformed = fill_total_fare(df)
            to_update = transformed.loc[transformed["total_fare_corrected"], ["id", "total_fare"]]
            if not to_update.empty:
                update_total_fare(connection, STAGING_RAW_TABLE, to_update)
                logger.info(
                    "Recomputed total_fare for %d row(s) in batch %s", len(to_update), run_id
                )
            else:
                logger.info("No total_fare corrections needed for batch %s", run_id)
        except Exception:
            logger.exception("Failed transforming batch %s", run_id)
            raise
        finally:
            connection.close()

    @task()
    def kpi_avg_fare_by_airline(run_id: str) -> list[dict]:
        connection = get_mysql_connection()
        try:
            df = fetch_batch(connection, STAGING_RAW_TABLE, batch_id=run_id)
            result = compute_avg_fare_by_airline(df)
            logger.info("Computed avg fare by airline: %d airline(s)", len(result))
            return result.to_dict("records")
        except Exception:
            logger.exception("Failed computing avg fare by airline for batch %s", run_id)
            raise
        finally:
            connection.close()

    @task()
    def kpi_seasonal_variation(run_id: str) -> list[dict]:
        connection = get_mysql_connection()
        try:
            df = fetch_batch(connection, STAGING_RAW_TABLE, batch_id=run_id)
            result = compute_seasonal_fare_variation(df)
            logger.info("Computed seasonal fare variation: %d season(s)", len(result))
            return result.to_dict("records")
        except Exception:
            logger.exception("Failed computing seasonal fare variation for batch %s", run_id)
            raise
        finally:
            connection.close()

    @task()
    def kpi_bookings_by_airline(run_id: str) -> list[dict]:
        connection = get_mysql_connection()
        try:
            df = fetch_batch(connection, STAGING_RAW_TABLE, batch_id=run_id)
            result = compute_bookings_by_airline(df)
            logger.info("Computed bookings by airline: %d airline(s)", len(result))
            return result.to_dict("records")
        except Exception:
            logger.exception("Failed computing bookings by airline for batch %s", run_id)
            raise
        finally:
            connection.close()

    @task()
    def kpi_top_routes(run_id: str) -> list[dict]:
        connection = get_mysql_connection()
        try:
            df = fetch_batch(connection, STAGING_RAW_TABLE, batch_id=run_id)
            result = compute_top_routes(df)
            logger.info("Computed top routes: %d route(s)", len(result))
            return result.to_dict("records")
        except Exception:
            logger.exception("Failed computing top routes for batch %s", run_id)
            raise
        finally:
            connection.close()

    @task(**DB_IO_RETRY_KWARGS)
    def load_to_postgres(
        avg_fare: list[dict],
        seasonal: list[dict],
        bookings: list[dict],
        top_routes: list[dict],
        run_id: str,
    ) -> None:
        mysql_connection = get_mysql_connection()
        postgres_connection = get_postgres_connection()
        try:
            clean_df = fetch_batch(mysql_connection, STAGING_RAW_TABLE, batch_id=run_id)
            clean_df = clean_df[["batch_id", *STAGING_COLUMNS]]

            loads = {
                ANALYTICS_CLEAN_TABLE: clean_df,
                KPI_AVG_FARE_BY_AIRLINE_TABLE: tag_batch(avg_fare, run_id),
                KPI_SEASONAL_VARIATION_TABLE: tag_batch(seasonal, run_id),
                KPI_BOOKINGS_BY_AIRLINE_TABLE: tag_batch(bookings, run_id),
                KPI_TOP_ROUTES_TABLE: tag_batch(top_routes, run_id),
            }
            for table_name, df in loads.items():
                inserted = delete_then_insert(postgres_connection, table_name, run_id, df)
                logger.info("Loaded %d row(s) into %s (batch_id=%s)", inserted, table_name, run_id)
        except Exception:
            logger.exception("Failed loading batch %s into Postgres", run_id)
            raise
        finally:
            mysql_connection.close()
            postgres_connection.close()

    staging = create_staging_tables()
    ingest = ingest_csv_to_mysql()
    validate = validate_and_quarantine()
    transform = transform_add_total_fare()

    avg_fare = kpi_avg_fare_by_airline()
    seasonal = kpi_seasonal_variation()
    bookings = kpi_bookings_by_airline()
    top_routes = kpi_top_routes()

    load = load_to_postgres(
        avg_fare=avg_fare, seasonal=seasonal, bookings=bookings, top_routes=top_routes
    )

    staging >> ingest >> validate >> transform >> [avg_fare, seasonal, bookings, top_routes] >> load


flight_price_pipeline()
