"""Flight Price Analysis pipeline: CSV -> MySQL staging -> validate/transform -> Postgres analytics."""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from plugins.common.config import (
    CSV_FILE,
    SQL_DIR,
    STAGING_RAW_TABLE,
    STAGING_REJECTED_TABLE,
)
from plugins.common.db import get_mysql_connection
from plugins.common.logger import get_logger
from plugins.common.sql_io import delete_rows_by_id, fetch_batch
from plugins.tasks.ingest import bulk_insert, prepare_for_staging, read_csv
from plugins.tasks.schema import apply_ddl
from plugins.tasks.validate import insert_rejected, split_valid_invalid

logger = get_logger(__name__)


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

    @task()
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

    create_staging_tables() >> ingest_csv_to_mysql() >> validate_and_quarantine()


flight_price_pipeline()
