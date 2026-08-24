"""Flight Price Analysis pipeline: CSV -> MySQL staging -> validate/transform -> Postgres analytics."""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from plugins.common.config import SQL_DIR
from plugins.common.db import get_mysql_connection
from plugins.common.logger import get_logger
from plugins.tasks.schema import apply_ddl

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

    create_staging_tables()


flight_price_pipeline()
