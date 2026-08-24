"""Airflow Connection lookups + plain DB client factories.

Kept separate from plugins/tasks/: this module imports Airflow (via BaseHook), so
only DAG-assembly code should import it. Task modules take a plain DB connection
object as a parameter instead, which keeps them testable without Airflow installed.
"""

from __future__ import annotations

import mysql.connector
import psycopg2
from airflow.hooks.base import BaseHook

from plugins.common.config import MYSQL_CONN_ID, POSTGRES_CONN_ID


def get_mysql_connection(conn_id: str = MYSQL_CONN_ID):
    conn = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host=conn.host,
        port=conn.port or 3306,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
    )


def get_postgres_connection(conn_id: str = POSTGRES_CONN_ID):
    conn = BaseHook.get_connection(conn_id)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )
