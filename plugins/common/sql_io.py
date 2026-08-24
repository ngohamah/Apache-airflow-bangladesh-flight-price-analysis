"""Generic DB-API 2.0 helpers shared across task modules.

No Airflow imports — works with any connection (mysql-connector, psycopg2,
sqlite3), so callers stay testable with a mock or an in-memory DB.
"""

from __future__ import annotations

import pandas as pd


def fetch_batch(connection, table_name: str, batch_id: str) -> pd.DataFrame:
    """Read every row for batch_id from table_name into a DataFrame."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table_name} WHERE batch_id = %s", (batch_id,))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return pd.DataFrame(rows, columns=columns)


def delete_rows_by_id(connection, table_name: str, ids: list) -> int:
    """Delete rows from table_name whose id is in ids. No-op if ids is empty."""
    if not ids:
        return 0
    cursor = connection.cursor()
    try:
        placeholders = ", ".join(["%s"] * len(ids))
        cursor.execute(f"DELETE FROM {table_name} WHERE id IN ({placeholders})", tuple(ids))
        connection.commit()
        deleted = cursor.rowcount
    finally:
        cursor.close()
    return deleted
