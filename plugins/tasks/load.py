"""Idempotent Postgres load: delete-then-insert per batch_id for the analytics tables.

Pure functions apart from delete_then_insert (which, like other task modules,
takes a DB-API 2.0 connection as a parameter) — no Airflow imports.
"""

from __future__ import annotations

import pandas as pd


def tag_batch(records: list[dict], batch_id: str) -> pd.DataFrame:
    """Convert a list of dict records (e.g. an upstream task's XCom return value)
    into a DataFrame tagged with batch_id as its first column.
    """
    df = pd.DataFrame(records)
    df.insert(0, "batch_id", batch_id)
    return df


def delete_then_insert(connection, table_name: str, batch_id: str, df: pd.DataFrame) -> int:
    """Delete any existing rows for batch_id in table_name, then bulk-insert df.

    Runs as a single transaction, so a rerun of the same batch_id atomically
    replaces its previous rows rather than duplicating them.
    """
    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    rows = list(df.astype(object).where(df.notna(), None).itertuples(index=False, name=None))

    cursor = connection.cursor()
    try:
        cursor.execute(f"DELETE FROM {table_name} WHERE batch_id = %s", (batch_id,))
        if rows:
            cursor.executemany(insert_sql, rows)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
    return len(rows)
