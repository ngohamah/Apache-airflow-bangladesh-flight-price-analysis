"""CSV -> MySQL staging ingestion.

Pure functions only, aside from bulk_insert's use of a passed-in DB-API 2.0
connection — no Airflow imports, so these are testable with a plain mock or
sqlite3 connection.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from plugins.common.config import COLUMN_RENAME_MAP, STAGING_RAW_TABLE

STAGING_COLUMNS = tuple(COLUMN_RENAME_MAP.values())


def read_csv(csv_path: Path) -> pd.DataFrame:
    """Read the raw flight price CSV as-is (no cleaning — that's validate.py's job)."""
    return pd.read_csv(csv_path)


def prepare_for_staging(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Rename raw CSV columns to staging table columns and tag every row with batch_id."""
    prepared = df.rename(columns=COLUMN_RENAME_MAP)[list(STAGING_COLUMNS)].copy()
    prepared.insert(0, "batch_id", batch_id)
    return prepared


def bulk_insert(connection, df: pd.DataFrame, table_name: str = STAGING_RAW_TABLE) -> int:
    """Insert every row of df into table_name in a single batched round trip.

    Uses cursor.executemany rather than a per-row loop — one prepared statement,
    one network round trip for the whole batch.
    """
    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    clean_df = df.astype(object).where(df.notna(), None)
    rows = list(clean_df.itertuples(index=False, name=None))

    cursor = connection.cursor()
    try:
        cursor.executemany(insert_sql, rows)
        connection.commit()
    finally:
        cursor.close()
    return len(rows)
