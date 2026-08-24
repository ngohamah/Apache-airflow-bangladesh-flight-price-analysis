"""Row-level data validation + quarantine logic.

Pure functions apart from insert_rejected (which, like ingest.bulk_insert, takes
a DB-API 2.0 connection as a parameter) — no Airflow imports, so these are
testable with a mock or sqlite3 connection.
"""

from __future__ import annotations

import json
import re

import pandas as pd

from plugins.common.config import STAGING_REJECTED_TABLE

NUMERIC_FARE_COLUMNS = ("base_fare", "tax_surcharge", "total_fare")
REQUIRED_NON_NULL_COLUMNS = (
    "airline",
    "source",
    "destination",
    "base_fare",
    "tax_surcharge",
    "total_fare",
    "seasonality",
)
AIRPORT_CODE_COLUMNS = ("source", "destination")
AIRPORT_CODE_PATTERN = re.compile(r"^[A-Z]{3}$")


def coerce_numeric(df: pd.DataFrame, columns=NUMERIC_FARE_COLUMNS) -> pd.DataFrame:
    """Force fare columns to numeric, turning anything unparseable into NaN."""
    coerced = df.copy()
    for column in columns:
        coerced[column] = pd.to_numeric(coerced[column], errors="coerce")
    return coerced


def _is_blank(series: pd.Series) -> pd.Series:
    return series.isna() | (series.astype(str).str.strip() == "")


def find_rejection_reasons(df: pd.DataFrame) -> pd.Series:
    """Return, per row, a comma-joined string of failed checks (empty = valid row)."""
    checks: dict[str, pd.Series] = {}

    for column in REQUIRED_NON_NULL_COLUMNS:
        checks[f"missing_{column}"] = _is_blank(df[column])

    for column in NUMERIC_FARE_COLUMNS:
        checks[f"negative_{column}"] = df[column].fillna(0) < 0

    for column in AIRPORT_CODE_COLUMNS:
        not_blank = ~_is_blank(df[column])
        matches_code = df[column].astype(str).str.match(AIRPORT_CODE_PATTERN)
        checks[f"invalid_{column}_code"] = not_blank & ~matches_code

    reason_df = pd.DataFrame(checks, index=df.index)
    return reason_df.apply(lambda row: ", ".join(row.index[row]), axis=1)


def split_valid_invalid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Coerce fare types, then split df into (clean, rejected_with_reason)."""
    coerced = coerce_numeric(df)
    reasons = find_rejection_reasons(coerced)
    is_invalid = reasons != ""

    rejected = coerced.loc[is_invalid].copy()
    rejected["rejection_reason"] = reasons.loc[is_invalid]

    clean = coerced.loc[~is_invalid].copy()
    return clean, rejected


def insert_rejected(
    connection, rejected: pd.DataFrame, table_name: str = STAGING_REJECTED_TABLE
) -> int:
    """Insert rejected rows (serialized as JSON) with their reasons into table_name."""
    if rejected.empty:
        return 0

    rows = [
        (
            row["batch_id"],
            json.dumps(row.drop(labels=["rejection_reason"]).to_dict(), default=str),
            row["rejection_reason"],
        )
        for _, row in rejected.iterrows()
    ]
    insert_sql = (
        f"INSERT INTO {table_name} (batch_id, raw_row, rejection_reason) VALUES (%s, %s, %s)"
    )

    cursor = connection.cursor()
    try:
        cursor.executemany(insert_sql, rows)
        connection.commit()
    finally:
        cursor.close()
    return len(rows)
