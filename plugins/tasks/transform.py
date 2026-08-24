"""Total Fare backfill + KPI computation.

Pure functions apart from update_total_fare (which, like other task modules,
takes a DB-API 2.0 connection as a parameter) — no Airflow imports.
"""

from __future__ import annotations

import pandas as pd

from plugins.common.config import FARE_TOLERANCE_BDT, TOP_ROUTES_LIMIT


def fill_total_fare(df: pd.DataFrame, tolerance: float = FARE_TOLERANCE_BDT) -> pd.DataFrame:
    """Recompute total_fare wherever it's missing or inconsistent with
    base_fare + tax_surcharge beyond tolerance. Adds a 'total_fare_corrected'
    bool column marking which rows changed, for the caller to log/persist.
    """
    corrected = df.copy()
    expected = corrected["base_fare"] + corrected["tax_surcharge"]
    needs_fix = corrected["total_fare"].isna() | (
        (corrected["total_fare"] - expected).abs() > tolerance
    )
    corrected["total_fare"] = corrected["total_fare"].where(~needs_fix, expected)
    corrected["total_fare_corrected"] = needs_fix
    return corrected


def update_total_fare(connection, table_name: str, updates: pd.DataFrame) -> int:
    """Batch-update total_fare for the given rows (must have 'id' and 'total_fare')."""
    if updates.empty:
        return 0
    rows = list(updates[["total_fare", "id"]].itertuples(index=False, name=None))
    cursor = connection.cursor()
    try:
        cursor.executemany(f"UPDATE {table_name} SET total_fare = %s WHERE id = %s", rows)
        connection.commit()
    finally:
        cursor.close()
    return len(rows)


def compute_avg_fare_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """KPI: mean total fare grouped by airline."""
    return (
        df.groupby("airline", as_index=False)["total_fare"]
        .mean()
        .rename(columns={"total_fare": "avg_total_fare"})
    )


def compute_seasonal_fare_variation(df: pd.DataFrame) -> pd.DataFrame:
    """KPI: average fare and booking count grouped by seasonality (peak vs. regular)."""
    return df.groupby("seasonality", as_index=False)["total_fare"].agg(
        avg_total_fare="mean", booking_count="count"
    )


def compute_bookings_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """KPI: total number of bookings per airline."""
    return df.groupby("airline", as_index=False).size().rename(columns={"size": "booking_count"})


def compute_top_routes(df: pd.DataFrame, limit: int = TOP_ROUTES_LIMIT) -> pd.DataFrame:
    """KPI: top source-destination pairs ranked by booking count."""
    routes = (
        df.groupby(["source", "destination"], as_index=False)
        .size()
        .rename(columns={"size": "booking_count"})
        .sort_values("booking_count", ascending=False)
        .head(limit)
        .reset_index(drop=True)
    )
    routes["rank"] = routes.index + 1
    return routes
