import math
from unittest.mock import MagicMock

import pandas as pd

from plugins.tasks.ingest import STAGING_COLUMNS, bulk_insert, prepare_for_staging


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Airline": ["Biman", "Novoair"],
            "Source": ["DAC", "CXB"],
            "Source Name": ["Dhaka Airport", "Cox's Bazar Airport"],
            "Destination": ["CXB", "DAC"],
            "Destination Name": ["Cox's Bazar Airport", "Dhaka Airport"],
            "Departure Date & Time": ["2025-01-01 06:00:00", "2025-01-02 07:00:00"],
            "Arrival Date & Time": ["2025-01-01 07:00:00", "2025-01-02 08:00:00"],
            "Duration (hrs)": [1.0, 1.0],
            "Stopovers": ["Direct", "Direct"],
            "Aircraft Type": ["ATR 72", "ATR 72"],
            "Class": ["Economy", "Economy"],
            "Booking Source": ["Online Website", "Travel Agency"],
            "Base Fare (BDT)": [5000.0, float("nan")],
            "Tax & Surcharge (BDT)": [500.0, 500.0],
            "Total Fare (BDT)": [5500.0, 5700.0],
            "Seasonality": ["Regular", "Eid"],
            "Days Before Departure": [10, 5],
        }
    )


def test_prepare_for_staging_renames_columns_and_adds_batch_id():
    prepared = prepare_for_staging(_sample_df(), batch_id="batch-123")

    assert list(prepared.columns) == ["batch_id"] + list(STAGING_COLUMNS)
    assert (prepared["batch_id"] == "batch-123").all()
    assert prepared.loc[0, "airline"] == "Biman"
    assert prepared.loc[1, "seasonality"] == "Eid"


def test_bulk_insert_executes_once_with_all_rows_and_converts_nan_to_none():
    prepared = prepare_for_staging(_sample_df(), batch_id="batch-123")
    connection = MagicMock()
    cursor = connection.cursor.return_value

    inserted = bulk_insert(connection, prepared, table_name="flights_raw")

    assert inserted == 2
    cursor.executemany.assert_called_once()
    insert_sql, rows = cursor.executemany.call_args.args
    assert insert_sql.startswith("INSERT INTO flights_raw")
    assert len(rows) == 2
    base_fare_index = list(prepared.columns).index("base_fare")
    assert rows[1][base_fare_index] is None  # NaN converted to NULL, not left as NaN
    assert not any(
        isinstance(value, float) and math.isnan(value) for row in rows for value in row
    )
    connection.commit.assert_called_once()
    cursor.close.assert_called_once()
