from unittest.mock import MagicMock

import pandas as pd

from plugins.tasks.transform import (
    compute_avg_fare_by_airline,
    compute_bookings_by_airline,
    compute_seasonal_fare_variation,
    compute_top_routes,
    fill_total_fare,
    update_total_fare,
)


def test_fill_total_fare_recomputes_missing_and_inconsistent_values_only():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "base_fare": [100.0, 100.0, 100.0, 100.0],
            "tax_surcharge": [20.0, 20.0, 20.0, 20.0],
            "total_fare": [float("nan"), 120.005, 200.0, 120.0],
        }
    )

    corrected = fill_total_fare(df, tolerance=0.01)

    assert corrected["total_fare"].tolist() == [120.0, 120.005, 120.0, 120.0]
    assert corrected["total_fare_corrected"].tolist() == [True, False, True, False]


def test_update_total_fare_batches_updates_and_skips_empty_input():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    updates = pd.DataFrame({"id": [1, 2], "total_fare": [120.0, 130.0]})

    updated = update_total_fare(connection, "flights_raw", updates)

    assert updated == 2
    cursor.executemany.assert_called_once_with(
        "UPDATE flights_raw SET total_fare = %s WHERE id = %s", [(120.0, 1), (130.0, 2)]
    )
    connection.commit.assert_called_once()

    empty_connection = MagicMock()
    assert update_total_fare(empty_connection, "flights_raw", updates.iloc[0:0]) == 0
    empty_connection.cursor.assert_not_called()


def _kpi_sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "airline": ["A", "A", "A", "B", "B", "B"],
            "source": ["DAC", "DAC", "DAC", "CXB", "CXB", "DAC"],
            "destination": ["CXB", "CXB", "CXB", "DAC", "DAC", "SPD"],
            "seasonality": ["Regular", "Eid", "Regular", "Regular", "Eid", "Regular"],
            "total_fare": [100.0, 200.0, 150.0, 300.0, 400.0, 500.0],
        }
    )


def test_compute_avg_fare_by_airline():
    result = compute_avg_fare_by_airline(_kpi_sample_df()).set_index("airline")["avg_total_fare"]

    assert result["A"] == 150.0
    assert result["B"] == 400.0


def test_compute_seasonal_fare_variation():
    result = compute_seasonal_fare_variation(_kpi_sample_df()).set_index("seasonality")

    assert result.loc["Regular", "avg_total_fare"] == 262.5
    assert result.loc["Regular", "booking_count"] == 4
    assert result.loc["Eid", "avg_total_fare"] == 300.0
    assert result.loc["Eid", "booking_count"] == 2


def test_compute_bookings_by_airline():
    result = compute_bookings_by_airline(_kpi_sample_df()).set_index("airline")["booking_count"]

    assert result["A"] == 3
    assert result["B"] == 3


def test_compute_top_routes_ranks_by_booking_count_descending():
    result = compute_top_routes(_kpi_sample_df(), limit=10)

    assert result[["source", "destination", "booking_count", "rank"]].values.tolist() == [
        ["DAC", "CXB", 3, 1],
        ["CXB", "DAC", 2, 2],
        ["DAC", "SPD", 1, 3],
    ]


def test_compute_top_routes_respects_limit():
    result = compute_top_routes(_kpi_sample_df(), limit=2)

    assert len(result) == 2
