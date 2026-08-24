from unittest.mock import MagicMock

import pandas as pd

from plugins.tasks.validate import (
    coerce_numeric,
    find_rejection_reasons,
    insert_rejected,
    split_valid_invalid,
)


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "batch_id": ["b1"] * 4,
            "airline": ["Biman", None, "Novoair", "US-Bangla"],
            "source": ["DAC", "CXB", "dac1", "DAC"],
            "destination": ["CXB", "DAC", "DAC", "DAC"],  # row 3: same as source
            "base_fare": [5000.0, 4000.0, 3000.0, -100.0],
            "tax_surcharge": [500.0, 400.0, 300.0, 100.0],
            "total_fare": [5500.0, 4400.0, 3300.0, 0.0],
            "seasonality": ["Regular", "Regular", "Eid", "Regular"],
        }
    )


def test_coerce_numeric_turns_unparseable_values_into_nan():
    df = pd.DataFrame(
        {"base_fare": ["100", "not-a-number"], "tax_surcharge": [1, 2], "total_fare": [1, 2]}
    )

    coerced = coerce_numeric(df)

    assert coerced["base_fare"].tolist()[0] == 100.0
    assert pd.isna(coerced["base_fare"].tolist()[1])


def test_find_rejection_reasons_flags_missing_negative_and_bad_codes():
    reasons = find_rejection_reasons(_sample_df())

    assert reasons.tolist() == [
        "",
        "missing_airline",
        "invalid_source_code",
        "negative_base_fare",
    ]


def test_split_valid_invalid_separates_clean_from_rejected():
    clean, rejected = split_valid_invalid(_sample_df())

    assert len(clean) == 1
    assert clean.iloc[0]["airline"] == "Biman"
    assert len(rejected) == 3
    assert set(rejected["rejection_reason"]) == {
        "missing_airline",
        "invalid_source_code",
        "negative_base_fare",
    }


def test_split_valid_invalid_on_all_clean_rows_returns_empty_rejected():
    df = _sample_df().iloc[[0]]

    clean, rejected = split_valid_invalid(df)

    assert len(clean) == 1
    assert rejected.empty


def test_insert_rejected_serializes_rows_as_json_and_skips_empty_input():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    _, rejected = split_valid_invalid(_sample_df())

    inserted = insert_rejected(connection, rejected, table_name="flights_rejected")

    assert inserted == 3
    cursor.executemany.assert_called_once()
    connection.commit.assert_called_once()

    empty_connection = MagicMock()
    assert insert_rejected(empty_connection, rejected.iloc[0:0]) == 0
    empty_connection.cursor.assert_not_called()
