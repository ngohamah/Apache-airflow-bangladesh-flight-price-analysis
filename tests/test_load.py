from unittest.mock import MagicMock

import pytest

from plugins.tasks.load import delete_then_insert, tag_batch


def test_tag_batch_adds_batch_id_as_first_column():
    records = [{"airline": "A", "avg_total_fare": 100.0}, {"airline": "B", "avg_total_fare": 200.0}]

    df = tag_batch(records, batch_id="batch-123")

    assert list(df.columns) == ["batch_id", "airline", "avg_total_fare"]
    assert (df["batch_id"] == "batch-123").all()


def test_delete_then_insert_deletes_then_inserts_and_commits():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    df = tag_batch([{"airline": "A", "booking_count": 5}], batch_id="batch-123")

    inserted = delete_then_insert(connection, "kpi_bookings_by_airline", "batch-123", df)

    assert inserted == 1
    delete_call, insert_call = cursor.execute.call_args_list[0], cursor.executemany.call_args_list[0]
    assert delete_call.args == (
        "DELETE FROM kpi_bookings_by_airline WHERE batch_id = %s",
        ("batch-123",),
    )
    assert insert_call.args[0].startswith("INSERT INTO kpi_bookings_by_airline")
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_delete_then_insert_on_empty_df_still_deletes_but_skips_insert():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    df = tag_batch([], batch_id="batch-123")
    df["dummy"] = []  # give it at least one column to build an INSERT statement from

    inserted = delete_then_insert(connection, "kpi_top_routes", "batch-123", df)

    assert inserted == 0
    cursor.execute.assert_called_once()
    cursor.executemany.assert_not_called()
    connection.commit.assert_called_once()


def test_delete_then_insert_rolls_back_on_failure():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.executemany.side_effect = RuntimeError("boom")
    df = tag_batch([{"airline": "A", "booking_count": 5}], batch_id="batch-123")

    with pytest.raises(RuntimeError):
        delete_then_insert(connection, "kpi_bookings_by_airline", "batch-123", df)

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
