from unittest.mock import MagicMock

from plugins.common.sql_io import delete_rows_by_id, fetch_batch


def test_fetch_batch_builds_dataframe_from_cursor_description():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.description = [("id",), ("batch_id",), ("airline",)]
    cursor.fetchall.return_value = [(1, "b1", "Biman"), (2, "b1", "Novoair")]

    df = fetch_batch(connection, "flights_raw", batch_id="b1")

    cursor.execute.assert_called_once_with(
        "SELECT * FROM flights_raw WHERE batch_id = %s", ("b1",)
    )
    assert list(df.columns) == ["id", "batch_id", "airline"]
    assert df.iloc[1]["airline"] == "Novoair"


def test_delete_rows_by_id_executes_delete_with_placeholders():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.rowcount = 2

    deleted = delete_rows_by_id(connection, "flights_raw", [10, 11])

    cursor.execute.assert_called_once_with(
        "DELETE FROM flights_raw WHERE id IN (%s, %s)", (10, 11)
    )
    connection.commit.assert_called_once()
    assert deleted == 2


def test_delete_rows_by_id_is_noop_for_empty_ids():
    connection = MagicMock()

    assert delete_rows_by_id(connection, "flights_raw", []) == 0
    connection.cursor.assert_not_called()
