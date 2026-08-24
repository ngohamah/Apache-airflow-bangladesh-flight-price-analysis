from unittest.mock import MagicMock

from plugins.tasks.schema import apply_ddl, split_statements


def test_split_statements_strips_and_filters_empty():
    sql_text = "CREATE TABLE a (id INT);\n\n  ;\nCREATE TABLE b (id INT);"

    assert split_statements(sql_text) == [
        "CREATE TABLE a (id INT)",
        "CREATE TABLE b (id INT)",
    ]


def test_apply_ddl_executes_each_statement_and_commits():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    sql_text = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);"

    count = apply_ddl(connection, sql_text)

    assert count == 2
    assert cursor.execute.call_count == 2
    connection.commit.assert_called_once()
    cursor.close.assert_called_once()
