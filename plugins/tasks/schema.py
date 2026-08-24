"""Pure functions for applying SQL DDL scripts. No Airflow/DB-driver imports —
takes any DB-API 2.0 connection, so it's testable with a plain mock or sqlite3.
"""

from __future__ import annotations


def split_statements(sql_text: str) -> list[str]:
    """Split a .sql file's text into individual, non-empty statements."""
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def apply_ddl(connection, sql_text: str) -> int:
    """Execute every statement in sql_text against connection. Returns how many ran."""
    statements = split_statements(sql_text)
    cursor = connection.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
        connection.commit()
    finally:
        cursor.close()
    return len(statements)
