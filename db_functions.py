from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row


MIN_QUERY_LEN = 6
DEFAULT_LIMIT = 200

# Один источник правды для fetch и для предпросмотра на фронте.
FUNCTIONS_SEARCH_SQL = """
SELECT DISTINCT ON (t.schema_name, t.function_name)
       t.version_id,
       t.function_name,
       t.schema_name,
       t.source_code,
       t.rowversion,
       t.employee_id,
       t.pg_user,
       t.is_from_compare
FROM version_tab t
WHERE t.function_name ILIKE %(query)s
   OR t.source_code ILIKE %(query)s
ORDER BY t.schema_name, t.function_name, t.rowversion DESC
LIMIT %(limit)s;
"""


def functions_search_sql_preview(query: str, limit: int = DEFAULT_LIMIT) -> str:
    """
    Текст запроса для UI (копипаст в psql).
    Параметры подставлены как литералы; апострофы в шаблоне экранированы.
    Реальное выполнение в fetch_functions по-прежнему через bind-параметры.
    """
    clean = query.strip()
    pattern = f"%{clean}%"
    lit = pattern.replace("'", "''")
    lim = int(limit)
    return (
        "-- ILIKE pattern (same as bound %(query)s in app)\n"
        f"SELECT DISTINCT ON (t.schema_name, t.function_name)\n"
        f"       t.version_id,\n"
        f"       t.function_name,\n"
        f"       t.schema_name,\n"
        f"       t.source_code,\n"
        f"       t.rowversion,\n"
        f"       t.employee_id,\n"
        f"       t.pg_user,\n"
        f"       t.is_from_compare\n"
        f"FROM version_tab t\n"
        f"WHERE t.function_name ILIKE '{lit}'\n"
        f"   OR t.source_code ILIKE '{lit}'\n"
        f"ORDER BY t.schema_name, t.function_name, t.rowversion DESC\n"
        f"LIMIT {lim};"
    )


@dataclass(frozen=True)
class FunctionRecord:
    version_id: int
    function_name: str
    schema_name: str
    source_code: str
    rowversion: str | None
    employee_id: int | None
    pg_user: str | None
    is_from_compare: bool | None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.function_name}"


def _load_db_settings() -> dict[str, Any]:
    load_dotenv()

    required_keys = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(
            f"Missing database settings in .env: {missing_list}"
        )

    settings: dict[str, Any] = {
        "host": os.getenv("PGHOST"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
    }

    sslmode = os.getenv("PGSSLMODE")
    if sslmode:
        settings["sslmode"] = sslmode

    return settings


def fetch_functions(query: str, limit: int = DEFAULT_LIMIT) -> list[FunctionRecord]:
    clean_query = query.strip()
    if len(clean_query) < MIN_QUERY_LEN:
        raise ValueError(f"Search query must be at least {MIN_QUERY_LEN} characters")

    sql = FUNCTIONS_SEARCH_SQL

    params = {
        "query": f"%{clean_query}%",
        "limit": limit,
    }

    with psycopg.connect(**_load_db_settings(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        FunctionRecord(
            version_id=row["version_id"],
            function_name=row["function_name"],
            schema_name=row["schema_name"],
            source_code=row["source_code"] or "",
            rowversion=str(row["rowversion"]) if row["rowversion"] is not None else None,
            employee_id=row["employee_id"],
            pg_user=row["pg_user"],
            is_from_compare=row["is_from_compare"],
        )
        for row in rows
    ]
