"""Map Hotdata metadata to Ibis dtypes."""

from __future__ import annotations

import ibis.expr.datatypes as dt
from ibis.backends.sql.datatypes import PostgresType


def dtype_from_hotdata_sql_type(sql_type: str | None, *, nullable: bool) -> dt.DataType:
    """Best-effort mapping from Hotdata `/information_schema` column `data_type` strings."""
    if not sql_type:
        return dt.String(nullable=nullable)
    try:
        return PostgresType.from_string(sql_type.strip(), nullable=nullable)
    except Exception:  # ibis/sqlglot raise a variety of parse errors; fall back to String
        return dt.String(nullable=nullable)
