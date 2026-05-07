"""Map Hotdata metadata and JSON cells to Ibis dtypes."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import ibis.expr.datatypes as dt
from ibis.backends.sql.datatypes import PostgresType


def dtype_from_hotdata_sql_type(sql_type: str | None, *, nullable: bool) -> dt.DataType:
    """Best-effort mapping from Hotdata `/information_schema` column `data_type` strings."""
    if not sql_type:
        return dt.String(nullable=nullable)
    try:
        return PostgresType.from_string(sql_type.strip(), nullable=nullable)
    except Exception:
        return dt.String(nullable=nullable)


def dtype_from_json_value(value: Any) -> dt.DataType | None:
    """Infer an Ibis dtype from a deserialized JSON cell (no nullability signal)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return dt.Boolean()
    if isinstance(value, int):
        return dt.Int64()
    if isinstance(value, float):
        return dt.Float64()
    if isinstance(value, Decimal):
        return dt.Decimal(precision=None, scale=None)
    if isinstance(value, str):
        return dt.String()
    if isinstance(value, dict):
        return dt.JSON()
    if isinstance(value, list):
        return dt.Array(dt.JSON())

    return dt.String()
