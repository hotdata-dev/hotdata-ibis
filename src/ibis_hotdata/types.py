"""Map Hotdata metadata to Ibis dtypes."""

from __future__ import annotations

import ibis.expr.datatypes as dt
from ibis.backends.sql.datatypes import PostgresType

# Arrow-style type names returned by Hotdata's information_schema when tables are
# loaded from Parquet/Arrow sources.  PostgresType.from_string() treats these as
# USERDEFINED unknowns, so we resolve them explicitly before falling through.
_ARROW_TYPE_MAP: dict[str, type[dt.DataType]] = {
    # dates
    "date32": dt.Date,
    "date64": dt.Date,
    # floats
    "float16": dt.Float16,
    "float32": dt.Float32,
    "float64": dt.Float64,
    # unsigned ints
    "uint8": dt.UInt8,
    "uint16": dt.UInt16,
    "uint32": dt.UInt32,
    "uint64": dt.UInt64,
    # strings
    "utf8": dt.String,
    "largeutf8": dt.String,
    # binary
    "largebinary": dt.Binary,
    # time
    "time32": dt.Time,
    "time64": dt.Time,
}


def dtype_from_hotdata_sql_type(sql_type: str | None, *, nullable: bool) -> dt.DataType:
    """Best-effort mapping from Hotdata `/information_schema` column `data_type` strings.

    Hotdata may return either SQL-style names (``INTEGER``, ``VARCHAR``, ``DOUBLE
    PRECISION``, …) or Arrow-style names (``Date32``, ``Float64``, ``Utf8``, …).
    SQL-style names are delegated to the Postgres dialect parser; Arrow-style names
    are resolved via an explicit lookup table before falling back to the parser.
    """
    if not sql_type:
        return dt.String(nullable=nullable)

    # Arrow-style names (case-insensitive lookup).
    arrow_cls = _ARROW_TYPE_MAP.get(sql_type.strip().lower())
    if arrow_cls is not None:
        return arrow_cls(nullable=nullable)

    try:
        return PostgresType.from_string(sql_type.strip(), nullable=nullable)
    except Exception:  # ibis/sqlglot raise a variety of parse errors; fall back to String
        return dt.String(nullable=nullable)
