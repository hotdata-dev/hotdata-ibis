"""Map Hotdata metadata to Ibis dtypes."""

from __future__ import annotations

import re

import ibis.expr.datatypes as dt
from ibis.backends.sql.datatypes import PostgresType

# Arrow-style type names returned by Hotdata's information_schema when tables are
# loaded from Parquet/Arrow sources.  PostgresType.from_string() treats these as
# USERDEFINED unknowns, so we resolve them explicitly before falling through.
_ARROW_TYPE_MAP: dict[str, type[dt.DataType]] = {
    # dates
    "date32": dt.Date,
    "date64": dt.Date,
    # floats — "halffloat" is PyArrow's str() name for float16
    "float16": dt.Float16,
    "float32": dt.Float32,
    "float64": dt.Float64,
    "halffloat": dt.Float16,
    # signed ints — must override Postgres parser: Postgres "int8" means 8-byte (64-bit),
    # but Arrow "int8" means 8-bit.  int16/32/64 parse correctly via Postgres.
    "int8": dt.Int8,
    # unsigned ints
    "uint8": dt.UInt8,
    "uint16": dt.UInt16,
    "uint32": dt.UInt32,
    "uint64": dt.UInt64,
    # strings — "large_string" / "largeutf8" are PyArrow large-offset variants
    "utf8": dt.String,
    "largeutf8": dt.String,
    "large_string": dt.String,
    # binary
    "largebinary": dt.Binary,
    # time
    "time32": dt.Time,
    "time64": dt.Time,
}

# Regex patterns for Arrow parametric types whose string representation includes
# embedded parameters (unit, timezone, precision, value type, …).
_TIMESTAMP_RE = re.compile(r"^timestamp\[(\w+)(?:,\s*tz=(.+))?\]$", re.IGNORECASE)
_DURATION_RE = re.compile(r"^duration\[(\w+)\]$", re.IGNORECASE)
_DECIMAL_RE = re.compile(r"^decimal128?\((\d+),\s*(\d+)\)$", re.IGNORECASE)
_LIST_RE = re.compile(r"^(?:large_)?list<item:\s*(.+)>$", re.IGNORECASE)

# Map Arrow time-unit strings to Ibis IntervalUnit strings.
_ARROW_UNIT_TO_IBIS: dict[str, str] = {
    "s": "s",
    "ms": "ms",
    "us": "us",
    "ns": "ns",
}


def _parse_parametric_arrow_type(raw: str, *, nullable: bool) -> dt.DataType | None:
    """Try to parse an Arrow parametric type string into an Ibis DataType.

    Returns ``None`` if ``raw`` does not match any known parametric pattern,
    allowing the caller to fall through to the Postgres dialect parser.
    """
    m = _TIMESTAMP_RE.match(raw)
    if m:
        tz: str | None = m.group(2).strip() if m.group(2) else None
        return dt.Timestamp(timezone=tz, nullable=nullable)

    m = _DURATION_RE.match(raw)
    if m:
        unit = _ARROW_UNIT_TO_IBIS.get(m.group(1).lower(), "s")
        return dt.Interval(unit=unit, nullable=nullable)

    m = _DECIMAL_RE.match(raw)
    if m:
        return dt.Decimal(precision=int(m.group(1)), scale=int(m.group(2)), nullable=nullable)

    m = _LIST_RE.match(raw)
    if m:
        value_type = dtype_from_hotdata_sql_type(m.group(1).strip(), nullable=True)
        return dt.Array(value_type=value_type, nullable=nullable)

    return None


def dtype_from_hotdata_sql_type(sql_type: str | None, *, nullable: bool) -> dt.DataType:
    """Best-effort mapping from Hotdata `/information_schema` column `data_type` strings.

    Hotdata may return either SQL-style names (``INTEGER``, ``VARCHAR``, ``DOUBLE
    PRECISION``, …) or Arrow-style names (``Date32``, ``Float64``, ``Utf8``, …).
    SQL-style names are delegated to the Postgres dialect parser; Arrow-style names
    are resolved via an explicit lookup table or parametric pattern before falling
    back to the parser.
    """
    if not sql_type:
        return dt.String(nullable=nullable)

    raw = sql_type.strip()

    # Arrow-style names (case-insensitive lookup).
    arrow_cls = _ARROW_TYPE_MAP.get(raw.lower())
    if arrow_cls is not None:
        return arrow_cls(nullable=nullable)

    # Arrow parametric types (timestamp[us], duration[ms], decimal128(p,s), list<…>).
    parametric = _parse_parametric_arrow_type(raw, nullable=nullable)
    if parametric is not None:
        return parametric

    try:
        return PostgresType.from_string(raw, nullable=nullable)
    except Exception:  # ibis/sqlglot raise a variety of parse errors; fall back to String
        return dt.String(nullable=nullable)
