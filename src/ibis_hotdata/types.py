"""Map Hotdata metadata to Ibis dtypes."""

from __future__ import annotations

import re

import ibis.expr.datatypes as dt
import pyarrow as pa
from ibis.backends.sql.datatypes import PostgresType
from ibis.formats.pyarrow import PyArrowType

# Simple Arrow type strings → PyArrow instances.  Covers non-parametric types
# that the Postgres dialect parser does not know (Arrow-specific names, unsigned
# ints) or mis-maps (Arrow "int8" = 8-bit; Postgres "int8" = 8-byte / int64).
# All scalar types that can appear as list/map element types must be listed here
# because element type strings are resolved via this map, not the Postgres parser.
_PA_TYPE_MAP: dict[str, pa.DataType] = {
    # dates
    "date32": pa.date32(),
    "date64": pa.date64(),
    # floats — "halffloat" is PyArrow's str() name for float16
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "halffloat": pa.float16(),
    # signed ints — Arrow "int8" ≠ Postgres "int8" (8-byte/int64); all four
    # listed here so they resolve correctly when used as list element types
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    # unsigned ints (Postgres parser returns Unknown for all of these)
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "utf8": pa.utf8(),
    "largeutf8": pa.large_utf8(),
    "large_string": pa.large_utf8(),
    "string": pa.string(),
    "utf8view": pa.string_view(),
    # binary
    "binary": pa.binary(),
    "largebinary": pa.large_binary(),
    # boolean / null
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "null": pa.null(),
    # time — unit is absent from these bare string forms; the unit does not
    # affect the Ibis type (both time32 and time64 map to dt.Time)
    "time32": pa.time32("ms"),
    "time64": pa.time64("us"),
}

# Regex patterns for parametric Arrow types that embed parameters in the string.
_TIMESTAMP_RE = re.compile(r"^timestamp\[(\w+)(?:,\s*tz=(.+))?\]$", re.IGNORECASE)
_DURATION_RE = re.compile(r"^duration\[(\w+)\]$", re.IGNORECASE)
_DECIMAL_RE = re.compile(r"^decimal(?:128|256)?\((\d+),\s*(\d+)\)$", re.IGNORECASE)
_LIST_RE = re.compile(r"^(large_)?list<item:\s*(.+)>$", re.IGNORECASE)
# PyArrow appends " not null" when a list's item field is non-nullable.
_NOT_NULL_SUFFIX_RE = re.compile(r"\s+not\s+null$", re.IGNORECASE)


def _pa_type_from_arrow_str(raw: str) -> pa.DataType | None:
    """Best-effort: Arrow type string → PyArrow DataType, or ``None`` if not recognised.

    Handles simple names (via ``_PA_TYPE_MAP``) and parametric forms
    (timestamp, duration, decimal, list/large_list).  Returns ``None`` if the
    string is not a known Arrow type, allowing the caller to fall through to the
    Postgres dialect parser or String fallback.
    """
    s = raw.strip()

    # Simple non-parametric types.
    pa_type = _PA_TYPE_MAP.get(s.lower())
    if pa_type is not None:
        return pa_type

    # timestamp[unit] or timestamp[unit, tz=…]
    m = _TIMESTAMP_RE.match(s)
    if m:
        unit = m.group(1).lower()
        tz_raw = m.group(2)
        tz: str | None = tz_raw.strip() if tz_raw else None
        try:
            return pa.timestamp(unit, tz=tz)
        except Exception:
            return None

    # duration[unit] — unknown units return None so the caller falls through
    m = _DURATION_RE.match(s)
    if m:
        try:
            return pa.duration(m.group(1).lower())
        except Exception:
            return None

    # decimal / decimal128 / decimal256
    m = _DECIMAL_RE.match(s)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        try:
            # decimal128 supports precision 1-38; fall back to decimal256 for wider values
            return (
                pa.decimal128(precision, scale)
                if precision <= 38
                else pa.decimal256(precision, scale)
            )
        except Exception:
            return None

    # list<item: T> or large_list<item: T> (recursive for nested types)
    m = _LIST_RE.match(s)
    if m:
        is_large = bool(m.group(1))
        item_raw = m.group(2).strip()
        item_not_null = bool(_NOT_NULL_SUFFIX_RE.search(item_raw))
        item_str = _NOT_NULL_SUFFIX_RE.sub("", item_raw).strip()
        item_pa_type = _pa_type_from_arrow_str(item_str)
        if item_pa_type is None:
            return None
        item_field = pa.field("item", item_pa_type, nullable=not item_not_null)
        return pa.large_list(item_field) if is_large else pa.list_(item_field)

    return None


def dtype_from_hotdata_sql_type(sql_type: str | None, *, nullable: bool) -> dt.DataType:
    """Best-effort mapping from Hotdata ``/information_schema`` column ``data_type`` strings.

    Hotdata may return either SQL-style names (``INTEGER``, ``VARCHAR``, ``DOUBLE
    PRECISION``, …) or Arrow-style names (``Date32``, ``Float64``, ``Utf8``, …).
    Arrow-style names are resolved via PyArrow's type system and converted to Ibis
    types using the Ibis-PyArrow bridge; SQL-style names fall through to the Postgres
    dialect parser.
    """
    if not sql_type:
        return dt.String(nullable=nullable)

    raw = sql_type.strip()

    # Try to parse as an Arrow type string (simple or parametric).
    pa_type = _pa_type_from_arrow_str(raw)
    if pa_type is not None:
        return PyArrowType.to_ibis(pa_type).copy(nullable=nullable)

    # Fall through to Postgres dialect parser for SQL-style type names.
    try:
        return PostgresType.from_string(raw, nullable=nullable)
    except Exception:  # ibis/sqlglot raise a variety of parse errors; fall back to String
        return dt.String(nullable=nullable)
