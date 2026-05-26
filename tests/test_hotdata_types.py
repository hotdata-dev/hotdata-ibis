from __future__ import annotations

import pytest

import ibis.expr.datatypes as dt

from ibis_hotdata.types import dtype_from_hotdata_sql_type


@pytest.mark.parametrize(
    ("sql_type", "nullable", "expected_cls"),
    [
        ("BIGINT", False, dt.Integer),
        ("varchar", True, dt.String),
        ("DOUBLE PRECISION", True, dt.Float64),
        (None, True, dt.String),
    ],
)
def test_dtype_from_hotdata_sql_type_mapped(sql_type, nullable, expected_cls):
    out = dtype_from_hotdata_sql_type(sql_type, nullable=nullable)
    assert out.nullable is nullable
    assert isinstance(out, expected_cls)


def test_dtype_from_hotdata_timestamp_variant():
    out = dtype_from_hotdata_sql_type("TIMESTAMP WITHOUT TIME ZONE", nullable=False)
    assert isinstance(out, dt.Timestamp)


def test_dtype_from_hotdata_vendor_name_maps_or_string_fallback():
    out = dtype_from_hotdata_sql_type("unknown_vendor_type", nullable=True)
    assert out.nullable is True
    assert isinstance(out, (dt.String, dt.Unknown))


def test_dtype_from_hotdata_malformed_fallback_string():
    out = dtype_from_hotdata_sql_type('"', nullable=False)
    assert isinstance(out, dt.String)


@pytest.mark.parametrize(
    ("sql_type", "nullable", "expected_cls"),
    [
        # Arrow-style names returned when tables are loaded from Parquet/Arrow sources
        ("Date32", True, dt.Date),
        ("Date64", False, dt.Date),
        ("Float32", True, dt.Float32),
        ("Float64", False, dt.Float64),
        ("UInt8", True, dt.UInt8),
        ("UInt16", True, dt.UInt16),
        ("UInt32", True, dt.UInt32),
        ("UInt64", True, dt.UInt64),
        ("Utf8", True, dt.String),
        ("LargeUtf8", False, dt.String),
        ("LargeBinary", True, dt.Binary),
        ("Time32", True, dt.Time),
        ("Time64", False, dt.Time),
        # Previously missing: signed int8 (Postgres "int8" means int64, not int8)
        ("int8", True, dt.Int8),
        ("Int8", False, dt.Int8),
        # Previously missing: halffloat (PyArrow's str() name for float16)
        ("halffloat", True, dt.Float16),
        ("HALFFLOAT", False, dt.Float16),
        # Previously missing: large_string (PyArrow large-offset string variant)
        ("large_string", True, dt.String),
        ("Large_String", False, dt.String),
        # Case-insensitive
        ("date32", True, dt.Date),
        ("FLOAT64", True, dt.Float64),
        ("UTF8", True, dt.String),
    ],
)
def test_dtype_from_hotdata_arrow_type_names(sql_type, nullable, expected_cls):
    out = dtype_from_hotdata_sql_type(sql_type, nullable=nullable)
    assert out.nullable is nullable
    assert isinstance(out, expected_cls)


@pytest.mark.parametrize(
    ("sql_type", "expected_tz", "expected_scale"),
    [
        ("timestamp[s]", None, 0),
        ("timestamp[ms]", None, 3),
        ("timestamp[us]", None, 6),
        ("timestamp[ns]", None, 9),
        ("timestamp[us, tz=UTC]", "UTC", 6),
        ("timestamp[us, tz=America/New_York]", "America/New_York", 6),
        ("TIMESTAMP[MS]", None, 3),
    ],
)
def test_dtype_from_hotdata_arrow_timestamp(sql_type, expected_tz, expected_scale):
    out = dtype_from_hotdata_sql_type(sql_type, nullable=True)
    assert isinstance(out, dt.Timestamp)
    assert out.timezone == expected_tz
    assert out.scale == expected_scale
    assert out.nullable is True


@pytest.mark.parametrize(
    ("sql_type", "expected_unit"),
    [
        ("duration[s]", "s"),
        ("duration[ms]", "ms"),
        ("duration[us]", "us"),
        ("duration[ns]", "ns"),
        ("DURATION[MS]", "ms"),
    ],
)
def test_dtype_from_hotdata_arrow_duration(sql_type, expected_unit):
    out = dtype_from_hotdata_sql_type(sql_type, nullable=False)
    assert isinstance(out, dt.Interval)
    assert out.unit.value == expected_unit
    assert out.nullable is False


def test_dtype_from_hotdata_arrow_duration_unknown_unit_falls_back():
    # An unrecognised duration unit should not silently map to seconds;
    # it falls through to the Postgres parser (which returns Unknown) or String fallback.
    out = dtype_from_hotdata_sql_type("duration[foo]", nullable=True)
    assert not isinstance(out, dt.Interval)  # must not produce a valid Interval


@pytest.mark.parametrize(
    ("sql_type", "expected_precision", "expected_scale"),
    [
        ("decimal128(10, 3)", 10, 3),
        ("decimal128(38, 0)", 38, 0),
        ("decimal256(76, 38)", 76, 38),
        ("decimal(5, 2)", 5, 2),
        ("DECIMAL128(18, 6)", 18, 6),
        # decimal12 is NOT a valid form — should not be matched by the decimal regex
    ],
)
def test_dtype_from_hotdata_arrow_decimal(sql_type, expected_precision, expected_scale):
    out = dtype_from_hotdata_sql_type(sql_type, nullable=True)
    assert isinstance(out, dt.Decimal)
    assert out.precision == expected_precision
    assert out.scale == expected_scale
    assert out.nullable is True


@pytest.mark.parametrize(
    ("sql_type", "expected_value_cls", "expected_item_nullable"),
    [
        ("list<item: int32>", dt.Int32, True),
        ("list<item: utf8>", dt.String, True),
        ("list<item: float64>", dt.Float64, True),
        ("large_list<item: int64>", dt.Int64, True),
        ("LIST<ITEM: UINT8>", dt.UInt8, True),
        # Non-nullable item fields — PyArrow appends " not null"
        ("list<item: int32 not null>", dt.Int32, False),
        ("list<item: utf8 not null>", dt.String, False),
        ("large_list<item: float32 not null>", dt.Float32, False),
    ],
)
def test_dtype_from_hotdata_arrow_list(sql_type, expected_value_cls, expected_item_nullable):
    out = dtype_from_hotdata_sql_type(sql_type, nullable=True)
    assert isinstance(out, dt.Array)
    assert isinstance(out.value_type, expected_value_cls)
    assert out.value_type.nullable is expected_item_nullable
    assert out.nullable is True


def test_dtype_from_hotdata_arrow_decimal12_not_matched():
    # "decimal12" (only the trailing 8 made optional) must NOT match the decimal regex.
    # The Postgres parser handles bare "decimal" forms; decimal12 is not a real type.
    out = dtype_from_hotdata_sql_type("decimal12(10, 3)", nullable=True)
    assert not isinstance(out, dt.Decimal)  # falls through to Unknown or String
