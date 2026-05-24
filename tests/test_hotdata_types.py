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
