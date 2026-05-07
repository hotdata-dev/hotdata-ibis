from __future__ import annotations

from decimal import Decimal

import pytest

import ibis.expr.datatypes as dt

from ibis_hotdata.types import dtype_from_hotdata_sql_type, dtype_from_json_value


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
    ("value", "expected_cls"),
    [
        (True, dt.Boolean),
        (42, dt.Int64),
        (3.14, dt.Float64),
        (Decimal("1.23"), dt.Decimal),
        ("hi", dt.String),
    ],
)
def test_dtype_from_json_primitive(value, expected_cls):
    out = dtype_from_json_value(value)
    assert isinstance(out, expected_cls)


def test_dtype_from_json_null_and_container():
    assert dtype_from_json_value(None) is None
    coll = dtype_from_json_value([1])
    assert isinstance(coll, dt.Array)
    blob = dtype_from_json_value({"a": 1})
    assert isinstance(blob, dt.JSON)
