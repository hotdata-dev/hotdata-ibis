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
