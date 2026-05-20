"""Helpers for Hotdata managed database connections."""

from __future__ import annotations

from typing import Any

MANAGED_SOURCE_TYPE = "managed"
DEFAULT_SCHEMA = "public"


def build_managed_config(schema: str, tables: list[str]) -> dict[str, Any]:
    return {
        "schemas": [
            {
                "name": schema,
                "tables": [{"name": table} for table in tables],
            }
        ]
    }
