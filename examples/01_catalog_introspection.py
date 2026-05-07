#!/usr/bin/env python3
"""
Inspect Hotdata catalogs (connections), databases (schemas), and tables.

From the repo root, after uv sync::

    HOTDATA_TOKEN=... HOTDATA_WORKSPACE_ID=... uv run python examples/01_catalog_introspection.py

Optional overrides: ``--catalog``, ``--schema`` (remote schema name), ``--table``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_examples = Path(__file__).resolve().parent
sys.path.insert(0, str(_examples))

import ibis

from _helpers import connect_kwargs, parsed_args, parser

_argp = parser("Inspect Hotdata via Ibis catalogs / schemas / tables.")
_argp.add_argument(
    "--catalog",
    dest="pick_catalog",
    default=os.environ.get("HOTDATA_PICK_CONNECTION") or None,
    help="Connection id to drill into (default: inferred or first catalog)",
)
_argp.add_argument(
    "--schema",
    dest="pick_schema",
    default=os.environ.get("HOTDATA_PICK_SCHEMA") or None,
    help="Remote schema / Ibis database to list tables",
)
_argp.add_argument(
    "--table",
    dest="pick_table",
    default=os.environ.get("HOTDATA_PICK_TABLE") or None,
    help="Print Ibis schema for this table (default: first listed table)",
)

_ns = parsed_args(_argp)
con = ibis.hotdata.connect(**connect_kwargs(_ns))


def main() -> None:
    cats = con.list_catalogs()
    print("connections (Ibis catalogs):", cats)
    if not cats:
        return

    picked = _ns.pick_catalog or _ns.default_connection or cats[0]
    if picked not in cats:
        print(f"WARNING: catalog {picked!r} not in workspace; using {cats[0]!r}")
        picked = cats[0]

    schemas = con.list_databases(catalog=picked)
    print(f"schemas under {picked !r} (Ibis databases):", schemas)
    if not schemas:
        return

    picked_schema = _ns.pick_schema or _ns.default_schema or schemas[0]
    if picked_schema not in schemas:
        print(f"WARNING: schema {picked_schema!r} not listed; using {schemas[0]!r}")
        picked_schema = schemas[0]

    tables = con.list_tables(database=(picked, picked_schema))
    print(f"tables under {picked!r}.{picked_schema!r}:")
    for name in tables:
        print(f"  - {name}")

    if not tables:
        return

    tbl_name = _ns.pick_table or tables[0]
    if tbl_name not in tables:
        print(f"Table {tbl_name!r} not in listing; skipping schema print.")
        return

    t = con.table(tbl_name, database=(picked, picked_schema))
    print(f"\ntable({tbl_name!r}).schema():")
    print(t.schema())


if __name__ == "__main__":
    main()
