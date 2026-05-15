#!/usr/bin/env python3
"""
Run arbitrary Hotdata-compatible SQL via ``con.sql`` and execute to pandas.

From the repo root::

    HOTDATA_API_KEY=... HOTDATA_WORKSPACE_ID=... \\
      uv run python examples/02_execute_sql.py \\
      'SELECT COUNT(*) AS n FROM tpch.tpch_sf1.customer'

The default SQL targets ``tpch.tpch_sf1.customer`` (TPC-H over Hotdata). Override
with a positional argument for any other federated reference
(``connection.schema.table``).
"""

from __future__ import annotations

import sys
from pathlib import Path

_examples = Path(__file__).resolve().parent
sys.path.insert(0, str(_examples))

import ibis

from _helpers import connect_kwargs, parsed_args, parser

_argp = parser("Execute SQL via Hotdata through Ibis.")
_argp.add_argument(
    "sql",
    nargs="?",
    default="SELECT COUNT(*) AS n FROM tpch.tpch_sf1.customer",
    help="SQL string (default: row count on tpch.tpch_sf1.customer)",
)
_ns = parsed_args(_argp)
con = ibis.hotdata.connect(**connect_kwargs(_ns))


def main() -> None:
    sql = (_ns.sql or "").strip().rstrip(";")
    tbl = con.sql(sql, dialect="postgres")
    print("Compiled SQL:")
    print(con.compile(tbl))

    pdf = tbl.execute()
    print("\nResult (pandas.DataFrame):\n")
    print(pdf)


if __name__ == "__main__":
    main()
