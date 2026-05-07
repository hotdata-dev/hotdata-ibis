#!/usr/bin/env python3
"""
Ibis table expressions on TPC-H, executed to pandas via Hotdata.

Hotdata SQL often uses a short federated prefix (e.g. ``tpch.tpch_sf1``) that may not
match the Ibis **catalog** string (connection id). Building from ``con.sql(...)`` keeps
qualifiers aligned with working ``SELECT ... FROM tpch.tpch_sf1.*`` queries.

From the repo root::

    HOTDATA_TOKEN=... HOTDATA_WORKSPACE_ID=... \\
      uv run python examples/04_ibis_table_workflows.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_examples = Path(__file__).resolve().parent
sys.path.insert(0, str(_examples))

import ibis

from _helpers import connect_kwargs, parsed_args, parser

_argp = parser("Ibis table workflows → pandas (Hotdata / TPC-H).")
_ns = parsed_args(_argp)
con = ibis.hotdata.connect(**connect_kwargs(_ns))

# Federation prefix as in ``examples/02_execute_sql.py`` (not always == Ibis catalog id).
FED = "tpch.tpch_sf1"


def main() -> None:
    customer = con.sql(f"SELECT * FROM {FED}.customer", dialect="postgres")
    orders = con.sql(f"SELECT * FROM {FED}.orders", dialect="postgres")

    print("— project + limit —")
    q1 = customer.select("c_custkey", "c_name", "c_mktsegment").limit(5)
    print(con.compile(q1))
    print(q1.execute(), end="\n\n")

    print("— filter + limit —")
    q2 = customer.filter(customer.c_mktsegment == "AUTOMOBILE").limit(5)
    print(con.compile(q2))
    print(q2.execute(), end="\n\n")

    print("— group by segment —")
    q3 = customer.group_by(customer.c_mktsegment).agg(n=customer.count())
    print(con.compile(q3))
    print(q3.execute(), end="\n\n")

    print("— join customer to orders —")
    q4 = (
        customer.join(orders, customer.c_custkey == orders.o_custkey)
        .select(customer.c_name, orders.o_totalprice, orders.o_orderkey)
        .limit(8)
    )
    print(con.compile(q4))
    print(q4.execute(), end="\n\n")

    print("— scalar aggregate —")
    expr = customer.c_acctbal.sum()
    print(con.compile(expr))
    print(expr.execute())


if __name__ == "__main__":
    main()
