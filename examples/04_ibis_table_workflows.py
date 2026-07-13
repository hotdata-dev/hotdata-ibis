#!/usr/bin/env python3
"""Ibis table expressions against a managed Hotdata database, executed to pandas.

Runs project+limit, filter+limit, group_by/agg, join, order_by, and a scalar
aggregate against two small managed tables (customer, orders). Distinct from
``examples/05_roundtrip_demo.py``: that one exercises the create/read/drop
lifecycle end to end; this one exercises the breadth of Ibis expressions this
backend's compiler + ``to_pyarrow`` result plumbing need to handle correctly
(multi-column projections, joins across tables, filters, ordering) -- not
just a single trivial column.

Ad hoc/federated querying (raw ``Connection`` tables, no managed database) is
not used here: it's a deprecated path, and every query now requires a
database scope server-side regardless.

Run against hosted Hotdata (the default) or a local cluster:

    HOTDATA_API_KEY=... HOTDATA_WORKSPACE=... \\
      uv run python examples/04_ibis_table_workflows.py

Point at a local cluster instead by exporting HOTDATA_API_BASE_URL=http://api.localhost
(plus a local key/workspace) before running.

Env:
    HOTDATA_API_KEY, HOTDATA_WORKSPACE   -- required
    HOTDATA_API_BASE_URL                 -- optional (default https://api.hotdata.dev)
"""

from __future__ import annotations

import os
import time

import ibis
import pandas as pd

DATABASE = "ibis_table_workflows_demo"
SCHEMA = "public"
API_BASE_URL = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")


def setup(api_url: str, token: str, workspace_id: str) -> str:
    """Create the managed database and load small customer/orders tables."""
    con = ibis.hotdata.connect(api_url=api_url, token=token, workspace_id=workspace_id)
    database_id = con.create_database(DATABASE, tables=["customer", "orders"], schema=SCHEMA)

    customer_df = pd.DataFrame(
        [
            {"c_custkey": 1, "c_name": "Alice", "c_mktsegment": "AUTOMOBILE", "c_acctbal": 100.0},
            {"c_custkey": 2, "c_name": "Bob", "c_mktsegment": "BUILDING", "c_acctbal": 200.0},
            {"c_custkey": 3, "c_name": "Carol", "c_mktsegment": "AUTOMOBILE", "c_acctbal": 300.0},
        ]
    )
    orders_df = pd.DataFrame(
        [
            {"o_orderkey": 100, "o_custkey": 1, "o_totalprice": 50.0},
            {"o_orderkey": 101, "o_custkey": 1, "o_totalprice": 75.0},
            {"o_orderkey": 102, "o_custkey": 3, "o_totalprice": 20.0},
        ]
    )
    con.create_table("customer", customer_df, database=(database_id, SCHEMA), overwrite=True)
    con.create_table("orders", orders_df, database=(database_id, SCHEMA), overwrite=True)
    con.disconnect()

    # Uploads are async; give the load a moment to finish before querying.
    time.sleep(2)
    return database_id


def run_workflows(api_url: str, token: str, workspace_id: str, database_id: str) -> None:
    con = ibis.hotdata.connect(
        api_url=api_url,
        token=token,
        workspace_id=workspace_id,
        default_schema=SCHEMA,
        database_id=database_id,
    )
    customer = con.table("customer", database=("default", SCHEMA))
    orders = con.table("orders", database=("default", SCHEMA))

    print("— project + limit —")
    q1 = customer.select("c_custkey", "c_name", "c_mktsegment").limit(2)
    print(con.compile(q1))
    print(q1.execute(), end="\n\n")

    print("— filter + limit —")
    q2 = customer.filter(customer.c_mktsegment == "AUTOMOBILE").limit(2)
    print(con.compile(q2))
    print(q2.execute(), end="\n\n")

    print("— group by segment —")
    q3 = customer.group_by(customer.c_mktsegment).agg(n=customer.count())
    print(con.compile(q3))
    print(q3.execute(), end="\n\n")

    print("— join customer to orders, ordered —")
    q4 = (
        customer.join(orders, customer.c_custkey == orders.o_custkey)
        .select(customer.c_name, orders.o_totalprice, orders.o_orderkey)
        .order_by("o_orderkey")
    )
    print(con.compile(q4))
    print(q4.execute(), end="\n\n")

    print("— scalar aggregate —")
    expr = customer.c_acctbal.sum()
    print(con.compile(expr))
    print(expr.execute())

    con.disconnect()


def cleanup(api_url: str, token: str, workspace_id: str, database_id: str) -> None:
    con = ibis.hotdata.connect(api_url=api_url, token=token, workspace_id=workspace_id)
    con.drop_database(database_id, force=True)
    con.disconnect()


def main() -> None:
    token = os.environ["HOTDATA_API_KEY"]
    workspace_id = os.environ["HOTDATA_WORKSPACE"]

    print("== SETUP (managed database: customer, orders) ==")
    database_id = setup(API_BASE_URL, token, workspace_id)
    print(f"created database_id={database_id}")

    print("\n== IBIS EXPRESSION WORKFLOWS ==")
    run_workflows(API_BASE_URL, token, workspace_id, database_id)

    print("\n== CLEANUP ==")
    cleanup(API_BASE_URL, token, workspace_id, database_id)


if __name__ == "__main__":
    main()
