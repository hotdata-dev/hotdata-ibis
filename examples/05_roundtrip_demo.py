#!/usr/bin/env python3
"""Ibis <-> Hotdata round trip: create a managed database, upload data, read it back.

Demonstrates the managed-database read contract that ``hotdata-dlt-destination``'s
live ibis backend depends on: bind ``database_id`` at connect time (resolved once,
up front, the way a caller who only has the database's *name* would), then read
through the ``"default"`` catalog -- both ``con.table(...)`` (pandas) and
``con.sql(...)`` (pyarrow).

Run against hosted Hotdata (the default) or a local cluster:

    HOTDATA_API_KEY=... HOTDATA_WORKSPACE=... \\
      uv run python examples/05_roundtrip_demo.py

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
from hotdata import ApiClient, Configuration
from hotdata.api.databases_api import DatabasesApi

DATABASE = "ibis_roundtrip_demo"
SCHEMA = "public"
API_BASE_URL = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")


def resolve_database_id(*, api_url: str, token: str, workspace_id: str, name: str) -> str:
    """Look up a managed database's id by name via the raw SDK.

    Illustrative: this is the same lookup ``hotdata-dlt-destination``'s live ibis
    backend wrapper does before calling ``ibis.hotdata.connect(database_id=...)``.
    """
    conf = Configuration(host=api_url, api_key=token, workspace_id=workspace_id)
    client = ApiClient(conf)
    try:
        databases_api = DatabasesApi(client)
        for db in databases_api.list_databases().databases:
            if db.name == name:
                return db.id
        raise LookupError(f"managed database {name!r} not found")
    finally:
        client.close()


def write(api_url: str, token: str, workspace_id: str) -> None:
    """Create the managed database and upload a small pandas DataFrame into it."""
    con = ibis.hotdata.connect(api_url=api_url, token=token, workspace_id=workspace_id)
    con.create_database(DATABASE, tables=["spans"], schema=SCHEMA, force=True)

    df = pd.DataFrame(
        [
            {"span_id": "a1", "model": "claude-opus-4-8", "latency_ms": 812, "ok": True},
            {"span_id": "a2", "model": "claude-sonnet-5", "latency_ms": 240, "ok": True},
            {"span_id": "a3", "model": "claude-opus-4-8", "latency_ms": 590, "ok": False},
        ]
    )
    con.create_table("spans", df, database=(DATABASE, SCHEMA), overwrite=True)
    con.disconnect()

    # Uploads are async; give the load a moment to finish before querying.
    time.sleep(2)


def read_via_ibis(api_url: str, token: str, workspace_id: str, database_id: str) -> None:
    """The contract the dlt destination's live ibis backend relies on.

    ``database_id`` is bound once, at connect time -- not resolved lazily per query --
    and every read goes through the ``"default"`` catalog.
    """
    con = ibis.hotdata.connect(
        api_url=api_url,
        token=token,
        workspace_id=workspace_id,
        default_schema=SCHEMA,
        database_id=database_id,
    )

    print("-- con.table(...).execute() -> pandas --")
    t = con.table("spans", database=("default", SCHEMA))
    print(t.execute())

    print("\n-- aggregate expression, compiled + executed --")
    q = t.group_by("model").agg(n=t.count(), avg_latency_ms=t.latency_ms.mean())
    print(con.compile(q))
    print(q.execute())

    print("\n-- con.sql(...).to_pyarrow() -> arrow --")
    at = con.sql(f'SELECT * FROM "default"."{SCHEMA}"."spans" ORDER BY span_id').to_pyarrow()
    print(at)

    con.disconnect()


def cleanup(api_url: str, token: str, workspace_id: str) -> None:
    con = ibis.hotdata.connect(api_url=api_url, token=token, workspace_id=workspace_id)
    con.drop_database(DATABASE, force=True)
    con.disconnect()


def main() -> None:
    token = os.environ["HOTDATA_API_KEY"]
    workspace_id = os.environ["HOTDATA_WORKSPACE"]

    print("== CREATE + WRITE (con.create_database / con.create_table) ==")
    write(API_BASE_URL, token, workspace_id)

    database_id = resolve_database_id(
        api_url=API_BASE_URL, token=token, workspace_id=workspace_id, name=DATABASE
    )
    print(f"resolved {DATABASE!r} -> database_id={database_id}")

    print("\n== READ, via the managed-database contract ==")
    read_via_ibis(API_BASE_URL, token, workspace_id, database_id)

    print("\n== CLEANUP ==")
    cleanup(API_BASE_URL, token, workspace_id)


if __name__ == "__main__":
    main()
