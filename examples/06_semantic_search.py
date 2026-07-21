#!/usr/bin/env python3
"""Vector search: create a managed table with an embedding column, query it with
``ibis_hotdata.vector.semantic_search``.

Uses small toy 4-dimensional vectors (not real embeddings) split into two obvious
clusters -- "pets" and "finance" -- so the nearest-neighbor ordering is easy to
eyeball. Demonstrates that the compiled SQL keeps the embedding column out of the
result set and orders by the aliased distance column ascending, which is the shape
Hotdata's query engine requires to route the query through its HNSW index rather
than falling back to a brute-force scan.

Run against hosted Hotdata (the default) or a local cluster:

    HOTDATA_API_KEY=... HOTDATA_WORKSPACE=... \\
      uv run python examples/06_semantic_search.py

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

from ibis_hotdata.vector import l2_distance, semantic_search

DATABASE = "ibis_semantic_search_demo"
SCHEMA = "public"
API_BASE_URL = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")

DOCS = pd.DataFrame(
    [
        {"doc_id": "d1", "text": "cats are great pets", "embedding": [0.90, 0.10, 0.00, 0.00]},
        {"doc_id": "d2", "text": "dogs are loyal companions", "embedding": [0.85, 0.15, 0.05, 0.00]},
        {"doc_id": "d3", "text": "stock market rose today", "embedding": [0.00, 0.00, 0.90, 0.10]},
        {"doc_id": "d4", "text": "interest rates and inflation", "embedding": [0.05, 0.00, 0.85, 0.10]},
    ]
)
QUERY_VECTOR = [0.88, 0.12, 0.00, 0.00]  # closest to the "pets" cluster (d1, d2)


def write(api_url: str, token: str, workspace_id: str) -> str:
    con = ibis.hotdata.connect(api_url=api_url, token=token, workspace_id=workspace_id)
    database_id = con.create_database(DATABASE, tables=["docs"], schema=SCHEMA)
    con.create_table("docs", DOCS, database=(database_id, SCHEMA), overwrite=True)
    con.disconnect()

    # Uploads are async; give the load a moment to finish before querying.
    time.sleep(2)
    return database_id


def query(api_url: str, token: str, workspace_id: str, database_id: str) -> None:
    con = ibis.hotdata.connect(
        api_url=api_url,
        token=token,
        workspace_id=workspace_id,
        default_schema=SCHEMA,
        database_id=database_id,
    )
    t = con.table("docs", database=("default", SCHEMA))

    print("-- semantic_search (cosine, default), compiled SQL --")
    cosine_expr = semantic_search(t, "embedding", QUERY_VECTOR, k=3)
    print(con.compile(cosine_expr))
    print(cosine_expr.execute())

    print("\n-- semantic_search with l2_distance instead --")
    l2_expr = semantic_search(t, "embedding", QUERY_VECTOR, k=3, distance_fn=l2_distance)
    print(l2_expr.execute())

    con.disconnect()


def cleanup(api_url: str, token: str, workspace_id: str, database_id: str) -> None:
    con = ibis.hotdata.connect(api_url=api_url, token=token, workspace_id=workspace_id)
    con.drop_database(database_id, force=True)
    con.disconnect()


def main() -> None:
    token = os.environ["HOTDATA_API_KEY"]
    workspace_id = os.environ["HOTDATA_WORKSPACE"]

    print("== CREATE + WRITE (con.create_database / con.create_table) ==")
    database_id = write(API_BASE_URL, token, workspace_id)
    print(f"created database_id={database_id}")

    print("\n== QUERY via semantic_search ==")
    query(API_BASE_URL, token, workspace_id, database_id)

    print("\n== CLEANUP ==")
    cleanup(API_BASE_URL, token, workspace_id, database_id)


if __name__ == "__main__":
    main()
