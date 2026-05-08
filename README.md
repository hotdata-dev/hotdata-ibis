# ibis-hotdata

Experimental [Ibis](https://ibis-project.org/) backend for [Hotdata](https://www.hotdata.dev/docs/api-reference): compile expressions with Ibis, run federated SQL over the Hotdata API. REST calls use the official **[hotdata](https://github.com/hotdata-dev/sdk-python)** Python SDK. Repo examples use **httpx** (listed under the **dev** dependency group).

**Requirements:** Python 3.10+, **ibis-framework** 10.x, **hotdata** ≥0.1.

## Install

```bash
uv pip install ibis-hotdata
# or: python -m pip install ibis-hotdata
```

## Connect

Programmatic API:

```python
import ibis

con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_TOKEN",
    workspace_id="ws_…",
    session_id=None,       # optional: X-Session-Id (sandbox)
    verify_ssl=True,
    timeout=120.0,
    default_connection=None,  # Hotdata connection id → Ibis catalog
    default_schema=None,      # remote schema → Ibis database
    poll_interval_s=0.25,
    poll_timeout_s=600.0,
)
```

URL style (token may live in the query string or the URL “password” segment):

```python
con = ibis.connect(
    "hotdata://api.hotdata.dev/?token=…&workspace_id=ws_…&verify_ssl=true"
)
```

**Mapping:** Ibis **catalog** = Hotdata connection id; **database** = remote schema; **table** = table name. SQL references look like `connection.schema.table`. With a single connection and schema, defaults are inferred; otherwise set `default_connection` / `default_schema` or qualify `con.table(..., database=(conn_id, schema))`.

**Execution:** SQL is compiled with Ibis’s **Postgres** SQLGlot compiler. The client submits queries asynchronously with `POST /v1/query`, polls `GET /v1/query-runs/{id}`, then downloads ready results as Arrow IPC from `GET /v1/results/{id}`. Tuning: `poll_interval_s`, `poll_timeout_s` on `connect()`.

**Types:** Typed tables come from Hotdata’s information schema. `con.sql(...)` types are inferred from a small preview query; see [Hotdata SQL](https://www.hotdata.dev/docs/sql) for server behavior.

**Not in v1:** Ibis `create_table`, embeddings, indexes. **Uploads:** use `upload_file` + `create_dataset_from_upload` on the connection object (or raw SQL); query datasets as `datasets.<schema>.<table>` per Hotdata.

## Development

```bash
uv sync --group dev   # pytest, ruff, httpx (for examples)
uv run pytest
uv run ruff check src tests examples
```

Lockfile CI: `uv sync --locked --group dev && uv run pytest`.

## TPC-H for the examples

Examples assume something like **`tpch.tpch_sf1.customer`**. Provision TPC-H in your workspace (commonly a **DuckDB** connection, then DuckDB’s `tpch` extension and `CALL dbgen(sf = 1)` — see [DuckDB TPC-H](https://www.duckdb.org/docs/current/core_extensions/tpch.html) and [Hotdata Quick Start](https://www.hotdata.dev/docs/quick-start)). If your data lives under `main` instead, pass `--default-schema` / `--default-connection` or set `HOTDATA_DEFAULT_*` (see `examples/_helpers.py`).

## Examples

Needs `HOTDATA_TOKEN` and `HOTDATA_WORKSPACE_ID`.

```bash
uv sync --group dev
export HOTDATA_TOKEN=…
export HOTDATA_WORKSPACE_ID=…
uv run python examples/01_catalog_introspection.py
uv run python examples/02_execute_sql.py 'SELECT COUNT(*) AS n FROM tpch.tpch_sf1.customer'
uv run python examples/03_connect_via_url.py
uv run python examples/04_ibis_table_workflows.py
```

### Ibis tables → pandas DataFrames

Calling **`.execute()`** on a table expression runs the compiled SQL on Hotdata and returns a **pandas** `DataFrame` (Ibis’s default for this backend).

Hotdata’s SQL often uses a **federated prefix** (for example `tpch.tpch_sf1`) that may not match the Ibis **catalog** string (the connection id). A reliable pattern is to start from **`con.sql("SELECT * FROM tpch.tpch_sf1.mytable", dialect="postgres")`**, then chain filters and aggregates—see **`examples/04_ibis_table_workflows.py`**.

When **`con.table("mytable")`** is enough (single connection/schema and names align with compiled SQL), the same operations apply:

```python
t = con.table("customer")  # or con.table("customer", database=(conn_id, "tpch_sf1"))

df = (
    t.filter(t.c_mktsegment == "AUTOMOBILE")
    .select("c_custkey", "c_name")
    .limit(100)
    .execute()
)

by_seg = t.group_by(t.c_mktsegment).agg(n=t.count()).execute()

o = con.table("orders")
orders_with_names = (
    t.join(o, t.c_custkey == o.o_custkey)
    .select(t.c_name, o.o_totalprice)
    .limit(50)
    .execute()
)

total = t.c_acctbal.sum().execute()
```

Other useful paths: **`.to_pyarrow()`** / **`.to_pyarrow_batches()`** for Arrow; **`con.sql("SELECT …", dialect="postgres")`** then chain the returned table expression.

## References

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [Hotdata API](https://www.hotdata.dev/docs/api-reference) · [Hotdata SQL](https://www.hotdata.dev/docs/sql)
- [Ibis](https://ibis-project.org/) · [Ibis backend hierarchy](https://ibis-project.org/concepts/backend-table-hierarchy.qmd)
