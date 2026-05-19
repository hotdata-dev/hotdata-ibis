# ibis-hotdata

Experimental [Ibis](https://ibis-project.org/) backend for [Hotdata](https://www.hotdata.dev/docs/api-reference): compile expressions with Ibis, run federated SQL over the Hotdata API. REST calls use the official **[hotdata](https://github.com/hotdata-dev/sdk-python)** Python SDK. Repo examples use **httpx** (listed under the **dev** dependency group).

**Requirements:** Python 3.10+, **ibis-framework** 10.x, **hotdata** ≥0.2.

## Install

```bash
uv pip install ibis-hotdata
# or: python -m pip install ibis-hotdata
```

## Features

- **Ibis connection API** — connect with `ibis.hotdata.connect(...)` or `ibis.connect("hotdata://...")`.
- **Hotdata catalog mapping** — expose Hotdata connections, schemas, and tables through Ibis catalogs, databases, and tables.
- **SQL-backed expression execution** — compile Ibis expressions with the Postgres SQLGlot compiler and execute them through Hotdata query APIs.
- **Typed table discovery** — load schema metadata from Hotdata information schema and map SQL types into Ibis types.
- **Arrow and pandas results** — materialize expressions as pandas DataFrames, PyArrow tables, or local Arrow record batches.
- **Raw SQL escape hatch** — use `con.sql(..., dialect="postgres")` when Hotdata-specific federated SQL is clearer than modeled Ibis expressions.
- **Dataset upload helpers** — upload local pandas or PyArrow data as Parquet-backed Hotdata datasets through `create_table`.
- **Dataset cleanup** — delete Hotdata-managed datasets through the limited `drop_table` implementation.

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

**Types:** Typed tables come from Hotdata’s information schema. `con.sql(...)` types are inferred from a small preview query and Arrow schema; see [Hotdata SQL](https://www.hotdata.dev/docs/sql) for server behavior.

## Ibis Support Overview

`ibis-hotdata` is a read-oriented SQL backend. It is useful for exploring Hotdata workspaces with Ibis expressions, running federated SQL, and materializing results locally, but it is not a full mutable database backend.

Supported today:

- **Connection setup:** `ibis.hotdata.connect(...)` and `ibis.connect("hotdata://...")` with token, workspace, optional sandbox session, TLS, timeout, and polling settings.
- **Catalog discovery:** `list_catalogs`, `list_databases`, `list_tables`, `current_catalog`, and `current_database` map Hotdata connections and remote schemas into Ibis' catalog/database/table hierarchy.
- **Table schemas:** `con.table(...)` uses Hotdata information schema column metadata and maps SQL types through Ibis' Postgres type parser.
- **SQL-backed expressions:** Ibis expressions compile with the Postgres SQLGlot compiler and execute through Hotdata. Common `SELECT` workloads such as projection, filtering, joins, grouping, aggregation, ordering, limits, scalar expressions, and `con.sql(...)` work when the generated SQL is accepted by Hotdata.
- **Result materialization:** `.execute()` returns pandas objects. `.to_pyarrow()` and `.to_pyarrow_batches()` use the Arrow IPC result data exposed by Hotdata without converting through JSON rows; batches are split locally after the result is downloaded.
- **Raw SQL escape hatch:** `con.sql("SELECT ...", dialect="postgres")` is the most reliable way to use Hotdata-specific federated table names or SQL that Ibis does not model directly.
- **Uploads and datasets:** `upload_file` plus `create_dataset_from_upload` can create Hotdata datasets; query them as `datasets.<schema>.<table>` after creation.
- **Limited `create_table`:** `con.create_table("name", pandas_df)` and `con.create_table("name", pyarrow_table)` serialize local data to Parquet, upload it, and create a Hotdata dataset. Schema-only calls create an empty Parquet-backed dataset.
- **Dataset-only `drop_table`:** `con.drop_table(...)` deletes matching Hotdata-managed datasets. It does not drop external source tables.

Not supported as full Ibis backend features:

- **General DDL and mutations:** Arbitrary remote `CREATE TABLE` / `DROP TABLE`, inserts, updates, deletes, overwrites, and schema-altering operations are not implemented. `create_table` and `drop_table` are limited to Hotdata datasets as described above.
- **Temporary tables and in-memory registration:** `supports_temporary_tables` is false, and in-memory tables are not uploaded automatically for joins.
- **Python UDFs:** `supports_python_udfs` is false.
- **Transactions and sessions as database state:** Hotdata sandbox sessions can be passed as `session_id`, but the backend does not expose transaction APIs.
- **Backend-native SQL dialect:** Compilation uses Ibis' Postgres dialect as the closest fit. Hotdata SQL and federation rules are authoritative, so not every Ibis expression that compiles is guaranteed to execute remotely.
- **Complete Ibis compliance:** The backend is experimental and has focused test coverage for connection, discovery, schema mapping, execution, uploads, and Arrow results. It has not yet been validated against the full Ibis backend test suite.
- **Hotdata platform APIs beyond SQL datasets:** embeddings, indexes, query history management, sandbox lifecycle management, and other Hotdata-specific APIs are outside the Ibis backend surface.

## Development

```bash
uv sync   # installs dev group by default (pytest, ruff, httpx for examples)
uv run pytest
uv run ruff check src tests examples
```

Lockfile CI: `uv sync --locked && uv run pytest`.

## TPC-H for the examples

Examples assume something like **`tpch.tpch_sf1.customer`**. Provision TPC-H in your workspace (commonly a **DuckDB** connection, then DuckDB’s `tpch` extension and `CALL dbgen(sf = 1)` — see [DuckDB TPC-H](https://www.duckdb.org/docs/current/core_extensions/tpch.html) and [Hotdata Quick Start](https://www.hotdata.dev/docs/quick-start)). If your data lives under `main` instead, pass `--default-schema` / `--default-connection` or set `HOTDATA_DEFAULT_*` (see `examples/_helpers.py`).

## Examples

Needs `HOTDATA_API_KEY` and `HOTDATA_WORKSPACE`.

```bash
uv sync
export HOTDATA_API_KEY=…
export HOTDATA_WORKSPACE=…
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
