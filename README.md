# hotdata-ibis

Use [Ibis](https://ibis-project.org/) to query and upload data in your [Hotdata](https://www.hotdata.dev/docs/api-reference) workspace — write Python expressions instead of SQL, get pandas or Arrow results back.

**Requirements:** Python 3.10+, **ibis-framework** 10.x, **hotdata** ≥0.2.3.

## Install

```bash
uv pip install hotdata-ibis
# or: pip install hotdata-ibis
```

## Quick start

```python
import ibis

con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_TOKEN",
    workspace_id="ws_…",
)

# List available tables
con.list_tables()

# Query with Ibis expressions
t = con.table("customer", database=("my_connection", "tpch_sf1"))
df = (
    t.filter(t.c_mktsegment == "AUTOMOBILE")
    .select("c_custkey", "c_name")
    .limit(100)
    .execute()      # returns a pandas DataFrame
)
```

## Connect

```python
con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_TOKEN",
    workspace_id="ws_…",
    default_connection="my_connection",  # skip qualifying every table reference
    default_schema="public",             # skip qualifying every table reference
    session_id=None,                     # optional sandbox session
    timeout=120.0,
    verify_ssl=True,
    poll_interval_s=0.25,
    poll_timeout_s=600.0,
)
```

URL style also works — token can go in the query string or the URL password segment:

```python
con = ibis.connect("hotdata://api.hotdata.dev/?token=…&workspace_id=ws_…")
```

**Table addressing:** Hotdata organizes data as `connection → schema → table`. In Ibis terms that maps to `catalog → database → table`. With a single connection and schema, defaults are inferred automatically. For multiple connections or schemas, pass `database=(connection_id, schema)` when referencing a table, or set `default_connection` / `default_schema` at connect time.

## Querying

### Ibis expressions

```python
t = con.table("orders")

# Filter, select, aggregate — all run as SQL on Hotdata
summary = (
    t.filter(t.status == "shipped")
    .group_by("region")
    .agg(total=t.amount.sum(), n=t.count())
    .order_by("total", ascending=False)
    .execute()
)
```

`.execute()` returns a **pandas DataFrame**. Use `.to_pyarrow()` for an Arrow table or `.to_pyarrow_batches()` for a record batch reader.

### Raw SQL

When you need Hotdata-specific syntax, federated table names, or SQL that Ibis doesn't model:

```python
df = con.sql(
    "SELECT region, SUM(amount) AS total FROM my_conn.public.orders GROUP BY region",
    dialect="postgres",
).execute()
```

You can chain Ibis expressions on the result of `con.sql(...)` the same way you would on `con.table(...)`.

### Discover what's available

```python
con.list_catalogs()                             # Hotdata connection ids
con.list_databases(catalog="my_connection")     # schemas for a connection
con.list_tables(database=("my_connection", "public"))
con.get_schema("orders", catalog="my_connection", database="public")
```

## Managed databases

Managed databases let you upload your own data (pandas DataFrames or PyArrow tables) and query it alongside your other Hotdata connections. They are provisioned on demand and scoped to your workspace.

```python
import time
import ibis
import pandas as pd

con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_TOKEN",
    workspace_id="ws_…",
)

# 1. Create the database and declare which tables you'll upload.
#    Table names must be declared here — uploads to undeclared names are rejected.
con.create_database("my-dataset", schema="public", tables=["orders"])

# 2. Upload data.
df = pd.DataFrame({"order_id": [1, 2, 3], "amount": [9.99, 49.99, 5.00]})
con.create_table("orders", df, database=("my-dataset", "public"), overwrite=True)

# 3. Uploads are asynchronous — wait a moment before querying.
time.sleep(2)

# 4. Query with Ibis expressions.
#    Managed tables use "default" as the catalog — the backend handles this automatically.
t = con.table("orders", database=("default", "public"))
result = t.filter(t.amount > 10).order_by("amount").execute()

# 5. Or with raw SQL.
result = con.sql('SELECT SUM(amount) AS total FROM "default"."public"."orders"').execute()

# 6. Clean up.
con.drop_table("orders", database=("my-dataset", "public"))
con.drop_database("my-dataset")
```

**Things to know:**
- Declare all table names in `create_database(..., tables=[...])` before uploading — you can't add them later without recreating the database.
- Use `database=("my-dataset", schema)` when uploading (`create_table`) or dropping tables (`drop_table`).
- Use `database=("default", schema)` when querying — managed tables always use `"default"` as the SQL catalog prefix.
- `create_table` accepts pandas DataFrames, PyArrow tables, or an Ibis schema for creating an empty table.
- Uploads use replace mode. Pass `overwrite=True` to replace a table that already exists; without it, uploading to an existing table raises an error.

## What's supported

| Feature | Status |
|---|---|
| `list_catalogs`, `list_databases`, `list_tables` | ✅ |
| `con.table(...)` with full schema metadata | ✅ |
| Ibis expressions: filter, select, join, group\_by, agg, order\_by, limit | ✅ |
| `con.sql(...)` raw SQL | ✅ |
| `.execute()` → pandas, `.to_pyarrow()`, `.to_pyarrow_batches()` | ✅ |
| `create_database` / `drop_database` (managed) | ✅ |
| `create_table` / `drop_table` (managed, Parquet upload) | ✅ |
| Temporary tables | ❌ |
| Python UDFs | ❌ |
| INSERT / UPDATE / DELETE on external connections | ❌ |

SQL compilation uses Ibis's Postgres dialect as the closest fit. Most common `SELECT` workloads run fine; complex expressions may generate SQL that Hotdata doesn't support — use `con.sql(...)` as a fallback.

## Development

```bash
uv sync   # installs dev group (pytest, ruff, httpx)
uv run pytest
uv run ruff check src tests
```

CI: `uv sync --locked && uv run pytest`.

## Examples

Set your credentials, then run any example script:

```bash
export HOTDATA_API_KEY=…
export HOTDATA_WORKSPACE=…
uv run python examples/01_catalog_introspection.py
uv run python examples/02_execute_sql.py 'SELECT COUNT(*) AS n FROM tpch.tpch_sf1.customer'
uv run python examples/03_connect_via_url.py
uv run python examples/04_ibis_table_workflows.py
```

The examples assume a TPC-H dataset at `tpch.tpch_sf1`. To provision it: create a DuckDB connection in Hotdata, then run `CALL dbgen(sf = 1)` using DuckDB's [tpch extension](https://duckdb.org/docs/extensions/tpch.html).

## References

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [Hotdata API reference](https://www.hotdata.dev/docs/api-reference) · [Hotdata SQL](https://www.hotdata.dev/docs/sql)
- [Ibis documentation](https://ibis-project.org/) · [Ibis backend concepts](https://ibis-project.org/concepts/backend-table-hierarchy.qmd)
