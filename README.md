# hotdata-ibis

Use [Ibis](https://ibis-project.org/) to create on-demand databases, upload data, and query with Python expressions — get pandas or Arrow results back without writing SQL.

**Requirements:** Python 3.10+, **ibis-framework** ≥10,<11, **hotdata** ≥0.2.3.

## Install

```bash
pip install hotdata-ibis
# or: uv pip install hotdata-ibis
```

## Quickstart: create a database and query it

```python
import time
import pandas as pd
import ibis

con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_KEY",
    workspace_id="ws_...",
)

# 1. Create a database and declare the tables you'll load
con.create_database("sales", schema="public", tables=["orders"])

# 2. Upload a pandas DataFrame (or PyArrow table)
df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "amount": [9.99, 49.99, 5.00],
    "region": ["west", "east", "west"],
})
con.create_table("orders", df, database=("sales", "public"), overwrite=True)

# 3. Uploads are async — wait briefly before querying
time.sleep(2)

# 4. Query with Ibis expressions
#    Managed tables are always accessed with catalog "default"
t = con.table("orders", database=("default", "public"))
result = (
    t.group_by("region")
    .agg(total=t.amount.sum())
    .order_by(ibis.desc("total"))
    .execute()  # returns a pandas DataFrame
)

# 5. Clean up
con.drop_table("orders", database=("sales", "public"))
con.drop_database("sales")
```

## Connect

```python
con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_KEY",
    workspace_id="ws_...",
    # optional
    session_id=None,           # sandbox id (X-Session-Id header)
    timeout=120.0,             # per-request HTTP timeout in seconds
    verify_ssl=True,           # False to skip TLS verification, or path to CA bundle
    default_connection=None,   # default catalog (connection id); auto-detected if only one exists
    default_schema=None,       # default schema; auto-detected if only one exists
    database_id=None,          # bind an existing managed database id at connect time
    poll_interval_s=0.25,      # polling interval for async queries
    poll_timeout_s=600.0,      # max time to wait for a query result
)
```

URL-style also works, with the same parameters as query string keys:

```python
con = ibis.connect(
    "hotdata://api.hotdata.dev/"
    "?token=...&workspace_id=ws_..."
    "&default_connection=my_conn&default_schema=public"
)
```

## Managed databases

Managed databases are the primary way to bring data into Hotdata with Ibis. Declare a database and its tables, upload data, and query immediately.

### Create and load

```python
# Declare the database and all table names up front
con.create_database("analytics", schema="public", tables=["events", "users"])

# Upload from a pandas DataFrame
con.create_table("events", events_df, database=("analytics", "public"), overwrite=True)

# PyArrow tables also work
import pyarrow as pa
table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})
con.create_table("users", table, database=("analytics", "public"), overwrite=True)

# Schema-only (no data): creates an empty table with the declared schema
import ibis.expr.schema as sch
con.create_table(
    "staging",
    schema=sch.Schema({"id": "int64", "ts": "timestamp"}),
    database=("analytics", "public"),
)
```

Table names must be declared when the database is created — you cannot upload to a table name that was not listed in `tables=`.

### Query

When querying, use `"default"` as the catalog:

```python
t = con.table("events", database=("default", "public"))

result = (
    t.filter(t.event_type == "click")
    .group_by("user_id")
    .agg(n=t.count())
    .execute()
)
```

Or with raw SQL:

```python
result = con.sql(
    'SELECT user_id, COUNT(*) AS n '
    'FROM "default"."public"."events" '
    'WHERE event_type = \'click\' '
    'GROUP BY user_id'
).execute()
```

### Delete

Pass `force=True` to silently skip errors when the database or table does not exist:

```python
con.drop_table("events", database=("analytics", "public"))
con.drop_table("events", database=("analytics", "public"), force=True)  # no-op if missing

con.drop_database("analytics")
con.drop_database("analytics", force=True)  # no-op if missing
```

### Addressing summary

| Operation | `database=` argument |
|-----------|----------------------|
| `create_table` / `drop_table` | `("your-database-name", schema)` |
| `con.table(...)` when querying | `("default", schema)` |

## Querying

### Ibis expressions

```python
t = con.table("orders", database=("default", "public"))

summary = (
    t.filter(t.amount > 10)
    .group_by("region")
    .agg(total=t.amount.sum(), n=t.count())
    .order_by(ibis.desc("total"))
    .execute()
)
```

`.execute()` returns a **pandas DataFrame**. `.to_pyarrow()` returns an Arrow table. `.to_pyarrow_batches()` returns a `RecordBatchReader` — note that Hotdata returns a single Arrow IPC payload per query, so this method downloads the full result first and then splits it into local batches.

### Raw SQL

```python
base = con.sql(
    'SELECT * FROM "default"."public"."orders"',
    dialect="postgres",
)
result = base.filter(base.amount > 10).execute()
```

You can chain Ibis expressions on the result of `con.sql(...)`.

## Connecting to existing sources

If you have existing databases or warehouses connected to your Hotdata workspace (Postgres, Snowflake, BigQuery, etc.), you can query them through the same Ibis connection:

```python
con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_KEY",
    workspace_id="ws_...",
    default_connection="my_postgres",
    default_schema="public",
)

t = con.table("orders")  # resolves to my_postgres.public.orders
```

Discover what's available:

```python
con.list_catalogs()                                    # connection IDs
con.list_databases(catalog="my_postgres")              # schemas
con.list_tables(database=("my_postgres", "public"))    # tables
```

## What's supported

| Feature | Status |
|---------|--------|
| `create_database` / `drop_database` (managed) | ✅ |
| `create_table` from pandas / PyArrow / schema-only | ✅ |
| `drop_table` | ✅ |
| `con.table(...)` with full schema metadata | ✅ |
| Ibis expressions: filter, select, join, group\_by, agg, order\_by, limit | ✅ |
| `con.sql(...)` raw SQL | ✅ |
| `.execute()` → pandas, `.to_pyarrow()`, `.to_pyarrow_batches()` | ✅ |
| `list_catalogs`, `list_databases`, `list_tables` | ✅ |
| Arrow / Parquet column types (timestamp, decimal, list, duration, …) | ✅ |
| Temporary tables | ❌ |
| In-memory tables (`ibis.memtable(...)`) | ❌ |
| Python UDFs | ❌ |
| INSERT / UPDATE / DELETE on external connections | ❌ |

SQL compilation uses Ibis's Postgres dialect. Column types returned by Hotdata's information schema are resolved via PyArrow's type system, so Parquet-loaded tables with Arrow-native types (timestamps with time zones, decimals, lists, durations) are mapped correctly to Ibis types.

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
export HOTDATA_API_KEY=...
export HOTDATA_WORKSPACE=...
uv run python examples/01_catalog_introspection.py
uv run python examples/02_execute_sql.py 'SELECT COUNT(*) AS n FROM tpch.tpch_sf1.customer'
uv run python examples/03_connect_via_url.py
uv run python examples/04_ibis_table_workflows.py
```

## References

- [Hotdata documentation](https://www.hotdata.dev/docs/ibis)
- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [Ibis documentation](https://ibis-project.org/)
