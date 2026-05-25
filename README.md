# hotdata-ibis

Use [Ibis](https://ibis-project.org/) to create on-demand databases, upload data, and query with Python expressions — get pandas or Arrow results back without writing SQL.

**Requirements:** Python 3.10+, **ibis-framework** 10.x, **hotdata** ≥0.2.3.

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
)
```

URL-style also works:

```python
con = ibis.connect("hotdata://api.hotdata.dev/?token=...&workspace_id=ws_...")
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
```

Table names must be declared when the database is created — you cannot add new table names later without recreating the database.

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

```python
con.drop_table("events", database=("analytics", "public"))
con.drop_database("analytics")
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

`.execute()` returns a **pandas DataFrame**. Use `.to_pyarrow()` for an Arrow table or `.to_pyarrow_batches()` to stream batches without materializing the full result.

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
| `create_table` / `drop_table` (DataFrame or Arrow upload) | ✅ |
| `con.table(...)` with full schema metadata | ✅ |
| Ibis expressions: filter, select, join, group\_by, agg, order\_by, limit | ✅ |
| `con.sql(...)` raw SQL | ✅ |
| `.execute()` → pandas, `.to_pyarrow()`, `.to_pyarrow_batches()` | ✅ |
| `list_catalogs`, `list_databases`, `list_tables` | ✅ |
| Temporary tables | ❌ |
| Python UDFs | ❌ |
| INSERT / UPDATE / DELETE on external connections | ❌ |

SQL compilation uses Ibis's Postgres dialect. Use `con.sql(...)` as a fallback for expressions that don't compile cleanly.

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
