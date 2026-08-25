# hotdata-ibis

Use [Ibis](https://ibis-project.org/) to create on-demand databases, upload data, and query with Python expressions — get pandas or Arrow results back without writing SQL.

**Requirements:** Python 3.10+, **ibis-framework** ≥12,<13, **hotdata** ≥0.7,<0.9.

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

# 1. Create a database and declare the tables you'll load.
#    Hotdata database names are not unique — create_database returns the id
#    you'll use for every subsequent operation on this database.
database_id = con.create_database("sales", tables=["orders"])

# 2. Upload a pandas DataFrame (or PyArrow table)
df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "amount": [9.99, 49.99, 5.00],
    "region": ["west", "east", "west"],
})
con.create_table("orders", df, database=(database_id, "main"), overwrite=True)

# 3. Uploads are async — wait briefly before querying
time.sleep(2)

# 4. Query with Ibis expressions
#    Managed tables are always accessed with catalog "default"
t = con.table("orders", database=("default", "main"))
result = (
    t.group_by("region")
    .agg(total=t.amount.sum())
    .order_by(ibis.desc("total"))
    .execute()  # returns a pandas DataFrame
)

# 5. Clean up
con.drop_table("orders", database=(database_id, "main"))
con.drop_database(database_id)
```

## Connect

```python
con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_KEY",
    workspace_id="ws_...",
    # optional
    timeout=120.0,             # per-request HTTP timeout in seconds
    verify_ssl=True,           # False to skip TLS verification, or path to CA bundle
    default_connection=None,   # default catalog (connection id); auto-detected if only one exists
    default_schema=None,       # default schema; auto-detected if only one exists
    database_id=None,          # bind an existing instant database id at connect time
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

## Instant databases

Instant databases are the primary way to bring data into Hotdata with Ibis. Declare a database and its tables, upload data, and query immediately.

### Create and load

```python
# Declare the database and all table names up front.
# Hotdata database names are not unique — create_database returns the id
# you'll use for every subsequent operation on this database.
database_id = con.create_database("analytics", tables=["events", "users"])

# Upload from a pandas DataFrame
con.create_table("events", events_df, database=(database_id, "main"), overwrite=True)

# PyArrow tables also work
import pyarrow as pa
table = pa.table({"id": [1, 2], "name": ["alice", "bob"]})
con.create_table("users", table, database=(database_id, "main"), overwrite=True)

# Schema-only (no data): creates an empty table with the declared schema
import ibis.expr.schema as sch
con.create_table(
    "staging",
    schema=sch.Schema({"id": "int64", "ts": "timestamp"}),
    database=(database_id, "main"),
)
```

Table names must be declared when the database is created — you cannot upload to a table name that was not listed in `tables=`.

### Query

When querying, use `"default"` as the catalog:

```python
t = con.table("events", database=("default", "main"))

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
    'FROM "default"."main"."events" '
    'WHERE event_type = \'click\' '
    'GROUP BY user_id'
).execute()
```

### Delete

Pass `force=True` to silently skip errors when the database or table does not exist:

```python
con.drop_table("events", database=(database_id, "main"))
con.drop_table("events", database=(database_id, "main"), force=True)  # no-op if missing

con.drop_database(database_id)
con.drop_database(database_id, force=True)  # no-op if missing
```

### Addressing summary

| Operation | `database=` argument |
|-----------|----------------------|
| `create_table` / `drop_table` | `(database_id, schema)` — the id returned by `create_database`, not its display name |
| `drop_database` | the id returned by `create_database`, not its display name |
| `con.table(...)` when querying | `("default", schema)` |

## Querying

### Ibis expressions

```python
t = con.table("orders", database=("default", "main"))

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
    'SELECT * FROM "default"."main"."orders"',
    dialect="postgres",
)
result = base.filter(base.amount > 10).execute()
```

You can chain Ibis expressions on the result of `con.sql(...)`.

## Vector search

`ibis_hotdata.vector` provides helpers for querying HNSW-indexed vector (embedding)
columns:

```python
from ibis_hotdata.vector import semantic_search, l2_distance

t = con.table("docs", database=("default", "main"))

result = semantic_search(t, "embedding", query_vector, k=10).execute()

# or with a different metric
result = semantic_search(t, "embedding", query_vector, k=10, distance_fn=l2_distance).execute()
```

`semantic_search` compiles to `ORDER BY <distance>(col, ARRAY[...]) ASC LIMIT k` with the
vector column excluded from the output — the SQL shape Hotdata's query engine requires to
route the query through its HNSW index instead of a brute-force scan.

Creating the HNSW index itself isn't wrapped by this package yet — use the `hotdata` SDK
directly:

```python
from hotdata import ApiClient, Configuration
from hotdata.api.indexes_api import IndexesApi
from hotdata.models.create_index_request import CreateIndexRequest

api = IndexesApi(ApiClient(Configuration(...)))
api.create_index(
    connection_id=connection_id,
    var_schema="main",
    table="docs",
    create_index_request=CreateIndexRequest(
        index_name="docs_embedding_idx",
        index_type="vector",
        columns=["embedding"],
        metric="cosine",
    ),
)
```

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
| Vector search (`ibis_hotdata.vector.semantic_search`) | ✅ |
| Temporary tables | ❌ |
| In-memory tables (`ibis.memtable(...)`) | ❌ |
| Python UDFs | ❌ |
| INSERT / UPDATE / DELETE on external connections | ❌ |

SQL compilation uses Ibis's Postgres dialect. Column types returned by Hotdata's information schema are resolved via PyArrow's type system, so Parquet-loaded tables with Arrow-native types (timestamps with time zones, decimals, lists, durations) are mapped correctly to Ibis types.

## Development

```bash
uv sync   # installs dev group (pytest, ruff, httpx)
uv run pytest
uv run ruff check src tests examples
```

CI: `uv sync --all-groups && uv run pytest -v`.

## Examples

Set your credentials, then run any example script:

```bash
export HOTDATA_API_KEY=...
export HOTDATA_WORKSPACE=...
uv run python examples/01_catalog_introspection.py
uv run python examples/02_execute_sql.py 'SELECT COUNT(*) AS n FROM tpch.tpch_sf1.customer'
uv run python examples/03_connect_via_url.py
uv run python examples/04_ibis_table_workflows.py
uv run python examples/05_roundtrip_demo.py
uv run python examples/06_semantic_search.py
```

## References

- [Hotdata documentation](https://www.hotdata.dev/docs/ibis)
- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [Ibis documentation](https://ibis-project.org/)
