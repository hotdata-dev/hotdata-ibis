# ibis-hotdata

Experimental [Ibis](https://ibis-project.org/) backend for [Hotdata](https://www.hotdata.dev/docs/api-reference)—federated, Postgres-compatible SQL executed over HTTPS.

Hotdata exposes `POST /v1/query`, optional asynchronous execution (`202` + `GET /v1/query-runs/{id}` + `GET /v1/results/{id}`), and catalog metadata via `GET /v1/information_schema`. This package forwards compiled Ibis SQL through those endpoints.

## Install

**From PyPI** (pick your installer):

```bash
uv pip install ibis-hotdata
# or
python -m pip install ibis-hotdata
```

Use Python **3.10+**. This package pins **`ibis-framework>=10,<11`** to match the Ibis major line.

## Connect

```python
import ibis

con = ibis.hotdata.connect(
    api_url="https://api.hotdata.dev",
    token="YOUR_API_TOKEN",
    workspace_id="ws_…",
    session_id=None,  # optional sandbox: X-Session-Id
    verify_ssl=True,
    timeout=120.0,
    default_connection=None,  # Hotdata connection id (Ibis “catalog”); see below
    default_schema=None,        # remote schema name (Ibis “database”)
    prefer_async=False,         # set True to prefer async query submission
)
```

### URL form

```python
con = ibis.connect(
    "hotdata://api.hotdata.dev/?token=…&workspace_id=ws_…&verify_ssl=true"
)
```

The host becomes `https://{host}` (plus any path on the URL). You may place the token in the password segment (`hotdata://x:TOKEN@host/…`) instead of the query string.

After `pip install`, both `ibis.hotdata.connect(...)` and `ibis.connect("hotdata://…")` resolve to this backend via the `ibis.backends` entry point.

## Headers and sessions

Per the [Hotdata API](https://www.hotdata.dev/docs/api-reference), the client sends:

- `Authorization: Bearer <token>`
- `X-Workspace-Id: <workspace_public_id>`
- optionally `X-Session-Id: <sandbox_public_id>` when `session_id` is set.

## Ibis identifiers vs Hotdata hierarchy

Following Ibis terminology ([catalog → database → table](https://ibis-project.org/concepts/backend-table-hierarchy.qmd)), this backend maps:

| Ibis surface | Hotdata meaning |
|-------------|----------------|
| **Catalog** | Connection **id** from `GET /v1/connections` (same identifier as `connection` on `information_schema` rows). |
| **Database** | Remote **schema name** surfaced by Hotdata. |
| **Table name** | Remote table name. |

Typical federated references in SQL are `connection.schema.table` (quoted as needed):

```python
orders = con.table("orders", database=("conn_abc", "public"))
```

If the workspace exposes **exactly one** connection and **one** schema discovered for it, defaults are inferred; otherwise provide `default_connection` / `default_schema` when connecting.

## SQL dialect and compilation

The backend reuses Ibis’s **PostgreSQL SQLGlot compiler** (`postgres` dialect) so expressions compile to Postgres-oriented SQL aligned with Hotdata’s documented Postgres-style surface. Operational SQL details and federation edge cases belong in the [Hotdata SQL docs](https://www.hotdata.dev/docs/sql)—this client does not re-validate server capabilities.

## Query execution and async

- By default queries use synchronous `POST /v1/query` with `"async": false`.
- With `prefer_async=True`, requests use `"async": true`. The HTTP client honors `202` by polling **`GET /v1/query-runs/{id}`** until `succeeded`, then **`GET /v1/results/{id}`** until tabular payload is available.
- You can tune `poll_interval_s` and `poll_timeout_s` on `connect()`.

## Types and result materialization

- **Known tables:** column types come from `information_schema` when `include_columns=true` and are parsed with the same `PostgresType` mapper Ibis uses for PostgreSQL, with graceful fallback to `string`.
- **`con.sql(...)`:**
  inferred via `SELECT * FROM (<your query>) AS ibis_hotdata_preview LIMIT 1`, using HTTP `columns`/`nullable` and the first JSON row shape for coarse inference (Decimals from JSON rarely round-trip cleanly; timestamps may appear as ISO strings unless the API returns richer metadata; nested structures map toward `JSON` / `Array(JSON)`).

Results are fetched into **pandas** by default (`execute`), matching core SQL backends. PyArrow batches follow Ibis’s `to_pyarrow` / `to_pyarrow_batches` path over the same row materialization.

## Out of scope (v1)

Table creation/DML helpers, uploads, embeddings, indexes, dataset lifecycle—these remain unimplemented unless you drive them explicitly with `.sql(...)`.

## Development

This repo uses **[uv](https://docs.astral.sh/uv/)** for environments and **`uv.lock`**.

```bash
uv sync               # editable project + dev group (pytest, pytest-httpserver, ruff)
uv run pytest
uv run ruff check src tests
```

Optional Python pin:

```bash
uv python pin 3.12
uv sync
```

CI-oriented checks:

```bash
uv sync --locked      # fail if uv.lock is out of date relative to pyproject.toml
uv run pytest
```

Without uv, use `pip install -e .` and install dev tools separately (`pytest`, `pytest-httpserver`, `ruff`).

Tests use **pytest-httpserver**; no workspace tokens are embedded in this repository.

## References

- [Hotdata API reference](https://www.hotdata.dev/docs/api-reference)
- [Hotdata SQL reference](https://www.hotdata.dev/docs/sql)
- [Ibis](https://ibis-project.org/)
