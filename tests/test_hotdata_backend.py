from __future__ import annotations

from collections.abc import Callable

import io
import json

import ibis
import ibis.common.exceptions as com
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from werkzeug.wrappers import Request, Response

pytest.importorskip("pytest_httpserver")
from pytest_httpserver import HTTPServer

# Managed database identifiers for mocked Hotdata (SQL shape ``sales.public.orders``).
MANAGED_CONN = "conn_sales"
MANAGED_NAME = "sales"
PUBLIC = "public"
TPCH_CONN = "tpch"
TPCH_SF1 = "tpch_sf1"
TPCH_CUSTOMER_COLS = [
    {"name": "c_custkey", "data_type": "INTEGER", "nullable": False},
    {"name": "c_name", "data_type": "VARCHAR", "nullable": False},
    {"name": "c_address", "data_type": "VARCHAR", "nullable": False},
    {"name": "c_nationkey", "data_type": "INTEGER", "nullable": False},
    {"name": "c_phone", "data_type": "VARCHAR", "nullable": False},
    {"name": "c_acctbal", "data_type": "DECIMAL(15, 2)", "nullable": False},
    {"name": "c_mktsegment", "data_type": "VARCHAR", "nullable": False},
    {"name": "c_comment", "data_type": "VARCHAR", "nullable": False},
]


def arrow_stream(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def managed_connections_response() -> dict:
    return {
        "connections": [
            {
                "id": MANAGED_CONN,
                "name": MANAGED_NAME,
                "source_type": "managed",
            }
        ]
    }


def information_schema_response(
    table_name: str,
    schema_name: str,
    columns: list[dict],
    *,
    connection: str = MANAGED_CONN,
) -> dict:
    return {
        "count": 1,
        "has_more": False,
        "limit": 500,
        "next_cursor": None,
        "tables": [
            {
                "connection": connection,
                "schema": schema_name,
                "table": table_name,
                "synced": True,
                "last_sync": None,
                "columns": columns,
            }
        ],
    }


def mock_managed_create_table_flow(
    httpserver: HTTPServer,
    *,
    table_name: str,
    schema_name: str = PUBLIC,
    columns: list[dict],
    on_upload: Callable[[Request], Response] | None = None,
    table_exists: bool = False,
) -> None:
    httpserver.expect_request("/v1/connections").respond_with_json(managed_connections_response())

    def default_upload(req: Request) -> Response:
        assert req.headers["Content-Type"] == "application/parquet"
        return Response(
            json.dumps(
                {
                    "id": "upl_1",
                    "status": "ready",
                    "size_bytes": len(req.get_data()),
                    "created_at": "2026-01-01T00:00:00Z",
                    "content_type": "application/parquet",
                }
            ),
            status=201,
            content_type="application/json",
        )

    httpserver.expect_request("/v1/files", method="POST").respond_with_handler(
        on_upload or default_upload
    )

    def on_load(req: Request) -> Response:
        body = req.get_json()
        assert body == {"mode": "replace", "upload_id": "upl_1"}
        return Response(
            json.dumps(
                {
                    "connection_id": MANAGED_CONN,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "row_count": 1,
                    "arrow_schema_json": "{}",
                }
            ),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_request(
        f"/v1/connections/{MANAGED_CONN}/schemas/{schema_name}/tables/{table_name}/loads",
        method="POST",
    ).respond_with_handler(on_load)

    info_calls = {"n": 0}

    def on_information_schema(req: Request) -> Response:
        info_calls["n"] += 1
        if table_exists or info_calls["n"] > 1:
            payload = information_schema_response(table_name, schema_name, columns)
        else:
            payload = {
                "count": 0,
                "tables": [],
                "has_more": False,
                "next_cursor": None,
                "limit": 500,
            }
        return Response(json.dumps(payload), status=200, content_type="application/json")

    httpserver.expect_request("/v1/information_schema").respond_with_handler(on_information_schema)


def test_connect_via_url(httpserver: HTTPServer, srv: str):
    url = f"hotdata://127.0.0.1:{httpserver.port}?token=tok&workspace_id=ws_demo&verify_ssl=false"
    con = ibis.connect(url)
    assert getattr(con, "name", "") == "hotdata"


def test_connect_via_url_password_token(httpserver: HTTPServer):
    token = "secret_pass"
    url = f"hotdata://u:{token}@127.0.0.1:{httpserver.port}/?workspace_id=ws_pw&verify_ssl=false"
    con = ibis.connect(url)
    assert getattr(con, "name", "") == "hotdata"


def test_sql_execution(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/query", method="POST").respond_with_json(
        {
            "query_run_id": "run1",
            "status": "queued",
            "status_url": "http://poll",
            "reason": None,
        },
        status=202,
    )
    httpserver.expect_request("/v1/query-runs/run1").respond_with_json(
        {
            "created_at": "2026-01-01T00:00:00Z",
            "snapshot_id": "snap",
            "sql_hash": "h",
            "sql_text": "select 1",
            "status": "succeeded",
            "result_id": "res1",
            "id": "run1",
        }
    )
    httpserver.expect_request("/v1/results/res1").respond_with_data(
        arrow_stream(pa.table({"x": [1]})),
        status=200,
        content_type="application/vnd.apache.arrow.stream",
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection=TPCH_CONN,
        default_schema=TPCH_SF1,
    )

    tbl = con.sql("SELECT 1 AS x", dialect="postgres")
    pdf = tbl.execute()
    assert list(pdf["x"]) == [1]


def test_to_pyarrow_uses_arrow_result(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/query", method="POST").respond_with_json(
        {
            "query_run_id": "run1",
            "status": "queued",
            "status_url": "http://poll",
            "reason": None,
        },
        status=202,
    )
    httpserver.expect_request("/v1/query-runs/run1").respond_with_json(
        {
            "created_at": "2026-01-01T00:00:00Z",
            "snapshot_id": "snap",
            "sql_hash": "h",
            "sql_text": "select 1",
            "status": "succeeded",
            "result_id": "res1",
            "id": "run1",
        }
    )
    httpserver.expect_request("/v1/results/res1").respond_with_data(
        arrow_stream(pa.table({"x": [1, 2]})),
        status=200,
        content_type="application/vnd.apache.arrow.stream",
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection=TPCH_CONN,
        default_schema=TPCH_SF1,
    )

    tbl = con.sql("SELECT 1 AS x", dialect="postgres")
    out = con.to_pyarrow(tbl)
    assert out.to_pydict() == {"x": [1, 2]}


def test_to_pyarrow_batches_uses_arrow_result(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/query", method="POST").respond_with_json(
        {
            "query_run_id": "run1",
            "status": "queued",
            "status_url": "http://poll",
            "reason": None,
        },
        status=202,
    )
    httpserver.expect_request("/v1/query-runs/run1").respond_with_json(
        {
            "created_at": "2026-01-01T00:00:00Z",
            "snapshot_id": "snap",
            "sql_hash": "h",
            "sql_text": "select 1",
            "status": "succeeded",
            "result_id": "res1",
            "id": "run1",
        }
    )
    httpserver.expect_request("/v1/results/res1").respond_with_data(
        arrow_stream(pa.table({"x": [1, 2, 3]})),
        status=200,
        content_type="application/vnd.apache.arrow.stream",
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection=TPCH_CONN,
        default_schema=TPCH_SF1,
    )

    tbl = con.sql("SELECT 1 AS x", dialect="postgres")
    with con.to_pyarrow_batches(tbl, chunk_size=2) as reader:
        out = reader.read_all()
    assert out.to_pydict() == {"x": [1, 2, 3]}


def test_create_table_from_pandas_uploads_managed_table(httpserver: HTTPServer, srv: str):
    uploaded: dict[str, pa.Table] = {}

    def on_upload(req: Request) -> Response:
        uploaded["table"] = pq.read_table(io.BytesIO(req.get_data()))
        return Response(
            json.dumps(
                {
                    "id": "upl_1",
                    "status": "ready",
                    "size_bytes": len(req.get_data()),
                    "created_at": "2026-01-01T00:00:00Z",
                    "content_type": "application/parquet",
                }
            ),
            status=201,
            content_type="application/json",
        )

    mock_managed_create_table_flow(
        httpserver,
        table_name="demo",
        columns=[{"name": "x", "data_type": "BIGINT", "nullable": True}],
        on_upload=on_upload,
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
    )

    table = con.create_table(
        "demo",
        pd.DataFrame({"x": [1, 2]}),
        database=(MANAGED_CONN, PUBLIC),
    )

    assert uploaded["table"].to_pydict() == {"x": [1, 2]}
    assert table.schema().names == ("x",)


def test_create_table_from_pyarrow_uploads_managed_table(httpserver: HTTPServer, srv: str):
    uploaded: dict[str, pa.Table] = {}

    def on_upload(req: Request) -> Response:
        uploaded["table"] = pq.read_table(io.BytesIO(req.get_data()))
        return Response(
            json.dumps(
                {
                    "id": "upl_1",
                    "status": "ready",
                    "size_bytes": len(req.get_data()),
                    "created_at": "2026-01-01T00:00:00Z",
                    "content_type": "application/parquet",
                }
            ),
            status=201,
            content_type="application/json",
        )

    mock_managed_create_table_flow(
        httpserver,
        table_name="arrow_demo",
        columns=[
            {"name": "x", "data_type": "BIGINT", "nullable": True},
            {"name": "y", "data_type": "VARCHAR", "nullable": True},
        ],
        on_upload=on_upload,
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    expr = con.create_table(
        "arrow_demo",
        pa.table({"x": [1], "y": ["a"]}),
        database=(MANAGED_NAME, PUBLIC),
    )

    assert uploaded["table"].to_pydict() == {"x": [1], "y": ["a"]}
    assert expr.schema().names == ("x", "y")


def test_create_table_schema_only_uploads_empty_parquet(httpserver: HTTPServer, srv: str):
    uploaded: dict[str, pa.Table] = {}

    def on_upload(req: Request) -> Response:
        uploaded["table"] = pq.read_table(io.BytesIO(req.get_data()))
        return Response(
            json.dumps(
                {
                    "id": "upl_1",
                    "status": "ready",
                    "size_bytes": len(req.get_data()),
                    "created_at": "2026-01-01T00:00:00Z",
                    "content_type": "application/parquet",
                }
            ),
            status=201,
            content_type="application/json",
        )

    mock_managed_create_table_flow(
        httpserver,
        table_name="empty_demo",
        columns=[
            {"name": "x", "data_type": "BIGINT", "nullable": True},
            {"name": "y", "data_type": "VARCHAR", "nullable": True},
        ],
        on_upload=on_upload,
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection=MANAGED_CONN,
        default_schema=PUBLIC,
    )
    expr = con.create_table("empty_demo", schema=ibis.schema({"x": "int64", "y": "string"}))

    assert uploaded["table"].num_rows == 0
    assert uploaded["table"].schema.names == ["x", "y"]
    assert expr.schema().names == ("x", "y")


def test_create_table_rejects_unsupported_options(httpserver: HTTPServer, srv: str):
    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)

    with pytest.raises(NotImplementedError, match="temporary"):
        con.create_table("tmp", pd.DataFrame({"x": [1]}), temp=True)
    with pytest.raises(com.IbisInputError, match="Requires database"):
        con.create_table("tmp", pd.DataFrame({"x": [1]}))
    with pytest.raises(com.IbisInputError, match="only one of obj or schema"):
        con.create_table(
            "tmp",
            pd.DataFrame({"x": [1]}),
            schema=ibis.schema({"x": "int64"}),
        )
    with pytest.raises(com.IbisInputError, match="pandas.DataFrame or pyarrow.Table"):
        con.create_table("tmp", obj=[{"x": 1}])


def test_create_table_overwrite_loads_with_replace(httpserver: HTTPServer, srv: str):
    mock_managed_create_table_flow(
        httpserver,
        table_name="demo",
        columns=[{"name": "x", "data_type": "BIGINT", "nullable": True}],
        table_exists=True,
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    con.create_table(
        "demo",
        pd.DataFrame({"x": [1]}),
        database=(MANAGED_CONN, PUBLIC),
        overwrite=True,
    )


def test_create_table_without_overwrite_rejects_existing_table(httpserver: HTTPServer, srv: str):
    mock_managed_create_table_flow(
        httpserver,
        table_name="demo",
        columns=[{"name": "x", "data_type": "BIGINT", "nullable": True}],
        table_exists=True,
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)

    with pytest.raises(com.IbisInputError, match="already exists"):
        con.create_table(
            "demo",
            pd.DataFrame({"x": [1]}),
            database=(MANAGED_CONN, PUBLIC),
        )


def test_create_database_posts_managed_connection(httpserver: HTTPServer, srv: str):
    def on_create(req: Request) -> Response:
        body = req.get_json()
        assert body == {
            "name": "sales",
            "source_type": "managed",
            "config": {
                "schemas": [{"name": "public", "tables": [{"name": "orders"}]}],
            },
            "skip_discovery": True,
        }
        return Response(
            json.dumps(
                {
                    "id": MANAGED_CONN,
                    "name": "sales",
                    "source_type": "managed",
                    "discovery_status": "skipped",
                    "tables_discovered": 0,
                }
            ),
            status=201,
            content_type="application/json",
        )

    httpserver.expect_request("/v1/connections", method="GET").respond_with_json({"connections": []})
    httpserver.expect_request("/v1/connections", method="POST").respond_with_handler(on_create)

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    con.create_database("sales", schema="public", tables=["orders"])


def test_drop_table_deletes_managed_table(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(managed_connections_response())
    httpserver.expect_request(
        f"/v1/connections/{MANAGED_CONN}/schemas/{PUBLIC}/tables/demo",
        method="DELETE",
    ).respond_with_data(b"", status=204)

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    con.drop_table("demo", database=(MANAGED_CONN, PUBLIC))


def test_drop_table_force_ignores_missing_table(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(managed_connections_response())
    httpserver.expect_request(
        f"/v1/connections/{MANAGED_CONN}/schemas/{PUBLIC}/tables/missing",
        method="DELETE",
    ).respond_with_data(b"not found", status=404)

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    con.drop_table("missing", database=(MANAGED_CONN, PUBLIC), force=True)


def test_drop_table_raises_for_non_managed_connection(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(
        {"connections": [{"id": TPCH_CONN, "name": "TPC-H", "source_type": "duckdb"}]}
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)

    with pytest.raises(com.IbisInputError, match="not a managed database"):
        con.drop_table("demo", database=(TPCH_CONN, PUBLIC))


def test_drop_database_deletes_managed_connection(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(managed_connections_response())
    httpserver.expect_request(f"/v1/connections/{MANAGED_CONN}", method="DELETE").respond_with_data(
        b"", status=204
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    con.drop_database(MANAGED_NAME)


def test_compile_scalar_no_roundtrip(httpserver: HTTPServer, srv: str):
    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection=TPCH_CONN,
        default_schema=TPCH_SF1,
    )
    expr = ibis.literal(41) + ibis.literal(1)
    sql = con.compile(expr)
    assert isinstance(sql, str)
    assert "41" in sql.replace(" ", "")


def test_information_schema_discovery(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(
        {"connections": [{"id": TPCH_CONN, "name": "TPC-H SF1", "source_type": "duckdb"}]}
    )
    payload = {
        "count": 1,
        "has_more": False,
        "limit": 500,
        "next_cursor": None,
        "tables": [
            {
                "connection": TPCH_CONN,
                "schema": TPCH_SF1,
                "table": "customer",
                "synced": True,
                "last_sync": None,
                "columns": TPCH_CUSTOMER_COLS,
            },
        ],
    }
    httpserver.expect_request("/v1/information_schema").respond_with_json(payload)

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    assert con.list_catalogs() == [TPCH_CONN]
    assert con.list_databases(catalog=TPCH_CONN) == [TPCH_SF1]
    assert con.list_tables(database=(TPCH_CONN, TPCH_SF1)) == ["customer"]
    expr = con.table("customer", database=(TPCH_CONN, TPCH_SF1))
    assert set(expr.columns) == {c["name"] for c in TPCH_CUSTOMER_COLS}


def test_information_schema_pagination_merges_pages(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(
        {"connections": [{"id": TPCH_CONN, "name": "TPC-H SF1", "source_type": "duckdb"}]}
    )

    rows_a = [
        {
            "connection": TPCH_CONN,
            "schema": TPCH_SF1,
            "table": "customer",
            "synced": True,
            "columns": None,
        },
    ]
    rows_b = [
        {
            "connection": TPCH_CONN,
            "schema": TPCH_SF1,
            "table": "lineitem",
            "synced": True,
            "columns": None,
        },
    ]
    calls = {"n": 0}

    def page(req: Request) -> Response:
        calls["n"] += 1
        if "cursor=page2" not in req.query_string.decode():
            payload = {
                "count": 1,
                "has_more": True,
                "limit": 500,
                "next_cursor": "page2",
                "tables": rows_a,
            }
        else:
            payload = {
                "count": 1,
                "has_more": False,
                "limit": 500,
                "next_cursor": None,
                "tables": rows_b,
            }
        return Response(
            json.dumps(payload),
            status=200,
            content_type="application/json",
            headers={"Content-Type": "application/json"},
        )

    httpserver.expect_request("/v1/information_schema").respond_with_handler(page)

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    names = con.list_tables(database=(TPCH_CONN, TPCH_SF1))
    assert names == ["customer", "lineitem"]
    assert calls["n"] == 2


def test_table_not_found(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/information_schema").respond_with_json(
        {"count": 0, "tables": [], "has_more": False, "next_cursor": None, "limit": 500},
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection=TPCH_CONN,
        default_schema=TPCH_SF1,
    )
    with pytest.raises(com.TableNotFound):
        con.table("gone", database=(TPCH_CONN, TPCH_SF1))


def test_list_tables_regex_like(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(
        {"connections": [{"id": TPCH_CONN, "name": "TPC-H SF1", "source_type": "duckdb"}]}
    )
    tbls = [
        {
            "connection": TPCH_CONN,
            "schema": TPCH_SF1,
            "table": "customer",
            "synced": True,
            "columns": None,
        },
        {
            "connection": TPCH_CONN,
            "schema": TPCH_SF1,
            "table": "lineitem",
            "synced": True,
            "columns": None,
        },
        {
            "connection": TPCH_CONN,
            "schema": TPCH_SF1,
            "table": "nation",
            "synced": True,
            "columns": None,
        },
    ]
    httpserver.expect_request("/v1/information_schema").respond_with_json(
        {
            "count": 3,
            "tables": tbls,
            "has_more": False,
            "next_cursor": None,
            "limit": 500,
        },
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)
    out = con.list_tables(database=(TPCH_CONN, TPCH_SF1), like=r"^(customer|lineitem)$")
    assert out == ["customer", "lineitem"]


def test_ambiguous_default_connection(httpserver: HTTPServer, srv: str):
    httpserver.expect_request("/v1/connections").respond_with_json(
        {
            "connections": [
                {"id": "a", "name": "one", "source_type": "x"},
                {"id": "b", "name": "two", "source_type": "y"},
            ],
        },
    )

    con = ibis.hotdata.connect(api_url=srv, token="tok", workspace_id="ws", verify_ssl=False)

    with pytest.raises(com.IbisInputError, match="Multiple"):
        _ = con.current_catalog


def test_x_session_header_on_query(httpserver: HTTPServer, srv: str):
    seen: list[str | None] = []

    def on_post(req: Request) -> Response:
        seen.append(req.headers.get("X-Session-Id"))
        return Response(
            json.dumps(
                {
                    "query_run_id": "run1",
                    "status": "queued",
                    "status_url": "http://poll",
                    "reason": None,
                }
            ),
            status=202,
            content_type="application/json",
        )

    httpserver.expect_request("/v1/query", method="POST").respond_with_handler(on_post)
    httpserver.expect_request("/v1/query-runs/run1").respond_with_json(
        {
            "created_at": "2026-01-01T00:00:00Z",
            "snapshot_id": "snap",
            "sql_hash": "h",
            "sql_text": "select 0",
            "status": "succeeded",
            "result_id": "res1",
            "id": "run1",
        }
    )
    httpserver.expect_request("/v1/results/res1").respond_with_data(
        arrow_stream(pa.table({"n": [0]})),
        status=200,
        content_type="application/vnd.apache.arrow.stream",
    )

    con = ibis.hotdata.connect(
        api_url=srv,
        token="tok",
        workspace_id="ws",
        session_id="sb_xyz",
        verify_ssl=False,
        default_connection=TPCH_CONN,
        default_schema=TPCH_SF1,
    )

    pdf = con.execute(ibis.literal(0).name("n"))
    assert pdf == 0
    assert len(seen) >= 1
    assert all(h == "sb_xyz" for h in seen)
