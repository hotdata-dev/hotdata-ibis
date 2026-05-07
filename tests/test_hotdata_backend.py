from __future__ import annotations

import ibis

pytest = __import__("pytest")

pytest.importorskip("pytest_httpserver")
from pytest_httpserver import HTTPServer


def _srv_base(httpserver: HTTPServer) -> str:
    return httpserver.url_for("/").rstrip("/")


def test_connect_via_url(httpserver: HTTPServer):
    url = (
        f"hotdata://127.0.0.1:{httpserver.port}"
        "?token=tok&workspace_id=ws_demo&verify_ssl=false"
    )
    con = ibis.connect(url)
    assert getattr(con, "name", "") == "hotdata"


def test_sql_execution(httpserver: HTTPServer):
    body = {
        "columns": ["x"],
        "nullable": [False],
        "rows": [[1]],
        "row_count": 1,
        "execution_time_ms": 3,
        "query_run_id": "qr-sync",
        "result_id": None,
        "warning": None,
    }
    httpserver.expect_request("/v1/query", method="POST").respond_with_json(body)

    con = ibis.hotdata.connect(
        api_url=_srv_base(httpserver),
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
        default_connection="c1",
        default_schema="public",
    )

    tbl = con.sql("SELECT 1 AS x", dialect="postgres")
    pdf = tbl.execute()
    assert list(pdf["x"]) == [1]


def test_information_schema_discovery(httpserver: HTTPServer):
    httpserver.expect_request("/v1/connections").respond_with_json(
        {"connections": [{"id": "cnx_pg", "name": "warehouse", "source_type": "postgres"}]}
    )
    payload = {
        "count": 1,
        "has_more": False,
        "limit": 500,
        "next_cursor": None,
        "tables": [
            {
                "connection": "cnx_pg",
                "schema": "public",
                "table": "orders",
                "synced": True,
                "last_sync": None,
                "columns": [
                    {"name": "id", "data_type": "BIGINT", "nullable": False},
                    {"name": "sku", "data_type": "VARCHAR", "nullable": True},
                ],
            }
        ],
    }
    httpserver.expect_request("/v1/information_schema").respond_with_json(payload)

    con = ibis.hotdata.connect(
        api_url=_srv_base(httpserver),
        token="tok",
        workspace_id="ws",
        verify_ssl=False,
    )
    assert con.list_catalogs() == ["cnx_pg"]
    assert con.list_databases(catalog="cnx_pg") == ["public"]
    assert con.list_tables(database=("cnx_pg", "public")) == ["orders"]
    expr = con.table("orders", database=("cnx_pg", "public"))
    assert set(expr.columns) == {"id", "sku"}
