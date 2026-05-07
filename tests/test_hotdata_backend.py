from __future__ import annotations

import json

import ibis
import ibis.common.exceptions as com
import pytest
from werkzeug.wrappers import Request, Response

pytest.importorskip("pytest_httpserver")
from pytest_httpserver import HTTPServer

# Federated identifiers for mocked Hotdata (matches SQL shape ``tpch.tpch_sf1.customer``).
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


def test_connect_via_url(httpserver: HTTPServer, srv: str):
    url = (
        f"hotdata://127.0.0.1:{httpserver.port}"
        "?token=tok&workspace_id=ws_demo&verify_ssl=false"
    )
    con = ibis.connect(url)
    assert getattr(con, "name", "") == "hotdata"


def test_connect_via_url_password_token(httpserver: HTTPServer):
    token = "secret_pass"
    url = (
        f"hotdata://u:{token}@127.0.0.1:{httpserver.port}"
        "/?workspace_id=ws_pw&verify_ssl=false"
    )
    con = ibis.connect(url)
    assert getattr(con, "name", "") == "hotdata"


def test_sql_execution(httpserver: HTTPServer, srv: str):
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

    sync = {
        "columns": ["n"],
        "nullable": [True],
        "rows": [[0]],
        "row_count": 1,
        "execution_time_ms": 1,
        "query_run_id": "qr",
        "result_id": None,
        "warning": None,
    }

    def on_post(req: Request) -> Response:
        seen.append(req.headers.get("X-Session-Id"))
        return Response(json.dumps(sync), status=200, content_type="application/json")

    httpserver.expect_request("/v1/query", method="POST").respond_with_handler(on_post)

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
