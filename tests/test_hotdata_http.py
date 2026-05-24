from __future__ import annotations

import io
import json

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from werkzeug.wrappers import Request, Response
from pytest_httpserver import HTTPServer

from ibis_hotdata.http import APPLICATION_ARROW_STREAM, HotdataAPIError, HotdataClient


_QR_META = {
    "created_at": "2026-01-01T00:00:00Z",
    "snapshot_id": "snap",
    "sql_hash": "h",
    "sql_text": "select 1",
}


def test_execute_query_async_poll(httpserver: HTTPServer):
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {
            "query_run_id": "run1",
            "status": "queued",
            "status_url": "http://poll",
            "reason": None,
        },
        status=202,
    )
    httpserver.expect_oneshot_request("/v1/query-runs/run1").respond_with_json(
        {
            **_QR_META,
            "status": "succeeded",
            "result_id": "res1",
            "id": "run1",
        }
    )

    table = pa.table({"n": [42]})
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    arrow_blob = sink.getvalue()

    httpserver.expect_oneshot_request("/v1/results/res1").respond_with_data(
        arrow_blob,
        status=200,
        content_type=APPLICATION_ARROW_STREAM,
    )

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="tok",
        workspace_id="ws1",
        verify_ssl=False,
    )
    body = client.execute_query(
        "select 41+1",
        poll_interval_s=0,
        poll_timeout_s=5,
    )
    client.close()

    assert body["format"] == "arrow"
    assert body["pa_table"].to_pydict() == {"n": [42]}


def test_query_error_raises(httpserver: HTTPServer):
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {"detail": "bad"}, status=500
    )
    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    with pytest.raises(HotdataAPIError):
        client.execute_query("select 1")
    client.close()


def test_result_arrow_poll_handles_accepted_result(httpserver: HTTPServer):
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {
            "query_run_id": "run1",
            "status": "queued",
            "status_url": "http://poll",
            "reason": None,
        },
        status=202,
    )
    httpserver.expect_oneshot_request("/v1/query-runs/run1").respond_with_json(
        {
            **_QR_META,
            "status": "succeeded",
            "result_id": "res1",
            "id": "run1",
        }
    )
    httpserver.expect_oneshot_request("/v1/results/res1").respond_with_json(
        {"result_id": "res1", "status": "processing"},
        status=202,
    )

    table = pa.table({"n": [42]})
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    httpserver.expect_oneshot_request("/v1/results/res1").respond_with_data(
        sink.getvalue(),
        status=200,
        content_type=APPLICATION_ARROW_STREAM,
    )

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    out = client.execute_query("select 1", poll_interval_s=0, poll_timeout_s=5)
    assert out["pa_table"].to_pydict() == {"n": [42]}
    client.close()


def test_async_query_run_failure(httpserver: HTTPServer):
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {
            "query_run_id": "bad",
            "status": "accepted",
            "status_url": "http://poll",
            "reason": None,
        },
        status=202,
    )
    httpserver.expect_oneshot_request("/v1/query-runs/bad").respond_with_json(
        {**_QR_META, "status": "failed", "error_message": "boom", "id": "bad"}
    )
    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    with pytest.raises(HotdataAPIError, match="boom"):
        client.execute_query(
            "select junk",
            poll_interval_s=0,
            poll_timeout_s=2,
        )
    client.close()


def test_list_connections_raises_on_http_error(httpserver: HTTPServer):
    httpserver.expect_request("/v1/connections").respond_with_data("nope", status=503)
    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    with pytest.raises(HotdataAPIError):
        client.list_connections()
    client.close()


def test_upload_file_then_load_managed_table(httpserver: HTTPServer):
    httpserver.expect_oneshot_request(
        "/v1/files",
        method="POST",
    ).respond_with_json(
        {
            "id": "upl_1",
            "status": "ready",
            "size_bytes": 3,
            "created_at": "2026-01-01T00:00:00Z",
            "content_type": "application/parquet",
        },
        status=201,
    )

    def on_load(req: Request) -> Response:
        body = req.get_json()
        assert body == {"mode": "replace", "upload_id": "upl_1"}
        payload = {
            "connection_id": "conn_sales",
            "schema_name": "public",
            "table_name": "demo_tbl",
            "row_count": 1,
            "arrow_schema_json": "{}",
        }
        return Response(json.dumps(payload), status=200, content_type="application/json")

    httpserver.expect_oneshot_request(
        "/v1/connections/conn_sales/schemas/public/tables/demo_tbl/loads",
        method="POST",
    ).respond_with_handler(on_load)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    up = client.upload_file(b"parquet", content_type="application/parquet")
    assert up["id"] == "upl_1"
    loaded = client.load_managed_table(
        "conn_sales",
        "public",
        "demo_tbl",
        upload_id=up["id"],
    )
    assert loaded["table_name"] == "demo_tbl"
    client.close()


def test_create_managed_database(httpserver: HTTPServer):
    def on_create(req: Request) -> Response:
        body = req.get_json()
        assert body == {
            "description": "sales",
            "schemas": [{"name": "public", "tables": [{"name": "orders"}]}],
        }
        return Response(
            json.dumps(
                {
                    "id": "db_sales",
                    "description": "sales",
                    "default_connection_id": "conn_sales",
                    "expires_at": None,
                }
            ),
            status=201,
            content_type="application/json",
        )

    httpserver.expect_oneshot_request("/v1/databases", method="POST").respond_with_handler(
        on_create
    )

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    out = client.create_managed_database("sales", schema="public", tables=["orders"])
    assert out["id"] == "db_sales"
    assert out["default_connection_id"] == "conn_sales"
    client.close()


def test_delete_managed_table_and_database(httpserver: HTTPServer):
    httpserver.expect_oneshot_request(
        "/v1/connections/conn_sales/schemas/public/tables/demo",
        method="DELETE",
    ).respond_with_data(b"", status=204)
    httpserver.expect_oneshot_request(
        "/v1/databases/db_sales",
        method="DELETE",
    ).respond_with_data(b"", status=204)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    client.delete_managed_table("conn_sales", "public", "demo")
    client.delete_database("db_sales")
    client.close()


def test_upload_file_accepts_content_type(httpserver: HTTPServer):
    def on_upload(req: Request) -> Response:
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

    httpserver.expect_oneshot_request("/v1/files", method="POST").respond_with_handler(on_upload)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    out = client.upload_file(b"parquet", content_type="application/parquet")
    assert out["id"] == "upl_1"
    client.close()
