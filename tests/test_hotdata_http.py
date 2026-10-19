from __future__ import annotations

import io
import json

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from conftest import mock_presigned_upload_flow
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request, Response

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


def test_execute_query_forwards_database_id_to_poll_and_result(httpserver: HTTPServer):
    """hotdata SDK >=0.6 requires X-Database-Id on get_query_run/get_result; the same
    database_id used to submit the query must be forwarded to both follow-up calls.
    """

    def on_query(req: Request) -> Response:
        assert req.headers["X-Database-Id"] == "db_sales"
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

    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_handler(on_query)

    def on_query_run(req: Request) -> Response:
        assert req.headers["X-Database-Id"] == "db_sales"
        return Response(
            json.dumps({**_QR_META, "status": "succeeded", "result_id": "res1", "id": "run1"}),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_oneshot_request("/v1/query-runs/run1").respond_with_handler(on_query_run)

    table = pa.table({"n": [42]})
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    arrow_blob = sink.getvalue()

    def on_result(req: Request) -> Response:
        assert req.headers["X-Database-Id"] == "db_sales"
        return Response(arrow_blob, status=200, content_type=APPLICATION_ARROW_STREAM)

    httpserver.expect_oneshot_request("/v1/results/res1").respond_with_handler(on_result)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="tok",
        workspace_id="ws1",
        verify_ssl=False,
    )
    body = client.execute_query(
        "select 41+1",
        database_id="db_sales",
        poll_interval_s=0,
        poll_timeout_s=5,
    )
    client.close()

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
    mock_presigned_upload_flow(httpserver, upload_id="upl_1")

    def on_load(req: Request) -> Response:
        body = req.get_json()
        assert body == {"async": False, "mode": "replace", "upload_id": "upl_1"}
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
    assert up["upload_id"] == "upl_1"
    loaded = client.load_managed_table(
        "conn_sales",
        "public",
        "demo_tbl",
        upload_id=up["upload_id"],
    )
    assert loaded["table_name"] == "demo_tbl"
    client.close()


def test_create_managed_database(httpserver: HTTPServer):
    def on_create(req: Request) -> Response:
        body = req.get_json()
        assert body == {
            "name": "sales",
            "schemas": [{"name": "public", "tables": [{"name": "orders"}]}],
        }
        return Response(
            json.dumps(
                {
                    "id": "db_sales",
                    "name": "sales",
                    "default_catalog": "default",
                    "default_schema": "main",
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
    def on_create_session(req: Request) -> Response:
        assert req.get_json()["content_type"] == "application/parquet"
        return Response(
            json.dumps(
                {
                    "mode": "single",
                    "url": httpserver.url_for("/mock-storage/upl_1"),
                    "headers": {},
                    "upload_id": "upl_1",
                    "finalize_token": "tok_1",
                }
            ),
            status=201,
            content_type="application/json",
        )

    mock_presigned_upload_flow(httpserver, upload_id="upl_1", on_create_session=on_create_session)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    out = client.upload_file(b"parquet", content_type="application/parquet")
    assert out["upload_id"] == "upl_1"
    client.close()


def _arrow_blob(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _client(httpserver: HTTPServer) -> HotdataClient:
    return HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="tok",
        workspace_id="ws1",
        verify_ssl=False,
    )


def test_execute_query_inline_arrow_single_request(httpserver: HTTPServer):
    """A query that finishes in time returns its rows in the POST response itself."""

    def on_query(req: Request) -> Response:
        assert req.headers["Accept"] == APPLICATION_ARROW_STREAM
        assert req.get_json() == {"async": True, "async_after_ms": 1000, "sql": "select 42"}
        return Response(
            _arrow_blob(pa.table({"n": [42]})),
            status=200,
            content_type=APPLICATION_ARROW_STREAM,
            headers={
                "X-Query-Run-Id": "run1",
                "X-Result-Id": "res1",
                "X-Execution-Time-Ms": "7",
                "X-Query-Warning": "careful",
            },
        )

    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_handler(on_query)

    client = _client(httpserver)
    body = client.execute_query("select 42")
    client.close()

    assert body["pa_table"].to_pydict() == {"n": [42]}
    assert body["result_id"] == "res1"
    assert body["query_run_id"] == "run1"
    assert body["execution_time_ms"] == 7
    assert body["warning"] == "careful"
    httpserver.check_assertions()


def test_execute_query_async_after_ms_none_sends_pure_async(httpserver: HTTPServer):
    def on_query(req: Request) -> Response:
        assert req.get_json() == {"async": True, "sql": "select 1"}
        return Response(
            _arrow_blob(pa.table({"n": [1]})), status=200, content_type=APPLICATION_ARROW_STREAM
        )

    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_handler(on_query)
    client = _client(httpserver)
    assert client.execute_query("select 1", async_after_ms=None)["pa_table"].num_rows == 1
    client.close()


def test_execute_query_too_large_to_inline_fetches_named_result(httpserver: HTTPServer):
    """202 with X-Result-Id: read the result directly, skipping the query-run poll."""
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {"query_run_id": "run1", "status": "running", "status_url": "http://poll"},
        status=202,
        headers={"X-Result-Id": "res1"},
    )
    httpserver.expect_oneshot_request("/v1/results/res1").respond_with_data(
        _arrow_blob(pa.table({"n": [1, 2]})), status=200, content_type=APPLICATION_ARROW_STREAM
    )

    client = _client(httpserver)
    body = client.execute_query("select n", poll_interval_s=0, poll_timeout_s=5)
    client.close()

    assert body["pa_table"].to_pydict() == {"n": [1, 2]}
    httpserver.check_assertions()


def test_execute_query_inline_error_carries_status(httpserver: HTTPServer):
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {"error": {"message": "syntax error"}}, status=400
    )
    client = _client(httpserver)
    with pytest.raises(HotdataAPIError, match="syntax error") as info:
        client.execute_query("selec 1")
    client.close()
    assert info.value.status_code == 400


def test_client_reads_credentials_from_env(httpserver: HTTPServer, monkeypatch):
    monkeypatch.setenv("HOTDATA_API_URL", httpserver.url_for("/").rstrip("/"))
    monkeypatch.setenv("HOTDATA_API_KEY", "envtok")
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_env")

    def on_query(req: Request) -> Response:
        assert req.headers["X-Workspace-Id"] == "ws_env"
        return Response(
            _arrow_blob(pa.table({"n": [1]})), status=200, content_type=APPLICATION_ARROW_STREAM
        )

    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_handler(on_query)
    client = HotdataClient(verify_ssl=False)
    assert client.execute_query("select 1")["pa_table"].num_rows == 1
    client.close()
    httpserver.check_assertions()


@pytest.mark.parametrize(
    ("missing", "match"),
    [("HOTDATA_API_KEY", "HOTDATA_API_KEY"), ("HOTDATA_WORKSPACE", "HOTDATA_WORKSPACE")],
)
def test_client_missing_credentials_raises(monkeypatch, missing, match):
    monkeypatch.setenv("HOTDATA_API_KEY", "envtok")
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_env")
    monkeypatch.delenv(missing)
    with pytest.raises(HotdataAPIError, match=match):
        HotdataClient()


_INDEX = {
    "index_name": "docs_idx",
    "index_type": "vector",
    "columns": ["embedding"],
    "algorithm": "ivf",
    "metric": "cosine",
    "status": "ready",
    "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:00:00Z",
}


def test_create_index_drops_unset_options(httpserver: HTTPServer):
    def on_create(req: Request) -> Response:
        assert req.get_json() == {
            "index_name": "docs_idx",
            "columns": ["embedding"],
            "index_type": "vector",
            "algorithm": "ivf",
            "metric": "cosine",
            "async": False,
        }
        return Response(json.dumps(_INDEX), status=201, content_type="application/json")

    httpserver.expect_oneshot_request(
        "/v1/connections/conn1/tables/main/docs/indexes", method="POST"
    ).respond_with_handler(on_create)

    client = _client(httpserver)
    out = client.create_index(
        "conn1",
        "main",
        "docs",
        index_name="docs_idx",
        columns=["embedding"],
        index_type="vector",
        algorithm="ivf",
        metric="cosine",
        nlist=None,
    )
    client.close()
    assert out["algorithm"] == "ivf"
    httpserver.check_assertions()


def test_create_index_rejects_invalid_option_before_sending(httpserver: HTTPServer):
    client = _client(httpserver)
    with pytest.raises(HotdataAPIError, match="Invalid index request"):
        client.create_index("c", "s", "t", index_name="i", columns=["x"], algorithm="bogus")
    client.close()
