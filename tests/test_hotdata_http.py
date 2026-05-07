from __future__ import annotations

import json

import pytest
from werkzeug.wrappers import Request, Response
from pytest_httpserver import HTTPServer

from ibis_hotdata.http import HotdataAPIError, HotdataClient


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

    preview = {
        "columns": ["n"],
        "nullable": [True],
        "rows": [[42]],
        "row_count": 1,
        "execution_time_ms": 1,
        "query_run_id": "qr",
        "result_id": "res1",
        "warning": None,
        "status": "ready",
    }
    httpserver.expect_oneshot_request("/v1/results/res1").respond_with_json(preview)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="tok",
        workspace_id="ws1",
        verify_ssl=False,
    )
    body = client.execute_query(
        "select 41+1",
        prefer_async=True,
        poll_interval_s=0,
        poll_timeout_s=5,
    )
    client.close()

    assert body["columns"] == ["n"]
    assert body["rows"] == [[42]]


def test_sync_error_raises(httpserver: HTTPServer):
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
        client.execute_query("select 1", prefer_async=False)
    client.close()


def test_sync_200_pad_shorter_nullable_array(httpserver: HTTPServer):
    body = {
        "columns": ["a", "b", "c"],
        "nullable": [False],
        "rows": [[1, 2, 3]],
        "row_count": 1,
        "execution_time_ms": 0,
        "query_run_id": "qr",
        "result_id": None,
        "warning": None,
    }
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(body)
    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    out = client.execute_query("select 1", prefer_async=False)
    assert len(out["nullable"]) == 3
    assert out["nullable"][0] is False


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
        client.execute_query("select junk", prefer_async=True, poll_interval_s=0, poll_timeout_s=2)
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


def test_upload_file_then_create_dataset(httpserver: HTTPServer):
    httpserver.expect_oneshot_request(
        "/v1/files",
        method="POST",
    ).respond_with_json(
        {
            "id": "upl_1",
            "status": "ready",
            "size_bytes": 3,
            "created_at": "2026-01-01T00:00:00Z",
            "content_type": None,
        },
        status=201,
    )

    def on_dataset(req: Request) -> Response:
        body = req.get_json()
        assert body["label"] == "demo"
        assert body["source"] == {"upload_id": "upl_1", "format": "csv"}
        assert body.get("table_name") == "demo_tbl"
        payload = {
            "id": "ds_1",
            "label": "demo",
            "schema_name": "main",
            "table_name": "demo_tbl",
            "status": "ready",
            "created_at": "2026-01-01T00:00:00Z",
        }
        return Response(json.dumps(payload), status=201, content_type="application/json")

    httpserver.expect_oneshot_request("/v1/datasets", method="POST").respond_with_handler(on_dataset)

    client = HotdataClient(
        api_url=httpserver.url_for("/").rstrip("/"),
        token="t",
        workspace_id="w",
        verify_ssl=False,
    )
    up = client.upload_file(b"a,b\n1,2")
    assert up["id"] == "upl_1"
    ds = client.create_dataset_from_upload(
        upload_id=up["id"],
        label="demo",
        table_name="demo_tbl",
        file_format="csv",
    )
    assert ds["schema_name"] == "main"
    assert ds["table_name"] == "demo_tbl"
    client.close()
