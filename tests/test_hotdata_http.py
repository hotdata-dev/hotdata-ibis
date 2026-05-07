from __future__ import annotations

import pytest
from pytest_httpserver import HTTPServer

from ibis_hotdata.http import HotdataAPIError, HotdataClient


def test_execute_query_async_poll(httpserver: HTTPServer):
    httpserver.expect_oneshot_request("/v1/query", method="POST").respond_with_json(
        {"query_run_id": "run1", "status": "queued", "status_url": "", "reason": None},
        status=202,
    )
    httpserver.expect_oneshot_request("/v1/query-runs/run1").respond_with_json(
        {
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
