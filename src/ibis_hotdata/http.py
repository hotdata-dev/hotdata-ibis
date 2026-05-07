"""HTTP client for the Hotdata REST API."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any, MutableMapping

import httpx


class HotdataAPIError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class HotdataClient:
    """Thin synchronous HTTP wrapper for `/v1/*` endpoints used by the Ibis backend."""

    def __init__(
        self,
        *,
        api_url: str,
        token: str,
        workspace_id: str,
        session_id: str | None = None,
        timeout: float = 120.0,
        verify_ssl: bool | str = True,
    ) -> None:
        base = api_url.rstrip("/")
        headers: dict[str, str] = {
            "Authorization": f"Bearer {token}",
            "X-Workspace-Id": workspace_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if session_id:
            headers["X-Session-Id"] = session_id
        self._client = httpx.Client(base_url=base, headers=headers, timeout=timeout, verify=verify_ssl)

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
    ) -> httpx.Response:
        r = self._client.request(method, path, params=params, json=json)
        return r

    def get_json(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        r = self.request("GET", path, params=params)
        if r.is_error:
            raise HotdataAPIError(
                f"Hotdata GET {path} failed: {r.text}",
                status_code=r.status_code,
                body=r.text,
            )
        return r.json()

    def execute_query(
        self,
        sql: str,
        *,
        prefer_async: bool = False,
        async_after_ms: int | None = None,
        poll_interval_s: float = 0.25,
        poll_timeout_s: float = 600.0,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "sql": sql,
            "async": prefer_async,
            "async_after_ms": async_after_ms,
        }
        r = self.request("POST", "/v1/query", json=payload)
        if r.status_code == 200:
            return self._normalize_result_payload(r.json())
        if r.status_code == 202:
            body = r.json()
            query_run_id = body["query_run_id"]
            deadline = time.monotonic() + poll_timeout_s
            while time.monotonic() < deadline:
                qr = self.get_json(f"/v1/query-runs/{query_run_id}")
                status = qr.get("status")
                if status == "failed":
                    raise HotdataAPIError(qr.get("error_message") or "Query run failed")
                if status == "succeeded":
                    result_id = qr.get("result_id")
                    if result_id is None:
                        raise HotdataAPIError("succeeded query run missing result_id")
                    return self._poll_result_ready(
                        result_id, deadline=deadline, poll_interval_s=poll_interval_s
                    )
                time.sleep(poll_interval_s)
            raise HotdataAPIError("Timeout waiting for asynchronous query")

        raise HotdataAPIError(
            f"Hotdata POST /v1/query failed: {r.text}",
            status_code=r.status_code,
            body=r.text,
        )

    def _poll_result_ready(
        self, result_id: str, *, deadline: float, poll_interval_s: float
    ) -> dict[str, Any]:
        while time.monotonic() < deadline:
            res = self.get_json(f"/v1/results/{result_id}")
            st = res.get("status")
            if st == "failed":
                raise HotdataAPIError(res.get("error_message") or "Result failed")
            if st == "ready" or (res.get("rows") is not None and res.get("columns")):
                return self._normalize_result_payload(res)
            time.sleep(poll_interval_s)
        raise HotdataAPIError("Timeout waiting for query result payload")

    @staticmethod
    def _normalize_result_payload(data: MutableMapping[str, Any]) -> dict[str, Any]:
        columns = list(data["columns"])
        nullable = list(data.get("nullable") or [])
        if len(nullable) < len(columns):
            nullable.extend([True] * (len(columns) - len(nullable)))
        elif len(nullable) > len(columns):
            nullable = nullable[: len(columns)]

        return {
            "columns": columns,
            "nullable": nullable,
            "rows": list(data["rows"]) if data.get("rows") is not None else [],
            "row_count": data.get("row_count"),
            "execution_time_ms": data.get("execution_time_ms"),
            "query_run_id": data.get("query_run_id"),
            "result_id": data.get("result_id"),
            "warning": data.get("warning"),
        }
