"""HTTP access to Hotdata via the official ``hotdata`` Python SDK (OpenAPI client)."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any, MutableMapping

from hotdata import ApiClient, Configuration
from hotdata.api import (
    ConnectionsApi,
    DatasetsApi,
    InformationSchemaApi,
    QueryApi,
    QueryRunsApi,
    ResultsApi,
    UploadsApi,
)
from hotdata.exceptions import ApiException
from hotdata.models import CreateDatasetRequest, DatasetSource, QueryRequest, UploadDatasetSource
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.query_response import QueryResponse


class HotdataAPIError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _from_api_exception(exc: ApiException) -> HotdataAPIError:
    msg = f"Hotdata API error: {exc.reason}"
    if exc.body:
        msg = f"{msg} {exc.body}"
    return HotdataAPIError(msg.strip(), status_code=exc.status, body=exc.body)


class HotdataClient:
    """Thin wrapper around the SDK used by the Ibis backend."""

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
        host = api_url.rstrip("/")
        conf = Configuration(host=host, api_key=token, workspace_id=workspace_id, session_id=session_id)
        if verify_ssl is False:
            conf.verify_ssl = False
        elif isinstance(verify_ssl, str):
            conf.ssl_ca_cert = verify_ssl
        self._timeout = timeout
        self._client = ApiClient(conf)
        self._query = QueryApi(self._client)
        self._query_runs = QueryRunsApi(self._client)
        self._results = ResultsApi(self._client)
        self._connections = ConnectionsApi(self._client)
        self._information_schema = InformationSchemaApi(self._client)
        self._uploads = UploadsApi(self._client)
        self._datasets = DatasetsApi(self._client)

    def close(self) -> None:
        return

    def _safe_call(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, _request_timeout=self._timeout, **kwargs)
        except ApiException as exc:
            raise _from_api_exception(exc) from exc

    def list_connections(self) -> dict[str, Any]:
        """GET ``/v1/connections``."""
        out = self._safe_call(self._connections.list_connections)
        return out.model_dump(by_alias=True, mode="json")

    def get_information_schema(self, params: Mapping[str, Any]) -> dict[str, Any]:
        """GET ``/v1/information_schema`` — ``params`` uses REST names (``schema`` not ``var_schema``)."""
        out = self._safe_call(
            self._information_schema.information_schema,
            connection_id=params.get("connection_id"),
            var_schema=params.get("schema"),
            table=params.get("table"),
            include_columns=params.get("include_columns"),
            limit=params.get("limit"),
            cursor=params.get("cursor"),
        )
        return out.model_dump(by_alias=True, mode="json")

    def execute_query(
        self,
        sql: str,
        *,
        prefer_async: bool = False,
        async_after_ms: int | None = None,
        poll_interval_s: float = 0.25,
        poll_timeout_s: float = 600.0,
    ) -> dict[str, Any]:
        req = QueryRequest(sql=sql, var_async=prefer_async, async_after_ms=async_after_ms)
        out = self._safe_call(self._query.query, req)
        if isinstance(out, QueryResponse):
            return self._normalize_result_payload(out.model_dump(by_alias=True))
        if isinstance(out, AsyncQueryResponse):
            query_run_id = out.query_run_id
            deadline = time.monotonic() + poll_timeout_s
            while time.monotonic() < deadline:
                qr = self._safe_call(self._query_runs.get_query_run, query_run_id)
                status = qr.status
                if status == "failed":
                    raise HotdataAPIError(qr.error_message or "Query run failed")
                if status == "succeeded":
                    result_id = qr.result_id
                    if result_id is None:
                        raise HotdataAPIError("succeeded query run missing result_id")
                    return self._poll_result_ready(
                        result_id, deadline=deadline, poll_interval_s=poll_interval_s
                    )
                time.sleep(poll_interval_s)
            raise HotdataAPIError("Timeout waiting for asynchronous query")
        raise HotdataAPIError("Unexpected query response type")

    def upload_file(self, data: bytes) -> dict[str, Any]:
        resp = self._safe_call(self._uploads.upload_file, data)
        return resp.model_dump(by_alias=True, mode="json")

    def create_dataset_from_upload(
        self,
        *,
        upload_id: str,
        label: str,
        table_name: str | None = None,
        file_format: str = "csv",
    ) -> dict[str, Any]:
        src = DatasetSource(UploadDatasetSource(upload_id=upload_id, format=file_format))
        fields: dict[str, Any] = {"label": label, "source": src}
        if table_name is not None:
            fields["table_name"] = table_name
        req = CreateDatasetRequest(**fields)
        resp = self._safe_call(self._datasets.create_dataset, req)
        return resp.model_dump(by_alias=True, mode="json")

    def _poll_result_ready(
        self, result_id: str, *, deadline: float, poll_interval_s: float
    ) -> dict[str, Any]:
        while time.monotonic() < deadline:
            res = self._safe_call(self._results.get_result, result_id)
            d = res.model_dump(by_alias=True)
            st = d.get("status")
            if st == "failed":
                raise HotdataAPIError(d.get("error_message") or "Result failed")
            if st == "ready" or (d.get("rows") is not None and d.get("columns")):
                return self._normalize_result_payload(d)
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
