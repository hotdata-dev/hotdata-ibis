"""HTTP access to Hotdata via the official ``hotdata`` Python SDK (OpenAPI client)."""

from __future__ import annotations

import io
import json
import time
from collections.abc import Callable, Mapping
from typing import Any, TypeVar

import pyarrow as pa
import pyarrow_hotfix  # noqa: F401
import pyarrow.ipc as pa_ipc

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
from hotdata.models import CreateDatasetRequest, DatasetSource, QueryRequest
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.dataset_source_one_of import DatasetSourceOneOf

T = TypeVar("T")

# Matches Hotdata / runtimedb ``GET /v1/results/{{id}}`` Arrow responses.
APPLICATION_ARROW_STREAM = "application/vnd.apache.arrow.stream"


def _sleep_until(deadline: float, interval: float) -> None:
    """Sleep up to ``interval`` s but never past ``deadline`` (cleaner timeout behavior)."""
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(min(interval, remaining))


class HotdataAPIError(Exception):
    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _from_api_exception(exc: ApiException) -> HotdataAPIError:
    body = exc.body
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8", errors="replace")
    msg = f"Hotdata API error: {exc.reason}"
    if body:
        msg = f"{msg} {body}"
    return HotdataAPIError(msg.strip(), status_code=exc.status, body=exc.body)


def _ipc_stream_bytes_to_table(data: bytes) -> pa.Table:
    with pa_ipc.open_stream(io.BytesIO(data)) as reader:
        return reader.read_all()


def _json_utf8(obj: bytes) -> Any:
    return json.loads(obj.decode("utf-8"))


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
        conf = Configuration(
            host=host, api_key=token, workspace_id=workspace_id, session_id=session_id
        )
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
        client = self._client
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()
            return
        pool = getattr(getattr(client, "rest_client", None), "pool_manager", None)
        if pool is not None:
            pool.clear()

    def _safe_call(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
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
        async_after_ms: int | None = None,
        poll_interval_s: float = 0.25,
        poll_timeout_s: float = 600.0,
    ) -> dict[str, Any]:
        req = QueryRequest(sql=sql, var_async=True, async_after_ms=async_after_ms)
        out = self._safe_call(self._query.query, req)
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
                    return self._poll_result_arrow(
                        result_id,
                        deadline=deadline,
                        poll_interval_s=poll_interval_s,
                    )
                _sleep_until(deadline, poll_interval_s)
            raise HotdataAPIError("Timeout waiting for asynchronous query")
        raise HotdataAPIError("Unexpected query response type")

    def upload_file(self, data: bytes, *, content_type: str | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if content_type is not None:
            kwargs["_content_type"] = content_type
        resp = self._safe_call(self._uploads.upload_file, data, **kwargs)
        return resp.model_dump(by_alias=True, mode="json")

    def list_datasets(self, *, limit: int = 1000, offset: int = 0) -> dict[str, Any]:
        resp = self._safe_call(self._datasets.list_datasets, limit=limit, offset=offset)
        return resp.model_dump(by_alias=True, mode="json")

    def delete_dataset(self, dataset_id: str) -> None:
        self._safe_call(self._datasets.delete_dataset, dataset_id)

    def create_dataset_from_upload(
        self,
        *,
        upload_id: str,
        label: str,
        table_name: str | None = None,
        file_format: str = "csv",
    ) -> dict[str, Any]:
        src = DatasetSource(
            DatasetSourceOneOf(
                type="upload",
                upload_id=upload_id,
                format=file_format,
            )
        )
        fields: dict[str, Any] = {"label": label, "source": src}
        if table_name is not None:
            fields["table_name"] = table_name
        req = CreateDatasetRequest(**fields)
        resp = self._safe_call(self._datasets.create_dataset, req)
        return resp.model_dump(by_alias=True, mode="json")

    def _poll_result_arrow(
        self,
        result_id: str,
        *,
        deadline: float,
        poll_interval_s: float,
    ) -> dict[str, Any]:
        """Poll ``GET /v1/results/{{id}}`` with ``Accept: application/vnd.apache.arrow.stream``."""
        while time.monotonic() < deadline:
            try:
                raw = self._results.get_result_without_preload_content(
                    result_id,
                    _headers={"Accept": APPLICATION_ARROW_STREAM},
                    _request_timeout=self._timeout,
                )
            except ApiException as exc:
                raise _from_api_exception(exc) from exc
            body = raw.read()
            status = raw.status
            ctype = (raw.headers.get("Content-Type") or "").split(";")[0].strip().lower()

            if status == 200 and ctype == APPLICATION_ARROW_STREAM.lower():
                table = _ipc_stream_bytes_to_table(body)
                return self._arrow_payload_from_table(table, result_id=result_id)

            if status == 202:
                _sleep_until(deadline, poll_interval_s)
                continue

            if status == 409:
                d = _json_utf8(body) if body else {}
                raise HotdataAPIError(
                    d.get("error_message") or "Result failed",
                    status_code=409,
                    body=d,
                )

            if status == 404:
                d = _json_utf8(body) if body else {}
                raise HotdataAPIError(
                    d.get("detail") or f"Result {result_id!r} not found",
                    status_code=404,
                    body=d,
                )

            raise HotdataAPIError(
                f"Unexpected GET /v1/results/{result_id} status {status}",
                status_code=status,
                body=body,
            )

        raise HotdataAPIError("Timeout waiting for Arrow query result")

    def _arrow_payload_from_table(
        self,
        table: pa.Table,
        *,
        result_id: str,
    ) -> dict[str, Any]:
        sch = table.schema
        columns = sch.names
        nullable = [sch.field(i).nullable for i in range(len(columns))]
        return {
            "format": "arrow",
            "pa_table": table,
            "columns": columns,
            "nullable": nullable,
            "rows": [],
            "result_id": result_id,
            "row_count": table.num_rows,
            "execution_time_ms": None,
            "query_run_id": None,
            "warning": None,
        }
