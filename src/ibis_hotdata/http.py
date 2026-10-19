"""HTTP access to Hotdata via the official ``hotdata`` Python SDK (OpenAPI client)."""

from __future__ import annotations

import http
import io
import json
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypeVar

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow_hotfix  # noqa: F401

# Top-level ``ConnectionsApi``/``DatabasesApi`` retry loads while the table is locked.
from hotdata import ApiClient, Configuration, ConnectionsApi, DatabasesApi
from hotdata.api import (
    IndexesApi,
    InformationSchemaApi,
    QueryApi,
    QueryRunsApi,
    ResultsApi,
)
from hotdata.exceptions import ApiException
from hotdata.models import QueryRequest
from hotdata.models.create_database_request import CreateDatabaseRequest
from hotdata.models.create_index_request import CreateIndexRequest
from hotdata.models.database_default_schema_decl import DatabaseDefaultSchemaDecl
from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl
from hotdata.models.load_managed_table_request import LoadManagedTableRequest
from hotdata.uploads import UploadError, UploadsApi

T = TypeVar("T")

# Matches Hotdata / runtimedb ``GET /v1/results/{{id}}`` Arrow responses.
APPLICATION_ARROW_STREAM = "application/vnd.apache.arrow.stream"

# Statuses that mean the query run is still in progress.
# runtimedb QueryRunStatus only emits "running", "succeeded", "failed".
_IN_FLIGHT = {"running"}


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


def _from_upload_error(exc: UploadError) -> HotdataAPIError:
    """Map the presigned-upload flow's ``UploadError`` (session/storage/finalize
    failures) onto our own error type, same shape as ``_from_api_exception``.
    """
    return HotdataAPIError(f"Hotdata upload error: {exc}", status_code=getattr(exc, "status", None))


def _from_raw_error(status: int, reason: str | None, body: bytes) -> HotdataAPIError:
    """Error for a non-success response read without the SDK's deserialization."""
    text = body.decode("utf-8", errors="replace") if body else ""
    msg = f"Hotdata API error: {reason or status}"
    if text:
        msg = f"{msg} {text}"
    return HotdataAPIError(msg.strip(), status_code=status, body=text)


def _int_header(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


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
        api_url: str | None = None,
        token: str | None = None,
        workspace_id: str | None = None,
        timeout: float = 120.0,
        verify_ssl: bool | str = True,
    ) -> None:
        # Unset values fall back to HOTDATA_API_URL / HOTDATA_API_KEY /
        # HOTDATA_WORKSPACE (resolved by the SDK's Configuration).
        host = api_url.rstrip("/") if api_url else None
        conf = Configuration(host=host, api_key=token, workspace_id=workspace_id)
        if not conf.api_key:
            raise HotdataAPIError("Missing Hotdata token: pass token= or set HOTDATA_API_KEY")
        if not conf.api_keys.get("WorkspaceId"):
            raise HotdataAPIError(
                "Missing Hotdata workspace: pass workspace_id= or set HOTDATA_WORKSPACE"
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
        self._databases = DatabasesApi(self._client)
        self._information_schema = InformationSchemaApi(self._client)
        self._indexes = IndexesApi(self._client)
        self._uploads = UploadsApi(self._client)

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
        """GET ``/v1/information_schema``.

        ``params`` uses REST names (``schema`` not ``var_schema``).
        """
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
        database_id: str | None = None,
        async_after_ms: int | None = 1000,
        poll_interval_s: float = 0.25,
        poll_timeout_s: float = 600.0,
    ) -> dict[str, Any]:
        """Run ``sql`` and return its result as an Arrow payload.

        Asks for an inline Arrow IPC response: a query that finishes within
        ``async_after_ms`` returns its rows in this one request. A slower or
        larger query falls back to polling the query run and its result.
        ``async_after_ms=None`` skips the inline attempt (always async).
        """
        req = QueryRequest(sql=sql, var_async=True)
        if async_after_ms is not None:
            req.async_after_ms = async_after_ms
        kwargs: dict[str, Any] = {}
        if database_id is not None:
            kwargs["x_database_id"] = database_id
        try:
            raw = self._query.query_without_preload_content(
                req,
                _headers={"Accept": APPLICATION_ARROW_STREAM},
                _request_timeout=self._timeout,
                **kwargs,
            )
        except ApiException as exc:
            raise _from_api_exception(exc) from exc
        body = raw.read()
        status = raw.status
        headers = raw.headers
        ctype = (headers.get("Content-Type") or "").split(";")[0].strip().lower()
        effective_database_id = database_id or ""
        deadline = time.monotonic() + poll_timeout_s

        if status == http.HTTPStatus.OK and ctype == APPLICATION_ARROW_STREAM.lower():
            return self._arrow_payload_from_table(
                _ipc_stream_bytes_to_table(body),
                result_id=headers.get("X-Result-Id"),
                query_run_id=headers.get("X-Query-Run-Id"),
                execution_time_ms=_int_header(headers.get("X-Execution-Time-Ms")),
                warning=headers.get("X-Query-Warning"),
            )

        if status in (http.HTTPStatus.OK, http.HTTPStatus.ACCEPTED):
            d = _json_utf8(body) if body else {}
            # Result too large to inline: the server names the result to fetch.
            result_id = headers.get("X-Result-Id") or (
                d.get("result_id") if status == http.HTTPStatus.OK else None
            )
            if result_id:
                return self._poll_result_arrow(
                    result_id,
                    database_id=effective_database_id,
                    deadline=deadline,
                    poll_interval_s=poll_interval_s,
                )
            query_run_id = d.get("query_run_id")
            if query_run_id:
                return self._poll_query_run(
                    query_run_id,
                    database_id=effective_database_id,
                    deadline=deadline,
                    poll_interval_s=poll_interval_s,
                )
            raise HotdataAPIError("Unexpected query response type", status_code=status, body=d)

        raise _from_raw_error(status, raw.reason, body)

    def _poll_query_run(
        self,
        query_run_id: str,
        *,
        database_id: str,
        deadline: float,
        poll_interval_s: float,
    ) -> dict[str, Any]:
        while time.monotonic() < deadline:
            qr = self._safe_call(
                self._query_runs.get_query_run,
                query_run_id,
                x_database_id=database_id,
            )
            status = qr.status
            if status == "failed":
                raise HotdataAPIError(qr.error_message or "Query run failed")
            if status == "succeeded":
                result_id = qr.result_id
                if result_id is None:
                    raise HotdataAPIError("succeeded query run missing result_id")
                return self._poll_result_arrow(
                    result_id,
                    database_id=database_id,
                    deadline=deadline,
                    poll_interval_s=poll_interval_s,
                )
            if status not in _IN_FLIGHT:
                raise HotdataAPIError(f"Unexpected query run status: {status!r}")
            _sleep_until(deadline, poll_interval_s)
        raise HotdataAPIError("Timeout waiting for asynchronous query")

    def upload_file(self, data: bytes, *, content_type: str | None = None) -> dict[str, Any]:
        """Direct-to-storage presigned upload: create session, ``PUT``, finalize.

        Returns the finalized upload record (``upload_id`` is what managed-table
        loads need) -- see ``hotdata.uploads.UploadsApi.upload_file``.
        """
        try:
            resp = self._uploads.upload_file(
                data, content_type=content_type, request_timeout=self._timeout
            )
        except UploadError as exc:
            raise _from_upload_error(exc) from exc
        except ApiException as exc:
            raise _from_api_exception(exc) from exc
        return resp.model_dump(by_alias=True, mode="json")

    def list_databases(self) -> dict[str, Any]:
        """GET ``/v1/databases``."""
        out = self._safe_call(self._databases.list_databases)
        return out.model_dump(by_alias=True, mode="json")

    def get_database(self, database_id: str) -> dict[str, Any]:
        """GET ``/v1/databases/{database_id}``."""
        out = self._safe_call(self._databases.get_database, database_id)
        return out.model_dump(by_alias=True, mode="json")

    def create_managed_database(
        self,
        name: str | None = None,
        *,
        schema: str = "public",
        tables: Sequence[str] = (),
    ) -> dict[str, Any]:
        """POST ``/v1/databases`` — creates an instant database.

        The database is created with an auto-provisioned default catalog.
        """
        schemas = None
        if tables:
            schemas = [
                DatabaseDefaultSchemaDecl(
                    name=schema,
                    tables=[DatabaseDefaultTableDecl(name=t) for t in tables],
                )
            ]
        req = CreateDatabaseRequest(name=name, schemas=schemas)
        resp = self._safe_call(self._databases.create_database, req)
        return resp.model_dump(by_alias=True, mode="json")

    def delete_database(self, database_id: str) -> None:
        """DELETE ``/v1/databases/{database_id}``."""
        self._safe_call(self._databases.delete_database, database_id)

    def load_managed_table(
        self,
        connection_id: str,
        schema: str,
        table: str,
        *,
        upload_id: str,
    ) -> dict[str, Any]:
        req = LoadManagedTableRequest(mode="replace", upload_id=upload_id, var_async=False)
        resp = self._safe_call(
            self._connections.load_managed_table,
            connection_id,
            schema,
            table,
            req,
        )
        return resp.model_dump(by_alias=True, mode="json")

    def delete_managed_table(self, connection_id: str, schema: str, table: str) -> None:
        self._safe_call(
            self._connections.delete_managed_table,
            connection_id,
            schema,
            table,
        )

    def create_index(
        self,
        connection_id: str,
        schema: str,
        table: str,
        *,
        index_name: str,
        columns: Sequence[str],
        **options: Any,
    ) -> dict[str, Any]:
        """POST ``.../tables/{table}/indexes`` (synchronous build).

        ``options`` are ``CreateIndexRequest`` fields; ``None`` values are dropped.
        """
        fields = {k: v for k, v in options.items() if v is not None}
        try:
            req = CreateIndexRequest(index_name=index_name, columns=list(columns), **fields)
        except ValueError as exc:  # pydantic: unknown field or invalid value
            raise HotdataAPIError(f"Invalid index request: {exc}") from exc
        resp = self._safe_call(self._indexes.create_index, connection_id, schema, table, req)
        return resp.model_dump(by_alias=True, mode="json")

    def list_indexes(self, connection_id: str, schema: str, table: str) -> list[dict[str, Any]]:
        """GET ``.../tables/{table}/indexes``."""
        resp = self._safe_call(self._indexes.list_indexes, connection_id, schema, table)
        return [i.model_dump(by_alias=True, mode="json") for i in resp.indexes]

    def delete_index(self, connection_id: str, schema: str, table: str, index_name: str) -> None:
        """DELETE ``.../tables/{table}/indexes/{index_name}``."""
        self._safe_call(self._indexes.delete_index, connection_id, schema, table, index_name)

    def _poll_result_arrow(
        self,
        result_id: str,
        *,
        database_id: str,
        deadline: float,
        poll_interval_s: float,
    ) -> dict[str, Any]:
        """Poll ``GET /v1/results/{{id}}`` with ``Accept: application/vnd.apache.arrow.stream``."""
        while time.monotonic() < deadline:
            try:
                raw = self._results.get_result_without_preload_content(
                    result_id,
                    x_database_id=database_id,
                    _headers={"Accept": APPLICATION_ARROW_STREAM},
                    _request_timeout=self._timeout,
                )
            except ApiException as exc:
                raise _from_api_exception(exc) from exc
            body = raw.read()
            status = raw.status
            ctype = (raw.headers.get("Content-Type") or "").split(";")[0].strip().lower()

            if status == http.HTTPStatus.OK and ctype == APPLICATION_ARROW_STREAM.lower():
                table = _ipc_stream_bytes_to_table(body)
                return self._arrow_payload_from_table(table, result_id=result_id)

            if status == http.HTTPStatus.ACCEPTED:
                _sleep_until(deadline, poll_interval_s)
                continue

            if status == http.HTTPStatus.CONFLICT:
                d = _json_utf8(body) if body else {}
                raise HotdataAPIError(
                    d.get("error_message") or "Result failed",
                    status_code=http.HTTPStatus.CONFLICT,
                    body=d,
                )

            if status == http.HTTPStatus.NOT_FOUND:
                d = _json_utf8(body) if body else {}
                raise HotdataAPIError(
                    d.get("detail") or f"Result {result_id!r} not found",
                    status_code=http.HTTPStatus.NOT_FOUND,
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
        result_id: str | None,
        query_run_id: str | None = None,
        execution_time_ms: int | None = None,
        warning: str | None = None,
    ) -> dict[str, Any]:
        sch = table.schema
        columns = sch.names
        nullable = [field.nullable for field in sch]
        return {
            "format": "arrow",
            "pa_table": table,
            "columns": columns,
            "nullable": nullable,
            "rows": [],
            "result_id": result_id,
            "row_count": table.num_rows,
            "execution_time_ms": execution_time_ms,
            "query_run_id": query_run_id,
            "warning": warning,
        }
