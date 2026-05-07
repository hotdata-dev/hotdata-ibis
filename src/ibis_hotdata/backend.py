"""Ibis backend for Hotdata (federated SQL over HTTP).

Identifier mapping:

* **Ibis catalog** ↔ Hotdata ``connection_id`` returned by ``GET /v1/connections`` /
  ``connection`` on ``GET /v1/information_schema`` rows.
* **Ibis database** ↔ remote **schema name** surfaced by Hotdata (`schema` column).
* **Ibis table name** ↔ remote table name (`table` column).

Use fully qualified SQL in Hotdata exactly as documented for federated Postgres-style
references (typically ``connection.schema.table``, quoting if needed).

See the README for dialect and typing limitations.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable, Mapping
from functools import cached_property
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qsl, unquote_plus

import ibis.backends.sql.compilers as sc
import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import sqlglot as sg
import sqlglot.expressions as sge
from ibis.backends import (
    CanListCatalog,
    CanListDatabase,
    HasCurrentCatalog,
    HasCurrentDatabase,
    NoExampleLoader,
)
from ibis.backends.sql import SQLBackend

from ibis_hotdata.http import HotdataAPIError, HotdataClient
from ibis_hotdata.types import dtype_from_hotdata_sql_type, dtype_from_json_value

_INFORMATION_SCHEMA_PAGE_SIZE = 500


def _ibis_err_from_hotdata(exc: HotdataAPIError) -> com.IbisError:
    return com.IbisError(str(exc))


if TYPE_CHECKING:
    from collections.abc import Iterator

    import pandas as pd


class HotdataRowsCursor:
    """DB-API–like cursor backed by prefetched rows (used by `_fetch_from_cursor`)."""

    def __init__(self, rows: list) -> None:
        self._rows = rows
        self._idx = 0

    def fetchmany(self, size: int = 1024) -> list:
        start = self._idx
        end = min(self._idx + size, len(self._rows))
        self._idx = end
        return [tuple(r) for r in self._rows[start:end]]

    def fetchall(self) -> list:
        return [tuple(r) for r in self._rows[self._idx :]]

    def close(self) -> None:
        self._idx = len(self._rows)

    def __iter__(self) -> Iterator[tuple]:
        while self._idx < len(self._rows):
            row = self._rows[self._idx]
            self._idx += 1
            yield tuple(row)


class Backend(
    SQLBackend,
    CanListCatalog,
    CanListDatabase,
    HasCurrentCatalog,
    HasCurrentDatabase,
    NoExampleLoader,
):
    """Hotdata-backed Ibis client."""

    name = "hotdata"
    compiler = sc.postgres.compiler
    supports_python_udfs = False
    supports_temporary_tables = False

    _http: HotdataClient
    _default_connection: str | None
    _default_schema: str | None

    def _from_url(self, url: ParseResult, **kwarg_overrides: Any):
        """Connect using ``hotdata://host`` or ``hotdata://host/path`` URLs.

        * Base URL defaults to ``https://{host}`` plus optional leading ``path``.
        * Query string may include ``token``, ``workspace_id``, ``session_id``,
          ``timeout``, ``verify_ssl`` (``true`` / ``false``), ``default_connection``,
          ``default_schema``, ``prefer_async``.
        * If ``token`` is omitted, ``urlparse`` password (`user:TOKEN@`) is accepted.
        """
        q = dict(parse_qsl(url.query, keep_blank_values=True))
        q.update(kwarg_overrides)

        netloc = url.netloc
        path_prefix = url.path.rstrip("/")
        if not netloc:
            raise com.IbisError(
                "hotdata:// URL requires a network location, e.g. hotdata://api.hotdata.dev/"
            )

        verify = q.pop("verify_ssl", None)
        if verify is None:
            verify_ssl: bool | str = True
        elif isinstance(verify, str):
            verify_ssl = verify.lower() in ("true", "1", "yes")
        else:
            verify_ssl = bool(verify)

        timeout = float(q.pop("timeout", "120"))
        api_url = q.pop("api_url", None) or ("https://" + netloc + path_prefix)

        token = q.pop("token", None) or (unquote_plus(url.password) if url.password else None)
        workspace_id = q.pop("workspace_id", None)

        prefer_async_s = q.pop("prefer_async", "false")

        kwargs = dict(
            api_url=api_url,
            token=token,
            workspace_id=workspace_id,
            session_id=q.pop("session_id", None),
            timeout=timeout,
            verify_ssl=verify_ssl,
            default_connection=q.pop("default_connection", None),
            default_schema=q.pop("default_schema", None),
            prefer_async=str(prefer_async_s).lower() in ("true", "1", "yes"),
            poll_interval_s=float(q.pop("poll_interval_s", "0.25")),
            poll_timeout_s=float(q.pop("poll_timeout_s", "600")),
        )

        self._convert_kwargs(kwargs)
        if not kwargs.get("token"):
            raise com.IbisError("Hotdata URL missing token (query parameter or password segment)")
        if not kwargs.get("workspace_id"):
            raise com.IbisError("Hotdata URL missing workspace_id query parameter")

        return self.connect(**kwargs)

    def do_connect(
        self,
        *,
        api_url: str,
        token: str,
        workspace_id: str,
        session_id: str | None = None,
        timeout: float = 120.0,
        verify_ssl: bool | str = True,
        default_connection: str | None = None,
        default_schema: str | None = None,
        prefer_async: bool = False,
        poll_interval_s: float = 0.25,
        poll_timeout_s: float = 600.0,
    ) -> None:
        """Create an Ibis client for a Hotdata workspace.

        Parameters
        ----------
        api_url
            Hotdata API base URL (e.g. ``https://api.hotdata.dev``).
        token
            API bearer token (``Authorization`` header).
        workspace_id
            Workspace public id (``X-Workspace-Id`` header).
        session_id
            Optional sandbox id (``X-Session-Id`` header).
        timeout
            HTTP timeout in seconds (per request).
        verify_ssl
            Passed through to the Hotdata SDK configuration (boolean or path to a CA bundle).
        default_connection
            Optional default **catalog** (Hotdata connection id). If omitted and the
            workspace exposes exactly one connection, it is chosen automatically;
            otherwise you must set this (or use fully qualified ``database=``).
        default_schema
            Optional default **database** (remote schema name). If omitted and only
            one schema exists for the default connection, it is chosen automatically.
        prefer_async
            When True, requests ``async: true`` on ``POST /v1/query`` (with polling).
        poll_interval_s
            Sleep between ``GET /v1/query-runs/{id}`` polls.
        poll_timeout_s
            Max wall-clock time spent waiting on async submissions.
        """
        self.disconnect()
        self._default_connection = default_connection
        self._default_schema = default_schema
        self._prefer_async = prefer_async
        self._poll_interval_s = poll_interval_s
        self._poll_timeout_s = poll_timeout_s

        self._http = HotdataClient(
            api_url=api_url,
            token=token,
            workspace_id=workspace_id,
            session_id=session_id,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )

    def disconnect(self) -> None:
        if getattr(self, "_http", None) is not None:
            self._http.close()

    # --- hierarchy ---------------------------------------------------------

    def _infer_default_connection(self) -> str:
        ids = self._connection_ids()
        if self._default_connection is not None:
            return self._default_connection
        if len(ids) == 1:
            self._default_connection = ids[0]
            return self._default_connection
        raise com.IbisInputError(
            "Multiple Hotdata connections in this workspace — pass default_connection="
            "... when connecting or qualify tables with database=('conn_id','schema')."
        )

    def _infer_default_schema(self, connection_id: str) -> str:
        schemas = sorted(
            {
                row["schema"]
                for row in self._iterate_information_schema(
                    {"connection_id": connection_id}, include_columns=False
                )
            }
        )
        if self._default_schema is not None:
            if self._default_schema not in schemas:
                raise com.IbisInputError(
                    f"Unknown schema {self._default_schema!r} for connection {connection_id!r}"
                )
            return self._default_schema
        if len(schemas) == 1:
            self._default_schema = schemas[0]
            return self._default_schema
        raise com.IbisInputError(
            "Could not infer default schema — pass default_schema=... when connecting"
            " or qualify with database=('connection_id','schema')."
        )

    @property
    def current_catalog(self) -> str:
        return self._infer_default_connection()

    @property
    def current_database(self) -> str:
        return self._infer_default_schema(self.current_catalog)

    # --- catalogs / databases ----------------------------------------------

    def _to_catalog_db_tuple(self, table_loc: sge.Table):
        """Use the compiler SQL dialect when stringifying qualifiers (backend name is not a dialect)."""

        dialect = self.dialect
        if (sg_cat := table_loc.args["catalog"]) is not None:
            sg_cat.args["quoted"] = False
            sg_cat = sg_cat.sql(dialect=dialect)
        if (sg_db := table_loc.args["db"]) is not None:
            sg_db.args["quoted"] = False
            sg_db = sg_db.sql(dialect=dialect)

        return sg_cat, sg_db

    def _connection_ids(self) -> list[str]:
        data = self._http.list_connections()
        return [c["id"] for c in data["connections"]]

    def list_catalogs(self, *, like: str | None = None) -> list[str]:
        names = self._connection_ids()
        return self._filter_with_like(names, like)

    def list_databases(
        self,
        *,
        like: str | None = None,
        catalog: str | None = None,
    ) -> list[str]:
        conn = catalog or self.current_catalog
        schemas = sorted(
            {
                row["schema"]
                for row in self._iterate_information_schema(
                    {"connection_id": conn}, include_columns=False
                )
            }
        )
        return self._filter_with_like(list(schemas), like)

    def list_tables(
        self,
        *,
        like: str | None = None,
        database: tuple[str, str] | str | None = None,
    ) -> list[str]:
        loc = database if database is not None else (self.current_catalog, self.current_database)
        table_loc = self._to_sqlglot_table(loc)
        catalog_part, schema_part = self._to_catalog_db_tuple(table_loc)
        params: dict[str, Any] = {}
        if catalog_part:
            params["connection_id"] = catalog_part
        if schema_part:
            params["schema"] = schema_part

        tables = sorted(
            {
                row["table"]
                for row in self._iterate_information_schema(params, include_columns=False)
            }
        )
        return self._filter_with_like(tables, like)

    def _iterate_information_schema(
        self, filters: Mapping[str, Any], *, include_columns: bool
    ) -> Iterable[dict[str, Any]]:
        cursor: str | None = None
        while True:
            params: dict[str, Any] = dict(filters)
            params["limit"] = _INFORMATION_SCHEMA_PAGE_SIZE
            params["include_columns"] = include_columns
            if cursor:
                params["cursor"] = cursor
            chunk = self._http.get_information_schema(params)
            yield from chunk["tables"]
            if not chunk.get("has_more"):
                break
            cursor = chunk.get("next_cursor")
            if cursor is None:
                break

    # --- schema / sql execution --------------------------------------------

    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        conn = catalog or self.current_catalog
        schema_name = database or self.current_database
        matches: list[dict[str, Any]] = []
        for row in self._iterate_information_schema(
            {"connection_id": conn, "schema": schema_name, "table": table_name},
            include_columns=True,
        ):
            if row["table"] == table_name and row["schema"] == schema_name:
                matches.append(row)
        if not matches:
            fqn = sg.table(table_name, db=schema_name, catalog=conn).sql(self.compiler.dialect)
            raise com.TableNotFound(fqn)

        columns = matches[0].get("columns") or []
        mapping: dict[str, dt.DataType] = {}
        for col in columns:
            name = col["name"]
            sql_t = col.get("data_type")
            null = bool(col.get("nullable", True))
            mapping[name] = dtype_from_hotdata_sql_type(sql_t, nullable=null)
        return sch.Schema(mapping)

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        stripped = query.strip().rstrip(";")
        preview_sql = f"SELECT * FROM ({stripped}) AS ibis_hotdata_preview LIMIT 1"
        try:
            data = self._http.execute_query(
                preview_sql,
                prefer_async=self._prefer_async,
                poll_interval_s=self._poll_interval_s,
                poll_timeout_s=self._poll_timeout_s,
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

        cols = data["columns"]
        nulls = data["nullable"]
        row0 = data["rows"][0] if data.get("rows") else None
        mapping: dict[str, dt.DataType] = {}
        for i, name in enumerate(cols):
            null = bool(nulls[i]) if i < len(nulls) else True
            if row0 is not None and i < len(row0):
                inferred = dtype_from_json_value(row0[i])
                if inferred is not None:
                    mapping[name] = inferred.copy(nullable=null)
                    continue
            mapping[name] = dt.String(nullable=null)
        return sch.Schema(mapping)

    @contextlib.contextmanager
    def _safe_raw_sql(
        self,
        query: str | sge.Expression,
    ) -> Iterator[HotdataRowsCursor]:
        if not isinstance(query, str):
            query = query.sql(dialect=self.compiler.dialect, pretty=True)

        try:
            payload = self._http.execute_query(
                query,
                prefer_async=self._prefer_async,
                poll_interval_s=self._poll_interval_s,
                poll_timeout_s=self._poll_timeout_s,
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

        cur = HotdataRowsCursor(payload["rows"])
        try:
            yield cur
        finally:
            cur.close()

    def _fetch_from_cursor(self, cursor, schema: sch.Schema) -> pd.DataFrame:
        import pandas as pd
        from ibis.formats.pandas import PandasData

        try:
            df = pd.DataFrame.from_records(iter(cursor), columns=schema.names, coerce_float=True)
        except Exception:
            cursor.close()
            raise
        df = PandasData.convert_table(df, schema)
        return df

    def upload_file(self, data: bytes) -> dict[str, Any]:
        """POST ``/v1/files``; returns the upload record (use ``id`` with :meth:`create_dataset_from_upload`)."""
        try:
            return self._http.upload_file(data)
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

    def create_dataset_from_upload(
        self,
        upload_id: str,
        label: str,
        *,
        table_name: str | None = None,
        file_format: str = "csv",
    ) -> dict[str, Any]:
        """POST ``/v1/datasets`` with an upload source—materializes a queryable dataset table.

        The response includes ``schema_name`` and ``table_name``. Reference the table in SQL as
        ``datasets.<schema_name>.<table_name>`` (see Hotdata ``datasets`` documentation).
        """
        try:
            return self._http.create_dataset_from_upload(
                upload_id=upload_id,
                label=label,
                table_name=table_name,
                file_format=file_format,
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

    def create_table(self, *_args: Any, **_kwargs: Any) -> ir.Table:
        raise NotImplementedError(
            "Hotdata does not implement Ibis create_table in v1; use upload_file + "
            "create_dataset_from_upload, then SQL or con.table with the returned names."
        )

    def drop_table(self, *_args: Any, **_kwargs: Any) -> None:
        raise NotImplementedError("Hotdata backend does not implement drop_table in v1.")

    def _register_in_memory_table(self, _op: ops.InMemoryTable) -> None:
        return

    @cached_property
    def version(self) -> str:
        try:
            v = pkg_version("ibis-hotdata")
        except PackageNotFoundError:
            v = "0.0.0"
        return f"ibis-hotdata {v} (Hotdata /v1/query)"
