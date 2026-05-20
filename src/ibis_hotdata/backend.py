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
import io
from collections.abc import Iterable, Mapping, Sequence
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
    CanCreateDatabase,
    CanListCatalog,
    CanListDatabase,
    HasCurrentCatalog,
    HasCurrentDatabase,
    NoExampleLoader,
)
from ibis.backends.sql import SQLBackend

from ibis_hotdata.http import HotdataAPIError, HotdataClient
from ibis_hotdata.managed import DEFAULT_SCHEMA, MANAGED_SOURCE_TYPE
from ibis_hotdata.types import dtype_from_hotdata_sql_type

_INFORMATION_SCHEMA_PAGE_SIZE = 500


def _ibis_err_from_hotdata(exc: HotdataAPIError) -> com.IbisError:
    return com.IbisError(str(exc))


if TYPE_CHECKING:
    from collections.abc import Iterator

    import pandas as pd


class Backend(
    SQLBackend,
    CanCreateDatabase,
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
          ``default_schema``, ``poll_interval_s``, ``poll_timeout_s``.
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

        kwargs = dict(
            api_url=api_url,
            token=token,
            workspace_id=workspace_id,
            session_id=q.pop("session_id", None),
            timeout=timeout,
            verify_ssl=verify_ssl,
            default_connection=q.pop("default_connection", None),
            default_schema=q.pop("default_schema", None),
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
        poll_interval_s: float = 0.25,
        poll_timeout_s: float = 600.0,
    ) -> None:
        """Create an Ibis client for a Hotdata workspace.

        Query execution always uses Hotdata's async path and downloads ready
        results as Arrow IPC from ``GET /v1/results/{id}``.

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
        poll_interval_s
            Sleep between ``GET /v1/query-runs/{id}`` polls.
        poll_timeout_s
            Max wall-clock time spent waiting on async submissions.
        """
        self.disconnect()
        self._default_connection = default_connection
        self._default_schema = default_schema
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

    def _resolve_connection(self, name_or_id: str) -> dict[str, Any]:
        data = self._http.list_connections()
        for conn in data["connections"]:
            if conn["id"] == name_or_id or conn.get("name") == name_or_id:
                return conn
        raise com.IbisError(f"Unknown Hotdata connection {name_or_id!r}")

    def _resolve_managed_connection(self, name_or_id: str) -> dict[str, Any]:
        conn = self._resolve_connection(name_or_id)
        if conn.get("source_type") != MANAGED_SOURCE_TYPE:
            raise com.IbisInputError(
                f"{name_or_id!r} is not a managed database "
                f"(source_type={conn.get('source_type')!r})"
            )
        return conn

    def _managed_table_synced(
        self,
        connection_id: str,
        schema_name: str,
        table_name: str,
    ) -> bool:
        """Return True only if the table exists and its last load has completed.

        A table whose ``synced`` flag is False is still being loaded; we treat
        it as writable (returns False) so that an in-progress load can be
        retried without requiring ``overwrite=True``. Tables not present in the
        information schema also return False (not yet created).
        """
        for row in self._iterate_information_schema(
            {"connection_id": connection_id, "schema": schema_name, "table": table_name},
            include_columns=False,
        ):
            if row["table"] == table_name and row["schema"] == schema_name:
                return bool(row.get("synced", True))
        return False

    def _table_location(
        self,
        database: tuple[str, str] | str | None,
    ) -> tuple[str, str]:
        if database is None:
            if self._default_connection is None or self._default_schema is None:
                raise com.IbisInputError(
                    "Requires database=(catalog, schema) or default_connection and default_schema"
                )
            conn = self._default_connection
            schema = self._default_schema
        elif isinstance(database, tuple):
            conn, schema = database
        else:
            conn = self._default_connection or self.current_catalog
            schema = database
            if conn is None:
                raise com.IbisInputError(
                    "create_table with database=schema requires default_connection or current catalog"
                )
        conn_record = self._resolve_managed_connection(conn)
        return conn_record["id"], schema

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
                poll_interval_s=self._poll_interval_s,
                poll_timeout_s=self._poll_timeout_s,
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

        from ibis.formats.pyarrow import PyArrowSchema

        return PyArrowSchema.to_ibis(data["pa_table"].schema)

    @contextlib.contextmanager
    def _safe_raw_sql(
        self,
        query: str | sge.Expression,
    ) -> Iterator[Any]:
        if not isinstance(query, str):
            query = query.sql(dialect=self.compiler.dialect, pretty=True)

        try:
            payload = self._http.execute_query(
                query,
                poll_interval_s=self._poll_interval_s,
                poll_timeout_s=self._poll_timeout_s,
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

        yield payload["pa_table"]

    def _fetch_from_cursor(self, cursor, schema: sch.Schema) -> pd.DataFrame:
        from ibis.formats.pandas import PandasData

        df = cursor.to_pandas()
        return PandasData.convert_table(df, schema)

    def to_pyarrow(
        self,
        expr: ir.Expr,
        /,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        **kwargs: Any,
    ):
        self._run_pre_execute_hooks(expr)
        table_expr = expr.as_table()
        sql = self.compile(table_expr, params=params, limit=limit, **kwargs)
        try:
            payload = self._http.execute_query(
                sql,
                poll_interval_s=self._poll_interval_s,
                poll_timeout_s=self._poll_timeout_s,
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc
        table = payload["pa_table"]
        arrow_schema = table_expr.schema().to_pyarrow()
        table = table.rename_columns(list(table_expr.columns)).cast(arrow_schema)
        return expr.__pyarrow_result__(table)

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        /,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        chunk_size: int = 1_000_000,
        **kwargs: Any,
    ):
        """Execute to Arrow and expose local record batches.

        Hotdata currently returns one Arrow IPC result for the full query. This
        method downloads that result first, then splits it into local batches.
        """
        import pyarrow as pa

        table = self.to_pyarrow(expr.as_table(), params=params, limit=limit, **kwargs)
        return pa.ipc.RecordBatchReader.from_batches(
            table.schema,
            table.to_batches(max_chunksize=chunk_size),
        )

    def upload_file(self, data: bytes, *, content_type: str | None = None) -> dict[str, Any]:
        """POST ``/v1/files``; returns the upload record (use ``id`` with managed table loads)."""
        try:
            return self._http.upload_file(data, content_type=content_type)
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

    def create_database(
        self,
        name: str,
        /,
        *,
        catalog: str | None = None,
        schema: str = DEFAULT_SCHEMA,
        tables: Sequence[str] | None = None,
        force: bool = False,
    ) -> None:
        """Create a managed Hotdata connection (Ibis catalog) with optional declared tables."""
        if catalog is not None:
            raise com.UnsupportedOperationError(
                "Hotdata create_database creates a managed connection (catalog); catalog= is not supported"
            )
        try:
            existing = self._resolve_connection(name)
        except com.IbisError:
            existing = None
        if existing is not None:
            if not force:
                raise com.IbisInputError(f"Managed database {name!r} already exists")
            if existing.get("source_type") != MANAGED_SOURCE_TYPE:
                raise com.IbisInputError(
                    f"{name!r} is not a managed database "
                    f"(source_type={existing.get('source_type')!r})"
                )
            return
        try:
            self._http.create_managed_database(name, schema=schema, tables=list(tables or ()))
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc

    def drop_database(
        self,
        name: str,
        /,
        *,
        catalog: str | None = None,
        force: bool = False,
    ) -> None:
        """Delete a managed Hotdata connection (Ibis catalog)."""
        if catalog is not None:
            raise com.UnsupportedOperationError(
                "Hotdata drop_database deletes a managed connection (catalog); catalog= is not supported"
            )
        try:
            conn = self._resolve_managed_connection(name)
        except com.IbisInputError:
            raise
        except com.IbisError:
            if force:
                return
            raise
        try:
            self._http.delete_connection(conn["id"])
        except HotdataAPIError as exc:
            if force and exc.status_code == 404:
                return
            raise _ibis_err_from_hotdata(exc) from exc

    def _local_table_to_parquet(self, obj: Any, schema: sch.Schema | None):
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        if obj is not None and schema is not None:
            raise com.IbisInputError("create_table accepts only one of obj or schema")

        if obj is None:
            if schema is None:
                raise com.IbisInputError("create_table requires a pandas/pyarrow object or schema")
            arrow_schema = schema.to_pyarrow()
            table = pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in arrow_schema],
                schema=arrow_schema,
            )
        elif isinstance(obj, pa.Table):
            table = obj
        elif isinstance(obj, pd.DataFrame):
            table = pa.Table.from_pandas(obj, preserve_index=False)
        else:
            raise com.IbisInputError(
                "create_table currently accepts pandas.DataFrame or pyarrow.Table"
            )

        sink = io.BytesIO()
        pq.write_table(table, sink)
        return sink.getvalue()

    def create_table(
        self,
        name: str,
        /,
        obj: Any = None,
        *,
        schema: sch.Schema | None = None,
        database: tuple[str, str] | str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> ir.Table:
        """Upload local data into a declared managed table.

        Hotdata loads always use ``replace`` mode (the only API option). When
        ``overwrite=False`` (the Ibis default), an existing synced table raises
        :class:`~ibis.common.exceptions.IbisInputError` instead of replacing it.
        """
        if temp:
            raise NotImplementedError("Hotdata does not support temporary tables.")

        data = self._local_table_to_parquet(obj, schema)
        connection_id, schema_name = self._table_location(database)
        if not overwrite and self._managed_table_synced(connection_id, schema_name, name):
            raise com.IbisInputError(
                f"Table {name!r} already exists; pass overwrite=True to replace"
            )
        upload = self.upload_file(data, content_type="application/parquet")
        try:
            self._http.load_managed_table(
                connection_id,
                schema_name,
                name,
                upload_id=upload["id"],
            )
        except HotdataAPIError as exc:
            raise _ibis_err_from_hotdata(exc) from exc
        return self.table(name, database=(connection_id, schema_name))

    def drop_table(
        self,
        name: str,
        /,
        *,
        database: tuple[str, str] | str | None = None,
        force: bool = False,
    ) -> None:
        try:
            connection_id, schema_name = self._table_location(database)
        except com.IbisInputError:
            raise
        except com.IbisError:
            if force:
                return
            raise
        try:
            self._http.delete_managed_table(connection_id, schema_name, name)
        except HotdataAPIError as exc:
            if force and exc.status_code == 404:
                return
            raise _ibis_err_from_hotdata(exc) from exc

    def _register_in_memory_table(self, _op: ops.InMemoryTable) -> None:
        return

    @cached_property
    def version(self) -> str:
        try:
            v = pkg_version("ibis-hotdata")
        except PackageNotFoundError:
            v = "0.0.0"
        return f"ibis-hotdata {v} (Hotdata /v1/query)"
