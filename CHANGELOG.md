# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-07-10

### Changed

- Bump `ibis-framework` `>=10.0,<11` → `>=12,<13` to unblock co-installing
  `hotdata-ibis` alongside `hotdata-dlt-destination` (which needs dlt 1.28 /
  ibis 12) in the same environment. No API drift: the compiler handle
  (`sc.postgres.compiler`), `SQLBackend`/mixin imports, `PyArrowSchema.to_ibis`,
  `PandasData.convert_table`, and the exception surface used by this backend
  are unchanged between ibis 10 and 12. Verified with the full offline test
  suite and a live read/write round trip (managed database created via
  `database_id` bound at connect time, catalog `"default"`, `con.table(...)`
  and `con.sql(...)` to both pandas and pyarrow) against a running Hotdata
  workspace.
- Raise the minimum `hotdata` SDK version to `>=0.6,<0.7` (was `>=0.5.0`).
  hotdata 0.6.0 made `X-Database-Id` required on the query-run and result
  endpoints (`get_query_run`, `get_result*`); `execute_query` now forwards the
  same `database_id` used to submit a query to its poll and result-fetch
  calls, matching the fix already applied on the `hotdata-dlt-destination`
  side.
- Raise the minimum `pyarrow` version to `>=16` (was `>=15`) — required for
  `pyarrow.string_view()`, used by the fix below.

### Fixed

- Map Arrow's `Utf8View` (the StringView layout PyArrow/Arrow >=16 introduced,
  which RuntimeDB's information schema now reports for plain string columns —
  both federated and managed-database) to `dt.String` instead of falling
  through to `dt.unknown(...)`. Confirmed against a live workspace: this
  affected both an existing federated TPC-H connection and a managed database
  populated via dlt, i.e. exactly the read path `hotdata-dlt-destination`'s
  live ibis backend depends on.

### Added

- `examples/05_roundtrip_demo.py`: a runnable example demonstrating the
  managed-database lifecycle end to end (`create_database` → `create_table`
  → bind `database_id` at connect time → read via `con.table()` / `con.sql()`
  → `drop_database`) — the specific contract `hotdata-dlt-destination`'s live
  ibis backend wrapper relies on. Distinct from the existing `01`-`04`
  examples, which all read pre-existing federated connections and never
  exercise the managed-database write/bind path.

## [0.1.6] - 2026-06-28

### Changed

- Raise the minimum `hotdata` SDK version to `>=0.5.0` (was `>=0.4.1`). Verified
  end-to-end against the 0.5.0 SDK: no API drift in the surface the backend uses,
  full offline test suite passing, and a live create/upload/query/drop flow.
- Renamed internal references and architecture guardrails from `hotdata-runtime`
  to `hotdata-framework` to track the renamed sibling package. `hotdata-ibis`
  continues to depend only on the `hotdata` SDK, not `hotdata-framework`.

## [0.1.5] - 2026-06-01

### Changed

- Release 0.1.5

## [0.1.4] - 2026-05-27

### Changed

- Release 0.1.4

## [0.1.3] - 2026-05-26

### Added

- Parametric Arrow type strings returned by the information schema are now mapped correctly:
  `timestamp[unit, tz=…]` (with scale preserved), `duration[unit]`, `decimal128(p, s)` /
  `decimal256(p, s)` / `decimal(p, s)`, and `list<item: T>` / `large_list<item: T>`
  (including non-nullable item fields). Previously all of these fell back to `String`.
- Simple Arrow type aliases extended: `halffloat` (PyArrow's name for `float16`),
  `large_string`, all four signed integer variants (`int8`–`int64`), and all four unsigned
  integer variants (`uint8`–`uint64`) are now resolved correctly.

### Changed

- **Default schema for managed databases changed from `"public"` to `"main"`** to match
  runtimedb's `DEFAULT_SCHEMA_NAME`. runtimedb always auto-inserts a `main` schema into
  every managed database; using `"public"` previously created a spurious empty `main`
  schema alongside the declared `public` one.
- Arrow type construction now goes through PyArrow's type system as the authoritative bridge
  (`pa.DataType` → `PyArrowType.to_ibis()`), replacing manual Ibis type construction.
- `_IN_FLIGHT` query-run statuses trimmed to `{"running"}` — runtimedb `QueryRunStatus`
  only emits `running`, `succeeded`, and `failed`; `queued` and `pending` are result
  statuses, not query-run statuses.

### Fixed

- Decimal regex tightened: `decimal128?` → `decimal(?:128|256)?` so `decimal12(…)` is
  no longer mistakenly matched as a decimal type.
- Unknown `duration` unit now falls through to the Postgres parser / `String` fallback
  instead of silently defaulting to seconds.

## [0.1.2] - 2026-05-24

### Added

- Arrow-style type names (`Date32`, `Float64`, `Utf8`, `LargeBinary`, etc.) returned by the information schema for Parquet/managed-table columns are now mapped to the correct Ibis types instead of falling back to `String`.
- `database_id` parameter on `connect()` to bind a pre-existing managed database at connect time, enabling `"default"` catalog references without calling `create_table` first.

### Changed

- Managed databases now use the dedicated `/v1/databases` API instead of the legacy `/v1/connections` API. `create_database` and `drop_database` are unaffected at the call site.
- `create_table` now returns a table reference with `("default", schema)` as the catalog, matching the SQL catalog prefix required for querying managed tables.
- `create_database` no longer silently swallows API errors during the existence check — only a genuine not-found result proceeds to creation.

### Fixed

- Querying managed tables via `con.table(..., database=("default", schema))` or `con.sql(...)` now correctly routes information-schema lookups through the underlying connection id rather than the literal string `"default"`.
- Cached `_database_id` is now always overwritten on each `create_table` / `drop_table` call, preventing a stale id from being sent in subsequent queries when different managed databases are used on the same connection.

## [0.1.1] - 2026-05-19

### Added

- Managed database support via `create_managed_database` and `load_managed_table`.

## [0.1.0] - 2026-05-06

### Added

- Initial release.
