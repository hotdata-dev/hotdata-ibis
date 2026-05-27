# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


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
