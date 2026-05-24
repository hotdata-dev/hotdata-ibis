# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
