# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-04-02

The transaction API has been redesigned to make savepoints explicit
rather than implicit. Previously, `WithTx` detected a parent
transaction in the context and silently created a savepoint, which
made it hard to reason about whether you were getting a new
transaction or a savepoint. The new design separates the two
operations: `WithTx` always opens a fresh transaction, and savepoints
are created explicitly via `Tx.Savepoint`. This gives the type system
a role in enforcing transactional intent — `Tx` means "you're in a
transaction," `Querier` means "you can run queries," and the compiler
catches misuse rather than leaving it to runtime.

### Breaking Changes

- **pg**: `Conn` interface renamed to `Querier`. All code referencing `pg.Conn` must be updated to `pg.Querier`.
- **pg**: `ExecFunc` is now generic: `ExecFunc[Q Querier]`. `WithConn` accepts `ExecFunc[Querier]`, `WithTx` accepts `ExecFunc[Tx]`.
- **pg**: `WithTx` callback now receives `pg.Tx` instead of `pg.Conn`. `Tx` extends `Querier` with a `Savepoint` method.
- **pg**: `WithTx` always opens a new connection and transaction. It no longer implicitly creates savepoints via context detection.
- **pg**: Removed `WithoutTx` — no longer needed since `WithTx` always starts a fresh transaction.
- **migrator**: `Migration.Apply` now takes `pg.Tx` instead of `pg.Querier` to make the transactional requirement explicit.

### Added

- **pg**: `Tx` interface with explicit `Savepoint(ctx, ExecFunc[Querier]) error` method for creating savepoints within a transaction.
- **pg**: `Querier` interface (renamed from `Conn`) representing the base capability of running SQL queries.

## [0.2.0] - 2026-04-02

### Breaking Changes

- **pg**: `ExecFunc` signature changed from `func(Conn) error` to `func(context.Context, Conn) error`. All callbacks passed to `WithConn`, `WithTx`, and `WithAdvisoryLock` must be updated to accept a `context.Context` as the first parameter. The provided context carries the active transaction, enabling savepoint support in nested calls.

### Added

- **pg**: Nested `WithTx` calls now create savepoints instead of independent transactions. If the inner callback fails, only the savepoint is rolled back; the outer transaction remains active.
- **pg**: `WithoutTx` function to obtain a context with the active transaction removed, useful when a nested call should start an independent transaction.

### Changed

- Updated dependencies

## [0.1.1] - 2026-02-05

### Fixed

- Fix trace export failed when attributes contains invalid UTF8 rune

## [0.1.0] - 2026-02-01

Initial release of the kit library.

### Added

- **log**: Structured logging wrapper around `slog` with OpenTelemetry trace correlation
- **pg**: PostgreSQL client with connection pooling, transactions, advisory locks, and observability
- **migrator**: File-based SQL migration runner with version tracking
- **httpserver**: HTTP server with tracing, metrics, and response rendering helpers
- **httpclient**: HTTP client with pooled/non-pooled transports and telemetry
- **worker**: Generic worker pool with backpressure and graceful shutdown
- **unit**: Application lifecycle management with config loading, metrics server, and trace exporter

[0.1.0]: https://github.com/gearnode/kit/releases/tag/v0.1.0
