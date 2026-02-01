# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
