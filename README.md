# Kit

[![Go Reference](https://pkg.go.dev/badge/go.gearno.de/kit.svg)](https://pkg.go.dev/go.gearno.de/kit)
[![Go Report Card](https://goreportcard.com/badge/go.gearno.de/kit)](https://goreportcard.com/report/go.gearno.de/kit)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](https://opensource.org/licenses/ISC)

Opinionated building blocks for production Go applications.

## About

Kit provides foundational packages for building production-grade Go services with sane defaults. It integrates observability primitives (OpenTelemetry tracing, Prometheus metrics, structured logging) as first-class concerns.

## Packages

| Package | Description |
|---------|-------------|
| `unit` | Application lifecycle, configuration, signal handling, and graceful shutdown |
| `log` | Structured logging with `slog`, trace correlation, and multiple output formats |
| `pg` | PostgreSQL client with connection pooling, tracing, and metrics |
| `migrator` | Database schema migrations with advisory locking |
| `httpserver` | HTTP server with request tracing and metrics |
| `httpclient` | HTTP client with telemetry and connection management |
| `worker` | Generic worker pool with backpressure |

## Requirements

- Go 1.25+

## Installation

```bash
go get go.gearno.de/kit
```

## Documentation

See the [Go package documentation](https://pkg.go.dev/go.gearno.de/kit) for API reference and examples.

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes before submitting a pull request.

## License

Kit is released under the [ISC License](LICENCE.txt).

## Author

Bryan Frimin
