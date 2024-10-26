// Copyright (c) 2024 Bryan Frimin <bryan@frimin.fr>.
//
// Permission to use, copy, modify, and/or distribute this software
// for any purpose with or without fee is hereby granted, provided
// that the above copyright notice and this permission notice appear
// in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
// WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
// AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
// CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
// OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
// NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
// CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

package pg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Option is a function that configures the Client during
	// initialization.
	Option func(c *Client)

	// Client provides a PostgreSQL client with a connection pool,
	// logging, tracing, and Prometheus metrics registration.
	Client struct {
		addr     string
		user     string
		password string
		database string

		poolSize int32

		tlsConfig *tls.Config

		pool *pgxpool.Pool

		tracerProvider trace.TracerProvider
		tracer         trace.Tracer
		logger         *log.Logger
		registerer     prometheus.Registerer
	}

	ExecFunc func(Conn) error
)

// WithLogger sets a custom logger.
func WithLogger(l *log.Logger) Option {
	return func(c *Client) {
		c.logger = l
	}
}

// WithAddr specifies the database address in "host:port" format.
func WithAddr(addr string) Option {
	return func(c *Client) {
		c.addr = addr
	}
}

// WithUser sets the database user.
func WithUser(user string) Option {
	return func(c *Client) {
		c.user = user
	}
}

// WithPassword sets the database password.
func WithPassword(password string) Option {
	return func(c *Client) {
		c.password = password
	}
}

// WithDatabase specifies the database to connect to.
func WithDatabase(database string) Option {
	return func(c *Client) {
		c.database = database
	}
}

// WithTLS configures TLS using the provided certificate for secure
// connections.
func WithTLS(cert *x509.Certificate) Option {
	return func(c *Client) {
		rootCAs := x509.NewCertPool()
		rootCAs.AddCert(cert)

		c.tlsConfig = &tls.Config{
			RootCAs:    rootCAs,
			MinVersion: tls.VersionTLS12,
		}
	}
}

// WithTracerProvider configures OpenTelemetry tracing with the
// provided tracer provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *Client) {
		c.tracerProvider = tp
	}
}

// WithRegisterer sets a custom Prometheus registerer for metrics.
func WithRegisterer(r prometheus.Registerer) Option {
	return func(c *Client) {
		c.registerer = r
	}
}

// NewClient creates a new database client with customizable options
// for logging, tracing, TLS, and Prometheus metrics.
//
// Default settings:
// - `addr` defaults to "localhost:5432".
// - `user` and `database` default to "postgres".
// - `poolSize` defaults to 10.
//
// Example:
//
//	client, err := pg.NewClient(
//	    pg.WithAddr("db.example.com:5432"),
//	    pg.WithUser("dbuser"),
//	    pg.WithPassword("password"),
//	)
//	if err != nil {
//	    panic(err)
//	}
//
// Options:
// - WithLogger: Sets a custom logger.
// - WithAddr: Sets the database address.
// - WithUser, WithPassword: Sets the database user credentials.
// - WithDatabase: Sets the target database.
// - WithTLS: Configures TLS with a specified certificate.
// - WithTracerProvider: Integrates OpenTelemetry tracing.
// - WithRegisterer: Sets a custom Prometheus registerer.
func NewClient(options ...Option) (*Client, error) {
	c := &Client{
		addr:           "localhost:5432",
		user:           "postgres",
		database:       "postgres",
		poolSize:       10,
		logger:         log.NewLogger(log.WithName("pg.client")),
		tracerProvider: otel.GetTracerProvider(),
		registerer:     prometheus.DefaultRegisterer,
	}

	for _, o := range options {
		o(c)
	}

	host, portStr, err := net.SplitHostPort(c.addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address: %w", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	config, _ := pgxpool.ParseConfig("")
	config.ConnConfig.Config.Host = host
	config.ConnConfig.Config.Port = uint16(port)
	config.ConnConfig.Config.User = c.user
	config.ConnConfig.Config.Password = c.password
	config.ConnConfig.Config.Database = c.database
	config.ConnConfig.Config.TLSConfig = c.tlsConfig
	config.MinConns = 1
	config.MaxConns = int32(c.poolSize)

	c.tracer = c.tracerProvider.Tracer(
		tracerName,
		trace.WithInstrumentationVersion(
			findOwnImportedVersion(),
		),
	)

	config.ConnConfig.Tracer = multitracer.New(
		&tracer{c.tracer},
		&tracelog.TraceLog{
			Logger:   &logger{c.logger}, // TODO not enable tracelog by default
			LogLevel: tracelog.LogLevelInfo,
		},
	)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("cannot create connection pool from config: %w", err)
	}

	c.registerer.MustRegister(
		newCollector(
			pool,
			map[string]string{
				"database": c.database,
				"user":     c.user,
				"addr":     c.addr,
			},
		),
	)

	c.pool = pool

	return c, nil
}

// Close closes the client's connection pool, releasing all resources.
func (c *Client) Close() {
	c.pool.Close()
}

// WithConn executes the given ExecFunc with a database connection
// from the pool.
//
// Example:
//
//	err := client.WithConn(ctx, func(conn pg.Conn) error {
//	    _, err := conn.Exec(ctx, "SELECT * FROM users")
//	    return err
//	})
//
// If tracing is enabled, this method creates a span named "WithConn"
// and logs any errors.
func (c *Client) WithConn(
	ctx context.Context,
	exec ExecFunc,
) error {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = c.tracer.Start(ctx, "WithConn")
		defer span.End()
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		err := fmt.Errorf("cannot acquire connection: %w", err)
		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}
	defer conn.Release()

	if err := exec(conn); err != nil {
		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}

	return nil
}

// WithTx executes the given ExecFunc within a transaction. This
// method begins a transaction, executing `exec` within it. If `exec`
// returns an error, the transaction is rolled back; otherwise, it
// commits.
//
// Example:
//
//	err := client.WithTx(ctx, func(tx pg.Conn) error {
//	    if _, err := tx.Exec(ctx, "DELETE FROM users WHERE id = $1 ", id); err != nil {
//	        return err
//	    }
//	    return nil
//	})
//
// If tracing is enabled, this method creates a span named "WithTx"
// and logs any errors.
func (c *Client) WithTx(
	ctx context.Context,
	exec ExecFunc,
) error {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = c.tracer.Start(ctx, "WithTx")
		defer span.End()
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		err := fmt.Errorf("cannot acquire connection: %w", err)
		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		err := fmt.Errorf("cannot begin transaction: %w", err)
		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}

	if err := exec(tx); err != nil {
		if err2 := tx.Rollback(ctx); err2 != nil {
			err = errors.Join(
				err,
				fmt.Errorf("cannot rollback transaction: %w", err2),
			)
		}

		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}

	if err := tx.Commit(ctx); err != nil {
		err := fmt.Errorf("cannot commit transaction: %w", err)
		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}

	return nil
}