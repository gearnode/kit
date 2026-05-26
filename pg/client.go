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
	"io"
	"net"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/kit/internal/version"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Option is a function that configures the Client during
	// initialization.
	Option func(c *Client)

	// Client provides a PostgreSQL client with a connection pool,
	// logging, tracing, and Prometheus metrics registration.
	Client struct {
		addr            string
		user            string
		password        string
		database        string
		applicationName string

		debug bool

		poolSize              int32
		minPoolSize           int32
		maxConnIdleTime       time.Duration
		maxConnLifetime       time.Duration
		maxConnLifetimeJitter time.Duration
		healthCheckPeriod     time.Duration

		statementCacheCapacity      int
		statementCacheCapacitySet   bool
		descriptionCacheCapacity    int
		descriptionCacheCapacitySet bool
		defaultQueryExecMode        pgx.QueryExecMode
		defaultQueryExecModeSet     bool

		tlsConfig *tls.Config

		pool *pgxpool.Pool

		tracerProvider trace.TracerProvider
		tracer         trace.Tracer
		logger         *log.Logger
		registerer     prometheus.Registerer
	}

	ExecFunc[Q Querier] func(context.Context, Q) error

	AdvisoryLock = uint32
)

const (
	BaseAdvisoryLockId uint32 = 42
)

// WithLogger sets a custom logger.
func WithLogger(l *log.Logger) Option {
	return func(c *Client) {
		c.logger = l.Named("pg.client")
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

// WithApplicationName sets the PostgreSQL application_name runtime
// parameter on every connection in the pool. This value is exposed by
// the server in pg_stat_activity, pg_stat_statements, log lines, and
// pg_locks, making it the primary handle for identifying the source
// of a query during incident response.
//
// PostgreSQL truncates application_name to NAMEDATALEN-1 (63 bytes by
// default) silently, so prefer short, stable identifiers such as the
// service name. An empty string leaves the runtime parameter unset
// (which falls back to whatever the libpq env, e.g. PGAPPNAME, or the
// server defaults choose).
func WithApplicationName(name string) Option {
	return func(c *Client) {
		c.applicationName = name
	}
}

// WithTLS configures TLS using the provided certificates for secure
// connections.
//
// The returned config enables a TLS client session cache so that
// reconnects (after MaxConnLifetime recycles, failovers, or pool
// refills) can resume a previous session and skip the full handshake.
// This noticeably reduces tail latency on pgxpool_acquire_duration_seconds
// for deployments where the database is reached over WAN or any link
// where the round-trip cost dominates connection construction.
func WithTLS(certs []*x509.Certificate) Option {
	return func(c *Client) {
		rootCAs := x509.NewCertPool()
		for _, cert := range certs {
			rootCAs.AddCert(cert)
		}

		host, _, err := net.SplitHostPort(c.addr)
		if err != nil {
			// If SplitHostPort fails, use the full addr as fallback
			// This handles cases where addr might be just a hostname without port
			host = c.addr
		}

		c.tlsConfig = &tls.Config{
			RootCAs:            rootCAs,
			InsecureSkipVerify: false,
			ServerName:         host,
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(0),
		}
	}
}

// WithUnsecureTLS enables TLS without verifying the server's
// certificate chain or hostname. Intended for development against
// self-signed databases; do not use in production.
func WithUnsecureTLS() Option {
	return func(c *Client) {
		c.tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(0),
		}
	}
}

// WithPoolSize sets the maximum number of connections the pool will
// open. It maps to pgxpool.Config.MaxConns.
func WithPoolSize(i int32) Option {
	return func(c *Client) {
		c.poolSize = i
	}
}

// WithMinPoolSize sets the minimum number of connections the pool
// keeps warm. It maps to pgxpool.Config.MinConns. Keeping a non-zero
// floor avoids paying the TCP + TLS + PostgreSQL startup handshake
// cost on the first acquire after an idle period, which otherwise
// shows up as tail latency on pgxpool_acquire_duration_seconds.
//
// When unset, the client defaults to 1 (historical behaviour).
func WithMinPoolSize(i int32) Option {
	return func(c *Client) {
		c.minPoolSize = i
	}
}

// WithMaxConnIdleTime sets how long a connection can be idle before
// it is eligible to be destroyed. It maps to
// pgxpool.Config.MaxConnIdleTime. A zero value leaves the pgx default
// (30 minutes) in place.
//
// Increasing this value reduces connection churn for bursty
// workloads, at the cost of holding idle connections longer.
func WithMaxConnIdleTime(d time.Duration) Option {
	return func(c *Client) {
		c.maxConnIdleTime = d
	}
}

// WithMaxConnLifetime sets the maximum lifetime of a connection
// before it is recycled. It maps to pgxpool.Config.MaxConnLifetime.
// A zero value leaves the pgx default (1 hour) in place.
//
// Increasing this value reduces connection churn for long-lived
// processes, at the cost of keeping the same backend connection
// open for longer.
func WithMaxConnLifetime(d time.Duration) Option {
	return func(c *Client) {
		c.maxConnLifetime = d
	}
}

// WithMaxConnLifetimeJitter sets the duration of randomness added to
// each connection's MaxConnLifetime, smearing recycle events across
// time. It maps to pgxpool.Config.MaxConnLifetimeJitter.
//
// Without jitter, connections opened around the same time (for
// example after a deploy) tend to be recycled together, producing
// synchronized reconnect storms that show up as latency spikes on
// pgxpool_acquire_duration_seconds. A non-zero jitter spreads those
// recycles out.
//
// When unset, the client defaults to 5 minutes. Pass a zero value
// explicitly via this option to disable jitter.
func WithMaxConnLifetimeJitter(d time.Duration) Option {
	return func(c *Client) {
		c.maxConnLifetimeJitter = d
	}
}

// WithHealthCheckPeriod sets how often the pool checks the health of
// its idle connections and enforces MinConns/MaxConnIdleTime/
// MaxConnLifetime. It maps to pgxpool.Config.HealthCheckPeriod. A
// zero value leaves the pgx default (1 minute) in place.
//
// Lowering the period makes the pool react faster to dropped
// connections (for example after a failover) and refill below
// MinConns more promptly, at the cost of slightly more background
// work.
func WithHealthCheckPeriod(d time.Duration) Option {
	return func(c *Client) {
		c.healthCheckPeriod = d
	}
}

// WithStatementCacheCapacity sets the maximum number of prepared
// statements pgx will keep cached per connection when the default
// query exec mode is QueryExecModeCacheStatement (pgx's own default).
// It maps to pgx.ConnConfig.StatementCacheCapacity.
//
// Cache hits avoid the PREPARE round-trip; misses pay one extra
// round-trip and either insert into the cache (if there is room) or
// evict an LRU entry. The pgx default is 512 statements per
// connection. Multiply by MaxConns to estimate the upper bound on
// server-side prepared statement count and on per-pool memory.
//
// Workloads that generate many distinct SQL strings (for example
// GraphQL resolvers that emit different SELECT lists or dynamic
// WHERE chains per request) can exceed 512 and start thrashing the
// cache, which manifests as recurring PREPARE round-trips on the hot
// path. Raise this when the working set of statements is larger than
// the default; lower it when memory pressure on the database matters
// more than per-query latency.
//
// Pass 0 to disable the cache entirely. This is required when sitting
// behind a connection pooler in transaction-pooling mode (for example
// pgbouncer) which may switch the underlying server connection
// between round-trips and invalidate any cached prepared statement.
// Negative values are ignored and the pgx default is left in place.
func WithStatementCacheCapacity(n int) Option {
	return func(c *Client) {
		if n < 0 {
			return
		}
		c.statementCacheCapacity = n
		c.statementCacheCapacitySet = true
	}
}

// WithDescriptionCacheCapacity sets the maximum number of statement
// descriptions (parameter and result types) pgx will keep cached per
// connection when the default query exec mode is
// QueryExecModeCacheDescribe. It maps to
// pgx.ConnConfig.DescriptionCacheCapacity.
//
// The description cache is the lighter-weight cousin of the statement
// cache: it stores only type information, not server-side prepared
// statements. The pgx default is 512.
//
// Pass 0 to disable. Negative values are ignored and the pgx default
// is left in place.
func WithDescriptionCacheCapacity(n int) Option {
	return func(c *Client) {
		if n < 0 {
			return
		}
		c.descriptionCacheCapacity = n
		c.descriptionCacheCapacitySet = true
	}
}

// WithDefaultQueryExecMode overrides pgx's default query execution
// mode for the pool. It maps to pgx.ConnConfig.DefaultQueryExecMode.
//
// pgx defaults to QueryExecModeCacheStatement, which auto-prepares
// and caches statements server-side. The other modes trade off
// round-trips, protocol features, and pooler compatibility:
//
//   - QueryExecModeCacheStatement: default; one round-trip per query
//     after the first, server-side prepared statements.
//   - QueryExecModeCacheDescribe: caches type descriptions only, no
//     server-side prepared statements; safe across schema changes.
//   - QueryExecModeDescribeExec: two round-trips per query, no
//     caching; safest against schema changes.
//   - QueryExecModeExec: extended protocol with text-format
//     parameters, no prepared statement, single round-trip; required
//     when sitting behind pgbouncer in transaction-pooling mode and
//     also setting WithStatementCacheCapacity(0).
//   - QueryExecModeSimpleProtocol: simple protocol with client-side
//     parameter interpolation; broadest pooler/proxy compatibility,
//     loses some pgx type machinery (notably []byte handling).
//
// Use this option together with WithStatementCacheCapacity(0) and
// WithDescriptionCacheCapacity(0) when running through a transaction
// pooler.
func WithDefaultQueryExecMode(mode pgx.QueryExecMode) Option {
	return func(c *Client) {
		c.defaultQueryExecMode = mode
		c.defaultQueryExecModeSet = true
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

// WithDebug enables debug logging for the client.
func WithDebug() Option {
	return func(c *Client) {
		c.debug = true
	}
}

// NewClient creates a new database client with customizable options
// for logging, tracing, TLS, and Prometheus metrics.
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
func NewClient(options ...Option) (*Client, error) {
	c := &Client{
		addr:                  "localhost:5432",
		user:                  "postgres",
		database:              "postgres",
		poolSize:              10,
		minPoolSize:           1,
		maxConnLifetimeJitter: 5 * time.Minute,
		logger:                log.NewLogger(log.WithOutput(io.Discard)),
		tracerProvider:        otel.GetTracerProvider(),
		registerer:            prometheus.DefaultRegisterer,
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
	config.ConnConfig.Host = host
	config.ConnConfig.Port = uint16(port)
	config.ConnConfig.User = c.user
	config.ConnConfig.Password = c.password
	config.ConnConfig.Database = c.database
	config.ConnConfig.TLSConfig = c.tlsConfig
	if c.applicationName != "" {
		if config.ConnConfig.RuntimeParams == nil {
			config.ConnConfig.RuntimeParams = map[string]string{}
		}
		config.ConnConfig.RuntimeParams["application_name"] = c.applicationName
	}
	if c.statementCacheCapacitySet {
		config.ConnConfig.StatementCacheCapacity = c.statementCacheCapacity
	}
	if c.descriptionCacheCapacitySet {
		config.ConnConfig.DescriptionCacheCapacity = c.descriptionCacheCapacity
	}
	if c.defaultQueryExecModeSet {
		config.ConnConfig.DefaultQueryExecMode = c.defaultQueryExecMode
	}
	config.MinConns = c.minPoolSize
	config.MaxConns = c.poolSize
	if c.maxConnIdleTime > 0 {
		config.MaxConnIdleTime = c.maxConnIdleTime
	}
	if c.maxConnLifetime > 0 {
		config.MaxConnLifetime = c.maxConnLifetime
	}
	if c.maxConnLifetimeJitter > 0 {
		config.MaxConnLifetimeJitter = c.maxConnLifetimeJitter
	}
	if c.healthCheckPeriod > 0 {
		config.HealthCheckPeriod = c.healthCheckPeriod
	}

	c.tracer = c.tracerProvider.Tracer(
		tracerName,
		trace.WithInstrumentationVersion(
			version.New(0).Alpha(1),
		),
	)

	tracers := []pgx.QueryTracer{
		&tracer{c.tracer},
	}

	if c.debug {
		tracers = append(
			tracers,
			&tracelog.TraceLog{
				Logger:   &logger{c.logger},
				LogLevel: tracelog.LogLevelInfo,
			},
		)
	}

	config.ConnConfig.Tracer = multitracer.New(tracers...)

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
//	err := client.WithConn(ctx, func(ctx context.Context, conn pg.Querier) error {
//	    _, err := conn.Exec(ctx, "SELECT * FROM users")
//	    return err
//	})
//
// If tracing is enabled, this method creates a span named "WithConn"
// and logs any errors.
func (c *Client) WithConn(
	ctx context.Context,
	exec ExecFunc[Querier],
) error {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = c.tracer.Start(
			ctx,
			"WithConn",
			trace.WithSpanKind(trace.SpanKindClient),
		)
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

	if err := exec(ctx, conn); err != nil {
		if rootSpan.IsRecording() {
			recordError(span, err)
		}

		return err
	}

	return nil
}

// WithTx executes the given ExecFunc within a new transaction. This
// method acquires a connection, begins a transaction, and executes
// exec within it. If exec returns an error, the transaction is rolled
// back; otherwise, it commits.
//
// Use Tx.Savepoint inside the callback to create savepoints within
// the transaction.
//
// Example:
//
//	err := client.WithTx(ctx, func(ctx context.Context, tx pg.Tx) error {
//	    if _, err := tx.Exec(ctx, "DELETE FROM users WHERE id = $1", id); err != nil {
//	        return err
//	    }
//
//	    // Savepoint failure does not roll back the DELETE above.
//	    // The callback receives a Tx, so savepoints can be nested.
//	    if err := tx.Savepoint(ctx, func(ctx context.Context, inner pg.Tx) error {
//	        _, err := inner.Exec(ctx, "INSERT INTO audit_log (...) VALUES (...)")
//	        return err
//	    }); err != nil {
//	        log.Warn("audit failed, continuing", "err", err)
//	    }
//
//	    return nil
//	})
//
// If tracing is enabled, this method creates a span named "WithTx"
// and logs any errors.
func (c *Client) WithTx(
	ctx context.Context,
	exec ExecFunc[Tx],
) error {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = c.tracer.Start(
			ctx,
			"WithTx",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		defer span.End()
	}

	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		err := fmt.Errorf("cannot acquire connection: %w", err)
		if span != nil {
			recordError(span, err)
		}

		return err
	}
	defer conn.Release()

	innerTx, err := conn.Begin(ctx)
	if err != nil {
		err := fmt.Errorf("cannot begin transaction: %w", err)
		if span != nil {
			recordError(span, err)
		}

		return err
	}

	tx := &pgxTx{inner: innerTx, tracer: c.tracer}

	if err := exec(ctx, tx); err != nil {
		if skipErr, ok := errors.AsType[*NoRollbackError](err); ok {
			if err2 := innerTx.Commit(ctx); err2 != nil {
				err = errors.Join(
					err,
					fmt.Errorf("cannot commit transaction: %w", err2),
				)
			}

			if span != nil {
				recordError(span, err)
			}

			return skipErr.Err
		}

		if err2 := innerTx.Rollback(ctx); err2 != nil {
			err = errors.Join(
				err,
				fmt.Errorf("cannot rollback transaction: %w", err2),
			)
		}

		if span != nil {
			recordError(span, err)
		}

		return err
	}

	if err := innerTx.Commit(ctx); err != nil {
		err := fmt.Errorf("cannot commit transaction: %w", err)
		if span != nil {
			recordError(span, err)
		}

		return err
	}

	return nil
}

func (c *Client) WithAdvisoryLock(
	ctx context.Context,
	id AdvisoryLock,
	f func(Querier) error,
) error {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = c.tracer.Start(
			ctx,
			"WithAdvisoryLock",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				attribute.Int("lock_id", int(id)),
			),
		)
		defer span.End()
	}

	return c.WithTx(
		ctx,
		func(txCtx context.Context, tx Tx) error {
			q := "SELECT pg_advisory_xact_lock($1, $2)"
			_, err := tx.Exec(txCtx, q, BaseAdvisoryLockId, id)
			if err != nil {
				err = fmt.Errorf("cannot acquire advisory lock: %w", err)
				if rootSpan.IsRecording() {
					span.SetStatus(codes.Error, err.Error())
					span.RecordError(err)
				}

				return err
			}

			err = f(tx)
			if err != nil {
				if rootSpan.IsRecording() {
					span.SetStatus(codes.Error, err.Error())
					span.RecordError(err)
				}

				return err
			}

			return nil
		},
	)
}

func (c *Client) RefreshTypes(ctx context.Context) error {
	conns := c.pool.AcquireAllIdle(ctx)
	for _, conn := range conns {
		if err := conn.Conn().Close(ctx); err != nil {
			return fmt.Errorf("cannot refresh postgresql type: %w", err)
		}
		conn.Release()
	}

	return nil
}
