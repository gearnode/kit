package pg_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/log"
	"go.gearno.de/kit/pg"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func selfSignedCert(t *testing.T) *x509.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert
}

func newTestClient(t *testing.T, extra ...pg.Option) *pg.Client {
	t.Helper()

	opts := []pg.Option{
		pg.WithAddr("localhost:5432"),
		pg.WithUser("kit"),
		pg.WithPassword("kit"),
		pg.WithDatabase("kit_test"),
		pg.WithLogger(log.NewLogger(log.WithOutput(io.Discard))),
		pg.WithRegisterer(prometheus.NewRegistry()),
	}
	opts = append(opts, extra...)

	client, err := pg.NewClient(opts...)
	if err != nil {
		t.Skipf("skipping: cannot create PostgreSQL client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			_, err := conn.Exec(ctx, "SELECT 1")
			return err
		},
	)
	if err != nil {
		client.Close()
		t.Skipf("skipping: cannot connect to PostgreSQL: %v", err)
	}

	t.Cleanup(client.Close)

	return client
}

func newTracedTestClient(t *testing.T) (*pg.Client, *tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	t.Helper()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sr),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	client := newTestClient(t,
		pg.WithTracerProvider(tp),
		pg.WithDebug(),
		pg.WithPoolSize(5),
	)

	return client, sr, tp
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name()
	}
	return names
}

// ---------------------------------------------------------------------------
// NewClient
// ---------------------------------------------------------------------------

func TestNewClient(t *testing.T) {
	t.Run(
		"connects and executes a query",
		func(t *testing.T) {
			client := newTestClient(t)
			ctx := context.Background()

			err := client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					var result int
					return conn.QueryRow(ctx, "SELECT 1").Scan(&result)
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"invalid address format",
		func(t *testing.T) {
			_, err := pg.NewClient(
				pg.WithAddr("no-port"),
				pg.WithRegisterer(prometheus.NewRegistry()),
			)
			require.Error(t, err)
		},
	)

	t.Run(
		"invalid port",
		func(t *testing.T) {
			_, err := pg.NewClient(
				pg.WithAddr("localhost:abc"),
				pg.WithRegisterer(prometheus.NewRegistry()),
			)
			require.Error(t, err)
		},
	)

	t.Run(
		"with TLS option",
		func(t *testing.T) {
			cert := selfSignedCert(t)
			client, err := pg.NewClient(
				pg.WithAddr("localhost:5432"),
				pg.WithUser("kit"),
				pg.WithPassword("kit"),
				pg.WithDatabase("kit_test"),
				pg.WithTLS([]*x509.Certificate{cert}),
				pg.WithRegisterer(prometheus.NewRegistry()),
			)
			if err == nil {
				client.Close()
			}
		},
	)

	t.Run(
		"with TLS addr fallback",
		func(t *testing.T) {
			cert := selfSignedCert(t)
			_, err := pg.NewClient(
				pg.WithAddr("just-host"),
				pg.WithTLS([]*x509.Certificate{cert}),
				pg.WithRegisterer(prometheus.NewRegistry()),
			)
			require.Error(t, err)
		},
	)

	t.Run(
		"with pool size option",
		func(t *testing.T) {
			_ = newTestClient(t, pg.WithPoolSize(2))
		},
	)
}

// ---------------------------------------------------------------------------
// WithConn
// ---------------------------------------------------------------------------

func TestWithConn(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	t.Run(
		"exec and query",
		func(t *testing.T) {
			err := client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"CREATE TEMPORARY TABLE test_with_conn (id serial PRIMARY KEY, name text NOT NULL)")
					if err != nil {
						return err
					}

					_, err = conn.Exec(ctx,
						"INSERT INTO test_with_conn (name) VALUES ($1)", "alice")
					if err != nil {
						return err
					}

					var name string
					err = conn.QueryRow(ctx,
						"SELECT name FROM test_with_conn WHERE name = $1", "alice").Scan(&name)
					if err != nil {
						return err
					}

					assert.Equal(t, "alice", name)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"propagates callback error",
		func(t *testing.T) {
			sentinel := errors.New("callback failed")
			err := client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					return sentinel
				},
			)
			require.ErrorIs(t, err, sentinel)
		},
	)
}

func TestWithConn_Tracing(t *testing.T) {
	client, sr, tp := newTracedTestClient(t)

	t.Run(
		"creates spans on success",
		func(t *testing.T) {
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx, "SELECT 1")
					return err
				},
			)
			require.NoError(t, err)
			span.End()

			newSpans := sr.Ended()[before:]
			assert.NotEmpty(t, newSpans)
			assert.Contains(t, spanNames(newSpans), "WithConn")
		},
	)

	t.Run(
		"records error in span",
		func(t *testing.T) {
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			sentinel := errors.New("traced error")
			err := client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					return sentinel
				},
			)
			require.ErrorIs(t, err, sentinel)
			span.End()

			for _, s := range sr.Ended()[before:] {
				if s.Name() == "WithConn" {
					assert.Equal(t, codes.Error, s.Status().Code)
				}
			}
		},
	)
}

// ---------------------------------------------------------------------------
// WithTx
// ---------------------------------------------------------------------------

func TestWithTx(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	setup := func(t *testing.T, table string) {
		t.Helper()
		err := client.WithConn(
			ctx,
			func(ctx context.Context, conn pg.Conn) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
				if err != nil {
					return err
				}
				_, err = conn.Exec(ctx,
					"CREATE TABLE "+table+" (id serial PRIMARY KEY, name text NOT NULL)")
				return err
			},
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = client.WithConn(
				context.Background(),
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
					return err
				},
			)
		})
	}

	t.Run(
		"commits on success",
		func(t *testing.T) {
			setup(t, "test_tx_commit")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_commit (name) VALUES ($1)", "bob")
					return err
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_commit WHERE name = $1", "bob").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 1, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"rolls back on error",
		func(t *testing.T) {
			setup(t, "test_tx_rollback")

			sentinel := errors.New("force rollback")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_rollback (name) VALUES ($1)", "charlie")
					if err != nil {
						return err
					}
					return sentinel
				},
			)
			require.ErrorIs(t, err, sentinel)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_rollback WHERE name = $1", "charlie").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 0, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"nested savepoint commits both",
		func(t *testing.T) {
			setup(t, "test_tx_nested")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_nested (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}

					return client.WithTx(
						ctx,
						func(ctx context.Context, conn pg.Conn) error {
							_, err := conn.Exec(ctx,
								"INSERT INTO test_tx_nested (name) VALUES ($1)", "inner")
							return err
						},
					)
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					var count int
					err := conn.QueryRow(ctx, "SELECT count(*) FROM test_tx_nested").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 2, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"nested savepoint rollback preserves outer",
		func(t *testing.T) {
			setup(t, "test_tx_savepoint")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_savepoint (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}

					_ = client.WithTx(
						ctx,
						func(ctx context.Context, conn pg.Conn) error {
							if _, err := conn.Exec(ctx,
								"INSERT INTO test_tx_savepoint (name) VALUES ($1)", "inner_fail"); err != nil {
								return err
							}
							return errors.New("inner error")
						},
					)

					return nil
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					var names []string
					rows, err := conn.Query(ctx, "SELECT name FROM test_tx_savepoint ORDER BY name")
					if err != nil {
						return err
					}
					defer rows.Close()

					for rows.Next() {
						var n string
						if err := rows.Scan(&n); err != nil {
							return err
						}
						names = append(names, n)
					}

					assert.Equal(t, []string{"outer"}, names)
					return rows.Err()
				},
			)
			require.NoError(t, err)
		},
	)
}

func TestWithTx_Tracing(t *testing.T) {
	client, sr, tp := newTracedTestClient(t)

	setup := func(t *testing.T, table string) {
		t.Helper()
		ctx := context.Background()
		err := client.WithConn(
			ctx,
			func(ctx context.Context, conn pg.Conn) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
				if err != nil {
					return err
				}
				_, err = conn.Exec(ctx,
					"CREATE TABLE "+table+" (id serial PRIMARY KEY, name text NOT NULL)")
				return err
			},
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = client.WithConn(
				context.Background(),
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
					return err
				},
			)
		})
	}

	t.Run(
		"commit creates spans",
		func(t *testing.T) {
			setup(t, "test_tx_trace_ok")
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_trace_ok (name) VALUES ($1)", "traced")
					return err
				},
			)
			require.NoError(t, err)
			span.End()

			newSpans := sr.Ended()[before:]
			assert.Contains(t, spanNames(newSpans), "WithTx")
		},
	)

	t.Run(
		"rollback records error",
		func(t *testing.T) {
			setup(t, "test_tx_trace_err")
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			sentinel := errors.New("traced tx error")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_trace_err (name) VALUES ($1)", "will_rollback")
					if err != nil {
						return err
					}
					return sentinel
				},
			)
			require.ErrorIs(t, err, sentinel)
			span.End()

			for _, s := range sr.Ended()[before:] {
				if s.Name() == "WithTx" {
					assert.Equal(t, codes.Error, s.Status().Code)
				}
			}
		},
	)

	t.Run(
		"nested savepoint creates spans",
		func(t *testing.T) {
			setup(t, "test_tx_trace_sp")
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_trace_sp (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}
					return client.WithTx(
						ctx,
						func(ctx context.Context, conn pg.Conn) error {
							_, err := conn.Exec(ctx,
								"INSERT INTO test_tx_trace_sp (name) VALUES ($1)", "inner")
							return err
						},
					)
				},
			)
			require.NoError(t, err)
			span.End()

			names := spanNames(sr.Ended()[before:])
			withTxCount := 0
			for _, n := range names {
				if n == "WithTx" {
					withTxCount++
				}
			}
			assert.GreaterOrEqual(t, withTxCount, 2, "expected outer + inner WithTx spans")
		},
	)

	t.Run(
		"nested savepoint rollback records error",
		func(t *testing.T) {
			setup(t, "test_tx_trace_sp_err")
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_tx_trace_sp_err (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}
					_ = client.WithTx(
						ctx,
						func(ctx context.Context, conn pg.Conn) error {
							return errors.New("inner savepoint error")
						},
					)
					return nil
				},
			)
			require.NoError(t, err)
			span.End()

			newSpans := sr.Ended()[before:]
			assert.NotEmpty(t, newSpans)
		},
	)
}

// ---------------------------------------------------------------------------
// WithoutTx
// ---------------------------------------------------------------------------

func TestWithoutTx(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	err := client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_without_tx")
			if err != nil {
				return err
			}
			_, err = conn.Exec(ctx,
				"CREATE TABLE test_without_tx (id serial PRIMARY KEY, name text NOT NULL)")
			return err
		},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = client.WithConn(
			context.Background(),
			func(ctx context.Context, conn pg.Conn) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_without_tx")
				return err
			},
		)
	})

	err = client.WithTx(
		ctx,
		func(txCtx context.Context, conn pg.Conn) error {
			_, err := conn.Exec(txCtx,
				"INSERT INTO test_without_tx (name) VALUES ($1)", "in_parent_tx")
			if err != nil {
				return err
			}

			freshCtx := pg.WithoutTx(txCtx)
			return client.WithTx(
				freshCtx,
				func(ctx context.Context, conn pg.Conn) error {
					_, err := conn.Exec(ctx,
						"INSERT INTO test_without_tx (name) VALUES ($1)", "independent")
					return err
				},
			)
		},
	)
	require.NoError(t, err)

	err = client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			var count int
			err := conn.QueryRow(ctx, "SELECT count(*) FROM test_without_tx").Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 2, count)
			return nil
		},
	)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// WithAdvisoryLock
// ---------------------------------------------------------------------------

func TestWithAdvisoryLock(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	t.Run(
		"acquires lock and executes",
		func(t *testing.T) {
			var executed bool
			err := client.WithAdvisoryLock(ctx, 1, func(conn pg.Conn) error {
				executed = true

				var locked bool
				err := conn.QueryRow(
					ctx,
					"SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = $1 AND objid = $2)",
					pg.BaseAdvisoryLockId, 1,
				).Scan(&locked)
				require.NoError(t, err)
				assert.True(t, locked)

				return nil
			})
			require.NoError(t, err)
			assert.True(t, executed)
		},
	)

	t.Run(
		"propagates callback error",
		func(t *testing.T) {
			sentinel := errors.New("lock callback failed")
			err := client.WithAdvisoryLock(ctx, 2, func(conn pg.Conn) error {
				return sentinel
			})
			require.ErrorIs(t, err, sentinel)
		},
	)
}

func TestWithAdvisoryLock_Tracing(t *testing.T) {
	client, sr, tp := newTracedTestClient(t)

	t.Run(
		"creates spans on success",
		func(t *testing.T) {
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithAdvisoryLock(ctx, 100, func(conn pg.Conn) error {
				return nil
			})
			require.NoError(t, err)
			span.End()

			names := spanNames(sr.Ended()[before:])
			assert.Contains(t, names, "WithAdvisoryLock")
			assert.Contains(t, names, "WithTx")
		},
	)

	t.Run(
		"records error in span",
		func(t *testing.T) {
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			sentinel := errors.New("traced lock error")
			err := client.WithAdvisoryLock(ctx, 101, func(conn pg.Conn) error {
				return sentinel
			})
			require.ErrorIs(t, err, sentinel)
			span.End()

			for _, s := range sr.Ended()[before:] {
				if s.Name() == "WithAdvisoryLock" {
					assert.Equal(t, codes.Error, s.Status().Code)
				}
			}
		},
	)
}

// ---------------------------------------------------------------------------
// SendBatch (exercises TraceBatchStart/Query/End)
// ---------------------------------------------------------------------------

func TestSendBatch(t *testing.T) {
	client, sr, tp := newTracedTestClient(t)
	bgCtx := context.Background()

	err := client.WithConn(
		bgCtx,
		func(ctx context.Context, conn pg.Conn) error {
			_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_batch")
			if err != nil {
				return err
			}
			_, err = conn.Exec(ctx,
				"CREATE TABLE test_batch (id serial PRIMARY KEY, name text NOT NULL)")
			return err
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.WithConn(
			bgCtx,
			func(ctx context.Context, conn pg.Conn) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_batch")
				return err
			},
		)
	})

	before := len(sr.Ended())

	ctx, span := tp.Tracer("test").Start(bgCtx, "test-root")
	err = client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			batch := &pgx.Batch{}
			batch.Queue("INSERT INTO test_batch (name) VALUES ($1)", "a")
			batch.Queue("INSERT INTO test_batch (name) VALUES ($1)", "b")

			br := conn.SendBatch(ctx, batch)
			for i := 0; i < 2; i++ {
				if _, err := br.Exec(); err != nil {
					return err
				}
			}
			return br.Close()
		},
	)
	require.NoError(t, err)
	span.End()

	names := spanNames(sr.Ended()[before:])
	assert.Contains(t, names, "db.batch.query")

	err = client.WithConn(
		bgCtx,
		func(ctx context.Context, conn pg.Conn) error {
			var count int
			err := conn.QueryRow(ctx, "SELECT count(*) FROM test_batch").Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 2, count)
			return nil
		},
	)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// CopyFrom (exercises TraceCopyFromStart/End)
// ---------------------------------------------------------------------------

func TestCopyFrom(t *testing.T) {
	client, sr, tp := newTracedTestClient(t)
	bgCtx := context.Background()

	err := client.WithConn(
		bgCtx,
		func(ctx context.Context, conn pg.Conn) error {
			_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_copy")
			if err != nil {
				return err
			}
			_, err = conn.Exec(ctx,
				"CREATE TABLE test_copy (id integer, name text)")
			return err
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.WithConn(
			bgCtx,
			func(ctx context.Context, conn pg.Conn) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_copy")
				return err
			},
		)
	})

	before := len(sr.Ended())

	ctx, span := tp.Tracer("test").Start(bgCtx, "test-root")
	err = client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			rows := pgx.CopyFromRows([][]any{
				{1, "alice"},
				{2, "bob"},
				{3, "carol"},
			})
			n, err := conn.CopyFrom(ctx, pgx.Identifier{"test_copy"}, []string{"id", "name"}, rows)
			if err != nil {
				return err
			}
			assert.Equal(t, int64(3), n)
			return nil
		},
	)
	require.NoError(t, err)
	span.End()

	names := spanNames(sr.Ended()[before:])
	assert.Contains(t, names, "db.copy")
}

// ---------------------------------------------------------------------------
// SQL error with tracing (exercises recordError + PgError branch)
// ---------------------------------------------------------------------------

func TestQueryError_Tracing(t *testing.T) {
	client, sr, tp := newTracedTestClient(t)

	before := len(sr.Ended())

	ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
	err := client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			_, err := conn.Exec(ctx, "SELECT * FROM table_that_does_not_exist_xyz")
			return err
		},
	)
	require.Error(t, err)
	span.End()

	newSpans := sr.Ended()[before:]

	var hasError bool
	for _, s := range newSpans {
		if s.Status().Code == codes.Error {
			hasError = true
			break
		}
	}
	assert.True(t, hasError, "expected at least one span with error status")
}

// ---------------------------------------------------------------------------
// RefreshTypes
// ---------------------------------------------------------------------------

func TestRefreshTypes(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	require.NoError(t, client.RefreshTypes(ctx))

	err := client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Conn) error {
			var n int
			return conn.QueryRow(ctx, "SELECT 42").Scan(&n)
		},
	)
	require.NoError(t, err)
}
