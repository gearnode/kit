package pg_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"fmt"
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
		func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
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

	t.Run(
		"with min pool size option",
		func(t *testing.T) {
			_ = newTestClient(
				t,
				pg.WithPoolSize(5),
				pg.WithMinPoolSize(2),
			)
		},
	)

	t.Run(
		"with max conn idle time option",
		func(t *testing.T) {
			_ = newTestClient(
				t,
				pg.WithMaxConnIdleTime(5*time.Minute),
			)
		},
	)

	t.Run(
		"with max conn lifetime option",
		func(t *testing.T) {
			_ = newTestClient(
				t,
				pg.WithMaxConnLifetime(15*time.Minute),
			)
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
				func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
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
			func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_commit (name) VALUES ($1)", "bob")
					return err
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
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
				func(ctx context.Context, conn pg.Querier) error {
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
		"savepoint commits both",
		func(t *testing.T) {
			setup(t, "test_tx_nested")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_nested (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}

					return tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							_, err := inner.Exec(ctx,
								"INSERT INTO test_tx_nested (name) VALUES ($1)", "inner")
							return err
						},
					)
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
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
		"savepoint rollback preserves outer",
		func(t *testing.T) {
			setup(t, "test_tx_savepoint")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_savepoint (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}

					_ = tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							if _, err := inner.Exec(ctx,
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
				func(ctx context.Context, conn pg.Querier) error {
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

	t.Run(
		"multiple sequential savepoints both succeed",
		func(t *testing.T) {
			setup(t, "test_tx_multi_sp")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					if err := tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							_, err := inner.Exec(ctx,
								"INSERT INTO test_tx_multi_sp (name) VALUES ($1)", "sp1")
							return err
						},
					); err != nil {
						return err
					}

					return tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							_, err := inner.Exec(ctx,
								"INSERT INTO test_tx_multi_sp (name) VALUES ($1)", "sp2")
							return err
						},
					)
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_multi_sp").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 2, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"sequential savepoints first succeeds second fails",
		func(t *testing.T) {
			setup(t, "test_tx_sp_mixed")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					if err := tx.Savepoint(ctx, func(ctx context.Context, inner pg.Tx) error {
						_, err := inner.Exec(ctx,
							"INSERT INTO test_tx_sp_mixed (name) VALUES ($1)", "kept")
						return err
					}); err != nil {
						return err
					}

					_ = tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							_, err := inner.Exec(ctx,
								"INSERT INTO test_tx_sp_mixed (name) VALUES ($1)", "discarded")
							if err != nil {
								return err
							}
							return errors.New("second savepoint fails")
						},
					)

					return nil
				},
			)
			require.NoError(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
					var names []string
					rows, err := conn.Query(ctx,
						"SELECT name FROM test_tx_sp_mixed ORDER BY name")
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

					assert.Equal(t, []string{"kept"}, names)
					return rows.Err()
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"savepoint error propagated rolls back entire tx",
		func(t *testing.T) {
			setup(t, "test_tx_sp_propagate")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_sp_propagate (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}

					return tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							return errors.New("savepoint failed")
						},
					)
				},
			)
			require.Error(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_sp_propagate").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 0, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)
}

func TestNoRollback(t *testing.T) {
	t.Run(
		"nil returns nil",
		func(t *testing.T) {
			assert.Nil(t, pg.NoRollback(nil))
		},
	)

	t.Run(
		"wraps error in NoRollbackError",
		func(t *testing.T) {
			sentinel := errors.New("inner")
			err := pg.NoRollback(sentinel)

			var nrErr *pg.NoRollbackError
			require.ErrorAs(t, err, &nrErr)
			assert.Equal(t, sentinel, nrErr.Err)
		},
	)

	t.Run(
		"Error delegates to inner",
		func(t *testing.T) {
			err := pg.NoRollback(errors.New("boom"))
			assert.Equal(t, "boom", err.Error())
		},
	)

	t.Run(
		"Unwrap returns inner",
		func(t *testing.T) {
			sentinel := errors.New("inner")
			err := pg.NoRollback(sentinel)
			assert.ErrorIs(t, err, sentinel)
		},
	)

	t.Run(
		"detectable through additional wrapping",
		func(t *testing.T) {
			sentinel := errors.New("root cause")
			wrapped := fmt.Errorf("context: %w", pg.NoRollback(sentinel))

			var nrErr *pg.NoRollbackError
			require.ErrorAs(t, wrapped, &nrErr)
			assert.ErrorIs(t, wrapped, sentinel)
		},
	)
}

func TestWithTx_NoRollback(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	setup := func(t *testing.T, table string) {
		t.Helper()
		err := client.WithConn(
			ctx,
			func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
					_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
					return err
				},
			)
		})
	}

	t.Run(
		"commits and returns inner error",
		func(t *testing.T) {
			setup(t, "test_tx_norollback")

			sentinel := errors.New("soft failure")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_norollback (name) VALUES ($1)", "committed")
					if err != nil {
						return err
					}
					return pg.NoRollback(sentinel)
				},
			)
			require.ErrorIs(t, err, sentinel)

			var nrErr *pg.NoRollbackError
			assert.False(t, errors.As(err, &nrErr),
				"returned error must not be wrapped in NoRollbackError")

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_norollback WHERE name = $1",
						"committed").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 1, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"works through additional wrapping",
		func(t *testing.T) {
			setup(t, "test_tx_norollback_wrap")

			sentinel := errors.New("root cause")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_norollback_wrap (name) VALUES ($1)", "kept")
					if err != nil {
						return err
					}
					return fmt.Errorf("extra context: %w", pg.NoRollback(sentinel))
				},
			)
			require.Error(t, err)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_norollback_wrap WHERE name = $1",
						"kept").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 1, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"normal error still rolls back",
		func(t *testing.T) {
			setup(t, "test_tx_norollback_control")

			sentinel := errors.New("hard failure")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_norollback_control (name) VALUES ($1)", "gone")
					if err != nil {
						return err
					}
					return sentinel
				},
			)
			require.ErrorIs(t, err, sentinel)

			err = client.WithConn(
				ctx,
				func(ctx context.Context, conn pg.Querier) error {
					var count int
					err := conn.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_norollback_control WHERE name = $1",
						"gone").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 0, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)
}

func TestWithTx_QuerierMethods(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	setup := func(t *testing.T, table string) {
		t.Helper()
		err := client.WithConn(
			ctx,
			func(ctx context.Context, conn pg.Querier) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
				if err != nil {
					return err
				}
				_, err = conn.Exec(ctx,
					"CREATE TABLE "+table+" (id integer, name text NOT NULL)")
				return err
			},
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = client.WithConn(
				context.Background(),
				func(ctx context.Context, conn pg.Querier) error {
					_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table)
					return err
				},
			)
		})
	}

	t.Run(
		"Query",
		func(t *testing.T) {
			setup(t, "test_tx_query")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_query (id, name) VALUES ($1, $2)", 1, "alice")
					if err != nil {
						return err
					}
					_, err = tx.Exec(ctx,
						"INSERT INTO test_tx_query (id, name) VALUES ($1, $2)", 2, "bob")
					if err != nil {
						return err
					}

					rows, err := tx.Query(ctx,
						"SELECT name FROM test_tx_query ORDER BY id")
					if err != nil {
						return err
					}
					defer rows.Close()

					var names []string
					for rows.Next() {
						var n string
						if err := rows.Scan(&n); err != nil {
							return err
						}
						names = append(names, n)
					}

					assert.Equal(t, []string{"alice", "bob"}, names)
					return rows.Err()
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"SendBatch",
		func(t *testing.T) {
			setup(t, "test_tx_batch")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					batch := &pgx.Batch{}
					batch.Queue(
						"INSERT INTO test_tx_batch (id, name) VALUES ($1, $2)", 1, "a")
					batch.Queue(
						"INSERT INTO test_tx_batch (id, name) VALUES ($1, $2)", 2, "b")

					br := tx.SendBatch(ctx, batch)
					for range 2 {
						if _, err := br.Exec(); err != nil {
							return err
						}
					}
					if err := br.Close(); err != nil {
						return err
					}

					var count int
					err := tx.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_batch").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 2, count)
					return nil
				},
			)
			require.NoError(t, err)
		},
	)

	t.Run(
		"CopyFrom",
		func(t *testing.T) {
			setup(t, "test_tx_copy")

			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					src := pgx.CopyFromRows([][]any{
						{1, "alice"},
						{2, "bob"},
						{3, "carol"},
					})
					n, err := tx.CopyFrom(ctx,
						pgx.Identifier{"test_tx_copy"},
						[]string{"id", "name"},
						src,
					)
					if err != nil {
						return err
					}
					assert.Equal(t, int64(3), n)

					var count int
					err = tx.QueryRow(ctx,
						"SELECT count(*) FROM test_tx_copy").Scan(&count)
					require.NoError(t, err)
					assert.Equal(t, 3, count)
					return nil
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
			func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, conn pg.Querier) error {
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
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
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
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
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
		"savepoint creates spans",
		func(t *testing.T) {
			setup(t, "test_tx_trace_sp")
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_trace_sp (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}
					return tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							_, err := inner.Exec(ctx,
								"INSERT INTO test_tx_trace_sp (name) VALUES ($1)", "inner")
							return err
						},
					)
				},
			)
			require.NoError(t, err)
			span.End()

			names := spanNames(sr.Ended()[before:])
			assert.Contains(t, names, "WithTx")
			assert.Contains(t, names, "Savepoint")
		},
	)

	t.Run(
		"savepoint rollback records error",
		func(t *testing.T) {
			setup(t, "test_tx_trace_sp_err")
			before := len(sr.Ended())

			ctx, span := tp.Tracer("test").Start(context.Background(), "test-root")
			err := client.WithTx(
				ctx,
				func(ctx context.Context, tx pg.Tx) error {
					_, err := tx.Exec(ctx,
						"INSERT INTO test_tx_trace_sp_err (name) VALUES ($1)", "outer")
					if err != nil {
						return err
					}
					_ = tx.Savepoint(
						ctx,
						func(ctx context.Context, inner pg.Tx) error {
							return errors.New("inner savepoint error")
						},
					)
					return nil
				},
			)
			require.NoError(t, err)
			span.End()

			for _, s := range sr.Ended()[before:] {
				if s.Name() == "Savepoint" {
					assert.Equal(t, codes.Error, s.Status().Code)
				}
			}
		},
	)
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
			err := client.WithAdvisoryLock(ctx, 1, func(conn pg.Querier) error {
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
			err := client.WithAdvisoryLock(ctx, 2, func(conn pg.Querier) error {
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
			err := client.WithAdvisoryLock(ctx, 100, func(conn pg.Querier) error {
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
			err := client.WithAdvisoryLock(ctx, 101, func(conn pg.Querier) error {
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
		func(ctx context.Context, conn pg.Querier) error {
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
			func(ctx context.Context, conn pg.Querier) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_batch")
				return err
			},
		)
	})

	before := len(sr.Ended())

	ctx, span := tp.Tracer("test").Start(bgCtx, "test-root")
	err = client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
			batch := &pgx.Batch{}
			batch.Queue("INSERT INTO test_batch (name) VALUES ($1)", "a")
			batch.Queue("INSERT INTO test_batch (name) VALUES ($1)", "b")

			br := conn.SendBatch(ctx, batch)
			for range 2 {
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
		func(ctx context.Context, conn pg.Querier) error {
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
		func(ctx context.Context, conn pg.Querier) error {
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
			func(ctx context.Context, conn pg.Querier) error {
				_, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_copy")
				return err
			},
		)
	})

	before := len(sr.Ended())

	ctx, span := tp.Tracer("test").Start(bgCtx, "test-root")
	err = client.WithConn(
		ctx,
		func(ctx context.Context, conn pg.Querier) error {
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
		func(ctx context.Context, conn pg.Querier) error {
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
		func(ctx context.Context, conn pg.Querier) error {
			var n int
			return conn.QueryRow(ctx, "SELECT 42").Scan(&n)
		},
	)
	require.NoError(t, err)
}
