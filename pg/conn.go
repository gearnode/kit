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
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"
)

// NoRollbackError wraps an error to signal that WithTx should
// commit the transaction instead of rolling back, while still
// propagating the inner error to the caller.
type NoRollbackError struct {
	Err error
}

func (e *NoRollbackError) Error() string {
	return e.Err.Error()
}

func (e *NoRollbackError) Unwrap() error {
	return e.Err
}

// NoRollback wraps err so that WithTx commits the transaction
// instead of rolling back. The inner error is still returned to the
// caller after the commit.
//
// A nil err is returned as-is (no wrapping).
func NoRollback(err error) error {
	if err == nil {
		return nil
	}

	return &NoRollbackError{Err: err}
}

type (
	// Querier represents something you can run SQL queries against.
	Querier interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)

		Query(context.Context, string, ...any) (pgx.Rows, error)
		QueryRow(context.Context, string, ...any) pgx.Row

		CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
		SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	}

	// Tx represents an active database transaction. It extends
	// Querier with the ability to create savepoints. The callback
	// receives a Tx so savepoints can be nested arbitrarily.
	Tx interface {
		Querier

		Savepoint(context.Context, ExecFunc[Tx]) error
	}

	pgxTx struct {
		inner  pgx.Tx
		tracer trace.Tracer
	}
)

func (t *pgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.inner.Exec(ctx, sql, args...)
}

func (t *pgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.inner.Query(ctx, sql, args...)
}

func (t *pgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.inner.QueryRow(ctx, sql, args...)
}

func (t *pgxTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return t.inner.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (t *pgxTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.inner.SendBatch(ctx, b)
}

// Savepoint executes fn within a savepoint. If fn returns an error,
// the savepoint is rolled back; otherwise, it is released. The outer
// transaction remains active regardless of the savepoint outcome.
//
// The callback receives a Tx, allowing nested savepoints.
func (t *pgxTx) Savepoint(ctx context.Context, fn ExecFunc[Tx]) error {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = t.tracer.Start(
			ctx,
			"Savepoint",
			trace.WithSpanKind(trace.SpanKindClient),
		)
		defer span.End()
	}

	sp, err := t.inner.Begin(ctx)
	if err != nil {
		err := fmt.Errorf("cannot create savepoint: %w", err)
		if span != nil {
			recordError(span, err)
		}

		return err
	}

	spTx := &pgxTx{inner: sp, tracer: t.tracer}

	if err := fn(ctx, spTx); err != nil {
		if err2 := sp.Rollback(ctx); err2 != nil {
			err = errors.Join(
				err,
				fmt.Errorf("cannot rollback savepoint: %w", err2),
			)
		}

		if span != nil {
			recordError(span, err)
		}

		return err
	}

	if err := sp.Commit(ctx); err != nil {
		err := fmt.Errorf("cannot release savepoint: %w", err)
		if span != nil {
			recordError(span, err)
		}

		return err
	}

	return nil
}
