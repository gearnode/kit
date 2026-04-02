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
	// Querier with the ability to create savepoints.
	Tx interface {
		Querier

		Savepoint(context.Context, ExecFunc[Querier]) error
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
func (t *pgxTx) Savepoint(ctx context.Context, fn ExecFunc[Querier]) error {
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

	if err := fn(ctx, sp); err != nil {
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
