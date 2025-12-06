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
	"database/sql"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

type (
	tracer struct {
		tracer trace.Tracer
	}
)

var (
	_ pgx.QueryTracer       = (*tracer)(nil)
	_ pgx.BatchTracer       = (*tracer)(nil)
	_ pgx.CopyFromTracer    = (*tracer)(nil)
	_ pgx.PrepareTracer     = (*tracer)(nil)
	_ pgx.ConnectTracer     = (*tracer)(nil)
	_ pgxpool.AcquireTracer = (*tracer)(nil)
)

const (
	tracerName = "go.gearno.de/kit/pg"

	// BatchSizeKey represents the batch size.
	BatchSizeKey = attribute.Key("db.operation.batch.size")

	// PrepareStmtNameKey represents the prepared statement name.
	PrepareStmtNameKey = attribute.Key("pgx.prepare_stmt.name")

	// RowsAffectedKey represents the number of rows affected.
	RowsAffectedKey = attribute.Key("pgx.rows_affected")

	// SQLStateKey represents PostgreSQL error code,
	// see https://www.postgresql.org/docs/current/errcodes-appendix.html.
	SQLStateKey = attribute.Key("db.response.status_code")
)

func connectionConfigAttributes(config *pgx.ConnConfig) []trace.SpanStartOption {
	if config != nil {
		return []trace.SpanStartOption{
			trace.WithAttributes(
				semconv.NetworkPeerAddress(config.Host),
				semconv.NetworkPeerPort(int(config.Port)),
				semconv.DBSystemPostgreSQL,
			),
		}
	}

	return nil
}

func sqlOperationName(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) > 0 {
		return strings.ToUpper(fields[0])
	}

	return "UNKNOWN"
}

func maybeRecordError(span trace.Span, err error) {
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		recordError(span, err)
	}
}

func recordError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		span.SetAttributes(
			SQLStateKey.String(pgErr.Code),
		)
	}
}

func (t *tracer) TraceQueryStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	operationName := sqlOperationName(data.SQL)
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBOperationName(operationName),
			semconv.DBQueryText(data.SQL),
		),
	}

	if conn != nil {
		cfg := conn.Config()
		opts = append(opts, connectionConfigAttributes(cfg)...)
	}

	ctx, _ = t.tracer.Start(ctx, "db.query", opts...)

	return ctx
}

func (t *tracer) TraceQueryEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryEndData,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	maybeRecordError(span, data.Err)

	if data.Err == nil {
		span.SetAttributes(
			RowsAffectedKey.Int64(
				data.CommandTag.RowsAffected(),
			),
		)
	}

	span.End()
}

func (t *tracer) TraceBatchStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchStartData,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	var size int
	if b := data.Batch; b != nil {
		size = b.Len()
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			BatchSizeKey.Int(size),
		),
	}

	if conn != nil {
		cfg := conn.Config()
		opts = append(opts, connectionConfigAttributes(cfg)...)
	}

	ctx, _ = t.tracer.Start(ctx, "db.batch.query", opts...)

	return ctx
}

func (t *tracer) TraceBatchQuery(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchQueryData,
) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return
	}

	operationName := sqlOperationName(data.SQL)
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBOperationName(operationName),
			semconv.DBQueryText(data.SQL),
		),
	}

	if conn != nil {
		cfg := conn.Config()
		opts = append(opts, connectionConfigAttributes(cfg)...)
	}

	_, span := t.tracer.Start(ctx, "db.query", opts...)
	maybeRecordError(span, data.Err)
	span.End()
}

func (t *tracer) TraceBatchEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceBatchEndData,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	maybeRecordError(span, data.Err)
	span.End()
}

func (t *tracer) TraceCopyFromStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceCopyFromStartData,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBCollectionName(
				data.TableName.Sanitize(),
			),
		),
	}

	if conn != nil {
		cfg := conn.Config()
		opts = append(opts, connectionConfigAttributes(cfg)...)
	}

	ctx, _ = t.tracer.Start(ctx, "db.copy", opts...)

	return ctx
}

func (t *tracer) TraceCopyFromEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceCopyFromEndData,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	maybeRecordError(span, data.Err)

	if data.Err == nil {
		span.SetAttributes(
			RowsAffectedKey.Int64(
				data.CommandTag.RowsAffected(),
			),
		)
	}

	span.End()
}

func (t *tracer) TracePrepareStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TracePrepareStartData,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	operationName := sqlOperationName(data.SQL)
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBOperationName(operationName),
			semconv.DBQueryText(data.SQL),
		),
	}

	if conn != nil {
		cfg := conn.Config()
		opts = append(opts, connectionConfigAttributes(cfg)...)
	}

	if data.Name != "" {
		opts = append(
			opts,
			trace.WithAttributes(
				PrepareStmtNameKey.String(data.Name),
			),
		)
	}

	ctx, _ = t.tracer.Start(ctx, "db.prepare-statement", opts...)

	return ctx
}

func (t *tracer) TracePrepareEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TracePrepareEndData,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	maybeRecordError(span, data.Err)

	span.End()
}

func (t *tracer) TraceConnectStart(
	ctx context.Context,
	data pgx.TraceConnectStartData,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
	}

	if data.ConnConfig != nil {
		cfg := data.ConnConfig
		opts = append(opts, connectionConfigAttributes(cfg)...)
	}

	ctx, _ = t.tracer.Start(ctx, "db.connect", opts...)

	return ctx
}

func (t *tracer) TraceConnectEnd(
	ctx context.Context,
	data pgx.TraceConnectEndData,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	maybeRecordError(span, data.Err)

	span.End()
}

func (t *tracer) TraceAcquireStart(
	ctx context.Context,
	pool *pgxpool.Pool,
	data pgxpool.TraceAcquireStartData,
) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
	}

	if pool != nil {
		poolCfg := pool.Config()
		if poolCfg != nil && poolCfg.ConnConfig != nil {
			cfg := poolCfg.ConnConfig
			opts = append(opts, connectionConfigAttributes(cfg)...)
		}
	}

	ctx, _ = t.tracer.Start(ctx, "pgx.pool.acquire", opts...)

	return ctx
}

func (t *tracer) TraceAcquireEnd(
	ctx context.Context,
	pool *pgxpool.Pool,
	data pgxpool.TraceAcquireEndData,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	maybeRecordError(span, data.Err)

	span.End()
}
