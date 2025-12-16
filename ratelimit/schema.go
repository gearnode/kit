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

package ratelimit

import (
	"context"
	"fmt"
	"time"

	"go.gearno.de/kit/log"
	"go.gearno.de/kit/pg"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ensureTable creates the rate_limits UNLOGGED table if it doesn't exist.
// UNLOGGED tables are faster because they don't write to the WAL, but
// data is lost on crash (acceptable for rate limiting).
func ensureTable(ctx context.Context, conn pg.Conn) error {
	q := `
CREATE UNLOGGED TABLE IF NOT EXISTS rate_limits (
    key           TEXT NOT NULL,
    window_start  BIGINT NOT NULL,
    count         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (key, window_start)
);

CREATE INDEX IF NOT EXISTS idx_rate_limits_cleanup 
ON rate_limits (window_start);
`
	_, err := conn.Exec(ctx, q)
	return err
}

// Cleanup removes expired rate limit entries from the database.
// It deletes all entries where the window_start is older than the
// specified duration. This should be called periodically to prevent
// unbounded table growth.
func (l *Limiter) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = l.tracer.Start(
			ctx,
			"ratelimit.Cleanup",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(
				attribute.Int64("ratelimit.cleanup_older_than_ms", olderThan.Milliseconds()),
			),
		)
		defer span.End()
	}

	cutoff := time.Now().Add(-olderThan).UnixMilli()
	var rowsDeleted int64

	err := l.pg.WithConn(ctx, func(conn pg.Conn) error {
		q := `DELETE FROM rate_limits WHERE window_start < $1`
		tag, err := conn.Exec(ctx, q, cutoff)
		if err != nil {
			return err
		}
		rowsDeleted = tag.RowsAffected()
		return nil
	})

	if err != nil {
		if rootSpan.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return 0, fmt.Errorf("cannot cleanup rate limits: %w", err)
	}

	if rootSpan.IsRecording() {
		span.SetAttributes(
			attribute.Int64("ratelimit.rows_deleted", rowsDeleted),
		)
	}

	l.logger.InfoCtx(ctx, "rate limit cleanup completed",
		log.Int64("rows_deleted", rowsDeleted),
		log.Duration("older_than", olderThan),
	)

	return rowsDeleted, nil
}


