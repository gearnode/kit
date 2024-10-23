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

package log

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type (
	Logger struct {
		logger     *slog.Logger
		output     io.Writer
		path       string
		level      *slog.LevelVar
		attributes []any
	}

	Option func(l *Logger)

	Level = slog.Level

	Attr = slog.Attr
)

var (
	LevelInfo  = slog.LevelInfo
	LevelError = slog.LevelError
	LevelWarn  = slog.LevelWarn
	LevelDebug = slog.LevelDebug
)

func WithLevel(level slog.Level) Option {
	return func(l *Logger) {
		l.level.Set(level)
	}
}

func WithOutput(w io.Writer) Option {
	return func(l *Logger) {
		l.output = w
	}

}

func WithName(name string) Option {
	return func(l *Logger) {
		l.path = name
	}
}

func WithAttributes(args ...any) Option {
	return func(l *Logger) {
		l.attributes = args
	}
}

func Any(k string, v any) Attr {
	return slog.Any(k, v)
}

func Bool(k string, v bool) Attr {
	return slog.Bool(k, v)
}

func Duration(k string, v time.Duration) Attr {
	return slog.Duration(k, v)
}

func Float64(k string, v float64) Attr {
	return slog.Float64(k, v)
}

func Int(k string, v int) Attr {
	return slog.Int(k, v)
}

func Int64(k string, v int64) Attr {
	return slog.Int64(k, v)
}

func String(k, v string) Attr {
	return slog.String(k, v)
}

func Time(k string, v time.Time) Attr {
	return slog.Time(k, v)
}

func Uint64(k string, v uint64) Attr {
	return slog.Uint64(k, v)
}

func Error(err error) Attr {
	return String("error", err.Error())
}

func NewLogger(options ...Option) *Logger {
	l := &Logger{
		output: os.Stderr,
		level:  new(slog.LevelVar),
	}

	for _, option := range options {
		option(l)
	}

	handler := slog.NewJSONHandler(
		l.output,
		&slog.HandlerOptions{
			Level: l.level,
		},
	)

	l.logger = slog.New(handler).With(l.attributes...)

	return l
}

func (l *Logger) With(args ...any) *Logger {
	return NewLogger(WithName(l.path), WithAttributes(args...))
}

func (l *Logger) Named(name string, options ...Option) *Logger {
	newPath := l.path
	if newPath != "" {
		newPath += "."
	}
	newPath += name

	options = append(options, WithName(newPath))

	return NewLogger(options...)
}

func (l *Logger) Log(ctx context.Context, level Level, msg string, args ...Attr) {
	var (
		span    = trace.SpanFromContext(ctx)
		spanCtx = span.SpanContext()
		traceID = spanCtx.TraceID().String()
		spanID  = spanCtx.SpanID().String()
	)

	args = append(
		args,
		slog.String("trace_id", traceID),
		slog.String("span_id", spanID),
	)

	l.logger.LogAttrs(ctx, level, msg, args...)
}

func (l *Logger) Info(msg string, args ...Attr) {
	l.Log(context.Background(), LevelInfo, msg, args...)
}

func (l *Logger) InfoCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelInfo, msg, args...)
}

func (l *Logger) Error(msg string, args ...Attr) {
	l.Log(context.Background(), LevelError, msg, args...)
}

func (l *Logger) ErrorCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelError, msg, args...)
}

func (l *Logger) Warn(msg string, args ...Attr) {
	l.Log(context.Background(), LevelWarn, msg, args...)
}

func (l *Logger) WarnCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelWarn, msg, args...)
}

func (l *Logger) Debug(msg string, args ...Attr) {
	l.Log(context.Background(), LevelDebug, msg, args...)
}

func (l *Logger) DebugCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelDebug, msg, args...)
}
