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
	// Logger represents a structured logger with tracing and
	// flexible output configuration.
	Logger struct {
		logger     *slog.Logger
		output     io.Writer
		path       string
		level      *slog.LevelVar
		attributes []Attr
	}

	// Option configures Logger during initialization.
	Option func(l *Logger)

	// Level defines log levels for filtering log messages.
	Level = slog.Level

	// Attr represents an attribute (key-value pair) added to log
	// entries for structured logging.
	Attr = slog.Attr
)

var (
	LevelInfo  = slog.LevelInfo
	LevelError = slog.LevelError
	LevelWarn  = slog.LevelWarn
	LevelDebug = slog.LevelDebug
)

// WithLevel sets the logging level for the Logger.
func WithLevel(level slog.Level) Option {
	return func(l *Logger) {
		l.level.Set(level)
	}
}

// WithOutput directs the log output to the specified io.Writer.
func WithOutput(w io.Writer) Option {
	return func(l *Logger) {
		l.output = w
	}

}

// WithName assigns a name to the Logger, useful for identifying the
// logging source in a multi-module setup.
func WithName(name string) Option {
	return func(l *Logger) {
		l.path = name
	}
}

// WithAttributes assigns default attributes to all log entries for
// the Logger.
func WithAttributes(attrs ...Attr) Option {
	return func(l *Logger) {
		l.attributes = attrs
	}
}

// Any creates a key-value attribute with any data type.
func Any(k string, v any) Attr {
	return slog.Any(k, v)
}

// Bool creates a boolean attribute.
func Bool(k string, v bool) Attr {
	return slog.Bool(k, v)
}

// Duration creates a duration attribute.
func Duration(k string, v time.Duration) Attr {
	return slog.Duration(k, v)
}

// Float64 creates a float64 attribute.
func Float64(k string, v float64) Attr {
	return slog.Float64(k, v)
}

// Int creates an integer attribute.
func Int(k string, v int) Attr {
	return slog.Int(k, v)
}

// Int64 creates an int64 attribute.
func Int64(k string, v int64) Attr {
	return slog.Int64(k, v)
}

// String creates a string attribute.
func String(k, v string) Attr {
	return slog.String(k, v)
}

// Time creates a time attribute.
func Time(k string, v time.Time) Attr {
	return slog.Time(k, v)
}

// Uint64 creates a uint64 attribute.
func Uint64(k string, v uint64) Attr {
	return slog.Uint64(k, v)
}

// Error creates an attribute from an error, storing the error message
// as a string.
func Error(err error) Attr {
	return String("error", err.Error())
}

// NewLogger initializes a new Logger with optional configurations for
// level, output, and default attributes.
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
	).WithAttrs(l.attributes)

	l.logger = slog.New(handler)

	return l
}

// With returns a new Logger with additional attributes, keeping the
// original Logger’s name and settings.
func (l *Logger) With(attrs ...Attr) *Logger {
	return NewLogger(
		WithName(l.path),
		WithOutput(l.output),
		WithLevel(l.level.Level()),
		WithAttributes(
			append(l.attributes, attrs...)...,
		),
	)
}

// Named returns a new Logger with a modified name, appending the
// given name to the current Logger’s path.
func (l *Logger) Named(name string, options ...Option) *Logger {
	newPath := l.path
	if newPath != "" {
		newPath += "."
	}
	newPath += name

	options = append(options, WithName(newPath))

	return NewLogger(options...)
}

// Log logs a message at the specified level with optional attributes,
// adding trace and span IDs if the context has a span.
func (l *Logger) Log(ctx context.Context, level Level, msg string, args ...Attr) {
	span := trace.SpanFromContext(ctx)

	if span.IsRecording() {
		var (
			spanCtx = span.SpanContext()
			traceID = spanCtx.TraceID().String()
			spanID  = spanCtx.SpanID().String()
		)

		args = append(
			args,
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
		)
	}

	l.logger.LogAttrs(ctx, level, msg, append(l.attributes, args...)...)
}

// Info logs an informational message with optional attributes.
func (l *Logger) Info(msg string, args ...Attr) {
	l.Log(context.Background(), LevelInfo, msg, args...)
}

// InfoCtx logs an informational message with tracing, using the
// provided context and attributes.
func (l *Logger) InfoCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelInfo, msg, args...)
}

// Error logs an error message with optional attributes.
func (l *Logger) Error(msg string, args ...Attr) {
	l.Log(context.Background(), LevelError, msg, args...)
}

// ErrorCtx logs an error message with tracing, using the provided
// context and attributes.
func (l *Logger) ErrorCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelError, msg, args...)
}

// Warn logs a warning message with optional attributes.
func (l *Logger) Warn(msg string, args ...Attr) {
	l.Log(context.Background(), LevelWarn, msg, args...)
}

// WarnCtx logs a warning message with tracing, using the provided
// context and attributes.
func (l *Logger) WarnCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelWarn, msg, args...)
}

// Debug logs a debug message with optional attributes.
func (l *Logger) Debug(msg string, args ...Attr) {
	l.Log(context.Background(), LevelDebug, msg, args...)
}

// DebugCtx logs a debug message with tracing, using the provided
// context and attributes.
func (l *Logger) DebugCtx(ctx context.Context, msg string, args ...Attr) {
	l.Log(ctx, LevelDebug, msg, args...)
}
