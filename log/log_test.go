package log

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func parseJSONLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	return m
}

func TestNewLoggerDefaults(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	l.Info("hello")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "INFO", m["level"])
	assert.Equal(t, "hello", m["msg"])
	assert.NotEmpty(t, m["time"])
}

func TestNewLoggerWithLevel(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithLevel(LevelError))

	l.Info("should not appear")
	assert.Empty(t, buf.String())

	l.Error("visible")
	m := parseJSONLog(t, &buf)
	assert.Equal(t, "ERROR", m["level"])
	assert.Equal(t, "visible", m["msg"])
}

func TestNewLoggerWithName(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithName("myapp"))

	l.Info("test")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "myapp", m["name"])
}

func TestNewLoggerWithAttributes(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(
		WithOutput(&buf),
		WithAttributes(String("service", "api"), Int("version", 2)),
	)

	l.Info("test")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "api", m["service"])
	assert.Equal(t, float64(2), m["version"])
}

func TestNewLoggerWithFormatText(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithFormat(FormatText))

	l.Info("text-log")
	assert.Contains(t, buf.String(), "text-log")
	assert.Contains(t, buf.String(), "level=INFO")
}

func TestNewLoggerWithFormatPretty(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithFormat(FormatPretty))

	l.Info("pretty-log")
	assert.Contains(t, buf.String(), "pretty-log")
}

func TestNewLoggerPanicsOnUnsupportedFormat(t *testing.T) {
	assert.Panics(t, func() {
		NewLogger(WithFormat("xml"))
	})
}

func TestSkipMatch(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(
		WithOutput(&buf),
		SkipMatch(func(level Level, msg string, attrs []Attr) bool {
			return msg == "skip-me"
		}),
	)

	l.Info("skip-me")
	assert.Empty(t, buf.String())

	l.Info("keep-me")
	m := parseJSONLog(t, &buf)
	assert.Equal(t, "keep-me", m["msg"])
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(
		WithOutput(&buf),
		WithName("base"),
		WithAttributes(String("env", "test")),
	)

	l2 := l.With(String("extra", "val"))

	l2.Info("with-test")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "test", m["env"])
	assert.Equal(t, "val", m["extra"])
	assert.Equal(t, "base", m["name"])
}

func TestLoggerWithPreservesMatch(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(
		WithOutput(&buf),
		SkipMatch(func(level Level, msg string, attrs []Attr) bool {
			return msg == "skip"
		}),
	)

	l2 := l.With(String("k", "v"))
	l2.Info("skip")
	assert.Empty(t, buf.String())
}

func TestLoggerNamed(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithName("parent"))

	child := l.Named("child")
	child.Info("named-test")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "parent.child", m["name"])
}

func TestLoggerNamedFromEmpty(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	child := l.Named("service")
	child.Info("test")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "service", m["name"])
}

func TestLoggerNamedPreservesMatch(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(
		WithOutput(&buf),
		SkipMatch(func(level Level, msg string, attrs []Attr) bool {
			return msg == "skip"
		}),
	)

	child := l.Named("child")
	child.Info("skip")
	assert.Empty(t, buf.String())
}

func TestLoggerNamedOptionOverride(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithLevel(LevelInfo))

	child := l.Named("child", WithLevel(LevelDebug))
	child.Debug("debug-visible")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "DEBUG", m["level"])
}

func TestLogLevelFiltering(t *testing.T) {
	tests := []struct {
		name      string
		logLevel  Level
		logFunc   string
		shouldLog bool
	}{
		{"info at info level", LevelInfo, "info", true},
		{"debug at info level", LevelInfo, "debug", false},
		{"warn at info level", LevelInfo, "warn", true},
		{"error at info level", LevelInfo, "error", true},
		{"debug at debug level", LevelDebug, "debug", true},
		{"info at error level", LevelError, "info", false},
		{"warn at error level", LevelError, "warn", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewLogger(WithOutput(&buf), WithLevel(tt.logLevel))

			switch tt.logFunc {
			case "info":
				l.Info("msg")
			case "debug":
				l.Debug("msg")
			case "warn":
				l.Warn("msg")
			case "error":
				l.Error("msg")
			}

			if tt.shouldLog {
				assert.NotEmpty(t, buf.String())
			} else {
				assert.Empty(t, buf.String())
			}
		})
	}
}

func TestLogWithTraceContext(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	l.InfoCtx(ctx, "traced")

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "traced", m["msg"])
}

func TestConvenienceMethods(t *testing.T) {
	methods := []struct {
		name  string
		level string
		call  func(l *Logger)
	}{
		{"Info", "INFO", func(l *Logger) { l.Info("msg") }},
		{"Warn", "WARN", func(l *Logger) { l.Warn("msg") }},
		{"Error", "ERROR", func(l *Logger) { l.Error("msg") }},
		{"Debug", "DEBUG", func(l *Logger) { l.Debug("msg") }},
	}

	for _, tt := range methods {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewLogger(WithOutput(&buf), WithLevel(LevelDebug))
			tt.call(l)

			m := parseJSONLog(t, &buf)
			assert.Equal(t, tt.level, m["level"])
			assert.Equal(t, "msg", m["msg"])
		})
	}
}

func TestCtxConvenienceMethods(t *testing.T) {
	methods := []struct {
		name  string
		level string
		call  func(l *Logger, ctx context.Context)
	}{
		{"InfoCtx", "INFO", func(l *Logger, ctx context.Context) { l.InfoCtx(ctx, "msg") }},
		{"WarnCtx", "WARN", func(l *Logger, ctx context.Context) { l.WarnCtx(ctx, "msg") }},
		{"ErrorCtx", "ERROR", func(l *Logger, ctx context.Context) { l.ErrorCtx(ctx, "msg") }},
		{"DebugCtx", "DEBUG", func(l *Logger, ctx context.Context) { l.DebugCtx(ctx, "msg") }},
	}

	for _, tt := range methods {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewLogger(WithOutput(&buf), WithLevel(LevelDebug))
			tt.call(l, context.Background())

			m := parseJSONLog(t, &buf)
			assert.Equal(t, tt.level, m["level"])
		})
	}
}

func TestConvenienceMethodsWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	l.Info("msg", String("key", "value"), Int("count", 42))

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "value", m["key"])
	assert.Equal(t, float64(42), m["count"])
}

func TestAttrHelpers(t *testing.T) {
	now := time.Now()
	dur := 5 * time.Second

	tests := []struct {
		name     string
		attr     Attr
		wantKey  string
		wantKind slog.Kind
	}{
		{"Any", Any("k", []int{1}), "k", slog.KindAny},
		{"Bool", Bool("k", true), "k", slog.KindBool},
		{"Duration", Duration("k", dur), "k", slog.KindDuration},
		{"Float64", Float64("k", 3.14), "k", slog.KindFloat64},
		{"Int", Int("k", 42), "k", slog.KindInt64},
		{"Int64", Int64("k", 99), "k", slog.KindInt64},
		{"String", String("k", "v"), "k", slog.KindString},
		{"Time", Time("k", now), "k", slog.KindTime},
		{"Uint64", Uint64("k", 7), "k", slog.KindUint64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantKey, tt.attr.Key)
			assert.Equal(t, tt.wantKind, tt.attr.Value.Kind())
		})
	}
}

func TestErrorAttr(t *testing.T) {
	err := errors.New("something broke")
	attr := Error(err)

	assert.Equal(t, "error", attr.Key)
	assert.Equal(t, "something broke", attr.Value.String())
}

func TestLogNameAlwaysPresent(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	l.Info("test")

	m := parseJSONLog(t, &buf)
	_, ok := m["name"]
	assert.True(t, ok, "name attribute should always be present")
}

func TestLogWithMultipleAttributes(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	l.Log(context.Background(), LevelInfo, "multi",
		String("a", "1"),
		Int("b", 2),
		Bool("c", true),
	)

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "1", m["a"])
	assert.Equal(t, float64(2), m["b"])
	assert.Equal(t, true, m["c"])
}

func TestErrorAttrFormat(t *testing.T) {
	attr := Error(fmt.Errorf("wrapped: %w", errors.New("inner")))
	assert.Equal(t, "error", attr.Key)
	assert.Contains(t, attr.Value.String(), "wrapped")
	assert.Contains(t, attr.Value.String(), "inner")
}
