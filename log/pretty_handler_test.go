package log

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPrettyHandler(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)

	assert.NotNil(t, h)
}

func TestNewPrettyHandlerWithOptions(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})

	assert.True(t, h.Enabled(context.Background(), slog.LevelWarn))
	assert.True(t, h.Enabled(context.Background(), slog.LevelError))
	assert.False(t, h.Enabled(context.Background(), slog.LevelInfo))
	assert.False(t, h.Enabled(context.Background(), slog.LevelDebug))
}

func TestPrettyHandlerEnabled(t *testing.T) {
	tests := []struct {
		name       string
		handlerLvl slog.Level
		logLvl     slog.Level
		want       bool
	}{
		{"info at info", slog.LevelInfo, slog.LevelInfo, true},
		{"warn at info", slog.LevelInfo, slog.LevelWarn, true},
		{"debug at info", slog.LevelInfo, slog.LevelDebug, false},
		{"error at error", slog.LevelError, slog.LevelError, true},
		{"info at error", slog.LevelError, slog.LevelInfo, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := NewPrettyHandler(&buf, &slog.HandlerOptions{
				Level: tt.handlerLvl,
			})
			assert.Equal(t, tt.want, h.Enabled(context.Background(), tt.logLvl))
		})
	}
}

func TestPrettyHandlerHandle(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	r := slog.NewRecord(time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC), slog.LevelInfo, "hello world", 0)
	r.AddAttrs(slog.String("key", "value"))

	err := h.Handle(context.Background(), r)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "hello world")
	assert.Contains(t, out, "key=")
	assert.Contains(t, out, "value")
	assert.Contains(t, out, "2025-01-15T10:30:00Z")
}

func TestPrettyHandlerHandleWithName(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "test", 0)
	r.AddAttrs(slog.String("name", "myservice"))

	err := h.Handle(context.Background(), r)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "myservice")
	assert.Contains(t, out, "test")
}

func TestPrettyHandlerHandleWithStack(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)

	r := slog.NewRecord(time.Now(), slog.LevelError, "crash", 0)
	r.AddAttrs(slog.String("stack", "goroutine 1 [running]:\nmain.main()"))

	err := h.Handle(context.Background(), r)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "crash")
	assert.Contains(t, out, "goroutine 1 [running]:")
}

func TestPrettyHandlerHandleErrorAttr(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)

	r := slog.NewRecord(time.Now(), slog.LevelError, "failed", 0)
	r.AddAttrs(slog.String("error", "connection refused"))

	err := h.Handle(context.Background(), r)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "error=")
	assert.Contains(t, out, "connection refused")
}

func TestPrettyHandlerWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)

	h2 := h.WithAttrs([]slog.Attr{slog.String("env", "test")})

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	err := h2.(*PrettyHandler).Handle(context.Background(), r)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "env=")
	assert.Contains(t, out, "test")
}

func TestPrettyHandlerWithGroup(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, nil)

	h2 := h.WithGroup("request")

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	r.AddAttrs(slog.String("method", "GET"))

	err := h2.(*PrettyHandler).Handle(context.Background(), r)
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, "request")
	assert.Contains(t, out, "method=")
}

func TestPrettyHandlerAllLevels(t *testing.T) {
	levels := []slog.Level{
		slog.LevelDebug,
		slog.LevelInfo,
		slog.LevelWarn,
		slog.LevelError,
	}

	for _, lvl := range levels {
		t.Run(lvl.String(), func(t *testing.T) {
			var buf bytes.Buffer
			h := NewPrettyHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})

			r := slog.NewRecord(time.Now(), lvl, "test", 0)
			err := h.Handle(context.Background(), r)
			require.NoError(t, err)
			assert.NotEmpty(t, buf.String())
		})
	}
}

func TestPrettyHandlerConcurrency(t *testing.T) {
	var buf bytes.Buffer
	h := NewPrettyHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	done := make(chan struct{})
	for i := range 50 {
		go func(n int) {
			defer func() { done <- struct{}{} }()
			r := slog.NewRecord(time.Now(), slog.LevelInfo, "concurrent", 0)
			r.AddAttrs(slog.Int("n", n))
			_ = h.Handle(context.Background(), r)
		}(i)
	}

	for range 50 {
		<-done
	}

	assert.NotEmpty(t, buf.String())
}

func TestLevelTags(t *testing.T) {
	for _, lvl := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		tag, ok := LevelTags[lvl]
		assert.True(t, ok, "LevelTags should contain %s", lvl)
		assert.NotEmpty(t, tag)
	}
}

func TestBufferPool(t *testing.T) {
	buf := getBuffer()
	assert.NotNil(t, buf)

	buf.WriteString("hello")
	freeBuffer(buf)

	buf2 := getBuffer()
	assert.NotNil(t, buf2)
}
