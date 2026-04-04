package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewWriter(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	w := l.NewWriter(LevelInfo)
	assert.NotNil(t, w)
}

func TestWriterWrite(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	w := l.NewWriter(LevelInfo)
	n, err := w.Write([]byte("hello from writer\n"))

	assert.NoError(t, err)
	assert.Equal(t, len("hello from writer\n"), n)

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "hello from writer", m["msg"])
	assert.Equal(t, "INFO", m["level"])
}

func TestWriterWriteTrimsWhitespace(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	w := l.NewWriter(LevelWarn)
	_, err := w.Write([]byte("  padded message  \n"))

	assert.NoError(t, err)

	m := parseJSONLog(t, &buf)
	assert.Equal(t, "padded message", m["msg"])
	assert.Equal(t, "WARN", m["level"])
}

func TestWriterAtDifferentLevels(t *testing.T) {
	levels := []struct {
		name  string
		level Level
		want  string
	}{
		{"info", LevelInfo, "INFO"},
		{"warn", LevelWarn, "WARN"},
		{"error", LevelError, "ERROR"},
		{"debug", LevelDebug, "DEBUG"},
	}

	for _, tt := range levels {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := NewLogger(WithOutput(&buf), WithLevel(LevelDebug))

			w := l.NewWriter(tt.level)
			_, err := w.Write([]byte("msg"))
			assert.NoError(t, err)

			m := parseJSONLog(t, &buf)
			assert.Equal(t, tt.want, m["level"])
		})
	}
}

func TestWriterRespectsLogLevel(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf), WithLevel(LevelError))

	w := l.NewWriter(LevelInfo)
	_, err := w.Write([]byte("should be filtered"))

	assert.NoError(t, err)
	assert.Empty(t, buf.String())
}

func TestWriterImplementsIOWriter(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(WithOutput(&buf))

	var _ interface{ Write([]byte) (int, error) } = l.NewWriter(LevelInfo)
}
