package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

// Handler is a colored slog handler.
type PrettyHandler struct {
	groups []string
	attrs  []slog.Attr

	opts slog.HandlerOptions

	mu  *sync.Mutex
	out io.Writer
}

var LevelTags = map[slog.Level]string{
	slog.LevelDebug: color.New(color.FgWhite, color.Bold).Sprint("DEBUG"),
	slog.LevelInfo:  color.New(color.FgBlue, color.Bold).Sprint("INFO"),
	slog.LevelWarn:  color.New(color.FgYellow, color.Bold).Sprint("WARN"),
	slog.LevelError: color.New(color.FgRed, color.Bold).Sprint("ERROR"),
}

// NewHandler creates a new [Handler] with the specified options. If opts is nil, uses [DefaultOptions].
func NewPrettyHandler(out io.Writer, opts *slog.HandlerOptions) *PrettyHandler {
	h := &PrettyHandler{out: out, mu: &sync.Mutex{}}
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	h.opts = *opts

	return h
}

func (h *PrettyHandler) clone() *PrettyHandler {
	return &PrettyHandler{
		groups: h.groups,
		attrs:  h.attrs,
		opts:   h.opts,
		mu:     h.mu,
		out:    h.out,
	}
}

// Enabled implements slog.Handler.Enabled .
func (h *PrettyHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

// Handle implements slog.Handler.Handle .
func (h *PrettyHandler) Handle(_ context.Context, r slog.Record) error {
	bf := getBuffer()
	bf.Reset()

	fmt.Fprint(bf, color.New(color.Faint).Sprint(r.Time.Format(time.RFC3339)))
	fmt.Fprint(bf, " ")

	fmt.Fprint(bf, LevelTags[r.Level])
	fmt.Fprint(bf, " ")

	// we need the attributes here, as we can print a longer string if there are no attributes
	stacktrace := ""
	name := ""
	var attrs []slog.Attr
	attrs = append(attrs, h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "stack" {
			stacktrace = a.Value.String()
			return true
		}
		if a.Key == "name" {
			name = a.Value.String()
			return true
		}
		attrs = append(attrs, a)
		return true
	})

	if name != "" {
		fmt.Fprint(bf, color.New(color.Faint, color.Bold).Sprint(name))
		fmt.Fprint(bf, " ")
	}

	if stacktrace != "" {
		if r.PC != 0 {
			f, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()

			filename := f.File
			lineStr := fmt.Sprintf(":%d", f.Line)
			formatted := fmt.Sprintf("%s ", filename+lineStr)
			fmt.Fprint(bf, formatted)
		}
	}

	fmt.Fprint(bf, color.New(color.FgHiWhite).Sprint(r.Message))

	for _, a := range attrs {
		fmt.Fprint(bf, " ")
		for i, g := range h.groups {
			fmt.Fprint(bf, color.New(color.FgWhite).Sprint(g))
			if i != len(h.groups) {
				fmt.Fprint(bf, color.New(color.FgWhite).Sprint("."))
			}
		}

		value := color.New(color.FgWhite).Sprint(a.Value.String())
		if strings.Contains(a.Key, "err") {
			fmt.Fprint(bf, color.New(color.FgRed).Sprintf("%s=", a.Key)+value)
		} else {
			fmt.Fprint(bf, color.New(color.Faint).Sprintf("%s=", a.Key)+value)
		}
	}

	if stacktrace != "" {
		fmt.Fprint(bf, "\n")
		fmt.Fprint(bf, stacktrace)
	}

	fmt.Fprint(bf, "\n")

	h.mu.Lock()
	_, err := io.Copy(h.out, bf)
	h.mu.Unlock()

	freeBuffer(bf)

	return err
}

// WithGroup implements slog.Handler.WithGroup .
func (h *PrettyHandler) WithGroup(name string) slog.Handler {
	h2 := h.clone()
	h2.groups = append(h2.groups, name)
	return h2
}

// WithAttrs implements slog.Handler.WithAttrs .
func (h *PrettyHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := h.clone()
	h2.attrs = append(h2.attrs, attrs...)
	return h2
}
