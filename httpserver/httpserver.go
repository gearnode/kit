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

package httpserver

import (
	"io"
	stdlog "log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type (
	Option func(o *Options)

	Options struct {
		tracerProvider trace.TracerProvider
		logger         *log.Logger
		registerer     prometheus.Registerer
	}
)

// WithLogger is an option setter for specifying a logger for HTTP
// telemetry and error logging.
func WithLogger(l *log.Logger) Option {
	return func(o *Options) {
		o.logger = l.Named("http.server")
	}
}

// WithTracerProvider configures OpenTelemetry tracing with the
// provided tracer provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *Options) {
		o.tracerProvider = tp
	}
}

// WithRegisterer sets a custom Prometheus registerer for metrics.
func WithRegisterer(r prometheus.Registerer) Option {
	return func(o *Options) {
		o.registerer = r
	}
}

func NewServer(addr string, h http.Handler, options ...Option) *http.Server {
	opts := &Options{
		logger:         log.NewLogger(log.WithOutput(io.Discard)),
		tracerProvider: otel.GetTracerProvider(),
		registerer:     prometheus.DefaultRegisterer,
	}

	for _, o := range options {
		o(opts)
	}

	logger := opts.logger.With(log.String("http_server_addr", addr))
	handler := newHandlerWrapper(
		h,
		logger,
		opts.tracerProvider,
		opts.registerer,
	)

	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ErrorLog:          stdlog.New(logger, "", 0),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       15 * time.Second,
	}
}
