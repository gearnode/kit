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

package httpclient

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Option defines a function signature for configuring options
	// within the httpclient package.
	Option func(o *Options)

	// Options holds configurable options for http transports used
	// within the package. This includes logging, metrics, and TLS
	// configurations.
	Options struct {
		tlsConfig *tls.Config

		tracerProvider trace.TracerProvider
		logger         *log.Logger
		registerer     prometheus.Registerer
	}
)

const (
	tracerName = "go.gearno.de/kit/httpclient"
)

// WithTLSConfig is an option setter for setting TLS configurations on
// HTTP transports.
func WithTLSConfig(c *tls.Config) Option {
	return func(o *Options) {
		o.tlsConfig = c
	}
}

// WithLogger is an option setter for specifying a logger for HTTP
// telemetry and error logging.
func WithLogger(l *log.Logger) Option {
	return func(o *Options) {
		o.logger = l.Named("http.client")
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

// DefaultTransport returns a new http.Transport with similar default
// values to http.DefaultTransport, but with idle connections and
// keepalives disabled.
func DefaultTransport(options ...Option) http.RoundTripper {
	opts := configureOptions(options)

	transport := createBaseTransport()
	transport.DisableKeepAlives = true
	transport.MaxIdleConnsPerHost = -1
	transport.TLSClientConfig = opts.tlsConfig

	return NewTelemetryRoundTripper(transport, opts.logger, opts.tracerProvider, opts.registerer)
}

// DefaultPooledTransport returns a new http.Transport with similar
// default values to http.DefaultTransport. Do not use this for
// transient transports as it can leak file descriptors over
// time. Only use this for transports that will be re-used for the
// same host(s).
func DefaultPooledTransport(options ...Option) http.RoundTripper {
	opts := configureOptions(options)

	transport := createBaseTransport()
	transport.MaxIdleConnsPerHost = runtime.GOMAXPROCS(0) + 1
	transport.TLSClientConfig = opts.tlsConfig

	return NewTelemetryRoundTripper(transport, opts.logger, opts.tracerProvider, opts.registerer)
}

// DefaultClient returns a new http.Client with similar default values
// to http.Client, but with a non-shared Transport, idle connections
// disabled, and keepalives disabled.
func DefaultClient(options ...Option) *http.Client {
	return &http.Client{
		Transport: DefaultTransport(options...),
	}
}

// DefaultPooledClient returns a new http.Client with similar default
// values to http.Client, but with a shared Transport. Do not use this
// function for transient clients as it can leak file descriptors over
// time. Only use this for clients that will be re-used for the same
// host(s).
func DefaultPooledClient(options ...Option) *http.Client {
	return &http.Client{
		Transport: DefaultPooledTransport(options...),
	}
}

func createBaseTransport() *http.Transport {
	dial := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dial.DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
}

func configureOptions(options []Option) *Options {
	opts := &Options{
		logger:         log.NewLogger(log.WithOutput(io.Discard)),
		tracerProvider: otel.GetTracerProvider(),
		registerer:     prometheus.DefaultRegisterer,
	}

	for _, o := range options {
		o(opts)
	}

	return opts
}
