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

		ssrfProtection      bool
		ssrfAllowLoopback   bool
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

// WithSSRFProtection enables server-side request forgery protection
// on the returned transport. When enabled, the underlying dialer
// rejects connections to addresses that resolve to loopback,
// private (RFC 1918), CGNAT (RFC 6598), link-local, multicast,
// unspecified, IPv4-mapped IPv6 of the same, ULA (RFC 4193), and
// other reserved ranges. The check runs on the actual peer
// address at connect time, which defeats DNS rebinding between
// any prior URL validation and the dial.
//
// When the resulting transport is built into an http.Client via
// DefaultClient or DefaultPooledClient, the client is also
// configured to refuse redirects whose scheme, host, or port
// differs from the original request, blocking redirect-based
// pivots.
//
// Use this option whenever the destination URL is influenced by
// untrusted input (for example, customer-supplied webhook
// endpoints or OAuth2 token URLs).
func WithSSRFProtection() Option {
	return func(o *Options) {
		o.ssrfProtection = true
	}
}

// WithSSRFAllowLoopback weakens WithSSRFProtection to permit dials to
// loopback addresses (127.0.0.0/8 and ::1). It exists for tests that
// stand up an httptest server on the loopback interface; production
// callers must not use it.
func WithSSRFAllowLoopback() Option {
	return func(o *Options) {
		o.ssrfAllowLoopback = true
	}
}

// DefaultTransport returns a new http.Transport with similar default
// values to http.DefaultTransport, but with idle connections and
// keepalives disabled.
func DefaultTransport(options ...Option) http.RoundTripper {
	opts := configureOptions(options)

	transport := createBaseTransport(opts)
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

	transport := createBaseTransport(opts)
	transport.MaxIdleConnsPerHost = runtime.GOMAXPROCS(0) + 1
	transport.TLSClientConfig = opts.tlsConfig

	return NewTelemetryRoundTripper(transport, opts.logger, opts.tracerProvider, opts.registerer)
}

// DefaultClient returns a new http.Client with similar default values
// to http.Client, but with a non-shared Transport, idle connections
// disabled, and keepalives disabled.
func DefaultClient(options ...Option) *http.Client {
	opts := configureOptions(options)
	return buildClient(DefaultTransport(options...), opts)
}

// DefaultPooledClient returns a new http.Client with similar default
// values to http.Client, but with a shared Transport. Do not use this
// function for transient clients as it can leak file descriptors over
// time. Only use this for clients that will be re-used for the same
// host(s).
func DefaultPooledClient(options ...Option) *http.Client {
	opts := configureOptions(options)
	return buildClient(DefaultPooledTransport(options...), opts)
}

func buildClient(rt http.RoundTripper, opts *Options) *http.Client {
	c := &http.Client{Transport: rt}
	if opts.ssrfProtection {
		c.CheckRedirect = noCrossOriginRedirects
	}
	return c
}

func createBaseTransport(opts *Options) *http.Transport {
	dial := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}
	if opts.ssrfProtection {
		dial.Control = makeSSRFDialControl(opts.ssrfAllowLoopback)
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
