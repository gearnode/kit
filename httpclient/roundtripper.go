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
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/crypto/uuid"
	"go.gearno.de/kit/internal/version"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type (
	// TelemetryRoundTripper is an http.RoundTripper that wraps
	// another http.RoundTripper to add telemetry capabilities. It
	// logs requests, measures request latency, and counts
	// requests using specified telemetry tools.
	TelemetryRoundTripper struct {
		logger *log.Logger
		tracer trace.Tracer

		requestsTotal          *prometheus.CounterVec
		requestDurationSeconds *prometheus.HistogramVec

		next http.RoundTripper
	}
)

var (
	_ http.RoundTripper = (*TelemetryRoundTripper)(nil)
)

// NewTelemetryRoundTripper creates a new TelemetryRoundTripper with
// the provided next RoundTripper, logger, and metric meter. It
// initializes and registers telemetry instruments for counting
// requests and measuring request latency.  It uses fallbacks for the
// logger and meter if nil references are provided.
func NewTelemetryRoundTripper(
	next http.RoundTripper,
	logger *log.Logger,
	tp trace.TracerProvider,
	registerer prometheus.Registerer,
) *TelemetryRoundTripper {
	metricLabels := []string{
		"method",
		"host",
		"flavor",
		"scheme",
		"status_code",
	}

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests made.",
		},
		metricLabels,
	)
	registerer.MustRegister(requestsTotal)

	requestDurationSeconds := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		metricLabels,
	)
	registerer.MustRegister(requestDurationSeconds)

	return &TelemetryRoundTripper{
		next:   next,
		logger: logger,
		tracer: tp.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(
				version.New(0).Alpha(1),
			),
		),
		requestsTotal:          requestsTotal,
		requestDurationSeconds: requestDurationSeconds,
	}
}

// RoundTrip executes a single HTTP transaction and records telemetry
// data including metrics and traces. It logs the request details,
// measures the request latency, and counts the request based on the
// response status. It sanitizes URLs to exclude query parameters and
// fragments for logging and tracing.
func (rt *TelemetryRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	var (
		r2        = r.Clone(r.Context())
		ctx       = r2.Context()
		start     = time.Now()
		requestID = r2.Header.Get("x-request-id")
	)

	if requestID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("cannot generate request-id: %w", err)
		}

		requestID = id.String()
	}
	r2.Header.Set("x-request-id", requestID)

	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
		logger   = rt.logger.With(
			log.String("http_request_method", r2.Method),
			log.String("http_request_scheme", r2.URL.Scheme),
			log.String("http_request_host", r2.URL.Host),
			log.String("http_request_path", r2.URL.Path),
			log.String("http_request_flavor", r2.Proto),
			log.String("http_request_user_agent", r2.UserAgent()),
			log.String("http_request_client_ip", r2.RemoteAddr),
			log.String("http_request_id", requestID),
		)
	)

	if rootSpan.IsRecording() {
		spanName := fmt.Sprintf("%s %s %s", r2.Method, r2.URL.Host, r2.URL.Path)
		ctx, span = rt.tracer.Start(
			ctx,
			spanName,
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				semconv.NetworkPeerAddress(r2.URL.Host),
				semconv.NetworkPeerPort(atoi(r2.URL.Port())),
				semconv.URLScheme(r2.URL.Scheme),
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r2.URL.String()),
				attribute.String("http.target", r2.URL.Path),
				attribute.String("http.host", r2.URL.Host),
				attribute.String("http.scheme", r2.URL.Scheme),
				attribute.String("http.flavor", r2.Proto),
				attribute.String("http.client_ip", r2.RemoteAddr),
				attribute.String("http.user_agent", r2.UserAgent()),
				attribute.String("http.request_id", requestID),
			),
		)
		defer span.End()

		propagator := otel.GetTextMapPropagator()
		propagator.Inject(ctx, propagation.HeaderCarrier(r2.Header))
	}

	resp, err := rt.next.RoundTrip(r2)
	if err != nil {
		rt.logger.ErrorCtx(ctx, "cannot execute http transaction", log.Error(err))

		if span.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}

		return nil, err
	}

	if rootSpan.IsRecording() {
		span.SetAttributes(
			attribute.Int("http.status_code", resp.StatusCode),
			attribute.String("http.status_text", resp.Status),
		)
	}

	duration := time.Since(start)

	metricLabels := prometheus.Labels{
		"method":      r2.Method,
		"host":        r2.URL.Host,
		"flavor":      r2.Proto,
		"scheme":      r2.URL.Scheme,
		"status_code": strconv.Itoa(resp.StatusCode),
	}

	rt.requestsTotal.With(metricLabels).Inc()
	rt.requestDurationSeconds.With(metricLabels).Observe(duration.Seconds())

	logLevel := log.LevelInfo
	logMessage := fmt.Sprintf("%s %s %d %s", r2.Method, r.URL.String(), resp.StatusCode, duration)
	if resp.StatusCode >= http.StatusInternalServerError {
		logLevel = log.LevelError
	}

	logger.Log(ctx, logLevel, logMessage, log.Int("http_response_status_code", resp.StatusCode))

	return resp, nil
}

func atoi(s string) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return v
}
