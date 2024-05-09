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
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"go.gearno.de/crypto/uuid"
	"go.gearno.de/x/panicf"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

type (
	// TelemetryRoundTripper is an http.RoundTripper that wraps
	// another http.RoundTripper to add telemetry capabilities. It
	// logs requests, measures request latency, and counts
	// requests using specified telemetry tools.
	TelemetryRoundTripper struct {
		logger   *slog.Logger
		meter    metric.Meter
		requests metric.Int64Counter
		latency  metric.Float64Histogram
		next     http.RoundTripper
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
func NewTelemetryRoundTripper(next http.RoundTripper, logger *slog.Logger, meter metric.Meter) *TelemetryRoundTripper {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	if meter == nil {
		meter = noop.Meter{}
	}

	requests, err := meter.Int64Counter(
		"http_requests_total",
		metric.WithDescription("Total number of HTTP requests by status code"),
	)
	if err != nil {
		panicf.Panic("cannot define http_requests_total metric counter: %w", err)
	}

	latency, err := meter.Float64Histogram(
		"http_request_duration_seconds",
		metric.WithDescription("Duration of HTTP requests"),
	)
	if err != nil {
		panicf.Panic("cannot define http_request_duration_seconds metrics histogram: %w", err)
	}

	return &TelemetryRoundTripper{
		next:     next,
		logger:   logger,
		meter:    meter,
		requests: requests,
		latency:  latency,
	}
}

// RoundTrip executes a single HTTP transaction and records telemetry
// data including metrics and traces. It logs the request details,
// measures the request latency, and counts the request based on the
// response status. It sanitizes URLs to exclude query parameters and
// fragments for logging and tracing.
func (rt *TelemetryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	ctx := req.Context()
	newReq := req.Clone(ctx)

	reqURL := sanitizeURL(newReq.URL)
	span := trace.SpanFromContext(ctx)
	spanCtx := span.SpanContext()

	requestID := newReq.Header.Get("x-request-id")
	if requestID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			panicf.Panic("cannot generate UUID: %w", err)
		}

		requestID = id.String()
	}

	logger := rt.logger.With(
		slog.String("http_request_method", newReq.Method),
		slog.String("http_request_host", reqURL.Host),
		slog.String("http_request_path", reqURL.Path),
		slog.String("http_request_flavor", newReq.Proto),
		slog.String("http_request_scheme", reqURL.Scheme),
		slog.String("http_request_user_agent", newReq.UserAgent()),
		slog.String("trace_id", spanCtx.TraceID().String()),
		slog.String("span_id", spanCtx.SpanID().String()),
		slog.String("http_request_id", requestID),
	)

	span.SetAttributes(
		attribute.String("http.method", newReq.Method),
		attribute.String("http.url", reqURL.String()),
		attribute.String("http.target", reqURL.Path),
		attribute.String("http.host", newReq.Host),
		attribute.String("http.scheme", reqURL.Scheme),
		attribute.String("http.flavor", newReq.Proto),
		attribute.String("http.client_ip", newReq.RemoteAddr),
		attribute.String("http.user_agent", newReq.UserAgent()),
		attribute.String("http.request_id", requestID),
	)

	newReq.Header.Set(
		"traceparent",
		fmt.Sprintf(
			"%s-%s-%s-%s",
			"00",
			spanCtx.TraceID().String(),
			spanCtx.SpanID().String(),
			spanCtx.TraceFlags().String(),
		),
	)

	newReq.Header.Set(
		"tracestate",
		spanCtx.TraceState().String(),
	)

	resp, err := rt.next.RoundTrip(newReq)
	if err != nil {
		logger.ErrorContext(ctx, "cannot execute http transaction", slog.Any("error", err))

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		return resp, err
	}

	span.SetAttributes(
		attribute.Int("http.status_code", resp.StatusCode),
		attribute.String("http.status_text", resp.Status),
	)

	duration := time.Since(start)
	metricAttributes := metric.WithAttributes(
		attribute.String("http_request_method", newReq.Method),
		attribute.String("http_request_host", reqURL.Host),
		attribute.String("http_request_path", reqURL.Path),
		attribute.String("http_request_flavor", newReq.Proto),
		attribute.String("http_request_scheme", reqURL.Scheme),
		attribute.Int("http_response_status_code", resp.StatusCode),
	)

	rt.requests.Add(ctx, 1, metricAttributes)
	rt.latency.Record(ctx, duration.Seconds(), metricAttributes)

	logLevel := slog.LevelInfo
	logMessage := fmt.Sprintf("%s %s %d %s", newReq.Method, reqURL.String(), resp.StatusCode, duration)
	if resp.StatusCode >= http.StatusInternalServerError {
		logLevel = slog.LevelError
	}

	logger.Log(ctx, logLevel, logMessage, slog.Int("http_response_status_code", resp.StatusCode))

	return resp, nil
}

func sanitizeURL(u *url.URL) *url.URL {
	u2 := *u
	u2.RawQuery = ""
	u2.RawFragment = ""
	u2.User = nil

	return &u2
}
