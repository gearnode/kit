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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/crypto/uuid"
	"go.gearno.de/kit/internal/version"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.22.0"
	"go.opentelemetry.io/otel/trace"
)

type (
	handlerWrapper struct {
		next            http.Handler
		requestsTotal   *prometheus.CounterVec
		requestDuration *prometheus.HistogramVec
		requestSize     *prometheus.HistogramVec
		responseSize    *prometheus.HistogramVec
		tracer          trace.Tracer
		logger          *log.Logger
	}
)

const (
	tracerName = "go.gearno.de/kit/httpserver"
)

var (
	internalErrorResponse = map[string]string{
		"error": "internal error",
	}
)

func newHandlerWrapper(
	next http.Handler,
	logger *log.Logger,
	tp trace.TracerProvider,
	registerer prometheus.Registerer,
) *handlerWrapper {
	metricLabels := []string{
		"method",
		"host",
		"flavor",
		"status_code",
		"path",
	}

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "http_server",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests made.",
		},
		metricLabels,
	)
	registerer.MustRegister(requestsTotal)

	requestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "http_server",
			Name:      "request_duration_seconds",
			Help:      "Duration of HTTP requests in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		metricLabels,
	)
	registerer.MustRegister(requestDuration)

	requestSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "http_server",
			Name:      "request_size_bytes",
			Help:      "Size of the HTTP request in bytes",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 5),
		},
		metricLabels,
	)
	registerer.MustRegister(requestSize)

	responseSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "http_server",
			Name:      "response_size_bytes",
			Help:      "Size of HTTP responses in bytes",
			Buckets:   prometheus.ExponentialBuckets(100, 10, 5),
		},
		metricLabels,
	)
	registerer.MustRegister(responseSize)

	return &handlerWrapper{
		next:   next,
		logger: logger,
		tracer: tp.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(
				version.New(0).Alpha(1),
			),
		),
		requestsTotal:   requestsTotal,
		requestDuration: requestDuration,
		requestSize:     requestSize,
		responseSize:    responseSize,
	}
}

// TODO X-Forwaded-* support
func (hw *handlerWrapper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Bypass for OPTIONS request to avoid telemetry, metrics and
	// logging noise.
	if r.Method == http.MethodOptions {
		hw.next.ServeHTTP(w, r)
		return
	}

	if r.URL.Path == "/health" {
		w.Header().Set("content-type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
		return
	}

	var (
		r2        = r.Clone(r.Context())
		ctx       = r2.Context()
		start     = time.Now()
		requestID = r2.Header.Get("x-request-id")
		ww        = NewWrapResponseWriter(w, r2.ProtoMajor)
		logger    = hw.logger.With(
			log.String("http_request_method", r2.Method),
			log.String("http_request_host", r2.Host),
			log.String("http_request_path", r2.URL.Path),
			log.String("http_request_flavor", r2.Proto),
			log.String("http_request_user_agent", r2.UserAgent()),
			log.String("http_request_client_ip", r2.RemoteAddr),
		)
	)

	if requestID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			logger.ErrorCtx(ctx, "cannot generate request id", log.Error(err))
		}

		requestID = id.String()
	}
	r2.Header.Set("x-request-id", requestID)
	ww.Header().Set("x-request-id", requestID)
	logger = logger.With(log.String("http_request_id", requestID))

	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		propagator := otel.GetTextMapPropagator()
		ctx = propagator.Extract(ctx, propagation.HeaderCarrier(r2.Header))

		spanName := fmt.Sprintf("%s %s %s", r2.Method, r2.URL.Host, r2.URL.Path)
		ctx, span = hw.tracer.Start(
			ctx,
			spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.NetworkPeerAddress(r2.URL.Host),
				semconv.NetworkPeerPort(atoi(r2.URL.Port())),
				semconv.URLScheme(r2.URL.Scheme),
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r2.URL.String()),
				attribute.String("http.target", r2.URL.Path),
				attribute.String("http.host", r2.Host),
				attribute.String("http.flavor", r2.Proto),
				attribute.String("http.client_ip", r2.RemoteAddr),
				attribute.String("http.user_agent", r2.UserAgent()),
				attribute.String("http.request_id", requestID),
			),
		)
		defer span.End()
	}

	// Hack to get route pattern from Chi. As today using the STD
	// router will require to much works to have proper sub router
	// support, a task for later.
	ctx = context.WithValue(ctx, chi.RouteCtxKey, chi.NewRouteContext())

	defer func() {
		duration := time.Since(start)
		hasPanic := false
		rvr := recover()
		if rvr != nil {
			hasPanic = true

			if err, ok := rvr.(error); ok {
				if rootSpan.IsRecording() {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}

			} else {
				if rootSpan.IsRecording() {
					span.SetStatus(codes.Error, fmt.Sprintf("%v", rvr))
				}
			}

			stack := make([]byte, 1024)
			length := runtime.Stack(stack, false)

			logger = logger.With(
				log.Any("error", rvr),
				log.String("stacktrace", string(stack[:length])),
			)

			ww.WriteHeader(http.StatusInternalServerError)
			if err := json.NewEncoder(ww).Encode(internalErrorResponse); err != nil {
				logger.ErrorCtx(ctx, "cannot write internal error", log.Error(err))
			}
		}

		metricLabels := prometheus.Labels{
			"method":      r2.Method,
			"host":        r2.Host,
			"flavor":      r2.Proto,
			"status_code": strconv.Itoa(ww.Status()),
			"path":        chi.RouteContext(ctx).RoutePattern(),
		}

		hw.requestsTotal.With(metricLabels).Inc()
		hw.requestDuration.With(metricLabels).Observe(duration.Seconds())
		hw.requestSize.With(metricLabels).Observe(estimateRequestSize(r))
		hw.responseSize.With(metricLabels).Observe(float64(ww.BytesWritten()))

		var resSizeString string
		if ww.BytesWritten() < 1000 {
			resSizeString = fmt.Sprintf("%dB", ww.BytesWritten())
		} else if ww.BytesWritten() < 1_000_000 {
			resSizeString = fmt.Sprintf("%.1fkB", float64(ww.BytesWritten())/1e3)
		} else if ww.BytesWritten() < 1_000_000_000 {
			resSizeString = fmt.Sprintf("%.1fMB", float64(ww.BytesWritten())/1e6)
		} else {
			resSizeString = fmt.Sprintf("%.1fGB", float64(ww.BytesWritten())/1e9)
		}

		msg := fmt.Sprintf(
			"%s %s %d %s %s",
			r2.Method,
			r2.URL.Path,
			ww.Status(),
			resSizeString,
			duration,
		)

		logger.With(
			log.Int("http_reponse_size", ww.BytesWritten()),
			log.Int("http_response_status", ww.Status()),
		)

		if ww.Status() > 499 && !hasPanic {
			span.SetStatus(codes.Error, fmt.Sprintf("%d status code", ww.Status()))
		}

		if ww.Status() > 499 || hasPanic {
			logger.ErrorCtx(ctx, msg)
		} else {
			logger.InfoCtx(ctx, msg)
		}
	}()

	hw.next.ServeHTTP(ww, r2.WithContext(ctx))
}

func atoi(s string) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return v
}

func estimateRequestSize(r *http.Request) float64 {
	s := 0
	if r.URL != nil {
		s = len(r.URL.Path)
	}

	s += len(r.Method)
	s += len(r.Proto)
	for name, values := range r.Header {
		s += len(name)
		for _, value := range values {
			s += len(value)
		}
	}
	s += len(r.Host)

	// NOTE: r.Form and r.MultipartForm are assumed to be included
	// in r.URL.

	if r.ContentLength != -1 {
		s += int(r.ContentLength)
	}

	return float64(s)
}
