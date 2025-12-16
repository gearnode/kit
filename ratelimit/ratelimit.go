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

package ratelimit

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/kit/internal/version"
	"go.gearno.de/kit/log"
	"go.gearno.de/kit/pg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Option is a function that configures the Limiter during
	// initialization.
	Option func(l *Limiter)

	// Limiter provides a PostgreSQL-backed rate limiter using the
	// sliding window counter algorithm with an UNLOGGED table for
	// performance.
	Limiter struct {
		pg     *pg.Client
		logger *log.Logger
		tracer trace.Tracer

		cleanupInterval time.Duration
		cleanupOnce     sync.Once

		blockedCache sync.Map // key+window -> unblockAt (time.Time)

		requestsTotal  *prometheus.CounterVec
		checkDuration  *prometheus.HistogramVec
		cacheHitsTotal prometheus.Counter
	}

	// Rate defines the rate limit parameters.
	Rate struct {
		// Limit is the maximum number of requests allowed within the
		// Window duration.
		Limit int

		// Window is the time duration for the rate limit window.
		Window time.Duration
	}

	// Result contains the outcome of a rate limit check.
	Result struct {
		// Allowed indicates whether the request is permitted.
		Allowed bool

		// Limit is the maximum number of requests allowed in the window.
		Limit int

		// Remaining is the number of requests remaining in the current window.
		Remaining int

		// ResetAt is the time when the current window resets.
		ResetAt time.Time
	}
)

const (
	tracerName    = "go.gearno.de/kit/ratelimit"
	stmtNameAllow = "ratelimit_allow"
)

// WithLogger sets a custom logger for the limiter.
func WithLogger(l *log.Logger) Option {
	return func(lim *Limiter) {
		lim.logger = l.Named("ratelimit")
	}
}

// WithTracerProvider configures OpenTelemetry tracing with the
// provided tracer provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(l *Limiter) {
		l.tracer = tp.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(
				version.New(0).Alpha(1),
			),
		)
	}
}

// WithRegisterer sets a custom Prometheus registerer for metrics.
func WithRegisterer(r prometheus.Registerer) Option {
	return func(l *Limiter) {
		l.registerMetrics(r)
	}
}

// WithCleanupInterval sets the interval for background cleanup of
// expired rate limit entries. Default is 5 minutes.
func WithCleanupInterval(d time.Duration) Option {
	return func(l *Limiter) {
		l.cleanupInterval = d
	}
}

// NewLimiter creates a new rate limiter backed by PostgreSQL.
// It automatically creates the required UNLOGGED table if it doesn't exist.
func NewLimiter(pgClient *pg.Client, options ...Option) (*Limiter, error) {
	l := &Limiter{
		pg:              pgClient,
		logger:          log.NewLogger(log.WithOutput(io.Discard)),
		tracer:          otel.GetTracerProvider().Tracer(tracerName),
		cleanupInterval: 5 * time.Minute,
	}

	// Apply default metrics registration
	l.registerMetrics(prometheus.DefaultRegisterer)

	for _, o := range options {
		o(l)
	}

	// Ensure the rate_limits table exists
	ctx := context.Background()
	if err := l.pg.WithConn(ctx, func(conn pg.Conn) error {
		return ensureTable(ctx, conn)
	}); err != nil {
		return nil, fmt.Errorf("cannot ensure rate_limits table: %w", err)
	}

	return l, nil
}

func (l *Limiter) registerMetrics(r prometheus.Registerer) {
	l.requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "ratelimit",
			Name:      "requests_total",
			Help:      "Total number of rate limit checks.",
		},
		[]string{"allowed"},
	)
	if err := r.Register(l.requestsTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			l.requestsTotal = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}

	l.checkDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "ratelimit",
			Name:      "check_duration_seconds",
			Help:      "Duration of rate limit checks in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"allowed"},
	)
	if err := r.Register(l.checkDuration); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			l.checkDuration = are.ExistingCollector.(*prometheus.HistogramVec)
		}
	}

	l.cacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: "ratelimit",
			Name:      "cache_hits_total",
			Help:      "Total number of blocked cache hits (DB calls avoided).",
		},
	)
	if err := r.Register(l.cacheHitsTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			l.cacheHitsTotal = are.ExistingCollector.(prometheus.Counter)
		}
	}
}

// Allow checks if a single request is allowed for the given key and rate.
// It increments the counter and returns the result.
func (l *Limiter) Allow(ctx context.Context, key string, rate Rate) (*Result, error) {
	return l.AllowN(ctx, key, rate, 1)
}

// AllowN checks if n requests are allowed for the given key and rate.
// It increments the counter by n and returns the result.
func (l *Limiter) AllowN(ctx context.Context, key string, rate Rate, n int) (*Result, error) {
	start := time.Now()

	var (
		rootSpan = trace.SpanFromContext(ctx)
		span     trace.Span
	)

	if rootSpan.IsRecording() {
		ctx, span = l.tracer.Start(
			ctx,
			"ratelimit.AllowN",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(
				attribute.String("ratelimit.key", key),
				attribute.Int("ratelimit.limit", rate.Limit),
				attribute.Int64("ratelimit.window_ms", rate.Window.Milliseconds()),
				attribute.Int("ratelimit.n", n),
			),
		)
		defer span.End()
	}

	now := time.Now()
	windowStart := now.Truncate(rate.Window)
	prevWindowStart := windowStart.Add(-rate.Window)
	resetAt := windowStart.Add(rate.Window)

	// Fast path: check local blocked cache
	cacheKey := fmt.Sprintf("%s:%d", key, rate.Window.Milliseconds())
	if unblockAt, ok := l.blockedCache.Load(cacheKey); ok {
		if now.Before(unblockAt.(time.Time)) {
			l.cacheHitsTotal.Inc()

			result := &Result{
				Allowed:   false,
				Limit:     rate.Limit,
				Remaining: 0,
				ResetAt:   unblockAt.(time.Time),
			}

			if rootSpan.IsRecording() {
				span.SetAttributes(
					attribute.Bool("ratelimit.allowed", false),
					attribute.Bool("ratelimit.cache_hit", true),
				)
			}

			l.recordMetrics(false, time.Since(start))
			return result, nil
		}
		l.blockedCache.Delete(cacheKey)
	}

	// Slow path: check database
	var currentCount, prevCount int

	err := l.pg.WithConn(ctx, func(conn pg.Conn) error {
		q := `
INSERT INTO rate_limits (key, window_start, count)
VALUES ($1, $2, $3)
ON CONFLICT (key, window_start) 
DO UPDATE SET count = rate_limits.count + $3
RETURNING 
    count,
    (SELECT COALESCE(count, 0) FROM rate_limits 
     WHERE key = $1 AND window_start = $4) as prev_count
`
		row := conn.QueryRow(ctx, q, key, windowStart.UnixMilli(), n, prevWindowStart.UnixMilli())
		return row.Scan(&currentCount, &prevCount)
	})

	if err != nil {
		if rootSpan.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return nil, fmt.Errorf("cannot check rate limit: %w", err)
	}

	// Calculate effective count using sliding window
	elapsed := now.Sub(windowStart)
	weight := float64(rate.Window-elapsed) / float64(rate.Window)
	effectiveCount := currentCount + int(float64(prevCount)*weight)

	allowed := effectiveCount <= rate.Limit
	remaining := rate.Limit - effectiveCount
	if remaining < 0 {
		remaining = 0
	}

	// Cache blocked keys
	if !allowed {
		l.blockedCache.Store(cacheKey, resetAt)
	}

	if rootSpan.IsRecording() {
		span.SetAttributes(
			attribute.Bool("ratelimit.allowed", allowed),
			attribute.Bool("ratelimit.cache_hit", false),
			attribute.Int("ratelimit.current_count", currentCount),
			attribute.Int("ratelimit.prev_count", prevCount),
			attribute.Int("ratelimit.effective_count", effectiveCount),
			attribute.Int("ratelimit.remaining", remaining),
		)
	}

	result := &Result{
		Allowed:   allowed,
		Limit:     rate.Limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}

	l.recordMetrics(allowed, time.Since(start))

	return result, nil
}

func (l *Limiter) recordMetrics(allowed bool, duration time.Duration) {
	allowedStr := "true"
	if !allowed {
		allowedStr = "false"
	}

	l.requestsTotal.WithLabelValues(allowedStr).Inc()
	l.checkDuration.WithLabelValues(allowedStr).Observe(duration.Seconds())
}

