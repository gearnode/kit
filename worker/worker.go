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

package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.gearno.de/kit/internal/version"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ErrNoTask is returned by Handler.Claim when there are no tasks
// available for processing. The worker uses this sentinel to
// distinguish "no work" from real errors.
var ErrNoTask = errors.New("worker: no task available")

const (
	tracerName = "go.gearno.de/kit/worker"
)

type (
	// Handler defines the operations a worker needs to claim,
	// process, and manage tasks of type T.
	Handler[T any] interface {
		// Claim acquires the next available task.
		// Implementations must return ErrNoTask when no work is
		// available.
		Claim(ctx context.Context) (T, error)

		// Process performs the actual work on a claimed task.
		// Implementations are responsible for handling their own
		// failures (e.g. updating status, retrying, logging).
		Process(ctx context.Context, task T) error
	}

	// StaleRecoverer is an optional interface that a Handler can
	// implement to recover tasks stuck in a processing state. When
	// the handler implements this interface, RecoverStale is called
	// at the beginning of each polling cycle.
	StaleRecoverer interface {
		RecoverStale(ctx context.Context) error
	}

	// Option configures a Worker.
	Option func(*options)

	options struct {
		interval       time.Duration
		maxConcurrency int
		tracerProvider trace.TracerProvider
		registerer     prometheus.Registerer
	}

	// Worker polls for tasks using a Handler and processes them
	// concurrently up to a configurable limit.
	Worker[T any] struct {
		name           string
		handler        Handler[T]
		logger         *log.Logger
		tracer         trace.Tracer
		interval       time.Duration
		maxConcurrency int

		pollCyclesTotal      *prometheus.CounterVec
		claimErrorsTotal     *prometheus.CounterVec
		claimDuration        *prometheus.HistogramVec
		recoverStaleDuration *prometheus.HistogramVec
		tasksTotal           *prometheus.CounterVec
		taskDuration         *prometheus.HistogramVec
	}
)

// WithInterval sets the polling interval between work cycles.
// Default is 10 seconds.
func WithInterval(d time.Duration) Option {
	return func(o *options) { o.interval = d }
}

// WithMaxConcurrency sets the maximum number of tasks processed
// concurrently. Values less than 1 are ignored. Default is 5.
func WithMaxConcurrency(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.maxConcurrency = n
		}
	}
}

// WithTracerProvider configures OpenTelemetry tracing with the
// provided tracer provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) {
		o.tracerProvider = tp
	}
}

// WithRegisterer sets a custom Prometheus registerer for metrics.
func WithRegisterer(r prometheus.Registerer) Option {
	return func(o *options) {
		o.registerer = r
	}
}

// New creates a Worker named name that uses handler to claim and
// process tasks. The name identifies this worker in metrics, logs,
// and traces.
func New[T any](name string, handler Handler[T], logger *log.Logger, opts ...Option) *Worker[T] {
	o := options{
		interval:       10 * time.Second,
		maxConcurrency: 5,
		tracerProvider: otel.GetTracerProvider(),
		registerer:     prometheus.DefaultRegisterer,
	}

	for _, opt := range opts {
		opt(&o)
	}

	workerLabel := []string{"worker"}
	metricLabels := []string{"worker", "status"}

	pollCyclesTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "poll_cycles_total",
			Help:      "Total number of polling cycles executed.",
		},
		workerLabel,
	)
	if err := o.registerer.Register(pollCyclesTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			pollCyclesTotal = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	claimErrorsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "claim_errors_total",
			Help:      "Total number of task claim errors.",
		},
		workerLabel,
	)
	if err := o.registerer.Register(claimErrorsTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			claimErrorsTotal = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	claimDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "worker",
			Name:      "claim_duration_seconds",
			Help:      "Duration of task claim operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		workerLabel,
	)
	if err := o.registerer.Register(claimDuration); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			claimDuration = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			panic(err)
		}
	}

	recoverStaleDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "worker",
			Name:      "recover_stale_duration_seconds",
			Help:      "Duration of stale task recovery operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		workerLabel,
	)
	if err := o.registerer.Register(recoverStaleDuration); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			recoverStaleDuration = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			panic(err)
		}
	}

	tasksTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "worker",
			Name:      "tasks_total",
			Help:      "Total number of tasks processed.",
		},
		metricLabels,
	)
	if err := o.registerer.Register(tasksTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			tasksTotal = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	taskDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "worker",
			Name:      "task_duration_seconds",
			Help:      "Duration of task processing in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		metricLabels,
	)
	if err := o.registerer.Register(taskDuration); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			taskDuration = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			panic(err)
		}
	}

	return &Worker[T]{
		name:    name,
		handler: handler,
		logger:  logger.Named(name),
		tracer: o.tracerProvider.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(
				version.New(0).Alpha(1),
			),
		),
		interval:             o.interval,
		maxConcurrency:       o.maxConcurrency,
		pollCyclesTotal:      pollCyclesTotal,
		claimErrorsTotal:     claimErrorsTotal,
		claimDuration:        claimDuration,
		recoverStaleDuration: recoverStaleDuration,
		tasksTotal:           tasksTotal,
		taskDuration:         taskDuration,
	}
}

// Run starts the worker loop. It blocks until ctx is cancelled, then
// waits for all in-flight tasks to complete before returning.
func (w *Worker[T]) Run(ctx context.Context) error {
	var (
		wg     sync.WaitGroup
		sem    = make(chan struct{}, w.maxConcurrency)
		ticker = time.NewTicker(w.interval)
	)

	defer ticker.Stop()
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			w.pollCyclesTotal.WithLabelValues(w.name).Inc()

			nonCancelableCtx := context.WithoutCancel(ctx)
			if sr, ok := w.handler.(StaleRecoverer); ok {
				recoverStart := time.Now()
				if err := sr.RecoverStale(nonCancelableCtx); err != nil {
					w.logger.ErrorCtx(nonCancelableCtx, "cannot recover stale tasks", log.Error(err))
				}
				w.recoverStaleDuration.WithLabelValues(w.name).Observe(time.Since(recoverStart).Seconds())
			}

			for {
				if err := w.processNext(ctx, sem, &wg); err != nil {
					if !errors.Is(err, ErrNoTask) {
						w.claimErrorsTotal.WithLabelValues(w.name).Inc()
						w.logger.ErrorCtx(nonCancelableCtx, "cannot claim task", log.Error(err))
					}
					break
				}
			}
		}
	}
}

func (w *Worker[T]) processNext(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup) error {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	nonCancelableCtx := context.WithoutCancel(ctx)

	claimStart := time.Now()
	task, err := w.handler.Claim(nonCancelableCtx)
	w.claimDuration.WithLabelValues(w.name).Observe(time.Since(claimStart).Seconds())
	if err != nil {
		<-sem
		return err
	}

	wg.Go(
		func() {
			defer func() { <-sem }()

			processCtx, span := w.tracer.Start(
				nonCancelableCtx,
				"worker.process",
				trace.WithSpanKind(trace.SpanKindInternal),
				trace.WithAttributes(
					attribute.String("worker.name", w.name),
				),
			)
			defer span.End()

			start := time.Now()
			err := w.handler.Process(processCtx, task)
			duration := time.Since(start)

			status := "succeeded"
			if err != nil {
				status = "failed"
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				w.logger.ErrorCtx(
					processCtx,
					"task processing failed",
					log.Error(err),
					log.Duration("duration", duration),
				)
			} else {
				w.logger.InfoCtx(
					processCtx,
					"task processing succeeded",
					log.Duration("duration", duration),
				)
			}

			w.tasksTotal.WithLabelValues(w.name, status).Inc()
			w.taskDuration.WithLabelValues(w.name, status).Observe(duration.Seconds())
		},
	)

	return nil
}
