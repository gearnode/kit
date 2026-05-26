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

type (
	// PeriodicHandler defines a single operation to execute on each
	// tick of a PeriodicWorker.
	PeriodicHandler interface {
		Run(ctx context.Context) error
	}

	// PeriodicWorker runs a PeriodicHandler on a fixed interval,
	// skipping ticks when the previous run is still in progress.
	PeriodicWorker struct {
		name     string
		handler  PeriodicHandler
		logger   *log.Logger
		tracer   trace.Tracer
		interval time.Duration

		cyclesTotal  *prometheus.CounterVec
		runsTotal    *prometheus.CounterVec
		runDuration  *prometheus.HistogramVec
		skippedTotal *prometheus.CounterVec
	}
)

// NewPeriodic creates a PeriodicWorker named name that runs handler
// on every tick. The name identifies this worker in metrics, logs,
// and traces.
func NewPeriodic(name string, handler PeriodicHandler, logger *log.Logger, opts ...Option) *PeriodicWorker {
	o := options{
		interval:       10 * time.Second,
		tracerProvider: otel.GetTracerProvider(),
		registerer:     prometheus.DefaultRegisterer,
	}

	for _, opt := range opts {
		opt(&o)
	}

	workerLabel := []string{"worker"}
	statusLabels := []string{"worker", "status"}

	cyclesTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "periodic_worker",
			Name:      "cycles_total",
			Help:      "Total number of periodic worker cycles.",
		},
		workerLabel,
	)
	if err := o.registerer.Register(cyclesTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			cyclesTotal = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	runsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "periodic_worker",
			Name:      "runs_total",
			Help:      "Total number of periodic handler runs.",
		},
		statusLabels,
	)
	if err := o.registerer.Register(runsTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			runsTotal = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	runDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: "periodic_worker",
			Name:      "run_duration_seconds",
			Help:      "Duration of periodic handler runs in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		statusLabels,
	)
	if err := o.registerer.Register(runDuration); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			runDuration = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			panic(err)
		}
	}

	skippedTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: "periodic_worker",
			Name:      "skipped_total",
			Help:      "Total number of ticks skipped because the previous run was still in progress.",
		},
		workerLabel,
	)
	if err := o.registerer.Register(skippedTotal); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			skippedTotal = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			panic(err)
		}
	}

	return &PeriodicWorker{
		name:    name,
		handler: handler,
		logger:  logger.Named(name),
		tracer: o.tracerProvider.Tracer(
			tracerName,
			trace.WithInstrumentationVersion(
				version.New(0).Alpha(1),
			),
		),
		interval:     o.interval,
		cyclesTotal:  cyclesTotal,
		runsTotal:    runsTotal,
		runDuration:  runDuration,
		skippedTotal: skippedTotal,
	}
}

// Run starts the periodic worker loop. It blocks until ctx is
// cancelled, then waits for the in-flight run to complete before
// returning.
func (w *PeriodicWorker) Run(ctx context.Context) error {
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		ticker = time.NewTicker(w.interval)
	)

	defer ticker.Stop()
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			w.cyclesTotal.WithLabelValues(w.name).Inc()

			if !mu.TryLock() {
				w.skippedTotal.WithLabelValues(w.name).Inc()
				w.logger.WarnCtx(ctx, "skipping tick: previous run still in progress")
				continue
			}

			wg.Go(func() {
				defer mu.Unlock()
				w.run(ctx)
			})
		}
	}
}

func (w *PeriodicWorker) run(ctx context.Context) {
	nonCancelableCtx := context.WithoutCancel(ctx)

	runCtx, span := w.tracer.Start(
		nonCancelableCtx,
		"periodic_worker.run",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("worker.name", w.name),
		),
	)
	defer span.End()

	start := time.Now()
	err := w.handler.Run(runCtx)
	duration := time.Since(start)

	status := "succeeded"
	if err != nil {
		status = "failed"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		w.logger.ErrorCtx(
			runCtx,
			"run failed",
			log.Error(err),
			log.Duration("duration", duration),
		)
	} else {
		w.logger.InfoCtx(
			runCtx,
			"run succeeded",
			log.Duration("duration", duration),
		)
	}

	w.runsTotal.WithLabelValues(w.name, status).Inc()
	w.runDuration.WithLabelValues(w.name, status).Observe(duration.Seconds())
}
