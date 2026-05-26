package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/worker"
)

type periodicHandler struct {
	runFn func(ctx context.Context) error
}

func (h *periodicHandler) Run(ctx context.Context) error {
	if h.runFn != nil {
		return h.runFn(ctx)
	}
	return nil
}

func TestPeriodicWorkerRunsHandler(t *testing.T) {
	var calls atomic.Int64

	h := &periodicHandler{
		runFn: func(ctx context.Context) error {
			calls.Add(1)
			return nil
		},
	}

	w := worker.NewPeriodic("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return calls.Load() >= 3
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestPeriodicWorkerStopsOnContextCancel(t *testing.T) {
	h := &periodicHandler{}

	w := worker.NewPeriodic("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to stop")
	}
}

func TestPeriodicWorkerWaitsForInFlightRun(t *testing.T) {
	var completed atomic.Int64
	started := make(chan struct{})

	h := &periodicHandler{
		runFn: func(ctx context.Context) error {
			close(started)
			time.Sleep(100 * time.Millisecond)
			completed.Add(1)
			return nil
		},
	}

	w := worker.NewPeriodic("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run to start")
	}

	cancel()

	select {
	case <-done:
		assert.Equal(t, int64(1), completed.Load())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker to drain")
	}
}

func TestPeriodicWorkerSkipsOverlappingRuns(t *testing.T) {
	var (
		concurrent atomic.Int64
		peak       atomic.Int64
		runs       atomic.Int64
	)

	h := &periodicHandler{
		runFn: func(ctx context.Context) error {
			cur := concurrent.Add(1)
			defer concurrent.Add(-1)

			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}

			runs.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	}

	w := worker.NewPeriodic("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return runs.Load() >= 3
	}, 5*time.Second, 5*time.Millisecond)

	cancel()
	<-done

	assert.Equal(t, int64(1), peak.Load(),
		"peak concurrency must be 1: overlapping runs should be skipped")
}

func TestPeriodicWorkerContinuesAfterError(t *testing.T) {
	var calls atomic.Int64

	h := &periodicHandler{
		runFn: func(ctx context.Context) error {
			n := calls.Add(1)
			if n == 1 {
				return errors.New("transient failure")
			}
			return nil
		},
	}

	w := worker.NewPeriodic("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return calls.Load() >= 3
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
