package worker_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/log"
	"go.gearno.de/kit/worker"
)

func testLogger() *log.Logger {
	return log.NewLogger(log.WithOutput(io.Discard))
}

func testWorkerOpts(opts ...worker.Option) []worker.Option {
	base := []worker.Option{
		worker.WithInterval(10 * time.Millisecond),
		worker.WithRegisterer(prometheus.NewRegistry()),
	}
	return append(base, opts...)
}

type testHandler struct {
	mu        sync.Mutex
	tasks     []int
	claimFn   func(ctx context.Context) (int, error)
	processFn func(ctx context.Context, task int) error
}

func (h *testHandler) Claim(ctx context.Context) (int, error) {
	if h.claimFn != nil {
		return h.claimFn(ctx)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.tasks) == 0 {
		return 0, worker.ErrNoTask
	}

	t := h.tasks[0]
	h.tasks = h.tasks[1:]
	return t, nil
}

func (h *testHandler) Process(ctx context.Context, task int) error {
	if h.processFn != nil {
		return h.processFn(ctx, task)
	}
	return nil
}

type recoverableHandler struct {
	testHandler
	recoverCalls atomic.Int64
}

func (h *recoverableHandler) RecoverStale(ctx context.Context) error {
	h.recoverCalls.Add(1)
	return nil
}

func TestWorkerProcessesTasks(t *testing.T) {
	processed := make(chan int, 10)

	h := &testHandler{
		tasks: []int{1, 2, 3},
		processFn: func(ctx context.Context, task int) error {
			processed <- task
			return nil
		},
	}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var got []int
	for i := 0; i < 3; i++ {
		select {
		case task := <-processed:
			got = append(got, task)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for task processing")
		}
	}

	assert.ElementsMatch(t, []int{1, 2, 3}, got)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWorkerStopsOnContextCancel(t *testing.T) {
	h := &testHandler{}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

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

func TestWorkerWaitsForInFlightTasks(t *testing.T) {
	var completed atomic.Int64
	started := make(chan struct{})

	h := &testHandler{
		tasks: []int{1},
		processFn: func(ctx context.Context, task int) error {
			close(started)
			time.Sleep(100 * time.Millisecond)
			completed.Add(1)
			return nil
		},
	}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task to start")
	}

	cancel()

	select {
	case <-done:
		assert.Equal(t, int64(1), completed.Load())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker to drain")
	}
}

func TestWorkerRespectsMaxConcurrency(t *testing.T) {
	var (
		current atomic.Int64
		peak    atomic.Int64
	)

	processed := make(chan int, 20)

	h := &testHandler{
		tasks: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		processFn: func(ctx context.Context, task int) error {
			cur := current.Add(1)
			defer current.Add(-1)

			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}

			time.Sleep(50 * time.Millisecond)
			processed <- task
			return nil
		},
	}

	w := worker.New[int]("test", h, testLogger(),
		testWorkerOpts(worker.WithMaxConcurrency(2))...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	for i := 0; i < 10; i++ {
		select {
		case <-processed:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for task %d", i+1)
		}
	}

	cancel()
	<-done

	assert.LessOrEqual(t, peak.Load(), int64(2))
}

func TestWorkerContinuesAfterProcessError(t *testing.T) {
	processed := make(chan int, 10)

	h := &testHandler{
		tasks: []int{1, 2, 3},
		processFn: func(ctx context.Context, task int) error {
			if task == 1 {
				return errors.New("processing failed")
			}
			processed <- task
			return nil
		},
	}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var got []int
	for i := 0; i < 2; i++ {
		select {
		case task := <-processed:
			got = append(got, task)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for task processing")
		}
	}

	assert.ElementsMatch(t, []int{2, 3}, got)

	cancel()
	<-done
}

func TestWorkerContinuesAfterClaimError(t *testing.T) {
	var claimCalls atomic.Int64
	processed := make(chan int, 10)

	h := &testHandler{
		claimFn: func(ctx context.Context) (int, error) {
			n := claimCalls.Add(1)
			switch {
			case n == 1:
				return 0, errors.New("database connection lost")
			case n == 2:
				return 42, nil
			default:
				return 0, worker.ErrNoTask
			}
		},
		processFn: func(ctx context.Context, task int) error {
			processed <- task
			return nil
		},
	}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case task := <-processed:
		assert.Equal(t, 42, task)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task processing")
	}

	cancel()
	<-done
}

func TestWorkerCallsRecoverStale(t *testing.T) {
	h := &recoverableHandler{}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	require.Eventually(t, func() bool {
		return h.recoverCalls.Load() > 0
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	<-done
}

func TestWorkerSkipsRecoverStaleWhenNotImplemented(t *testing.T) {
	h := &testHandler{}

	w := worker.New[int]("test", h, testLogger(), testWorkerOpts()...)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := w.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWithMaxConcurrencyIgnoresInvalidValues(t *testing.T) {
	var (
		current atomic.Int64
		peak    atomic.Int64
	)

	processed := make(chan int, 20)

	h := &testHandler{
		tasks: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		processFn: func(ctx context.Context, task int) error {
			cur := current.Add(1)
			defer current.Add(-1)

			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}

			time.Sleep(30 * time.Millisecond)
			processed <- task
			return nil
		},
	}

	w := worker.New[int]("test", h, testLogger(),
		testWorkerOpts(worker.WithMaxConcurrency(0))...)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	for i := 0; i < 10; i++ {
		select {
		case <-processed:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for task processing")
		}
	}

	cancel()
	<-done

	assert.Greater(t, peak.Load(), int64(2),
		"peak concurrency should exceed 2, proving MaxConcurrency(0) was ignored")
}
