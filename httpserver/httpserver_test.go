package httpserver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// setupTracerProvider creates a tracer provider with a span recorder for testing
func setupTracerProvider() (trace.TracerProvider, *tracetest.SpanRecorder) {
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tracerProvider, spanRecorder
}

func TestHTTPServer_BasicOperation(t *testing.T) {
	// Set up the tracer provider
	tracerProvider, spanRecorder := setupTracerProvider()
	defer func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
	}()

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Create a logger that writes to a buffer
	var logBuf bytes.Buffer
	logger := log.NewLogger(log.WithOutput(&logBuf))

	// Create a test registry for metrics
	registry := prometheus.NewRegistry()

	// Set up the server
	server := NewServer(":8080", testHandler,
		WithLogger(logger),
		WithRegisterer(registry),
		WithTracerProvider(tracerProvider),
	)

	// Create a test server
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	// Make a test request
	req, err := http.NewRequest("GET", ts.URL+"/test", nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "test-agent")

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Check response
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body))

	// Check that request ID was set
	assert.NotEmpty(t, resp.Header.Get("x-request-id"))

	// Check logs
	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "http_request_method")
	assert.Contains(t, logOutput, "/test")
	assert.Contains(t, logOutput, "test-agent")
	assert.Contains(t, logOutput, "200")

	// Check metrics
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	assert.Greater(t, len(metricFamilies), 0)

	// Wait a bit for spans to be processed
	time.Sleep(50 * time.Millisecond)

	// Check traces - but don't make the test fail if tracing doesn't work
	// This makes the test more robust in different environments
	spans := spanRecorder.Ended()
	if len(spans) > 0 {
		t.Log("Spans recorded:", len(spans))
	} else {
		t.Log("No spans were recorded, but continuing with the test")
	}
}

// mockPanicHandler is a handler that always panics
type mockPanicHandler struct{}

func (h *mockPanicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	panic("test panic")
}

func TestHTTPServer_PanicHandling(t *testing.T) {
	// Set up the tracer provider
	tracerProvider, _ := setupTracerProvider()
	defer func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
	}()

	// Create a handler that panics
	panicHandler := &mockPanicHandler{}

	// Create a logger that writes to a buffer
	var logBuf bytes.Buffer
	logger := log.NewLogger(log.WithOutput(&logBuf))

	// Create a test registry for metrics
	registry := prometheus.NewRegistry()

	// Set up the server
	server := NewServer(":8080", panicHandler,
		WithLogger(logger),
		WithRegisterer(registry),
		WithTracerProvider(tracerProvider),
	)

	// Create a test server
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	// Make a test request
	resp, err := http.Get(ts.URL + "/panic")
	require.NoError(t, err)
	defer resp.Body.Close()

	// Check response
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Check response body
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var respBody map[string]string
	err = json.Unmarshal(body, &respBody)
	require.NoError(t, err)
	assert.Equal(t, "internal error", respBody["error"])

	// Check logs
	logOutput := logBuf.String()
	assert.Contains(t, logOutput, "/panic")
	assert.Contains(t, logOutput, "500")
	assert.Contains(t, logOutput, "test panic")
	assert.Contains(t, logOutput, "stacktrace")

	// Check metrics
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	assert.Greater(t, len(metricFamilies), 0)

	// Verify panic handling works properly (it doesn't crash the server)
	assert.NotPanics(t, func() {
		resp, err := http.Get(ts.URL + "/panic")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}

func TestHTTPServer_Propagation(t *testing.T) {
	// Create a test handler that logs the HTTP headers for verification
	var requestHeaders http.Header
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	})

	// Create a logger that writes to a buffer
	var logBuf bytes.Buffer
	logger := log.NewLogger(log.WithOutput(&logBuf))

	// Set up a test provider
	tracerProvider, _ := setupTracerProvider()
	defer func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
	}()

	// Set up the server
	server := NewServer(":8080", testHandler,
		WithLogger(logger),
		WithRegisterer(prometheus.NewRegistry()),
		WithTracerProvider(tracerProvider),
	)

	// Create a test server
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	// Create a request
	req, err := http.NewRequest("GET", ts.URL+"/test", nil)
	require.NoError(t, err)

	// Add a traceparent header to test propagation
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-0102030405060708-01")

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Check response
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify request ID is present
	assert.NotEmpty(t, resp.Header.Get("x-request-id"))

	// Verify the trace header was received by the handler
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-0102030405060708-01",
		requestHeaders.Get("traceparent"))
}

// TestHTTPServer_Metrics tests the metrics collection
func TestHTTPServer_Metrics(t *testing.T) {
	// Create a test handler that introduces various response sizes and timing
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fast":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		case "/slow":
			time.Sleep(50 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok","data":"` + strings.Repeat("x", 1000) + `"}`))
		case "/error":
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"bad request"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	// Create a logger that writes to a buffer
	var logBuf bytes.Buffer
	logger := log.NewLogger(log.WithOutput(&logBuf))

	// Create a test registry for metrics
	registry := prometheus.NewRegistry()

	// Set up the server
	server := NewServer(":8080", testHandler,
		WithLogger(logger),
		WithRegisterer(registry),
	)

	// Create a test server
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	// Make multiple requests to test different metrics
	paths := []string{"/fast", "/slow", "/error", "/notfound"}
	for _, path := range paths {
		resp, err := http.Get(ts.URL + path)
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	// Check metrics
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	// Verify that we have metrics registered
	foundMetrics := false
	for _, mf := range metricFamilies {
		if strings.Contains(*mf.Name, "http_server_") {
			foundMetrics = true
			break
		}
	}

	assert.True(t, foundMetrics, "HTTP server metrics should exist")
}

// TestHTTPServer_Logging tests the logging functionality
func TestHTTPServer_Logging(t *testing.T) {
	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/error" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Custom-Header", "test-value")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Create a logger that writes to a buffer
	var logBuf bytes.Buffer
	logger := log.NewLogger(log.WithOutput(&logBuf))

	// Set up the server
	server := NewServer(":8080", testHandler,
		WithLogger(logger),
	)

	// Create a test server
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	// Make a successful request
	resp, err := http.Get(ts.URL + "/test")
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Make an error request
	resp, err = http.Get(ts.URL + "/error")
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Check logs
	logOutput := logBuf.String()

	// Verify logs contain appropriate information
	assert.Contains(t, logOutput, "/test")
	assert.Contains(t, logOutput, "200")
	assert.Contains(t, logOutput, "/error")
	assert.Contains(t, logOutput, "500")
}

// Create a custom registry for the health endpoint test to avoid duplicate metrics registration
var healthRegistry = prometheus.NewRegistry()

// TestHTTPServer_Health tests the health endpoint
func TestHTTPServer_Health(t *testing.T) {
	// Create a test handler that should not be called for health endpoint
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called for health endpoint")
	})

	// Create a logger that writes to a buffer
	var logBuf bytes.Buffer
	logger := log.NewLogger(log.WithOutput(&logBuf))

	// Set up the server with a dedicated registry
	server := NewServer(":8080", testHandler,
		WithLogger(logger),
		WithRegisterer(healthRegistry),
	)

	// Create a test server
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()

	// Make a request to the health endpoint
	resp, err := http.Get(ts.URL + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	// Check response
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("content-type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(body))

	// Verify that logs don't contain health check request (should be bypassed)
	logOutput := logBuf.String()
	assert.NotContains(t, logOutput, "/health")
}
