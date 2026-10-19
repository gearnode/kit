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

package unit

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.gearno.de/kit/internal/otelutils"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/yaml"
)

type (
	Unit struct {
		name        string
		version     string
		environment string

		logger *log.Logger
		config *Config
		main   Runnable
	}

	Runnable interface {
		Run(context.Context, *log.Logger, prometheus.Registerer, trace.TracerProvider) error
	}

	Configurable interface {
		GetConfiguration() any
	}

	Config struct {
		Metrics         MetricsConfig `json:"metrics"`
		Tracing         TracingConfig `json:"tracing"`
		ShutdownTimeout int           `json:"shutdown-timeout"`
	}

	MetricsConfig struct {
		Addr string `json:"addr"`
	}

	TracingConfig struct {
		Addr          string            `json:"addr"`
		Insecure      bool              `json:"insecure"`
		Headers       map[string]string `json:"headers,omitempty"`
		MaxBatchSize  int               `json:"max-batch-size"`
		BatchTimeout  int               `json:"batch-timeout"`
		ExportTimeout int               `json:"export-timeout"`
		MaxQueueSize  int               `json:"max-queue-size"`
		SamplingRatio float64           `json:"sampling-ratio"`
	}
)

func NewUnit(main Runnable, name, version, environment string) *Unit {
	return &Unit{
		name:        name,
		version:     version,
		environment: environment,
		main:        main,
		config: &Config{
			ShutdownTimeout: 30,
			Metrics: MetricsConfig{
				Addr: "127.0.0.1:9090",
			},
			Tracing: TracingConfig{
				Addr:          "localhost:4318",
				MaxBatchSize:  1024,
				BatchTimeout:  10,
				ExportTimeout: 15,
				MaxQueueSize:  5000,
				SamplingRatio: 0.1,
			},
		},
	}
}

func (u *Unit) Run() error {
	return u.RunContext(context.Background())
}

func (u *Unit) RunContext(parentCtx context.Context) error {
	filename := flag.String("cfg-file", "", "the path of the configuration file")
	printCfg := flag.Bool("print-cfg", false, "print the loaded cfg and exit")
	format := flag.String("format", "json", "log format")
	help := flag.Bool("help", false, "show this help message")
	version := flag.Bool("version", false, "show the service version")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		return nil
	}

	if *version {
		fmt.Printf("version: %s\n", u.version)
		return nil
	}

	if *filename != "" {
		if err := u.loadConfigurationFromFile(*filename); err != nil {
			return fmt.Errorf("cannot load configuration from %q file: %w", *filename, err)
		}
	}

	if err := u.config.Validate(); err != nil {
		return fmt.Errorf("invalid unit configuration: %w", err)
	}
	switch log.Format(*format) {
	case log.FormatJSON, log.FormatPretty, log.FormatText:
	default:
		return fmt.Errorf("unsupported log format %q", *format)
	}

	if *printCfg {
		config := map[string]any{"unit": u.config}
		if configurable, ok := u.main.(Configurable); ok {
			config[u.name] = configurable.GetConfiguration()
		}
		redacted, err := redactConfiguration(config)
		if err != nil {
			return fmt.Errorf("cannot redact configuration: %w", err)
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "\t")

		if err := encoder.Encode(redacted); err != nil {
			return fmt.Errorf("cannot encode configuration: %w", err)
		}

		return nil
	}

	u.logger = log.NewLogger(
		log.WithName(u.name),
		log.WithAttributes(
			log.String("version", u.version),
			log.String("environment", u.environment),
		),
		log.WithFormat(*format),
	)
	logger := u.logger.Named("unit")

	runCtx, cancel := context.WithCancelCause(parentCtx)
	defer cancel(context.Canceled)

	wg := sync.WaitGroup{}
	metricsInitialized := make(chan prometheus.Registerer, 1)
	tracingInitialized := make(chan trace.TracerProvider, 1)

	metricsServerCtx, stopMetricsServer := context.WithCancel(context.Background())
	defer stopMetricsServer()

	wg.Go(
		func() {
			if err := u.runMetricsServer(metricsServerCtx, metricsInitialized); err != nil {
				cancel(fmt.Errorf("metrics server crashed: %w", err))
			}

			logger.Info("metrics server shutdown")
		},
	)

	tracingExporterCtx, stopTracingExporter := context.WithCancel(context.Background())
	defer stopTracingExporter()

	wg.Go(
		func() {
			if err := u.runTracingExporter(tracingExporterCtx, tracingInitialized); err != nil {
				cancel(fmt.Errorf("traces exporter crashed: %w", err))
			}

			logger.Info("trace exporter shutdown")
		},
	)

	var registry prometheus.Registerer
	var traceProvider trace.TracerProvider

	select {
	case registry = <-metricsInitialized:
	case <-runCtx.Done():
		return context.Cause(runCtx)
	}

	select {
	case traceProvider = <-tracingInitialized:
	case <-runCtx.Done():
		return context.Cause(runCtx)
	}

	wg.Go(
		func() {

			if err := u.main.Run(runCtx, u.logger, registry, traceProvider); err != nil {
				cancel(err)
			}
		},
	)

	signalCtx, stop := signal.NotifyContext(runCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-signalCtx.Done()

	cancel(context.Canceled)
	stopMetricsServer()
	stopTracingExporter()

	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()
	select {
	case <-shutdownComplete:
	case <-time.After(time.Duration(u.config.ShutdownTimeout) * time.Second):
		return fmt.Errorf("shutdown timed out after %d seconds", u.config.ShutdownTimeout)
	}

	if cause := context.Cause(runCtx); cause != nil {
		return cause
	}

	return context.Canceled
}

func (u *Unit) runMetricsServer(ctx context.Context, initialized chan<- prometheus.Registerer) error {
	logger := u.logger.Named("unit.metrics")

	logger.InfoCtx(ctx, "starting metrics server")

	registry := prometheus.NewPedanticRegistry()
	metricsHandler := promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{
			EnableOpenMetrics:   true,
			MaxRequestsInFlight: 10,
			ErrorHandling:       promhttp.ContinueOnError,
			ErrorLog:            stdlog.New(logger.NewWriter(log.LevelError), "", 0),
		},
	)

	httpServer := &http.Server{
		Addr: u.config.Metrics.Addr,
		Handler: http.TimeoutHandler(
			metricsHandler,
			5*time.Second,
			"request timed out",
		),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	logger.Info("starting metrics server", log.String("addr", httpServer.Addr))
	listener, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		return fmt.Errorf("cannot listen on %q: %w", httpServer.Addr, err)
	}
	defer func() { _ = listener.Close() }()

	initialized <- registry

	serverErrCh := make(chan error, 1)
	go func() {
		err = httpServer.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("cannot serve http request: %w", err)
		}
		close(serverErrCh)
	}()

	logger.Info("metrics server started")

	select {
	case err := <-serverErrCh:
		return err
	case <-ctx.Done():
	}

	logger.InfoCtx(ctx, "shutting down metrics server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("cannot shutdown http server: %w", err)
	}

	return ctx.Err()
}

func (u *Unit) runTracingExporter(ctx context.Context, initialized chan<- trace.TracerProvider) error {
	logger := u.logger.Named("unit.traces")
	config := u.config.Tracing

	logger.InfoCtx(ctx, "starting traces exporter", log.String("addr", config.Addr))

	otel.SetErrorHandler(&otelErrorHandler{logger: logger})

	exporterOptions := []otlptracehttp.Option{
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		otlptracehttp.WithEndpoint(config.Addr),
		otlptracehttp.WithRetry(
			otlptracehttp.RetryConfig{
				Enabled:         true,
				InitialInterval: 500 * time.Millisecond,
				MaxInterval:     5 * time.Second,
				MaxElapsedTime:  5 * time.Minute,
			},
		),
		otlptracehttp.WithTimeout(15 * time.Second),
	}
	if config.Insecure {
		exporterOptions = append(exporterOptions, otlptracehttp.WithInsecure())
	}
	if len(config.Headers) > 0 {
		exporterOptions = append(exporterOptions, otlptracehttp.WithHeaders(config.Headers))
	}
	exporter := otlptracehttp.NewUnstarted(exporterOptions...)

	if err := exporter.Start(ctx); err != nil {
		return fmt.Errorf("cannot create otel exporter: %w", err)
	}

	var sampler traceSdk.Sampler
	if config.SamplingRatio >= 1.0 {
		sampler = traceSdk.AlwaysSample()
		logger.Info("using AlwaysSample sampler")
	} else if config.SamplingRatio <= 0.0 {
		sampler = traceSdk.NeverSample()
		logger.Info("using NeverSample sampler")
	} else {
		sampler = traceSdk.TraceIDRatioBased(config.SamplingRatio)
		logger.Info("using TraceIDRatioBased sampler", log.Float64("ratio", config.SamplingRatio))
	}

	traceProvider := traceSdk.NewTracerProvider(
		traceSdk.WithSampler(sampler),
		traceSdk.WithBatcher(
			exporter,
			traceSdk.WithMaxExportBatchSize(config.MaxBatchSize),
			traceSdk.WithBatchTimeout(time.Duration(config.BatchTimeout)*time.Second),
			traceSdk.WithExportTimeout(time.Duration(config.ExportTimeout)*time.Second),
			traceSdk.WithMaxQueueSize(config.MaxQueueSize),
		),
		traceSdk.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceName(u.name),
				semconv.ServiceVersion(u.version),
				semconv.DeploymentEnvironmentName(u.environment),
			),
		),
	)

	utf8TraceProvider := otelutils.WrapTracerProvider(traceProvider)
	otel.SetTracerProvider(utf8TraceProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	initialized <- utf8TraceProvider

	logger.Info("trace exporter started")

	<-ctx.Done()

	logger.InfoCtx(ctx, "shutting down traces exporter")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := traceProvider.ForceFlush(shutdownCtx); err != nil {
		return fmt.Errorf("cannot flush remaining spans: %w", err)
	}

	if err := traceProvider.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("cannot shutdown provider: %w", err)
	}

	if err := exporter.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("cannot shutdown exporter: %w", err)
	}

	return ctx.Err()
}

func (u *Unit) loadConfigurationFromFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	blob, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read file: %w", err)
	}

	blob, err = yaml.YAMLToJSON(blob)
	if err != nil {
		return fmt.Errorf("cannot convert yaml to json: %w", err)
	}

	config := map[string]any{}
	if err := json.Unmarshal(blob, &config); err != nil {
		return fmt.Errorf("cannot decode file: %w", err)
	}

	if _, ok := config["unit"]; ok {
		encoded, err := json.Marshal(config["unit"])
		if err != nil {
			return fmt.Errorf("cannot encode %q config section: %w", "unit", err)
		}
		if err := json.Unmarshal(encoded, u.config); err != nil {
			return fmt.Errorf("cannot decode %q config section: %w", "unit", err)
		}
	}

	if configurable, ok := u.main.(Configurable); ok {
		if _, ok := config[u.name]; ok {
			encoded, err := json.Marshal(config[u.name])
			if err != nil {
				return fmt.Errorf("cannot encode %q config section: %w", u.name, err)
			}
			if err := json.Unmarshal(encoded, configurable.GetConfiguration()); err != nil {
				return fmt.Errorf("cannot decode %q config section: %w", u.name, err)
			}
		}
	}

	return nil
}

func (c *Config) Validate() error {
	if c == nil {
		return errors.New("configuration is nil")
	}
	if c.ShutdownTimeout < 1 {
		return errors.New("shutdown-timeout must be greater than zero")
	}
	if _, _, err := net.SplitHostPort(c.Metrics.Addr); err != nil {
		return fmt.Errorf("invalid metrics address %q: %w", c.Metrics.Addr, err)
	}
	if _, _, err := net.SplitHostPort(c.Tracing.Addr); err != nil {
		return fmt.Errorf("invalid tracing address %q: %w", c.Tracing.Addr, err)
	}
	if c.Tracing.MaxBatchSize < 1 {
		return errors.New("tracing max-batch-size must be greater than zero")
	}
	if c.Tracing.MaxQueueSize < c.Tracing.MaxBatchSize {
		return errors.New("tracing max-queue-size must be greater than or equal to max-batch-size")
	}
	if c.Tracing.BatchTimeout < 1 || c.Tracing.ExportTimeout < 1 {
		return errors.New("tracing timeouts must be greater than zero")
	}
	if c.Tracing.SamplingRatio < 0 || c.Tracing.SamplingRatio > 1 {
		return errors.New("tracing sampling-ratio must be between zero and one")
	}
	return nil
}

func redactConfiguration(config any) (any, error) {
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, err
	}
	redactValue(normalized)
	return normalized, nil
}

func redactValue(value any) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if sensitiveConfigKey(key) {
				value[key] = "[REDACTED]"
			} else {
				redactValue(child)
			}
		}
	case []any:
		for _, child := range value {
			redactValue(child)
		}
	}
}

func sensitiveConfigKey(key string) bool {
	key = strings.ToLower(key)
	for _, marker := range []string{"authorization", "cookie", "credential", "password", "private-key", "secret", "token"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}
