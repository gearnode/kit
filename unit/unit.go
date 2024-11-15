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
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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
		Metrics MetricsConfig `json:"metrics"`
		Tracing TracingConfig `json:"tracing"`
	}

	MetricsConfig struct {
		Addr string `json:"addr"`
	}

	TracingConfig struct {
		Addr          string `json:"addr"`
		MaxBatchSize  int    `json:"max-batch-size"`
		BatchTimeout  int    `json:"batch-timeout"`
		ExportTimeout int    `json:"export-timeout"`
		MaxQueueSize  int    `json:"max-queue-size"`
	}
)

func NewUnit(name string, version, environment string) *Unit {
	return &Unit{
		name: name,
		logger: log.NewLogger(
			log.WithName(name),
			log.WithAttributes(
				log.String("version", version),
				log.String("environment", environment),
			),
		),
		config: &Config{
			Metrics: MetricsConfig{
				Addr: ":9090",
			},
			Tracing: TracingConfig{
				Addr:          ":4317",
				MaxBatchSize:  1024,
				BatchTimeout:  10,
				ExportTimeout: 15,
				MaxQueueSize:  5000,
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

	if *printCfg {
		config := map[string]any{"unit": u.config}
		if configurable, ok := u.main.(Configurable); ok {
			config[u.name] = configurable.GetConfiguration()
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "\t")

		if err := encoder.Encode(config); err != nil {
			return fmt.Errorf("cannot encode configuration: %w", err)
		}

		return nil
	}

	logger := u.logger.Named("unit")

	ctx, cancel := context.WithCancelCause(parentCtx)
	defer cancel(context.Canceled)

	wg := sync.WaitGroup{}
	metricsInitialized := make(chan prometheus.Registerer)
	tracingInitialized := make(chan trace.TracerProvider)

	metricsServerCtx, stopMetricsServer := context.WithCancel(context.Background())
	defer stopMetricsServer()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := u.runMetricsServer(metricsServerCtx, metricsInitialized); err != nil {
			cancel(fmt.Errorf("metrics server crashed: %w", err))
		}

		logger.Info("metrics server shutdown")
	}()

	tracingExporterCtx, stopTracingExporter := context.WithCancel(context.Background())
	defer stopTracingExporter()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := u.runTracingExporter(tracingExporterCtx, tracingInitialized); err != nil {
			cancel(fmt.Errorf("traces exporter crashed: %w", err))
		}

		logger.Info("metrics server shutdown")
	}()

	var registry prometheus.Registerer
	var traceProvider trace.TracerProvider

	select {
	case registry = <-metricsInitialized:
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	select {
	case traceProvider = <-tracingInitialized:
	case <-ctx.Done():
		return context.Cause(ctx)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := u.main.Run(ctx, u.logger, registry, traceProvider); err != nil {
			cancel(err)
		}
	}()

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	stopMetricsServer()
	stopTracingExporter()

	wg.Wait()

	return context.Cause(ctx)
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
			ErrorLog:            stdlog.New(logger, "", 0),
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
	defer listener.Close()

	initialized <- registry

	serverErrCh := make(chan error, 1)
	go func() {
		err = httpServer.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- fmt.Errorf("cannot server http request: %w", err)
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
	logger := u.logger.Named("unit.metrics")
	config := u.config.Tracing

	logger.InfoCtx(ctx, "starting traces exporter", log.String("addr", config.Addr))

	exporter := otlptracehttp.NewUnstarted(
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		otlptracehttp.WithRetry(
			otlptracehttp.RetryConfig{
				Enabled:         true,
				InitialInterval: 500 * time.Millisecond,
				MaxInterval:     5 * time.Second,
				MaxElapsedTime:  5 * time.Minute,
			},
		),
		otlptracehttp.WithTimeout(15*time.Second),
	)

	if err := exporter.Start(ctx); err != nil {
		return fmt.Errorf("cannot create otel exporter: %w", err)
	}

	traceProvider := traceSdk.NewTracerProvider(
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
				semconv.DeploymentEnvironment(u.environment),
			),
		),
	)

	initialized <- traceProvider

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
		encoded, _ := json.Marshal(config["uniq"])
		if err := json.Unmarshal(encoded, u.config); err != nil {
			return fmt.Errorf("cannot decode %q config section: %w", "uniq", err)
		}
	}

	if configurable, ok := u.main.(Configurable); !ok {
		if _, ok := config[u.name]; ok {
			encoded, _ := json.Marshal(config[u.name])
			if err := json.Unmarshal(encoded, configurable.GetConfiguration()); err != nil {
				return fmt.Errorf("cannot decode %q config section: %w", u.name, err)
			}
		}
	}

	return nil
}
