package otelutils

import (
	"context"
	"errors"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestUTF8TracerProvider_SanitizesAllStrings(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 'a'})
	if utf8.ValidString(invalid) {
		t.Fatalf("test setup failed: string should be invalid UTF-8")
	}

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(rec),
	)

	wrapped := WrapTracerProvider(tp)
	tr := wrapped.Tracer(invalid)

	invalidKey := attribute.Key(invalid)
	invalidKV := invalidKey.String(invalid)
	invalidSliceKV := invalidKey.StringSlice([]string{invalid, "ok"})

	_, span := tr.Start(
		context.Background(),
		invalid,
		trace.WithAttributes(invalidKV, invalidSliceKV),
	)

	span.SetStatus(codes.Error, invalid)
	span.AddEvent(invalid, trace.WithAttributes(invalidKV))
	span.RecordError(errors.New(invalid), trace.WithAttributes(invalidKV))
	span.SetAttributes(invalidKV)
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}

	s := ended[0]
	if !utf8.ValidString(s.Name()) {
		t.Fatalf("span name must be valid UTF-8: %q", s.Name())
	}

	st := s.Status()
	if !utf8.ValidString(st.Description) {
		t.Fatalf("status description must be valid UTF-8: %q", st.Description)
	}

	scope := s.InstrumentationScope()
	if !utf8.ValidString(scope.Name) {
		t.Fatalf("instrumentation scope name must be valid UTF-8: %q", scope.Name)
	}
	if scope.Version != "" && !utf8.ValidString(scope.Version) {
		t.Fatalf("instrumentation scope version must be valid UTF-8: %q", scope.Version)
	}

	assertValidKeyValues(t, s.Attributes())
	for _, ev := range s.Events() {
		if !utf8.ValidString(ev.Name) {
			t.Fatalf("event name must be valid UTF-8: %q", ev.Name)
		}
		assertValidKeyValues(t, ev.Attributes)
	}
	for _, l := range s.Links() {
		assertValidKeyValues(t, l.Attributes)
	}
}

func assertValidKeyValues(t *testing.T, kvs []attribute.KeyValue) {
	t.Helper()
	for _, kv := range kvs {
		if !utf8.ValidString(string(kv.Key)) {
			t.Fatalf("attribute key must be valid UTF-8: %q", string(kv.Key))
		}
		switch kv.Value.Type() {
		case attribute.STRING:
			if !utf8.ValidString(kv.Value.AsString()) {
				t.Fatalf("attribute string value must be valid UTF-8: %q", kv.Value.AsString())
			}
		case attribute.STRINGSLICE:
			for _, s := range kv.Value.AsStringSlice() {
				if !utf8.ValidString(s) {
					t.Fatalf("attribute string slice value must be valid UTF-8: %q", s)
				}
			}
		}
	}
}

