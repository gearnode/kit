// Copyright (c) 2026.
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

package otelutils

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
	"go.opentelemetry.io/otel/trace/noop"
)

// UTF8TracerProvider wraps a tracer provider and guarantees that any strings
// recorded through the trace API are valid UTF-8 (span names, status
// descriptions, error messages, event names, and all string/string-slice
// attributes).
//
// This is important because OTLP/protobuf rejects invalid UTF-8 in string
// fields and will fail the entire export batch.
type UTF8TracerProvider struct {
	embedded.TracerProvider

	next trace.TracerProvider
}

// WrapTracerProvider returns a tracer provider wrapper that sanitizes all
// string data before it reaches the SDK/exporter.
func WrapTracerProvider(next trace.TracerProvider) trace.TracerProvider {
	if next == nil {
		return nil
	}
	return &UTF8TracerProvider{next: next}
}

func (tp *UTF8TracerProvider) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	if tp == nil {
		return noop.NewTracerProvider().Tracer(ToValidUTF8(name), options...)
	}
	if tp.next == nil {
		return noop.NewTracerProvider().Tracer(ToValidUTF8(name), options...)
	}
	return &utf8Tracer{
		next: tp.next.Tracer(ToValidUTF8(name), options...),
		tp:   tp,
	}
}

type utf8Tracer struct {
	embedded.Tracer

	next trace.Tracer
	tp   *UTF8TracerProvider
}

func (t *utf8Tracer) Start(ctx context.Context, name string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(options...)

	opts := make([]trace.SpanStartOption, 0, 5)

	// Preserve config while sanitizing all attributes.
	if cfg.NewRoot() {
		opts = append(opts, trace.WithNewRoot())
	}
	if sk := cfg.SpanKind(); sk != trace.SpanKindUnspecified {
		opts = append(opts, trace.WithSpanKind(sk))
	}
	if ts := cfg.Timestamp(); !ts.IsZero() {
		opts = append(opts, trace.WithTimestamp(ts))
	}
	if links := cfg.Links(); len(links) > 0 {
		sanitized := make([]trace.Link, 0, len(links))
		for _, l := range links {
			sanitized = append(sanitized, trace.Link{
				SpanContext: l.SpanContext,
				Attributes:  sanitizeKeyValues(l.Attributes),
			})
		}
		opts = append(opts, trace.WithLinks(sanitized...))
	}
	if attrs := cfg.Attributes(); len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(sanitizeKeyValues(attrs)...))
	}

	ctx2, s := t.next.Start(ctx, ToValidUTF8(name), opts...)
	return ctx2, &utf8Span{next: s, tp: t.tp}
}

type utf8Span struct {
	embedded.Span

	next trace.Span
	tp   *UTF8TracerProvider
}

func (s *utf8Span) End(options ...trace.SpanEndOption) { s.next.End(options...) }
func (s *utf8Span) SpanContext() trace.SpanContext     { return s.next.SpanContext() }
func (s *utf8Span) IsRecording() bool                  { return s.next.IsRecording() }

func (s *utf8Span) SetStatus(code codes.Code, description string) {
	s.next.SetStatus(code, ToValidUTF8(description))
}

func (s *utf8Span) SetName(name string) { s.next.SetName(ToValidUTF8(name)) }

func (s *utf8Span) SetAttributes(kv ...attribute.KeyValue) {
	s.next.SetAttributes(sanitizeKeyValues(kv)...)
}

func (s *utf8Span) AddEvent(name string, options ...trace.EventOption) {
	cfg := trace.NewEventConfig(options...)

	opts := make([]trace.EventOption, 0, 3)
	if ts := cfg.Timestamp(); !ts.IsZero() {
		opts = append(opts, trace.WithTimestamp(ts))
	}
	if cfg.StackTrace() {
		opts = append(opts, trace.WithStackTrace(true))
	}
	if attrs := cfg.Attributes(); len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(sanitizeKeyValues(attrs)...))
	}

	s.next.AddEvent(ToValidUTF8(name), opts...)
}

func (s *utf8Span) AddLink(link trace.Link) {
	s.next.AddLink(trace.Link{
		SpanContext: link.SpanContext,
		Attributes:  sanitizeKeyValues(link.Attributes),
	})
}

func (s *utf8Span) RecordError(err error, options ...trace.EventOption) {
	cfg := trace.NewEventConfig(options...)

	opts := make([]trace.EventOption, 0, 3)
	if ts := cfg.Timestamp(); !ts.IsZero() {
		opts = append(opts, trace.WithTimestamp(ts))
	}
	if cfg.StackTrace() {
		opts = append(opts, trace.WithStackTrace(true))
	}
	if attrs := cfg.Attributes(); len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(sanitizeKeyValues(attrs)...))
	}

	s.next.RecordError(SanitizeError(err), opts...)
}

func (s *utf8Span) TracerProvider() trace.TracerProvider {
	// Return the wrapper provider so downstream Tracer() calls remain sanitized.
	return s.tp
}

func sanitizeKeyValues(in []attribute.KeyValue) []attribute.KeyValue {
	if len(in) == 0 {
		return in
	}

	out := make([]attribute.KeyValue, 0, len(in))
	for _, kv := range in {
		if !kv.Valid() {
			continue
		}
		key := attribute.Key(ToValidUTF8(string(kv.Key)))
		switch kv.Value.Type() {
		case attribute.STRING:
			out = append(out, key.String(ToValidUTF8(kv.Value.AsString())))
		case attribute.STRINGSLICE:
			ss := kv.Value.AsStringSlice()
			if len(ss) == 0 {
				out = append(out, attribute.KeyValue{Key: key, Value: kv.Value})
				continue
			}
			cp := make([]string, len(ss))
			for i := range ss {
				cp[i] = ToValidUTF8(ss[i])
			}
			out = append(out, key.StringSlice(cp))
		default:
			out = append(out, attribute.KeyValue{Key: key, Value: kv.Value})
		}
	}
	return out
}

