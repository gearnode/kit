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

package httpclient

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.gearno.de/kit/log"
	"go.opentelemetry.io/otel/trace/noop"
)

type MockRoundTripper struct {
	mock.Mock
}

func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	args := m.Called(req)
	return args.Get(0).(*http.Response), args.Error(1)
}

// NoopRegisterer implements prometheus.Registerer but does nothing.
type NoopRegisterer struct{}

func (NoopRegisterer) Register(prometheus.Collector) error  { return nil }
func (NoopRegisterer) MustRegister(...prometheus.Collector) {}
func (NoopRegisterer) Unregister(prometheus.Collector) bool { return false }

func TestNewTelemetryRoundTripper(t *testing.T) {
	mockRT := new(MockRoundTripper)
	logger := log.NewLogger(log.WithOutput(io.Discard))

	tr := NewTelemetryRoundTripper(
		mockRT,
		logger,
		noop.NewTracerProvider(),
		NoopRegisterer{},
	)
	assert.NotNil(t, tr)
}

func TestRoundTrip(t *testing.T) {
	mockRT := new(MockRoundTripper)
	logger := log.NewLogger(log.WithOutput(io.Discard))

	tr := NewTelemetryRoundTripper(
		mockRT,
		logger,
		noop.NewTracerProvider(),
		NoopRegisterer{},
	)

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
		),
	)
	defer server.Close()

	url, _ := url.Parse(server.URL)
	req := &http.Request{
		URL:    url,
		Method: "GET",
		Header: http.Header{
			"User-Agent": []string{"test-agent"},
		},
	}

	expectedResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString("OK")),
	}

	mockRT.On("RoundTrip", mock.AnythingOfType("*http.Request")).Return(expectedResponse, nil)

	response, err := tr.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)
	mockRT.AssertExpectations(t)
}
