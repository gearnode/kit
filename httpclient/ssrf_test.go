// Copyright (c) 2026 Bryan Frimin <bryan@frimin.fr>.
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
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsBlockedAddr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		addr    string
		blocked bool
	}{
		// IPv4: blocked.
		{"0.0.0.0", true},
		{"0.1.2.3", true},
		{"10.0.0.1", true},
		{"10.255.255.255", true},
		{"100.64.0.1", true},
		{"100.127.255.255", true},
		{"127.0.0.1", true},
		{"127.255.255.254", true},
		{"169.254.169.254", true},
		{"172.16.0.1", true},
		{"172.31.255.254", true},
		{"192.0.0.1", true},
		{"192.0.2.1", true},
		{"192.168.1.1", true},
		{"198.18.0.1", true},
		{"198.19.255.254", true},
		{"198.51.100.1", true},
		{"203.0.113.1", true},
		{"224.0.0.1", true},
		{"239.255.255.255", true},
		{"240.0.0.1", true},
		{"255.255.255.255", true},

		// IPv4: allowed.
		{"1.1.1.1", false},
		{"8.8.8.8", false},
		{"9.255.255.255", false},
		{"100.63.255.255", false},
		{"100.128.0.0", false},
		{"172.15.255.255", false},
		{"172.32.0.0", false},
		{"192.167.255.255", false},
		{"192.169.0.0", false},
		{"198.17.255.255", false},
		{"198.20.0.0", false},

		// IPv6: blocked.
		{"::", true},
		{"::1", true},
		{"fc00::1", true},
		{"fd00::1", true},
		{"fe80::1", true},
		{"ff00::1", true},
		{"ff02::1", true},
		{"::ffff:127.0.0.1", true},
		{"::ffff:10.0.0.1", true},
		{"::ffff:169.254.169.254", true},
		{"64:ff9b::1.2.3.4", true},
		{"2001:db8::1", true},
		{"100::1", true},

		// IPv6: allowed.
		{"2606:4700:4700::1111", false},
		{"2001:4860:4860::8888", false},
	}

	for _, c := range cases {
		t.Run(c.addr, func(t *testing.T) {
			t.Parallel()
			ip, err := netip.ParseAddr(c.addr)
			require.NoError(t, err)
			assert.Equal(t, c.blocked, isBlockedAddr(ip))
		})
	}
}

func TestSSRFDialControl_RejectsLoopback(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := DefaultPooledClient(WithSSRFProtection(), WithRegisterer(NoopRegisterer{}))

	_, err := client.Get(server.URL)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrBlockedAddress)
}

func TestSSRFDialControl_AllowsPublicByDefault(t *testing.T) {
	t.Parallel()

	// Without SSRF protection, the same loopback request must succeed
	// so we know the protection is the only reason for the failure
	// in the previous test.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := DefaultPooledClient(WithRegisterer(NoopRegisterer{}))

	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestNoCrossOriginRedirects(t *testing.T) {
	t.Parallel()

	// Build two upstreams. The first redirects to the second; the
	// client (with SSRF protection) must refuse the cross-origin hop.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Force the redirect to look like a different origin even though
		// both servers are loopback. We are testing the CheckRedirect
		// rule, not the dialer; so disable SSRF on this client.
		http.Redirect(w, r, target.URL+"/x", http.StatusFound)
	}))
	defer redirector.Close()

	client := &http.Client{CheckRedirect: noCrossOriginRedirects}
	resp, err := client.Get(redirector.URL)
	require.Error(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}
	assert.True(t, errors.Is(err, ErrCrossOriginRedirect),
		"expected ErrCrossOriginRedirect, got %v", err)
}

func TestNoCrossOriginRedirects_AllowsSameOrigin(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/end", http.StatusFound)
	})
	mux.HandleFunc("/end", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{CheckRedirect: noCrossOriginRedirects}
	resp, err := client.Get(server.URL + "/start")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
