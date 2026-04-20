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
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
)

// ErrBlockedAddress is returned when a dial is rejected because the
// resolved peer address belongs to a blocked range.
var ErrBlockedAddress = errors.New("httpclient: address blocked by SSRF protection")

// ErrCrossOriginRedirect is returned when a redirect target changes
// scheme, host, or port and the client is configured to refuse such
// redirects.
var ErrCrossOriginRedirect = errors.New("httpclient: cross-origin redirect blocked by SSRF protection")

// extraBlockedPrefixes are reserved or otherwise unsafe ranges that
// netip.Addr's helper predicates do not cover. RFC 1918 (private),
// loopback, link-local, multicast, and unspecified are handled by the
// stdlib helpers in isBlockedAddr.
var extraBlockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),          // RFC 1122 "this network"
	netip.MustParsePrefix("100.64.0.0/10"),      // RFC 6598 CGNAT
	netip.MustParsePrefix("192.0.0.0/24"),       // RFC 6890 IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),       // RFC 5737 TEST-NET-1
	netip.MustParsePrefix("198.18.0.0/15"),      // RFC 2544 benchmarking
	netip.MustParsePrefix("198.51.100.0/24"),    // RFC 5737 TEST-NET-2
	netip.MustParsePrefix("203.0.113.0/24"),     // RFC 5737 TEST-NET-3
	netip.MustParsePrefix("240.0.0.0/4"),        // RFC 1112 reserved (incl. 255.255.255.255)
	netip.MustParsePrefix("64:ff9b::/96"),       // RFC 6052 IPv4/IPv6 translation
	netip.MustParsePrefix("64:ff9b:1::/48"),     // RFC 8215 IPv4/IPv6 local translation
	netip.MustParsePrefix("100::/64"),           // RFC 6666 discard prefix
	netip.MustParsePrefix("2001::/23"),          // IETF protocol assignments
	netip.MustParsePrefix("2001:db8::/32"),      // RFC 3849 documentation
}

// makeSSRFDialControl returns a net.Dialer.Control function that
// rejects connections to IP addresses in private, loopback,
// link-local, multicast, CGNAT, or other reserved ranges. The check
// runs after DNS resolution on the actual peer address, which
// defeats DNS rebinding between any prior URL validation and the
// dial.
//
// When allowLoopback is true, 127.0.0.0/8 and ::1 are permitted. The
// option is intended for tests that bind httptest servers to the
// loopback interface and must not be used in production.
//
// Only TCP and UDP networks are permitted; unix sockets and other
// network types are refused outright.
func makeSSRFDialControl(allowLoopback bool) func(network, address string, _ syscall.RawConn) error {
	return func(network, address string, _ syscall.RawConn) error {
		switch network {
		case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		default:
			return fmt.Errorf("%w: refusing non-IP network %q", ErrBlockedAddress, network)
		}

		addrPort, err := netip.ParseAddrPort(address)
		if err != nil {
			return fmt.Errorf("%w: cannot parse peer address %q: %v", ErrBlockedAddress, address, err)
		}

		ip := addrPort.Addr()
		if ip.Is4In6() {
			ip = ip.Unmap()
		}

		if allowLoopback && ip.IsLoopback() {
			return nil
		}

		if isBlockedAddr(ip) {
			return fmt.Errorf("%w: %s", ErrBlockedAddress, ip)
		}

		return nil
	}
}

// isBlockedAddr reports whether ip falls in a range that should be
// refused for outbound HTTP from an SSRF perspective.
func isBlockedAddr(ip netip.Addr) bool {
	if !ip.IsValid() {
		return true
	}

	// Unwrap IPv4-in-IPv6 (::ffff:a.b.c.d) so a single check covers
	// both representations.
	if ip.Is4In6() {
		ip = ip.Unmap()
	}

	if ip.IsUnspecified() ||
		ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsInterfaceLocalMulticast() {
		return true
	}

	for _, prefix := range extraBlockedPrefixes {
		if prefix.Contains(ip) {
			return true
		}
	}

	return false
}

// noCrossOriginRedirects is an http.Client.CheckRedirect function that
// blocks redirects whose scheme, host, or port differs from the
// original request. It is paired with WithSSRFProtection on the
// stdlib clients returned by DefaultClient and DefaultPooledClient
// to defeat redirect-based pivots into internal services.
func noCrossOriginRedirects(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	original := via[0].URL
	if !sameOrigin(original, req.URL) {
		return fmt.Errorf("%w: %s -> %s", ErrCrossOriginRedirect, originString(original), originString(req.URL))
	}
	return nil
}

func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		defaultedPort(a) == defaultedPort(b)
}

func originString(u *url.URL) string {
	return u.Scheme + "://" + u.Host
}

func defaultedPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}
