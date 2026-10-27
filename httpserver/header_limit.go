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

package httpserver

import (
	"fmt"
	"net/http"
)

const (
	// maxHostLen is the longest Host value. A domain name in text form
	// is at most 254 octets with the trailing root dot (RFC 1035), and a
	// port adds ":" plus 5 digits.
	maxHostLen = 260 // bytes

	// maxHeaderValueLen is the maximum size of one header value.
	// nginx limits one header field to 8 KiB. 16 KiB leaves room for a
	// long JWT and for a Cookie header that carries several cookies.
	maxHeaderValueLen = 16 << 10 // 16 KiB

	// maxHeaderBytes is the maximum size of the request line and
	// headers read by net/http. The net/http default is 1 MiB.
	maxHeaderBytes = 100 << 10 // 100 KiB
)

// checkHeadersLen reports the first header that exceeds its limit.
// Host is limited to maxHostLen. Every other value is limited to
// maxHeaderValueLen. A nil error means every value is within its limit.
func checkHeadersLen(r *http.Request) error {
	if len(r.Host) > maxHostLen {
		return fmt.Errorf("header value too long: Host")
	}

	for name, values := range r.Header {
		for _, value := range values {
			if len(value) > maxHeaderValueLen {
				return fmt.Errorf("header value too long: %s", name)
			}
		}
	}

	return nil
}
