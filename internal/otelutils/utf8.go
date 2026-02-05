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
	"strings"
	"unicode/utf8"
)

// ToValidUTF8 ensures a string is valid UTF-8. If not, invalid byte sequences
// are replaced with the Unicode replacement character.
func ToValidUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}

type sanitizedError struct {
	err error
}

func (e sanitizedError) Error() string {
	if e.err == nil {
		return ""
	}
	return ToValidUTF8(e.err.Error())
}

func (e sanitizedError) Unwrap() error { return e.err }

// SanitizeError wraps an error so Error() is guaranteed to be valid UTF-8.
// This is useful when recording errors in OpenTelemetry spans, where invalid
// UTF-8 would cause OTLP/protobuf encoding to reject the payload.
func SanitizeError(err error) error {
	if err == nil {
		return nil
	}
	if utf8.ValidString(err.Error()) {
		return err
	}
	return sanitizedError{err: err}
}

