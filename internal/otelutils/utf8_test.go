package otelutils

import (
	"errors"
	"testing"
	"unicode/utf8"
)

func TestToValidUTF8(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 'a'})
	if utf8.ValidString(invalid) {
		t.Fatalf("test setup failed: string should be invalid UTF-8")
	}

	got := ToValidUTF8(invalid)
	if !utf8.ValidString(got) {
		t.Fatalf("expected valid UTF-8, got invalid: %q", got)
	}
}

func TestSanitizeError(t *testing.T) {
	invalid := string([]byte{0xff, 0xfe, 'a'})
	err := errors.New(invalid)
	if utf8.ValidString(err.Error()) {
		t.Fatalf("test setup failed: error string should be invalid UTF-8")
	}

	serr := SanitizeError(err)
	if serr == nil {
		t.Fatalf("expected non-nil error")
	}
	if !utf8.ValidString(serr.Error()) {
		t.Fatalf("expected sanitized error string to be valid UTF-8, got: %q", serr.Error())
	}
}

