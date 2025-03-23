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
	"encoding/json"
	"net/http"
	"strings"

	"go.gearno.de/x/panicf"
)

func RenderJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		panicf.Panic("cannot json encode value: %w", err)
	}
}

func RenderText(w http.ResponseWriter, statusCode int, v string) {
	w.Header().Set("content-type", "text/plain; charset=ut8")
	w.WriteHeader(statusCode)
	_, err := w.Write([]byte(v))
	if err != nil {
		panicf.Panic("cannot write text response: %w", err)
	}
}

func RenderError(w http.ResponseWriter, statusCode int, err error) {
	response := map[string]string{
		"error":   strings.ReplaceAll(strings.ToLower(http.StatusText(statusCode)), " ", "_"),
		"message": err.Error(),
	}

	RenderJSON(w, statusCode, response)
}
