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

package log

import (
	"context"
	"io"
	"strings"
)

type (
	Writer struct {
		logger *Logger
		level  Level
	}
)

var (
	_ io.Writer = (*Writer)(nil)
)

func (l *Logger) NewWriter(level Level) io.Writer {
	return &Writer{
		logger: l,
		level:  level,
	}
}

func (w *Writer) Write(b []byte) (n int, err error) {
	w.logger.Log(context.Background(), w.level, strings.TrimSpace(string(b)))
	return len(b), nil
}
