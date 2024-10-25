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

package pg

import (
	"context"

	"github.com/jackc/pgx/v5/tracelog"
	"go.gearno.de/kit/log"
)

type (
	logger struct {
		logger *log.Logger
	}
)

var (
	_ tracelog.Logger = (*logger)(nil)
)

func (l *logger) Log(
	ctx context.Context,
	level tracelog.LogLevel,
	msg string,
	data map[string]any,
) {
	attrs := make([]log.Attr, 0, len(data))
	for k, v := range data {
		attrs = append(attrs, log.Any(k, v))
	}

	var lvl log.Level
	switch level {
	case tracelog.LogLevelTrace:
		lvl = log.LevelDebug - 1
		attrs = append(attrs, log.Any("PGX_LOG_LEVEL", level))
	case tracelog.LogLevelDebug:
		lvl = log.LevelDebug
	case tracelog.LogLevelInfo:
		lvl = log.LevelInfo
	case tracelog.LogLevelWarn:
		lvl = log.LevelWarn
	case tracelog.LogLevelError:
		lvl = log.LevelError
	default:
		lvl = log.LevelError
		attrs = append(attrs, log.Any("INVALID_PGX_LOG_LEVEL", level))
	}

	l.logger.Log(ctx, lvl, msg, attrs...)
}
