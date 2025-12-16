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

package ratelimit

import (
	"context"
	"time"

	"go.gearno.de/kit/log"
)

// StartCleanup starts a background goroutine that periodically removes
// expired rate limit entries from the database. The goroutine stops
// when the provided context is cancelled.
//
// This method is safe to call multiple times; only the first call will
// start the cleanup goroutine.
//
// Example:
//
//	limiter.StartCleanup(ctx) // starts background cleanup
//	// ... application runs ...
//	cancel() // stops cleanup when context is cancelled
func (l *Limiter) StartCleanup(ctx context.Context) {
	l.cleanupOnce.Do(func() {
		go l.runCleanupLoop(ctx)
	})
}

func (l *Limiter) runCleanupLoop(ctx context.Context) {
	l.logger.InfoCtx(ctx, "starting rate limit cleanup loop",
		log.Duration("interval", l.cleanupInterval),
	)

	ticker := time.NewTicker(l.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.logger.InfoCtx(ctx, "stopping rate limit cleanup loop")
			return
		case <-ticker.C:
			// Clean up entries older than 2x the cleanup interval
			// This ensures we keep enough history for sliding window calculations
			olderThan := l.cleanupInterval * 2

			if _, err := l.Cleanup(ctx, olderThan); err != nil {
				l.logger.ErrorCtx(ctx, "rate limit cleanup failed",
					log.Error(err),
				)
			}
		}
	}
}

