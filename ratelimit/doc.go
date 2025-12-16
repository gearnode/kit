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

// Package ratelimit provides a PostgreSQL-backed rate limiter using the
// sliding window counter algorithm.
//
// # Overview
//
// The rate limiter uses an UNLOGGED PostgreSQL table for high performance
// writes. UNLOGGED tables don't write to the Write-Ahead Log (WAL), making
// them 2-5x faster for writes. The trade-off is that data is lost on crash,
// which is acceptable for rate limiting since the data is ephemeral.
//
// # Algorithm
//
// The sliding window counter algorithm tracks requests in both the current
// and previous time windows, then interpolates between them based on how
// much of the current window has elapsed. This provides smoother rate
// limiting compared to fixed window counters, avoiding the "boundary spike"
// problem where requests at window boundaries could exceed the intended rate.
//
// # Performance Optimizations
//
// The implementation includes several optimizations:
//
//   - UNLOGGED table: Skips WAL writes for faster inserts
//   - Single atomic query: Uses INSERT ... ON CONFLICT DO UPDATE with
//     RETURNING to check and increment in one round-trip
//   - Local blocked cache: Caches rate-limited keys in memory to skip
//     database calls for keys known to be blocked
//   - Prepared statements: Pre-compiles queries for faster execution
//
// # Usage
//
// Basic usage:
//
//	// Create the limiter (automatically creates table if not exists)
//	limiter, err := ratelimit.NewLimiter(pgClient,
//	    ratelimit.WithLogger(logger),
//	    ratelimit.WithTracerProvider(tp),
//	    ratelimit.WithRegisterer(registry),
//	    ratelimit.WithCleanupInterval(5 * time.Minute),
//	)
//	if err != nil {
//	    return err
//	}
//
//	// Start background cleanup (stops when ctx is cancelled)
//	limiter.StartCleanup(ctx)
//
//	// Check rate limit
//	result, err := limiter.Allow(ctx, "user:123", ratelimit.Rate{
//	    Limit:  100,
//	    Window: time.Minute,
//	})
//	if err != nil {
//	    return err
//	}
//
//	if !result.Allowed {
//	    // Return 429 Too Many Requests
//	    w.Header().Set("X-RateLimit-Limit", strconv.Itoa(result.Limit))
//	    w.Header().Set("X-RateLimit-Remaining", "0")
//	    w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt.Unix(), 10))
//	    w.Header().Set("Retry-After", strconv.Itoa(int(time.Until(result.ResetAt).Seconds())))
//	    w.WriteHeader(http.StatusTooManyRequests)
//	    return
//	}
//
//	// Request allowed
//	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(result.Limit))
//	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
//	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(result.ResetAt.Unix(), 10))
//
// # Metrics
//
// The following Prometheus metrics are exposed:
//
//   - ratelimit_requests_total{allowed}: Counter of rate limit checks
//   - ratelimit_check_duration_seconds{allowed}: Histogram of check durations
//   - ratelimit_cache_hits_total: Counter of blocked cache hits (DB calls avoided)
//
// # Tracing
//
// OpenTelemetry spans are created for Allow, AllowN, and Cleanup operations
// with attributes including the rate limit key, limit, window, and result.
package ratelimit

