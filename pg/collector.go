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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	collector struct {
		pool *pgxpool.Pool

		acquireTotal            *prometheus.Desc
		acquireDurationSeconds  *prometheus.Desc
		acquiredConnections     *prometheus.Desc
		canceledAcquireTotal    *prometheus.Desc
		constructingConnections *prometheus.Desc
		emptyAcquireTotal       *prometheus.Desc
		idleConnections         *prometheus.Desc
		maxConnections          *prometheus.Desc
		totalConnections        *prometheus.Desc
		newConnectionsTotal     *prometheus.Desc
		maxLifetimeDestroyTotal *prometheus.Desc
		maxIdleDestroyTotal     *prometheus.Desc
	}

	statWrapper struct {
		stats *pgxpool.Stat
	}
)

func newCollector(pool *pgxpool.Pool, labels map[string]string) *collector {
	return &collector{
		pool: pool,

		acquireTotal: prometheus.NewDesc(
			"pgxpool_acquire_total",
			"Cumulative count of successful acquires from the pool.",
			nil,
			labels,
		),
		acquireDurationSeconds: prometheus.NewDesc(
			"pgxpool_acquire_duration_seconds",
			"Total duration of all successful acquires from the pool in seconds.",
			nil,
			labels,
		),
		acquiredConnections: prometheus.NewDesc(
			"pgxpool_acquired_connections",
			"Number of currently acquired connections in the pool.",
			nil,
			labels,
		),
		canceledAcquireTotal: prometheus.NewDesc(
			"pgxpool_canceled_acquire_total",
			"Cumulative count of acquires from the pool that were canceled by a context.",
			nil,
			labels,
		),
		constructingConnections: prometheus.NewDesc(
			"pgxpool_constructing_connections",
			"Number of conns with construction in progress in the pool.",
			nil,
			labels,
		),
		emptyAcquireTotal: prometheus.NewDesc(
			"pgxpool_empty_acquire_total",
			"Cumulative count of successful acquires from the pool that waited for a resource to be released or constructed because the pool was empty.",
			nil,
			labels,
		),
		idleConnections: prometheus.NewDesc(
			"pgxpool_idle_connections",
			"Number of currently idle conns in the pool.",
			nil,
			labels,
		),
		maxConnections: prometheus.NewDesc(
			"pgxpool_max_connections",
			"Maximum size of the pool.",
			nil,
			labels,
		),
		totalConnections: prometheus.NewDesc(
			"pgxpool_total_connections",
			"Total number of resources currently in the pool. The value is the sum of ConstructingConns, AcquiredConns, and IdleConns.",
			nil,
			labels,
		),
		newConnectionsTotal: prometheus.NewDesc(
			"pgxpool_new_connections_total",
			"Cumulative count of new connections opened.",
			nil,
			labels,
		),
		maxLifetimeDestroyTotal: prometheus.NewDesc(
			"pgxpool_max_lifetime_destroy_total",
			"Cumulative count of connections destroyed because they exceeded MaxConnLifetime. ",
			nil,
			labels,
		),
		maxIdleDestroyTotal: prometheus.NewDesc(
			"pgxpool_max_idle_destroy_total",
			"Cumulative count of connections destroyed because they exceeded MaxConnIdleTime.",
			nil,
			labels,
		),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *collector) Collect(metrics chan<- prometheus.Metric) {
	stats := &statWrapper{c.pool.Stat()}

	metrics <- prometheus.MustNewConstMetric(
		c.acquireTotal,
		prometheus.CounterValue,
		stats.acquireCount(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.acquireDurationSeconds,
		prometheus.CounterValue,
		stats.acquireDuration(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.acquiredConnections,
		prometheus.GaugeValue,
		stats.acquiredConns(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.canceledAcquireTotal,
		prometheus.CounterValue,
		stats.canceledAcquireCount(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.constructingConnections,
		prometheus.GaugeValue,
		stats.constructingConns(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.emptyAcquireTotal,
		prometheus.CounterValue,
		stats.emptyAcquireCount(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.idleConnections,
		prometheus.GaugeValue,
		stats.idleConns(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.maxConnections,
		prometheus.GaugeValue,
		stats.maxConns(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.totalConnections,
		prometheus.GaugeValue,
		stats.totalConns(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.newConnectionsTotal,
		prometheus.CounterValue,
		stats.newConnsCount(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.maxLifetimeDestroyTotal,
		prometheus.CounterValue,
		stats.maxLifetimeDestroyCount(),
	)
	metrics <- prometheus.MustNewConstMetric(
		c.maxIdleDestroyTotal,
		prometheus.CounterValue,
		stats.maxIdleDestroyCount(),
	)
}

func (w *statWrapper) acquireCount() float64 {
	return float64(w.stats.AcquireCount())
}
func (w *statWrapper) acquireDuration() float64 {
	return float64(w.stats.AcquireDuration().Seconds())
}
func (w *statWrapper) acquiredConns() float64 {
	return float64(w.stats.AcquiredConns())
}
func (w *statWrapper) canceledAcquireCount() float64 {
	return float64(w.stats.CanceledAcquireCount())
}
func (w *statWrapper) constructingConns() float64 {
	return float64(w.stats.ConstructingConns())
}
func (w *statWrapper) emptyAcquireCount() float64 {
	return float64(w.stats.EmptyAcquireCount())
}
func (w *statWrapper) idleConns() float64 {
	return float64(w.stats.IdleConns())
}
func (w *statWrapper) maxConns() float64 {
	return float64(w.stats.MaxConns())
}
func (w *statWrapper) totalConns() float64 {
	return float64(w.stats.TotalConns())
}
func (w *statWrapper) newConnsCount() float64 {
	return float64(w.stats.NewConnsCount())
}
func (w *statWrapper) maxLifetimeDestroyCount() float64 {
	return float64(w.stats.MaxLifetimeDestroyCount())
}
func (w *statWrapper) maxIdleDestroyCount() float64 {
	return float64(w.stats.MaxIdleDestroyCount())
}
