package migration

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains the metrics collected across MVCC migrations.
type Metrics struct {
	// latencyMetric captures successful-migration latency; its observation count
	// doubles as the number of migrated repositories.
	latencyMetric prometheus.Histogram
	// writeBlockLatencyMetric captures how long a successful migration blocked
	// writes to the repository. Clients wait on this part of latencyMetric.
	writeBlockLatencyMetric prometheus.Histogram
	// packedRefsSizeMetric and looseRefsCountMetric describe the migrated
	// repositories, to tell typical repositories from outliers.
	packedRefsSizeMetric prometheus.Histogram
	looseRefsCountMetric prometheus.Histogram
	// failsMetric counts migration failures, labeled by reason.
	failsMetric *prometheus.CounterVec
}

func failMetricReason(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrBlockTimeout):
		return "lock_timeout"
	case errors.Is(err, errHousekeepingRunning):
		return "housekeeping_running"
	case errors.Is(err, errReftableModified):
		return "reftable_modified"
	default:
		return "migration_error"
	}
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() Metrics {
	return Metrics{
		latencyMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "gitaly_mvcc_migration_latency_seconds",
				Help: "Latency of a successful repository migration to the MVCC reference backend. The observation count represents the number of successfully migrated repositories.",
				// The default buckets top out at 10s; a large repository migration
				// runs much longer. 100ms to ~205s covers the range.
				Buckets: prometheus.ExponentialBuckets(0.1, 2, 12),
			},
		),
		writeBlockLatencyMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "gitaly_mvcc_migration_write_blocked_seconds",
				Help: "Time a successful repository migration to the MVCC reference backend blocked writes to the repository.",
				// 1ms to ~33s: the critical section only replays loose refs and
				// swaps the backend.
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
			},
		),
		packedRefsSizeMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "gitaly_mvcc_migration_packed_refs_bytes",
				Help: "Size of the packed-refs file of a repository successfully migrated to the MVCC reference backend.",
				// 1KiB to 4GiB.
				Buckets: prometheus.ExponentialBuckets(1024, 4, 12),
			},
		),
		looseRefsCountMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name: "gitaly_mvcc_migration_loose_refs",
				Help: "Number of loose references replayed while migrating a repository to the MVCC reference backend.",
				// 1 to ~262k.
				Buckets: prometheus.ExponentialBuckets(1, 4, 10),
			},
		),
		failsMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_mvcc_migration_failures_total",
				Help: "Total number of MVCC migration failures.",
			},
			[]string{"reason"},
		),
	}
}

// Describe implements prometheus.Collector.
func (m Metrics) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect implements prometheus.Collector.
func (m Metrics) Collect(metrics chan<- prometheus.Metric) {
	m.latencyMetric.Collect(metrics)
	m.writeBlockLatencyMetric.Collect(metrics)
	m.packedRefsSizeMetric.Collect(metrics)
	m.looseRefsCountMetric.Collect(metrics)
	m.failsMetric.Collect(metrics)
}

func (m Metrics) observeSuccess(latency time.Duration, stats migrationStats) {
	m.latencyMetric.Observe(latency.Seconds())
	m.writeBlockLatencyMetric.Observe(stats.writeBlocked.Seconds())
	m.packedRefsSizeMetric.Observe(float64(stats.packedRefsSize))
	m.looseRefsCountMetric.Observe(float64(stats.numLooseRefs))
}
