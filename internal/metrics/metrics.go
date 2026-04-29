/*
Copyright 2026 Tobias Hofmaenner.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package metrics holds arete's custom Prometheus metrics. They register
// onto controller-runtime's shared registry at package init, so the
// controller's existing /metrics endpoint exposes them alongside the
// built-in reconcile/queue metrics.
//
// All metrics carry (br, format) labels so PromQL aggregations work
// per-tenant or per-format. Stale series for deleted BRs hang around
// until the next controller restart — acceptable for now since BR
// deletion is rare and Prometheus won't alert on stale metrics if the
// alert query uses `last_over_time`.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ValidationRunsTotal counts completed E2/E3/E4 Jobs by outcome.
	// Labels: br, format, level (e2|e3|e4), result (success|failure).
	ValidationRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arete_validation_runs_total",
			Help: "Total completed E2/E3/E4 validation Jobs by outcome.",
		},
		[]string{"br", "format", "level", "result"},
	)

	// ValidationDurationSeconds tracks Job wall-clock time. Buckets cover
	// E2 (~30s), E3 (~1m), and E4 (1s..30m+). Useful for SLO alerts on
	// Job latency degradation.
	ValidationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "arete_validation_duration_seconds",
			Help:    "Wall-clock duration of E2/E3/E4 validation Jobs.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		},
		[]string{"br", "format", "level"},
	)

	// E4ThroughputBPS mirrors status.lastFullRetrieval.throughputBytesPerSec.
	// Trending this catches network/disk degradation before a real DR.
	E4ThroughputBPS = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arete_e4_throughput_bytes_per_sec",
			Help: "Bytes-per-second from the most recent E4 full retrieval.",
		},
		[]string{"br", "format"},
	)

	// E4BytesTransferred mirrors status.lastFullRetrieval.bytesTransferred.
	E4BytesTransferred = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arete_e4_bytes_transferred",
			Help: "Bytes transferred in the most recent E4 full retrieval.",
		},
		[]string{"br", "format"},
	)

	// BackupAgeSeconds is now() - claimedLastSuccessfulBackup. Updated
	// every reconcile so freshness alerts work reliably.
	BackupAgeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arete_backup_age_seconds",
			Help: "Seconds since the most recent successful backup.",
		},
		[]string{"br", "format"},
	)

	// InventoryObjects mirrors observedInventory.objectCount.
	InventoryObjects = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arete_inventory_objects",
			Help: "Total objects under the BackupRepository's S3 prefix.",
		},
		[]string{"br", "format"},
	)

	// InventoryBytes mirrors observedInventory.totalBytes. Trending this
	// catches runaway growth before it blows the cost budget.
	InventoryBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arete_inventory_bytes",
			Help: "Total bytes across all objects under the BackupRepository's S3 prefix.",
		},
		[]string{"br", "format"},
	)

	// ConditionState encodes each BackupRepository condition as a number:
	//   1  = True (healthy / passing)
	//   0  = False (broken — alert on this)
	//   -1 = Unknown (in flight, never run, or skipped)
	// One time series per (br, format, condition). The full condition
	// taxonomy from the CRD is exposed.
	ConditionState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arete_condition_state",
			Help: "BackupRepository condition state: 1=True, 0=False, -1=Unknown.",
		},
		[]string{"br", "format", "condition"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		ValidationRunsTotal,
		ValidationDurationSeconds,
		E4ThroughputBPS,
		E4BytesTransferred,
		BackupAgeSeconds,
		InventoryObjects,
		InventoryBytes,
		ConditionState,
	)
}
