// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquet // import "github.com/hrl20/otel-ducklakeexporter/internal/parquet"

// aggregateStats aggregates column statistics across pages.
func aggregateStats(stats []ColumnStats) []ColumnStats {
	// Map to track stats by column name
	statsMap := make(map[string]*ColumnStats)

	for _, stat := range stats {
		if existing, ok := statsMap[stat.ColumnName]; ok {
			// Update min value
			if stat.MinValue < existing.MinValue || existing.MinValue == "" {
				existing.MinValue = stat.MinValue
			}

			// Update max value
			if stat.MaxValue > existing.MaxValue {
				existing.MaxValue = stat.MaxValue
			}

			// Aggregate null count
			existing.NullCount += stat.NullCount
		} else {
			// Create new entry
			statCopy := stat
			statsMap[stat.ColumnName] = &statCopy
		}
	}

	// Convert map to slice
	result := make([]ColumnStats, 0, len(statsMap))
	for _, stat := range statsMap {
		result = append(result, *stat)
	}

	return result
}
