package gateway

import queryplanner "github.com/thesyncim/vibedb/planner"

// Re-export the cold statistics vocabulary at the gateway catalog boundary so
// topology publishers do not need to translate identical records.
type GroupStatistics = queryplanner.GroupStatistics
type TupleFrequency = queryplanner.TupleFrequency
type TableStatistics = queryplanner.TableStatistics
type PartitionStatistics = queryplanner.PartitionStatistics
type ColumnStatistics = queryplanner.ColumnStatistics
type ValueFrequency = queryplanner.ValueFrequency
type HistogramBucket = queryplanner.HistogramBucket
type Estimate = queryplanner.Estimate
type TableStatistic = queryplanner.TableStatistic

// Statistics returns the immutable statistics view for table in this exact
// routing generation. Missing statistics are ordinary and report false.
func (s *Snapshot) Statistics(table string) (TableStatistic, bool) {
	if s == nil || s.statistics == nil {
		return TableStatistic{}, false
	}
	return s.statistics.Table(table)
}

// PlannerStatisticsBytes reports the retained compact statistics directories,
// sparse skew/histogram runs, and interned string arena.
func (s *Snapshot) PlannerStatisticsBytes() uint64 {
	if s == nil || s.statistics == nil {
		return 0
	}
	return s.statistics.RetainedBytes()
}
