package store

import (
	"context"

	"github.com/thesyncim/vibedb/planner"
	vibejson "github.com/thesyncim/vibejson"
)

// Analyze collects bounded optimizer statistics from this immutable snapshot.
// catalogGeneration identifies the distributed topology publication, while
// partition identifies this disjoint source shard. MergePartitionStatistics
// combines shard results without summing overlapping distinct values.
func (s Snapshot) Analyze(ctx context.Context, catalogGeneration uint64, table, partition string, paths []string, groups [][]string, options planner.AnalyzeOptions) (*planner.PartitionAnalysis, error) {
	documents := func(yield func([]byte, error) bool) {
		s.Range(func(_ string, value vibejson.RawValue) bool { return yield(value.Bytes(), nil) })
	}
	return planner.AnalyzeDocuments(ctx, catalogGeneration, table, partition, paths, groups, documents, options)
}
