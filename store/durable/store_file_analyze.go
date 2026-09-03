package durable

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/planner"
)

// Analyze scans this pinned durable snapshot with bounded statistics state.
// The caller owns the snapshot lifetime and the surrounding maintenance budget.
func (s *Snapshot) Analyze(ctx context.Context, catalogGeneration uint64, table, partition string, paths []string, groups [][]string, options planner.AnalyzeOptions) (*planner.PartitionAnalysis, error) {
	documents := func(yield func([]byte, error) bool) {
		stopped := errors.New("statistics consumer stopped")
		err := s.RangeRaw(func(_, value []byte) error {
			if !yield(value, nil) {
				return stopped
			}
			return nil
		})
		if err != nil && !errors.Is(err, stopped) {
			yield(nil, err)
		}
	}
	return planner.AnalyzeDocuments(ctx, catalogGeneration, table, partition, paths, groups, documents, options)
}
