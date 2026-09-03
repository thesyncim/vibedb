package planner

import (
	"context"
	"iter"

	vibejson "github.com/thesyncim/vibejson"
)

// AnalyzeDocuments is the storage adapter for AnalyzePartition. Documents must
// come from one pinned snapshot. Only requested scalar paths are retained;
// absent paths and explicit JSON null share SQL NULL statistics. Non-scalar
// values are refused rather than silently counted as NULL.
func AnalyzeDocuments(ctx context.Context, generation uint64, table, partition string, paths []string, groups [][]string, documents iter.Seq2[[]byte, error], options AnalyzeOptions) (*PartitionAnalysis, error) {
	if documents == nil || len(paths) == 0 || len(paths) > 256 {
		return nil, statisticsError(table, "nil document source")
	}
	pointers := make([]vibejson.CompiledPointer, len(paths))
	for i, path := range paths {
		pointer, err := vibejson.CompilePointer(path)
		if err != nil {
			return nil, err
		}
		pointers[i] = pointer
	}
	rows := func(yield func(StatisticsRow, error) bool) {
		values := make([]string, len(paths))
		for document, err := range documents {
			if err != nil {
				yield(StatisticsRow{}, err)
				return
			}
			if !vibejson.Valid(document) {
				yield(StatisticsRow{}, statisticsError(table, "invalid source document"))
				return
			}
			for i, pointer := range pointers {
				value, exists, err := pointer.ScanFirstRawTrusted(document)
				if err != nil {
					yield(StatisticsRow{}, err)
					return
				}
				values[i] = "null"
				if exists {
					values[i] = string(value.Bytes())
				}
			}
			if !yield(StatisticsRow{Values: values, Bytes: uint64(len(document))}, nil) {
				return
			}
		}
	}
	return AnalyzePartition(ctx, generation, table, partition, paths, groups, rows, options)
}
