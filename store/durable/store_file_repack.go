package durable

import (
	"fmt"
	"os"

	"github.com/thesyncim/vibedb/store"
)

// Offline repack (vacuum-into) is the second half of the allocator/locality
// answer. Tablet-affine placement bounds how far steady-state churn scatters a
// live store; repack is the explicit, offline reset that restores a store to the
// pristine geometry a bulk build produces — perfect 1.00x lexical-adjacency
// locality and no reclaimable free space — after a run of churn has spent it.
//
// It is deliberately not incremental. A bulk build lays every leaf down in
// lexical order with no copy-on-write and no retired extents, so reading a
// quiescent store in order and rewriting it through that same path is both the
// simplest correct compaction and the one that reuses the most tested code.

// RepackReport summarises one offline repack.
type RepackReport struct {
	Documents     int
	SourceFileEnd int64
	OutputFileEnd int64
}

// Repack rewrites a quiescent store into a freshly clustered one. It opens src
// through the ordinary read path, streams every live (key, value) in bytewise
// lexical order via the snapshot scan, and writes them to out through the bulk
// primary path (CreateFromPrimary). The result has perfect 1.00x leaf adjacency
// and holds no reclaimable free space, because a bulk build places every leaf in
// order and retires nothing.
//
// src must be a quiescent, cleanly closed store: Repack opens it and takes a
// snapshot, so a concurrent writer would race the scan. out must be empty.
// Repack is inline-primary only, matching CreateFromPrimary; a store carrying
// indexes, float64 columns, schemas, compact document groups, or overflow
// values is rejected there.
func Repack(src, out *os.File, options Options) (RepackReport, error) {
	var report RepackReport
	if src == nil || out == nil {
		return report, fmt.Errorf(
			"vibejson: repack requires non-nil source and output files",
		)
	}
	source, err := Open(src, options)
	if err != nil {
		return report, err
	}
	defer source.Close()
	if state := source.state.Load(); state != nil {
		report.SourceFileEnd = int64(state.super.FileEnd)
	}

	snapshot, err := source.Snapshot()
	if err != nil {
		return report, err
	}
	defer snapshot.Close()

	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		return report, err
	}
	// RangeRaw visits the ordered primary graph in bytewise lexical order and
	// borrows key/value only for the callback. Builder.Append copies both, so
	// streaming the borrowed buffers straight in is safe and copy-free here.
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		if appendErr := builder.Append(string(key), value); appendErr != nil {
			return appendErr
		}
		report.Documents++
		return nil
	}); err != nil {
		return report, err
	}
	built, err := builder.Build()
	if err != nil {
		return report, err
	}
	fileEnd, err := CreateFromPrimary(built, out, options)
	if err != nil {
		return report, err
	}
	report.OutputFileEnd = fileEnd
	return report, nil
}
