package durable

import (
	"fmt"
	"os"
)

// Offline repack (vacuum-into) is the second half of the allocator/locality
// answer. Tablet-affine placement bounds how far steady-state churn scatters a
// live store; repack is the explicit, offline reset that restores a store to the
// compact geometry a monotonic batched load produces and no reclaimable free
// space — after a run of churn has spent it.
//
// It is deliberately not incremental. Reading a quiescent store in order and
// rewriting bounded batches through the ordinary mutation engine reuses the
// exact schema/index/overflow/opaque semantics exercised by serving writes.

// RepackReport summarises one offline repack.
type RepackReport struct {
	Documents         int
	SourceFileEnd     int64
	OutputFileEnd     int64
	OutputDeviceBytes uint64
}

// Repack rewrites a quiescent store into a freshly clustered one. It opens src
// through the ordinary read path, streams every live (key, value) in bytewise
// lexical order via the snapshot scan, and writes them to out through bounded
// native byte batches. The result holds no source-store reclaimable space and
// preserves every configured storage capability.
//
// src must be a quiescent, cleanly closed store: Repack opens it and takes a
// snapshot, so a concurrent writer would race the scan. out must be empty.
func Repack(src, out *os.File, options Options) (RepackReport, error) {
	var report RepackReport
	if src == nil || out == nil {
		return report, fmt.Errorf(
			"vibedb: repack requires non-nil source and output files",
		)
	}
	source, err := Open(src, options)
	if err != nil {
		return report, err
	}
	defer source.Close()
	if state := source.state.Load(); state != nil {
		report.SourceFileEnd = int64(state.fileEnd)
	}

	snapshot, err := source.Snapshot()
	if err != nil {
		return report, err
	}
	defer snapshot.Close()
	normalized, err := options.normalized()
	if err != nil {
		return report, err
	}

	// Inline JSON stores retain the pristine bulk-build geometry. Determine
	// overflow eligibility in a read-only pass, then collect byte-native rows;
	// no key crosses a string-shaped API. Schema and opaque stores use the
	// bounded serving-write path below because their bulk cutover is deliberately
	// unsupported today.
	bulkEligible := options.Collection.Schema == nil && !normalized.OpaqueValues
	bulkDocuments := 0
	if bulkEligible {
		if err := snapshot.RangeRaw(func(_, value []byte) error {
			bulkDocuments++
			if len(value) > normalized.InlineValueBytes {
				bulkEligible = false
			}
			return nil
		}); err != nil {
			return report, err
		}
	}
	if bulkEligible {
		if bulkDocuments == 0 {
			empty, createErr := Create(out, options)
			if createErr != nil {
				return report, createErr
			}
			if state := empty.state.Load(); state != nil {
				report.OutputFileEnd = int64(state.fileEnd)
			}
			return report, empty.Close()
		}
		records := make([]PrimaryBulkBytesRecord, 0, bulkDocuments)
		arena := make([]byte, 0)
		if err := snapshot.RangeRaw(func(key, value []byte) error {
			keyAt := len(arena)
			arena = append(arena, key...)
			keyCopy := arena[keyAt:len(arena):len(arena)]
			valueAt := len(arena)
			arena = append(arena, value...)
			valueCopy := arena[valueAt:len(arena):len(arena)]
			records = append(records, PrimaryBulkBytesRecord{
				Key: keyCopy, Value: valueCopy,
			})
			return nil
		}); err != nil {
			return report, err
		}
		fileEnd, err := CreateFromByteRecords(records, out, options)
		if err != nil {
			return report, err
		}
		report.Documents = len(records)
		report.OutputFileEnd = fileEnd
		return report, nil
	}

	buildOptions := options
	buildOptions.Durability = DurabilityBufferedVisible
	normalized, err = buildOptions.normalized()
	if err != nil {
		return report, err
	}
	target, err := Create(out, buildOptions)
	if err != nil {
		return report, err
	}
	targetOpen := true
	defer func() {
		if targetOpen {
			_ = target.Close()
		}
	}()

	// Keep both record count and retained bytes bounded. Offsets point into one
	// arena, avoiding a string conversion and one heap object per key/value.
	repackBatchRecords := min(256, normalized.MaxBatchDocuments)
	const repackBatchBytes = 4 << 20
	type record struct{ keyAt, keyEnd, valueAt, valueEnd int }
	records := make([]record, 0, repackBatchRecords)
	arena := make([]byte, 0, min(
		repackBatchBytes, normalized.MaxDocumentBytes+normalized.MaxKeyBytes,
	))
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		if err := target.Update(func(batch *WriteBatch) error {
			for _, row := range records {
				if err := batch.Put(
					arena[row.keyAt:row.keyEnd],
					arena[row.valueAt:row.valueEnd],
				); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		records = records[:0]
		arena = arena[:0]
		return nil
	}
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		if len(records) != 0 && (len(records) == cap(records) ||
			len(arena)+len(key)+len(value) > repackBatchBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		keyAt := len(arena)
		arena = append(arena, key...)
		keyEnd := len(arena)
		valueAt := len(arena)
		arena = append(arena, value...)
		records = append(records, record{
			keyAt: keyAt, keyEnd: keyEnd,
			valueAt: valueAt, valueEnd: len(arena),
		})
		report.Documents++
		return nil
	}); err != nil {
		return report, err
	}
	if err := flush(); err != nil {
		return report, err
	}
	if err := target.Flush(); err != nil {
		return report, err
	}
	report.OutputDeviceBytes = target.Stats().DeviceBytes
	if state := target.state.Load(); state != nil {
		report.OutputFileEnd = int64(state.fileEnd)
	}
	if err := target.Close(); err != nil {
		return report, err
	}
	targetOpen = false
	return report, nil
}
