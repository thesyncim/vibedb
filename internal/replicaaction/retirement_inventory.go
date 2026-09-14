package replicaaction

import (
	"bytes"
	"context"
	"slices"
)

// SourceRetirements returns the bounded durable source tombstones. Running
// requests have not yet proved removal and cannot exclude a replica at restart.
// An authorized retirement remains a tombstone even if the process stopped
// before closing the runtime or publishing Complete.
func (journal *FileJournal) SourceRetirements(ctx context.Context) ([]Record, error) {
	if journal == nil || ctx == nil {
		return nil, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil, ErrControl
	}
	if err := journal.settlePendingLocked(); err != nil {
		return nil, err
	}
	var records []Record
	for _, record := range journal.records {
		if !validRecord(record) {
			return nil, ErrControl
		}
		if record.Request.Kind != SourceRetirement ||
			(record.State != RetirementAuthorized && record.State != Complete) {
			continue
		}
		record.Request = cloneRequest(record.Request)
		records = append(records, record)
	}
	slices.SortFunc(records, func(a, b Record) int {
		return bytes.Compare(a.Request.Operation[:], b.Request.Operation[:])
	})
	return records, nil
}
