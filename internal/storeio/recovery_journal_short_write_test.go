package storeio

import (
	"errors"
	"io"
	"testing"
)

func TestRecoveryJournalRejectsShortNilWrites(t *testing.T) {
	entries := []RecoveryBatchEntry{{
		Kind:  RecoveryRecordKindPut,
		Key:   []byte("batch-key"),
		Value: []byte(`{"batch":true}`),
	}}
	tests := []struct {
		name  string
		write func(*testing.T, *RecoveryJournal) error
	}{
		{
			name: "header",
			write: func(_ *testing.T, rj *RecoveryJournal) error {
				return rj.writeHeader(rj.headerSlot^1, rj.header)
			},
		},
		{
			name: "record",
			write: func(_ *testing.T, rj *RecoveryJournal) error {
				_, err := rj.Append(
					RecoveryRecordKindPut, 2, []byte("key"), []byte(`{"v":1}`),
				)
				return err
			},
		},
		{
			name: "prepared-batch",
			write: func(t *testing.T, rj *RecoveryJournal) error {
				plan, err := rj.PrepareBatch(entries)
				if err != nil {
					t.Fatal(err)
				}
				_, err = rj.AppendPreparedBatch(2, entries, plan)
				return err
			},
		},
		{
			name: "prepared-delta-batch",
			write: func(t *testing.T, rj *RecoveryJournal) error {
				plan, err := rj.PrepareDeltaBatch(entries)
				if err != nil {
					t.Fatal(err)
				}
				_, err = rj.AppendPreparedDeltaBatch(2, entries, plan)
				return err
			},
		},
		{
			name: "prepared-conditional-batch",
			write: func(t *testing.T, rj *RecoveryJournal) error {
				plan, err := rj.PrepareConditionalBatch(entries)
				if err != nil {
					t.Fatal(err)
				}
				markerID := [16]byte{1}
				_, err = rj.AppendPreparedConditionalBatch(
					2, markerID, 1, 1, entries, plan,
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rj, _ := createTestJournal(t, 8<<10)
			defer rj.Close()
			beforeCursor, beforeSequence := rj.Cursor(), rj.NextSequence()
			rj.writeAt = func(p []byte, _ int64) (int, error) {
				return len(p) - 1, nil
			}
			if err := test.write(t, rj); !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("write = %v, want io.ErrShortWrite", err)
			}
			if rj.Cursor() != beforeCursor || rj.NextSequence() != beforeSequence {
				t.Fatalf(
					"short write advanced journal: cursor %d -> %d, sequence %d -> %d",
					beforeCursor, rj.Cursor(), beforeSequence, rj.NextSequence(),
				)
			}
		})
	}
}
