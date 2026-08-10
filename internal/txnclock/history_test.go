package txnclock

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestExternalHistoryOverflowIsCollectionLocalAndRestartable(t *testing.T) {
	var history ExternalHistory
	for i := 1; i <= HistoryKeys+1; i++ {
		history.RecordAt(uint64(i), 0, []string{fmt.Sprintf("k-%05d", i)})
	}
	if history.Floor != HistoryKeys+1 || history.Writes != nil {
		t.Fatalf("overflow floor=%d writes=%d", history.Floor, len(history.Writes))
	}
	if !history.ConflictPoint(0, "untouched") {
		t.Fatal("older exact dependency survived discarded history")
	}
	if history.ConflictPoint(HistoryKeys+1, "untouched") {
		t.Fatal("transaction begun at overflow revision conflicted")
	}
	history.RecordAt(HistoryKeys+2, HistoryKeys+1, []string{"new"})
	if history.Floor != 0 || history.Writes["new"] != HistoryKeys+2 {
		t.Fatalf("restart floor=%d writes=%v", history.Floor, history.Writes)
	}
}

func TestExternalHistoryPrunesBeforeOverflow(t *testing.T) {
	var history ExternalHistory
	for i := 1; i <= HistoryKeys; i++ {
		history.RecordAt(uint64(i), 0, []string{fmt.Sprintf("k-%05d", i)})
	}
	history.RecordAt(HistoryKeys+1, HistoryKeys, []string{"new"})
	if history.Floor != 0 {
		t.Fatalf("obsolete entries caused overflow floor=%d", history.Floor)
	}
	if len(history.Writes) != 1 || history.Writes["new"] != HistoryKeys+1 {
		t.Fatalf("pruned writes=%v", history.Writes)
	}
	if !history.ConflictCollection(HistoryKeys) {
		t.Fatal("coarse dependency missed newest write")
	}
}

func TestExternalHistoryOwnsInsertedKeys(t *testing.T) {
	var history ExternalHistory
	key := []byte("watched")
	borrowed := unsafe.String(unsafe.SliceData(key), len(key))
	history.RecordAt(1, 0, []string{borrowed})
	key[0] = 'X'
	if !history.ConflictPoint(0, "watched") {
		t.Fatal("history retained mutable caller storage")
	}
}

func TestExternalHistoryRecordAtRetainsDuplicateSafety(t *testing.T) {
	var history ExternalHistory
	keys := make([]string, HistoryKeys)
	for i := range keys {
		keys[i] = "same"
	}
	history.RecordAt(1, 0, keys)
	if history.Floor != 0 || len(history.Writes) != 1 || history.Writes["same"] != 1 {
		t.Fatalf("duplicate record floor=%d writes=%v", history.Floor, history.Writes)
	}
}

func TestExternalHistoryRecordUniqueAtBoundsAndOwnsKeys(t *testing.T) {
	var history ExternalHistory
	keys := make([]string, HistoryKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%05d", i)
	}
	borrowedBytes := []byte("k-00000")
	keys[0] = unsafe.String(unsafe.SliceData(borrowedBytes), len(borrowedBytes))
	history.RecordUniqueAt(1, 0, keys)
	if history.Floor != 0 || len(history.Writes) != HistoryKeys {
		t.Fatalf("unique record floor=%d writes=%d", history.Floor, len(history.Writes))
	}
	borrowedBytes[0] = 'X'
	if !history.ConflictPoint(0, "k-00000") {
		t.Fatal("unique history retained caller key slice storage")
	}
	history.RecordUniqueAt(2, 0, []string{"overflow"})
	if history.Floor != 2 || history.Writes != nil {
		t.Fatalf("unique overflow floor=%d writes=%d", history.Floor, len(history.Writes))
	}
}

func BenchmarkExternalHistoryRecordUniqueAt4096(b *testing.B) {
	keys := make([]string, HistoryKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%05d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var history ExternalHistory
		history.RecordUniqueAt(1, 0, keys)
	}
}
