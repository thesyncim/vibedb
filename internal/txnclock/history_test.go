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
