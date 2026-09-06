package driver

import (
	"github.com/thesyncim/vibedb/query"
	"testing"
)

func TestReplicatedReadReuseRetainsScrubbedResultWithinCacheBudget(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	lease := acquireReplicatedReadReuseData(t, f, &f.cut, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions())
	slot := lease.slot
	if err := queryReplicatedReadReuseData(t, lease, values, f.rows); err != nil {
		t.Fatal(err)
	}
	capacity := slot.reader.conn.exec.Result.ReuseCapacityBytes()
	if capacity == 0 || capacity > replicatedReadReuseResultBytes {
		t.Fatalf("fixture result capacity=%d", capacity)
	}
	if err := lease.Finish(nil); err != nil {
		t.Fatal(err)
	}
	result := &slot.reader.conn.exec.Result
	if result.ReuseCapacityBytes() != capacity || len(result.Columns) != 0 || result.RowCount != 0 {
		t.Fatal("result capacity lost or rows retained")
	}
	charged, ok := retainedReplicatedReadSlotBytes(slot)
	if !ok || charged != slot.retainedByte {
		t.Fatal("idle cache accounting differs")
	}
	retained := *result
	*result = query.Result{}
	without, ok := retainedReplicatedReadSlotBytes(slot)
	*result = retained
	if !ok || charged-without != capacity {
		t.Fatalf("result charge=%d want %d", charged-without, capacity)
	}
	// A fresh cut after a committed update must never see the retained cells.
	fresh, _ := f.updateRow(t, 0)
	defer fresh.Close()
	want := replicatedProjectedFieldRowsWant()[:64]
	want[0].values[2] = "1007"
	next := acquireReplicatedReadReuseData(t, f, fresh, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions())
	if next.slot != slot {
		t.Fatal("updated cut did not reuse result slot")
	}
	if got := next.slot.reader.conn.exec.Result.ReuseCapacityBytes(); got != capacity {
		t.Fatalf("fresh cut discarded retained result arrays: %d, want %d", got, capacity)
	}
	if &next.slot.reader.conn.exec.Result.Columns[:1][0] != &retained.Columns[:1][0] {
		t.Fatal("fresh cut replaced retained result backing")
	}
	if err := queryReplicatedReadReuseData(t, next, values, want); err != nil {
		_ = next.Abort(err)
		t.Fatal(err)
	}
	if err := next.Finish(nil); err != nil {
		t.Fatal(err)
	}
}
