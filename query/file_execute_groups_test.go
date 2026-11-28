package query

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestFileIntegerGroupsUsesTypedLaneWithReusableCancel(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot, heap := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			value := filePackedCountNumber(row)
			return fmt.Appendf(nil, `{"n":%d}`, value)
		},
	)
	query := Select(Path("n"), Count(), Sum("n")).GroupBy("n")

	var oracle Exec
	oracle.Options.Workers = 1
	if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
		t.Fatalf("generic oracle: %v", err)
	}

	var cancel CancelFlag
	execution := Exec{Options: ExecOptions{Workers: 1, Cancel: &cancel}}
	defer execution.Release()
	if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatalf("typed durable execution: %v", err)
	}
	if execution.Stats.Workers != 1 || execution.Stats.RowsScanned != rows ||
		execution.Stats.GroupedIntegerRows != rows || execution.Stats.Batches != 0 {
		t.Fatalf("typed stats=%+v, want one full grouped scan", execution.Stats)
	}
	if execution.Result.RowCount != oracle.Result.RowCount {
		t.Fatalf("row count=%d, want oracle %d", execution.Result.RowCount, oracle.Result.RowCount)
	}
	for col := range execution.Result.Columns {
		got, want := execution.Result.Columns[col], oracle.Result.Columns[col]
		if len(got.Cells) != len(want.Cells) {
			t.Fatalf("column %d cells=%d, want %d", col, len(got.Cells), len(want.Cells))
		}
		for row := range got.Cells {
			if string(got.Cells[row].JSON()) != string(want.Cells[row].JSON()) {
				t.Fatalf("column %d row %d=%s, want %s", col, row,
					got.Cells[row].JSON(), want.Cells[row].JSON())
			}
		}
	}

	// A canceled scan must not poison the retained open-address table or the
	// compact shape views. Resetting the same ordinary nonnil flag then reruns
	// the exact snapshot through the same direct lane.
	cancel.Cancel()
	if err := query.RunInto(&execution, FromFile(snapshot)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled grouped execution=%v, want ErrCanceled", err)
	}
	if execution.Result.RowCount != 0 || execution.Stats.GroupedIntegerRows != 0 {
		t.Fatalf("canceled result/stats=%+v/%+v, want cleared", execution.Result, execution.Stats)
	}
	cancel.Reset()
	if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatalf("grouped execution after reset: %v", err)
	}
	if execution.Stats.GroupedIntegerRows != rows {
		t.Fatalf("post-reset stats=%+v, want grouped rows=%d", execution.Stats, rows)
	}
}

func integerGroupWideValue(row int) int64 {
	x := uint64(row) + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	const mask = uint64(1<<54 - 1)
	return int64(x&mask) - int64(1<<53)
}

func TestFileIntegerGroupsPreservesSignedKeysShapeOrderAndLimit(t *testing.T) {
	const rows = 2 * (4 << 10)
	snapshot, heap := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			// Two physical shapes interleave the same random signed keys. Each
			// key occurs twice, which makes first-seen order observable while
			// keeping the target streams exact integer FOR data.
			key := integerGroupWideValue(row % (rows / 2))
			if row&1 == 0 {
				return fmt.Appendf(nil, `{"n":%d}`, key)
			}
			return fmt.Appendf(nil, `{"n":%d,"shape":1}`, key)
		},
	)

	cases := []struct {
		name  string
		query *Query
	}{
		{
			name:  "first-seen",
			query: Select(Path("n"), Count(), Sum("n")).GroupBy("n"),
		},
		{
			name: "reordered-desc-limit",
			query: Select(Sum("n"), Count(), Path("n")).
				GroupBy("n").OrderBy("n", Desc).Limit(17),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var oracle Exec
			oracle.Options.Workers = 1
			if err := tc.query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
				t.Fatalf("generic oracle: %v", err)
			}
			var execution Exec
			execution.Options.Workers = 1
			if err := tc.query.RunInto(&execution, FromFile(snapshot)); err != nil {
				t.Fatalf("typed durable execution: %v", err)
			}
			if execution.Stats.GroupedIntegerRows != rows ||
				execution.Stats.RowsScanned != rows {
				t.Fatalf("typed stats=%+v, want grouped scan of %d rows", execution.Stats, rows)
			}
			if execution.Result.RowCount != oracle.Result.RowCount {
				t.Fatalf("rows=%d, want oracle %d", execution.Result.RowCount, oracle.Result.RowCount)
			}
			for col := range execution.Result.Columns {
				got, want := execution.Result.Columns[col], oracle.Result.Columns[col]
				for row := range got.Cells {
					if string(got.Cells[row].JSON()) != string(want.Cells[row].JSON()) {
						t.Fatalf("column %d row %d=%s, want %s", col, row,
							got.Cells[row].JSON(), want.Cells[row].JSON())
					}
				}
			}
			execution.Release()
			oracle.Release()
		})
	}
}

func TestFileIntegerGroupsRF3ShapeProbe(t *testing.T) {
	const rows = 1024
	db, err := durable.OpenDatabase(t.TempDir(), durable.DatabaseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collection, err := db.CreateCollection(
		"rf3_sql_group", durable.Options{Collection: store.Options{ChunkDocuments: 64}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < rows; row++ {
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("row-%08d", row)),
			fmt.Appendf(nil, `{"bucket":%d,"score":%d}`, row%16, row%100),
		); err != nil {
			t.Fatal(err)
		}
	}
	statement, err := PrepareStatement(
		`SELECT bucket, COUNT(*), SUM(score) FROM rf3_sql_group GROUP BY bucket ORDER BY bucket`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	catalog, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	var cancel CancelFlag
	execution := Exec{Options: ExecOptions{Workers: 1, Cancel: &cancel}}
	defer execution.Release()
	cursor, err := statement.RunInto(
		&execution, FromFileDatabase(catalog, statement.Collection()), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Stats.GroupedIntegerRows != rows ||
		execution.Stats.RowsScanned != rows || execution.Stats.Batches != 0 {
		t.Fatalf("RF3 shape stats=%+v, want direct grouped scan", execution.Stats)
	}
	for bucket := 0; bucket < 16; bucket++ {
		if !cursor.Next() {
			t.Fatalf("cursor ended at bucket %d", bucket)
		}
		gotBucket, bucketOK := cursor.Cell(0).Int64()
		gotCount, countOK := cursor.Cell(1).Int64()
		gotSum, gotSumOK := cursor.Cell(2).Int64()
		if !bucketOK || !countOK || gotCount != rows/16 || !gotSumOK {
			t.Fatalf("bucket row %d = (%d,%d,%d,%t)", bucket,
				gotBucket, gotCount, gotSum, gotSumOK)
		}
		wantSum := int64(0)
		for row := bucket; row < rows; row += 16 {
			wantSum += int64(row % 100)
		}
		if gotBucket != int64(bucket) || gotSum != wantSum {
			t.Fatalf("bucket %d=(%d,%d), want (%d,%d)", bucket,
				gotBucket, gotSum, bucket, wantSum)
		}
	}
	if cursor.Next() {
		t.Fatal("cursor returned more than 16 groups")
	}
}

func TestFileIntegerGroupsFallsBackOnSumOverflow(t *testing.T) {
	snapshot, heap := filePackedIntegerExtremaSnapshot(t, 2,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			if row == 0 {
				return []byte(`{"group":1,"value":9223372036854775807}`)
			}
			return []byte(`{"group":1,"value":1}`)
		},
	)
	query := Select(Path("group"), Count(), Sum("value")).GroupBy("group")
	var oracle Exec
	if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
		t.Fatal(err)
	}
	var execution Exec
	execution.Options.Workers = 1
	if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.GroupedIntegerRows != 0 || execution.Stats.RowsScanned != 2 {
		t.Fatalf("overflow fallback stats=%+v, want generic full scan", execution.Stats)
	}
	if execution.Result.RowCount != oracle.Result.RowCount {
		t.Fatalf("rows=%d, want oracle %d", execution.Result.RowCount, oracle.Result.RowCount)
	}
	for col := range execution.Result.Columns {
		if string(execution.Result.Columns[col].Cells[0].JSON()) !=
			string(oracle.Result.Columns[col].Cells[0].JSON()) {
			t.Fatalf("column %d=%s, want %s", col,
				execution.Result.Columns[col].Cells[0].JSON(),
				oracle.Result.Columns[col].Cells[0].JSON())
		}
	}
}

func TestFileIntegerGroupsDeclinedScratchAndAggregateBudgets(t *testing.T) {
	const rows = filePackedCountTestRows
	snapshot, heap := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			return fmt.Appendf(nil, `{"n":%d}`, filePackedCountNumber(row))
		},
	)
	query := Select(Path("n"), Count(), Sum("n")).GroupBy("n")
	var oracle Exec
	if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
		t.Fatal(err)
	}

	t.Run("memory-fallback", func(t *testing.T) {
		var execution Exec
		execution.Options = ExecOptions{
			Workers: 1, MemoryBytes: 64 << 10, SpillDirectory: t.TempDir(),
			SpillBytes: -1,
		}
		if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if execution.Stats.GroupedIntegerRows != 0 ||
			execution.Stats.RowsScanned != rows {
			t.Fatalf("memory fallback stats=%+v", execution.Stats)
		}
		if execution.Result.RowCount != oracle.Result.RowCount {
			t.Fatalf("rows=%d, want oracle %d", execution.Result.RowCount, oracle.Result.RowCount)
		}
	})

	t.Run("aggregate-budget", func(t *testing.T) {
		var execution Exec
		execution.Options = ExecOptions{Workers: 1, AggregateBytes: aggregateAccBaseBytes}
		err := query.RunInto(&execution, FromFile(snapshot))
		var budgetErr *AggregateBudgetError
		if !errors.As(err, &budgetErr) {
			t.Fatalf("aggregate error=%v, want AggregateBudgetError", err)
		}
	})
}

func TestFileIntegerGroupsDeclinesLateNonIntegerShape(t *testing.T) {
	const rows = 1024
	snapshot, heap := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			if row == rows-1 {
				return []byte(`{"n":"late"}`)
			}
			return fmt.Appendf(nil, `{"n":%d}`, filePackedCountNumber(row))
		},
	)
	query := Select(Path("n"), Count()).GroupBy("n")
	var oracle, execution Exec
	oracle.Options.Workers, execution.Options.Workers = 1, 1
	if err := query.RunInto(&oracle, FromSnapshot(heap)); err != nil {
		t.Fatal(err)
	}
	if err := query.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.GroupedIntegerRows != 0 || execution.Stats.RowsScanned != rows {
		t.Fatalf("late noninteger stats=%+v, want generic full scan", execution.Stats)
	}
	if execution.Result.RowCount != oracle.Result.RowCount {
		t.Fatalf("rows=%d, want oracle %d", execution.Result.RowCount, oracle.Result.RowCount)
	}
}

func TestFileIntegerGroupsResultBudgetAndReuse(t *testing.T) {
	const rows = 128
	snapshot, _ := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte { return fmt.Appendf(nil, `{"n":%d}`, row%16) },
	)
	q := Select(Path("n"), Count(), Sum("n")).GroupBy("n")
	execution := Exec{Options: ExecOptions{Workers: 1, ResultRows: 1}}
	defer execution.Release()
	if err := q.RunInto(&execution, FromFile(snapshot)); !errors.Is(err, ErrResultBudget) {
		t.Fatalf("result rows error=%v", err)
	}
	if execution.Result.RowCount != 0 {
		t.Fatal("failed result retained rows")
	}
	execution.Options.ResultRows = 0
	execution.Options.ResultBytes = 1
	if err := q.RunInto(&execution, FromFile(snapshot)); !errors.Is(err, ErrResultBudget) {
		t.Fatalf("result bytes error=%v", err)
	}
	execution.Options.ResultBytes = 0
	if err := q.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if execution.Result.RowCount != 16 || execution.Stats.GroupedIntegerRows != rows {
		t.Fatalf("reused result/stats=%+v/%+v", execution.Result, execution.Stats)
	}
}

func TestIntegerGroupWorkspaceCapacityAccountingAndReset(t *testing.T) {
	var workspace integerGroupWorkspace
	defer workspace.release()
	// The exact initial reservation must fit eleven entries and the sixteenth
	// hash slot. This catches fixed-width accounting that understates fileGroup.
	shapeBytes := storeio.IntegerGroupScratchBytes(storeio.CompactPrimaryStripeMaxRows)
	bound := shapeBytes + 16*int64(unsafe.Sizeof(integerGroupSlot{})) +
		11*(int64(unsafe.Sizeof(integerGroupValue{}))+int64(unsafe.Sizeof(fileGroup{}))+
			int64(unsafe.Sizeof(scalar{}))+3*int64(unsafe.Sizeof(aggAcc{})))
	if !workspace.prepare(3, bound) {
		t.Fatal("exact capacity rejected")
	}
	for key := int64(0); key < 11; key++ {
		if err := workspace.add(key, 7, uint64(key), true, bound); err != nil {
			t.Fatal(err)
		}
	}
	if err := workspace.add(11, 7, 11, true, bound); !errors.Is(err, errIntegerGroupsDeclined) {
		t.Fatalf("over-capacity add=%v", err)
	}
	if workspace.prepare(3, bound-1) {
		t.Fatal("undercharged capacity admitted")
	}
	workspace.release()
	if !workspace.prepare(3, bound) {
		t.Fatal("reset capacity rejected")
	}
	if err := workspace.add(0, 2, 0, true, bound); err != nil {
		t.Fatal(err)
	}
	if workspace.groups[0].count != 1 || workspace.groups[0].sum != 2 {
		t.Fatal("old aggregate survived reset")
	}
}

func TestIntegerGroupWorkspaceGenerationWraparoundAndWarmReuse(t *testing.T) {
	var workspace integerGroupWorkspace
	defer workspace.release()
	const bound = 4 << 20
	if !workspace.prepare(2, bound) {
		t.Fatal("prepare failed")
	}
	// Force slotGen to wrap around on the next reset.
	workspace.slotGen = ^uint32(0)
	if err := workspace.add(42, 10, 0, true, bound); err != nil {
		t.Fatal(err)
	}
	if !workspace.prepare(2, bound) {
		t.Fatal("prepare across wraparound failed")
	}
	if workspace.slotGen != 1 {
		t.Fatalf("slotGen=%d after wraparound, want 1", workspace.slotGen)
	}
	if err := workspace.add(42, 5, 1, true, bound); err != nil {
		t.Fatal(err)
	}
	if len(workspace.groups) != 1 || workspace.groups[0].count != 1 || workspace.groups[0].sum != 5 {
		t.Fatalf("post-wraparound group=%+v, want count=1 sum=5", workspace.groups)
	}
}

func TestFileIntegerGroupsStringKeySkipsSpeculativeWorkspaceAllocation(t *testing.T) {
	const rows = 256
	snapshot, _ := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			return fmt.Appendf(nil, `{"k":"grp-%02d","v":%d}`, row%8, row)
		},
	)
	q := Select(Path("k"), Count()).GroupBy("k")
	var execution Exec
	execution.Options.Workers = 1
	defer execution.Release()
	for range 3 {
		if err := q.RunInto(&execution, FromFile(snapshot)); err != nil {
			t.Fatal(err)
		}
		if execution.Result.RowCount != 8 {
			t.Fatalf("got %d rows, want 8", execution.Result.RowCount)
		}
		if execution.Stats.GroupedIntegerRows != 0 {
			t.Fatalf("string key used integer lane: %+v", execution.Stats)
		}
		if execution.file.integerGroups.isPrepared() ||
			cap(execution.file.integerGroups.shapeWork) != 0 ||
			cap(execution.file.integerGroups.shapeSeen) != 0 {
			t.Fatal("string-key GROUP BY speculatively allocated integerGroupWorkspace")
		}
	}
}

func TestFileIntegerGroupsExecSwitchBetweenIntegerAndString(t *testing.T) {
	const rows = 256
	snapshot, _ := filePackedIntegerExtremaSnapshot(t, rows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			return fmt.Appendf(nil, `{"n":%d,"s":"grp-%02d","v":%d}`, row%8, row%8, row)
		},
	)
	intQ := Select(Path("n"), Count(), Sum("v")).GroupBy("n")
	strQ := Select(Path("s"), Count()).GroupBy("s")

	var execution Exec
	execution.Options.Workers = 1
	defer execution.Release()

	// Run integer GROUP BY first (populates integerGroupWorkspace).
	if err := intQ.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.GroupedIntegerRows != rows || !execution.file.integerGroups.isPrepared() {
		t.Fatalf("expected integer lane to populate workspace: stats=%+v", execution.Stats)
	}

	// Switch to string GROUP BY on the same Exec: must release integer workspace
	// so it does not linger uncounted while generic executor runs.
	if err := strQ.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.GroupedIntegerRows != 0 || execution.Result.RowCount != 8 {
		t.Fatalf("string switch stats=%+v rows=%d", execution.Stats, execution.Result.RowCount)
	}
	if execution.file.integerGroups.isPrepared() {
		t.Fatal("integer workspace was not released on fallback to string GROUP BY")
	}

	// Switch back to integer GROUP BY: must cleanly re-prepare and succeed.
	if err := intQ.RunInto(&execution, FromFile(snapshot)); err != nil {
		t.Fatal(err)
	}
	if execution.Stats.GroupedIntegerRows != rows || execution.Result.RowCount != 8 {
		t.Fatalf("integer re-entry stats=%+v rows=%d", execution.Stats, execution.Result.RowCount)
	}
}

func TestFileIntegerGroupsSupportsZeroAllocsEmptyAndMixedLeaves(t *testing.T) {
	// 1. Empty snapshot: SupportsIntegerGroups must return true, nil with 0 allocs.
	emptyFile, err := os.CreateTemp(t.TempDir(), "empty-*.vdb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptyFile.Close() })
	emptyOpts := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	emptySource := &store.Collection{}
	if _, err := emptySource.Put("seed", []byte(`{"k":1,"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.CreateFromPrimary(emptySource, emptyFile, emptyOpts); err != nil {
		t.Fatal(err)
	}
	emptyStore, err := durable.Open(emptyFile, emptyOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptyStore.Close() })
	if deleted, err := emptyStore.Delete([]byte("seed")); err != nil || !deleted {
		t.Fatalf("delete seed=(%v, %v)", deleted, err)
	}
	emptySnap, err := emptyStore.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = emptySnap.Close() })
	filter, err := durable.NewIntegerGroupFilter("/k", "/v")
	if err != nil {
		t.Fatal(err)
	}
	ok, err := emptySnap.SupportsIntegerGroups(filter)
	if err != nil || !ok {
		t.Fatalf("empty snapshot SupportsIntegerGroups=(%v, %v), want (true, nil)", ok, err)
	}

	// 2. String-key snapshot: SupportsIntegerGroups must return false, nil with 0 allocs.
	strSnap, _ := filePackedIntegerExtremaSnapshot(t, 128,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			return fmt.Appendf(nil, `{"k":"s-%02d","v":%d}`, row%8, row)
		},
	)
	ok, err = strSnap.SupportsIntegerGroups(filter)
	if err != nil || ok {
		t.Fatalf("string snapshot SupportsIntegerGroups=(%v, %v), want (false, nil)", ok, err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		sup, probeErr := strSnap.SupportsIntegerGroups(filter)
		if probeErr != nil || sup {
			t.Fatalf("probe=(%v, %v), want (false, nil)", sup, probeErr)
		}
	}); allocs != 0 {
		t.Fatalf("SupportsIntegerGroups allocated %v times/op, want 0", allocs)
	}

	// 3. Multi-leaf mixed-type snapshot: leaf 0 is integer, later leaf is string.
	// Reusing the same Exec must repeatedly decline cleanly and match the oracle.
	const mixedRows = 2 * storeio.CompactPrimaryStripeMaxRows
	mixedSnap, mixedHeap := filePackedIntegerExtremaSnapshot(t, mixedRows,
		durable.Options{Collection: store.Options{ChunkDocuments: 64}},
		func(row int) []byte {
			if row < storeio.CompactPrimaryStripeMaxRows {
				return fmt.Appendf(nil, `{"k":%d,"v":1}`, row%4)
			}
			return fmt.Appendf(nil, `{"k":"str-%d","v":1}`, row%4)
		},
	)
	q := Select(Path("k"), Count(), Sum("v")).GroupBy("k")
	var oracle Exec
	oracle.Options.Workers = 1
	if err := q.RunInto(&oracle, FromSnapshot(mixedHeap)); err != nil {
		t.Fatalf("oracle: %v", err)
	}
	var exec Exec
	exec.Options.Workers = 1
	defer exec.Release()
	for run := range 3 {
		if err := q.RunInto(&exec, FromFile(mixedSnap)); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if exec.Stats.GroupedIntegerRows != 0 {
			t.Fatalf("run %d: mixed-type snapshot used integer lane: %+v", run, exec.Stats)
		}
		if exec.file.integerGroups.isPrepared() {
			t.Fatalf("run %d: integer workspace not released after leaf-1 decline", run)
		}
		if exec.Result.RowCount != oracle.Result.RowCount {
			t.Fatalf("run %d: row count=%d, want %d", run, exec.Result.RowCount, oracle.Result.RowCount)
		}
	}
}
