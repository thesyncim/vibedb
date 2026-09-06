package driver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
)

const (
	replicatedReadReuseDataSQL = `SELECT id, bucket, score, payload
	FROM docs
	WHERE id >= ? AND id < ?
	ORDER BY id LIMIT 64`
	replicatedReadReuseScoreSQL = `SELECT id, score
	FROM docs
	WHERE id >= ? AND id < ?
	ORDER BY id LIMIT 64`
	replicatedReadReusePointSQL = `SELECT id, score FROM docs WHERE id = ?`
)

type replicatedReadReuseFixture struct {
	database *Database
	claim    *ReplicatedApply
	base     ReplicatedShardStoreIdentity
	epoch    uint64
	rows     []replicatedProjectedFieldRow
	applied  uint64
	cut      replicatedstate.DataReadCut
}

func newReplicatedReadReuseFixture(t *testing.T) *replicatedReadReuseFixture {
	t.Helper()
	_, database, binding, _ := prepareReplicatedTestRoot(
		t, "sql-replicated-read-reuse", false,
	)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(
		base, bootstrap, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := claim.Close(); err != nil {
			t.Errorf("close replicated apply: %v", err)
		}
	})
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	rows := replicatedProjectedFieldRowsWant()[:64]
	last := applyReplicatedProjectedFieldRows(t, database, claim, base, epoch, 2, rows)
	var cut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, last, &cut); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cut.Close(); err != nil {
			t.Errorf("close data read cut: %v", err)
		}
	})
	return &replicatedReadReuseFixture{
		database: database, claim: claim, base: base, epoch: epoch,
		rows: rows, applied: last, cut: cut,
	}
}

func (f *replicatedReadReuseFixture) updateRow(t *testing.T, row int) (*replicatedstate.DataReadCut, uint64) {
	t.Helper()
	if row < 0 || row >= len(f.rows) {
		t.Fatalf("update row %d outside fixture", row)
	}
	document := replicatedProjectedFieldDocument(row, true)
	index := f.applied + 1
	if _, err := f.claim.ApplyNormal(
		testReplicatedApplyMeta(index),
		testReplicatedApplyCommand(
			f.base, f.epoch, f.applied,
			replication.Mutation{
				Kind:  replication.MutationPut,
				Key:   testReplicatedApplyKey(t, f.database, document),
				Value: document,
			},
		),
	); err != nil {
		t.Fatalf("apply read-reuse update: %v", err)
	}
	var cut replicatedstate.DataReadCut
	if err := f.claim.DataReadCutInto(nil, index, &cut); err != nil {
		t.Fatal(err)
	}
	f.applied = index
	return &cut, index
}

func replicatedReadReuseOptions() query.ExecOptions {
	return query.ExecOptions{Workers: 1, ResultRows: -1, ResultBytes: -1}
}

func acquireReplicatedReadReuseData(
	t testing.TB,
	f *replicatedReadReuseFixture,
	cut *replicatedstate.DataReadCut,
	text string,
	parameterTypes []ParamType,
	options query.ExecOptions,
) *ReplicatedReadLease {
	t.Helper()
	lease, err := f.claim.AcquireReplicatedDataRead(
		context.Background(), cut, text, parameterTypes, false, options,
	)
	if err != nil {
		t.Fatalf("acquire data read lease: %v", err)
	}
	return lease
}

func queryReplicatedReadReuseData(
	t testing.TB,
	lease *ReplicatedReadLease,
	values []any,
	want []replicatedProjectedFieldRow,
) error {
	t.Helper()
	var cursor Cursor
	if err := lease.QueryInto(context.Background(), values, &cursor); err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	row := 0
	for cursor.Next() {
		if row >= len(want) {
			t.Fatalf("data reuse query returned more than %d rows", len(want))
		}
		expected := want[row]
		id, idOK := cursor.Cell(0).Text()
		bucket, bucketOK := cursor.Cell(1).Int64()
		score, scoreOK := cursor.Cell(2).Int64()
		payload, payloadOK := cursor.Cell(3).Text()
		if !idOK || !bucketOK || !scoreOK || !payloadOK {
			t.Fatalf("row %d types=(%s,%s,%s,%s), want scalar fields",
				row, cursor.Cell(0).JSON(), cursor.Cell(1).JSON(),
				cursor.Cell(2).JSON(), cursor.Cell(3).JSON())
		}
		expectedBucket, bucketErr := strconv.ParseInt(expected.values[1], 10, 64)
		expectedScore, scoreErr := strconv.ParseInt(expected.values[2], 10, 64)
		expectedPayload, payloadErr := strconv.Unquote(expected.values[3])
		if bucketErr != nil || scoreErr != nil || payloadErr != nil {
			t.Fatalf("row %d has malformed expected values=%v: bucket=%v score=%v payload=%v",
				row, expected.values, bucketErr, scoreErr, payloadErr)
		}
		if id != expected.id || bucket != expectedBucket || score != expectedScore ||
			payload != expectedPayload {
			t.Fatalf("row %d=(%q,%d,%d,%q), want id=%q bucket=%d score=%d payload=%q",
				row, id, bucket, score, payload, expected.id,
				expectedBucket, expectedScore, expectedPayload)
		}
		row++
	}
	if row != len(want) {
		t.Fatalf("data reuse query returned %d rows, want %d", row, len(want))
	}
	return nil
}

func queryReplicatedReadReuseScore(
	t testing.TB,
	lease *ReplicatedReadLease,
	values []any,
	want []replicatedProjectedFieldRow,
) error {
	t.Helper()
	var cursor Cursor
	if err := lease.QueryInto(context.Background(), values, &cursor); err != nil {
		return err
	}
	defer func() { _ = cursor.Close() }()
	row := 0
	for cursor.Next() {
		if row >= len(want) {
			t.Fatalf("score query returned more than %d rows", len(want))
		}
		id, idOK := cursor.Cell(0).Text()
		score, scoreOK := cursor.Cell(1).Int64()
		if !idOK || !scoreOK {
			t.Fatalf("score row %d types=(%s,%s)", row, cursor.Cell(0).JSON(), cursor.Cell(1).JSON())
		}
		if id != want[row].id || score != int64(row%100+7) {
			t.Fatalf("score row %d=(%q,%d), want (%q,%d)", row, id, score, want[row].id, row%100+7)
		}
		row++
	}
	if row != len(want) {
		t.Fatalf("score query returned %d rows, want %d", row, len(want))
	}
	return nil
}

func TestReplicatedReadReuseDataCutMutationAndParameterKey(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}

	first := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions(),
	)
	firstSlot := first.slot
	if err := queryReplicatedReadReuseData(t, first, values, f.rows); err != nil {
		_ = first.Abort(err)
		t.Fatal(err)
	}
	if err := first.Finish(nil); err != nil {
		t.Fatalf("finish first data lease: %v", err)
	}
	if _, ok := f.cut.Relation(1); !ok {
		t.Fatal("finishing a lease closed its caller-owned cut")
	}

	warm := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions(),
	)
	if warm.slot != firstSlot {
		t.Fatal("same prepared data shape did not reuse its cache slot")
	}
	if err := queryReplicatedReadReuseData(t, warm, values, f.rows); err != nil {
		_ = warm.Abort(err)
		t.Fatal(err)
	}
	if err := warm.Finish(nil); err != nil {
		t.Fatalf("finish warm data lease: %v", err)
	}

	newCut, _ := f.updateRow(t, 5)
	t.Cleanup(func() { _ = newCut.Close() })
	freshWant := replicatedProjectedFieldRowsWant()[:64]
	freshWant[5].values[2] = "1012"
	fresh := acquireReplicatedReadReuseData(
		t, f, newCut, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions(),
	)
	if fresh.slot != firstSlot {
		t.Fatal("new immutable cut did not reuse the prepared cache slot")
	}
	if err := queryReplicatedReadReuseData(t, fresh, values, freshWant); err != nil {
		_ = fresh.Abort(err)
		t.Fatal(err)
	}
	if err := fresh.Finish(nil); err != nil {
		t.Fatalf("finish fresh-cut lease: %v", err)
	}

	oldAgain := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions(),
	)
	if oldAgain.slot != firstSlot {
		t.Fatal("old immutable cut did not reuse the prepared cache slot")
	}
	if err := queryReplicatedReadReuseData(t, oldAgain, values, f.rows); err != nil {
		_ = oldAgain.Abort(err)
		t.Fatal(err)
	}
	if err := oldAgain.Finish(nil); err != nil {
		t.Fatalf("finish old-cut lease: %v", err)
	}

	score := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseScoreSQL, params, replicatedReadReuseOptions(),
	)
	if score.slot == firstSlot {
		t.Fatal("different SQL shape reused the data projection slot")
	}
	if err := queryReplicatedReadReuseScore(t, score, values, f.rows); err != nil {
		_ = score.Abort(err)
		t.Fatal(err)
	}
	if err := score.Finish(nil); err != nil {
		t.Fatalf("finish score-shape lease: %v", err)
	}

	unspecified := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseScoreSQL, nil, replicatedReadReuseOptions(),
	)
	if unspecified.slot == score.slot {
		t.Fatal("different parameter-type metadata reused the score cache slot")
	}
	if err := queryReplicatedReadReuseScore(t, unspecified, values, f.rows); err != nil {
		_ = unspecified.Abort(err)
		t.Fatal(err)
	}
	if err := unspecified.Finish(nil); err != nil {
		t.Fatalf("finish unspecified-parameter lease: %v", err)
	}
}

func TestReplicatedReadReusePointAlternatesHitsMissesAndUpdates(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	ctx := context.Background()
	paramTypes := []ParamType{ParamTypeText}
	options := replicatedReadReuseOptions()
	primaryPath := []byte(f.base.UserPrimaryKey)
	var resultBacking *query.ResultColumn
	for iteration, row := range []int{0, 1, 0, 1} {
		document := f.rows[row].doc
		key := testReplicatedApplyKey(t, f.database, document)
		lease, err := f.claim.AcquireReplicatedPointRead(
			ctx, 1, key, true, document, primaryPath,
			replicatedReadReusePointSQL, paramTypes, false, options,
		)
		if err != nil {
			t.Fatal(err)
		}
		result := &lease.slot.reader.conn.exec.Result
		if resultBacking != nil && (cap(result.Columns) == 0 || &result.Columns[:1][0] != resultBacking) {
			_ = lease.Abort(errors.New("point result backing lost"))
			t.Fatal("fresh point bind discarded retained result arrays")
		}
		var cursor Cursor
		if err := lease.QueryCandidateKeysInto(
			ctx, []any{f.rows[row].id}, primaryPath, [][]byte{key}, &cursor,
		); err != nil {
			_ = lease.Abort(err)
			t.Fatal(err)
		}
		resultBacking = &result.Columns[0]
		if !cursor.Next() || cursor.Cell(0).String() != fmt.Sprintf(`"%s"`, f.rows[row].id) {
			_ = cursor.Close()
			_ = lease.Abort(errors.New("point result mismatch"))
			t.Fatalf("point hit %d returned (%s,%s)", iteration, cursor.Cell(0).JSON(), cursor.Cell(1).JSON())
		}
		score, scoreOK := cursor.Cell(1).Int64()
		if !scoreOK || score != int64(row%100+7) {
			_ = cursor.Close()
			_ = lease.Abort(errors.New("point result mismatch"))
			t.Fatalf("point hit %d returned score=%s", iteration, cursor.Cell(1).JSON())
		}
		if cursor.Next() {
			_ = cursor.Close()
			_ = lease.Abort(errors.New("point result had extra row"))
			t.Fatal("point hit returned an extra row")
		}
		if err := cursor.Close(); err != nil {
			_ = lease.Abort(err)
			t.Fatal(err)
		}
		if err := lease.Finish(nil); err != nil {
			t.Fatalf("finish point hit %d: %v", iteration, err)
		}

		missID := fmt.Sprintf("missing-%d", iteration)
		missDoc := []byte(fmt.Sprintf(`{"id":%q}`, missID))
		missKey := testReplicatedApplyKey(t, f.database, missDoc)
		miss, err := f.claim.AcquireReplicatedPointRead(
			ctx, 1, missKey, false, nil, primaryPath,
			replicatedReadReusePointSQL, paramTypes, false, options,
		)
		if err != nil {
			t.Fatal(err)
		}
		var missCursor Cursor
		if err := miss.QueryCandidateKeysInto(
			ctx, []any{missID}, primaryPath, [][]byte{missKey}, &missCursor,
		); err != nil {
			_ = miss.Abort(err)
			t.Fatal(err)
		}
		if missCursor.Next() {
			_ = missCursor.Close()
			_ = miss.Abort(errors.New("point miss returned a row"))
			t.Fatalf("point miss %q returned a row", missID)
		}
		if err := missCursor.Close(); err != nil {
			_ = miss.Abort(err)
			t.Fatal(err)
		}
		if err := miss.Finish(nil); err != nil {
			t.Fatalf("finish point miss %d: %v", iteration, err)
		}
	}

	newCut, _ := f.updateRow(t, 0)
	_ = newCut.Close()
	updated := replicatedProjectedFieldDocument(0, true)
	updatedKey := testReplicatedApplyKey(t, f.database, updated)
	lease, err := f.claim.AcquireReplicatedPointRead(
		ctx, 1, updatedKey, true, updated, primaryPath,
		replicatedReadReusePointSQL, paramTypes, false, options,
	)
	if err != nil {
		t.Fatal(err)
	}
	var cursor Cursor
	if err := lease.QueryCandidateKeysInto(
		ctx, []any{f.rows[0].id}, primaryPath, [][]byte{updatedKey}, &cursor,
	); err != nil {
		_ = lease.Abort(err)
		t.Fatal(err)
	}
	if !cursor.Next() {
		_ = cursor.Close()
		_ = lease.Abort(errors.New("updated point mismatch"))
		t.Fatalf("updated point result=(%s,%s)", cursor.Cell(0).JSON(), cursor.Cell(1).JSON())
	}
	score, scoreOK := cursor.Cell(1).Int64()
	if !scoreOK || score != 1007 {
		_ = cursor.Close()
		_ = lease.Abort(errors.New("updated point mismatch"))
		t.Fatalf("updated point score=%s", cursor.Cell(1).JSON())
	}
	if err := cursor.Close(); err != nil {
		_ = lease.Abort(err)
		t.Fatal(err)
	}
	if err := lease.Finish(nil); err != nil {
		t.Fatalf("finish updated point: %v", err)
	}
}

func TestReplicatedReadReuseBudgetCancellationRollbackAndReuse(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	options := replicatedReadReuseOptions()

	warm := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, warm, values, f.rows); err != nil {
		_ = warm.Abort(err)
		t.Fatal(err)
	}
	if err := warm.Finish(nil); err != nil {
		t.Fatalf("finish warm lease: %v", err)
	}

	budgetOptions := options
	budgetOptions.ResultRows = 1
	budget := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, budgetOptions,
	)
	var budgetCursor Cursor
	budgetErr := budget.QueryInto(context.Background(), values, &budgetCursor)
	if !errors.Is(budgetErr, query.ErrResultBudget) {
		_ = budget.Abort(budgetErr)
		t.Fatalf("warm ResultRows error=%v, want ErrResultBudget", budgetErr)
	}
	if budgetCursor.Next() {
		_ = budgetCursor.Close()
		_ = budget.Abort(budgetErr)
		t.Fatal("result-budget failure exposed a partial cursor row")
	}
	if err := budgetCursor.Close(); err != nil {
		_ = budget.Abort(err)
		t.Fatal(err)
	}
	if err := budget.Finish(budgetErr); !errors.Is(err, query.ErrResultBudget) {
		t.Fatalf("finish ResultRows failure=%v, want ErrResultBudget", err)
	}

	afterBudget := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, afterBudget, values, f.rows); err != nil {
		_ = afterBudget.Abort(err)
		t.Fatal(err)
	}
	if err := afterBudget.Finish(nil); err != nil {
		t.Fatalf("finish after ResultRows failure: %v", err)
	}

	byteOptions := options
	byteOptions.ResultBytes = 1
	byteBudget := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, byteOptions,
	)
	var byteCursor Cursor
	byteErr := byteBudget.QueryInto(context.Background(), values, &byteCursor)
	if !errors.Is(byteErr, query.ErrResultBudget) {
		_ = byteBudget.Abort(byteErr)
		t.Fatalf("warm ResultBytes error=%v, want ErrResultBudget", byteErr)
	}
	if byteCursor.Next() {
		_ = byteCursor.Close()
		_ = byteBudget.Abort(byteErr)
		t.Fatal("byte-budget failure exposed a partial cursor row")
	}
	if err := byteCursor.Close(); err != nil {
		_ = byteBudget.Abort(err)
		t.Fatal(err)
	}
	if err := byteBudget.Finish(byteErr); !errors.Is(err, query.ErrResultBudget) {
		t.Fatalf("finish ResultBytes failure=%v, want ErrResultBudget", err)
	}

	afterBytes := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, afterBytes, values, f.rows); err != nil {
		_ = afterBytes.Abort(err)
		t.Fatal(err)
	}
	if err := afterBytes.Finish(nil); err != nil {
		t.Fatalf("finish after ResultBytes failure: %v", err)
	}

	var cancel query.CancelFlag
	cancel.Cancel()
	cancelOptions := options
	cancelOptions.Cancel = &cancel
	canceled := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, cancelOptions,
	)
	var canceledCursor Cursor
	canceledErr := canceled.QueryInto(context.Background(), values, &canceledCursor)
	if !errors.Is(canceledErr, query.ErrCanceled) {
		_ = canceled.Abort(canceledErr)
		t.Fatalf("warm cancellation error=%v, want ErrCanceled", canceledErr)
	}
	if canceledCursor.Next() {
		_ = canceledCursor.Close()
		_ = canceled.Abort(canceledErr)
		t.Fatal("canceled execution exposed a partial cursor row")
	}
	if err := canceledCursor.Close(); err != nil {
		_ = canceled.Abort(err)
		t.Fatal(err)
	}
	if err := canceled.Finish(canceledErr); !errors.Is(err, query.ErrCanceled) {
		t.Fatalf("finish cancellation=%v, want ErrCanceled", err)
	}
	cancel.Reset()

	afterCancel := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, afterCancel, values, f.rows); err != nil {
		_ = afterCancel.Abort(err)
		t.Fatal(err)
	}
	if err := afterCancel.Finish(nil); err != nil {
		t.Fatalf("finish after cancellation: %v", err)
	}
}

func TestReplicatedReadReuseLeasesAreExclusiveAndStaleHandlesClose(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	options := replicatedReadReuseOptions()

	first := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	firstSlot := first.slot
	second := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if second.slot == firstSlot {
		_ = first.Abort(ErrReplicatedReadLeaseClosed)
		_ = second.Abort(ErrReplicatedReadLeaseClosed)
		t.Fatal("two outstanding leases shared one exclusive cache slot")
	}
	if err := queryReplicatedReadReuseData(t, first, values, f.rows); err != nil {
		_ = first.Abort(err)
		_ = second.Abort(err)
		t.Fatal(err)
	}
	if err := queryReplicatedReadReuseData(t, second, values, f.rows); err != nil {
		_ = first.Abort(err)
		_ = second.Abort(err)
		t.Fatal(err)
	}
	if err := first.Finish(nil); err != nil {
		t.Fatalf("finish first outstanding lease: %v", err)
	}
	if err := first.QueryInto(context.Background(), values, &Cursor{}); !errors.Is(err, ErrReplicatedReadLeaseClosed) {
		t.Fatalf("stale first lease query=%v, want ErrReplicatedReadLeaseClosed", err)
	}

	reused := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if reused.slot != firstSlot {
		t.Fatalf("released slot was not reused: first=%p reused=%p", firstSlot, reused.slot)
	}
	if err := first.QueryInto(context.Background(), values, &Cursor{}); !errors.Is(err, ErrReplicatedReadLeaseClosed) {
		t.Fatalf("stale first lease after slot reuse=%v, want ErrReplicatedReadLeaseClosed", err)
	}
	if err := queryReplicatedReadReuseData(t, reused, values, f.rows); err != nil {
		_ = reused.Abort(err)
		_ = second.Abort(err)
		t.Fatal(err)
	}
	if err := reused.Finish(nil); err != nil {
		t.Fatalf("finish reused lease: %v", err)
	}
	if err := second.Finish(nil); err != nil {
		t.Fatalf("finish second outstanding lease: %v", err)
	}
}

func TestReplicatedReadReuseConcurrentIndependentLeases(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	options := replicatedReadReuseOptions()
	const consumers = 4
	errs := make(chan error, consumers)
	var group sync.WaitGroup
	group.Add(consumers)
	for range consumers {
		go func() {
			defer group.Done()
			lease, err := f.claim.AcquireReplicatedDataRead(
				context.Background(), &f.cut, replicatedReadReuseDataSQL,
				params, false, options,
			)
			if err != nil {
				errs <- err
				return
			}
			if err := queryReplicatedReadReuseData(t, lease, values, f.rows); err != nil {
				_ = lease.Abort(err)
				errs <- err
				return
			}
			if err := lease.Finish(nil); err != nil {
				errs <- err
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestReplicatedReadReuseUnsupportedShapesAreNeverRetained(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	unsupportedSQL := `SELECT id, score + 1 FROM docs WHERE id >= ? AND id < ? ORDER BY id LIMIT 64`
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	options := replicatedReadReuseOptions()

	for attempt := range 2 {
		lease, err := f.claim.AcquireReplicatedDataRead(
			context.Background(), &f.cut, unsupportedSQL, params, false, options,
		)
		if err != nil {
			if !errors.Is(err, ErrReplicatedReadReuseUnsupported) {
				t.Fatalf("unsupported acquire %d=%v", attempt, err)
			}
			continue
		}
		var cursor Cursor
		execErr := lease.QueryInto(context.Background(), values, &cursor)
		if execErr == nil {
			if !cursor.Next() {
				_ = cursor.Close()
				_ = lease.Abort(errors.New("unsupported query returned no rows"))
				t.Fatalf("unsupported query attempt %d returned no rows", attempt)
			}
		}
		_ = cursor.Close()
		if finishErr := lease.Finish(execErr); finishErr != nil && execErr == nil {
			t.Fatalf("finish unsupported query %d: %v", attempt, finishErr)
		}
	}

	cache := f.claim.readReuse
	cache.mu.Lock()
	for i := range cache.slots {
		if cache.slots[i].key.sql == unsupportedSQL && cache.slots[i].reader != nil {
			cache.mu.Unlock()
			t.Fatalf("unsupported slot %d retained a reader", i)
		}
	}
	cache.mu.Unlock()

	valid := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, valid, values, f.rows); err != nil {
		_ = valid.Abort(err)
		t.Fatal(err)
	}
	if err := valid.Finish(nil); err != nil {
		t.Fatalf("finish valid query after unsupported shape: %v", err)
	}
}

func TestReplicatedReadReuseRejectsForeignAndClosedCuts(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	options := replicatedReadReuseOptions()

	warm := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, warm, values, f.rows); err != nil {
		_ = warm.Abort(err)
		t.Fatal(err)
	}
	if err := warm.Finish(nil); err != nil {
		t.Fatalf("finish warm lease: %v", err)
	}

	_, foreignDatabase, foreignBinding, _ := prepareReplicatedTestRoot(
		t, "sql-replicated-read-reuse-foreign", false,
	)
	t.Cleanup(func() {
		if err := foreignDatabase.Close(); err != nil {
			t.Errorf("close foreign database: %v", err)
		}
	})
	foreignBase := requireReplicatedShardStoreBind(t, foreignDatabase, foreignBinding, "docs")
	foreignBootstrap := testReplicatedApplyBootstrap()
	foreignClaim, _, err := foreignDatabase.OpenReplicatedApply(
		foreignBase, foreignBootstrap, testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := foreignClaim.Close(); err != nil {
			t.Errorf("close foreign claim: %v", err)
		}
	})
	if _, err := foreignClaim.InstallSnapshot(foreignBootstrap); err != nil {
		t.Fatal(err)
	}
	foreignEpoch := applyReplicatedApplySessionOpen(t, foreignClaim, foreignBase, 2)
	var foreignCut replicatedstate.DataReadCut
	if err := foreignClaim.DataReadCutInto(nil, foreignEpoch, &foreignCut); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = foreignCut.Close() })
	if _, err := f.claim.AcquireReplicatedDataRead(
		context.Background(), &foreignCut, replicatedReadReuseDataSQL,
		params, false, options,
	); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("foreign cut acquire=%v, want ErrReplicatedApplyMismatch", err)
	}

	var closedCut replicatedstate.DataReadCut
	if err := f.claim.DataReadCutInto(nil, f.applied, &closedCut); err != nil {
		t.Fatal(err)
	}
	if err := closedCut.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.claim.AcquireReplicatedDataRead(
		context.Background(), &closedCut, replicatedReadReuseDataSQL,
		params, false, options,
	); !errors.Is(err, ErrReplicatedApplyMismatch) {
		t.Fatalf("closed cut acquire=%v, want ErrReplicatedApplyMismatch", err)
	}

	valid := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, valid, values, f.rows); err != nil {
		_ = valid.Abort(err)
		t.Fatal(err)
	}
	if err := valid.Finish(nil); err != nil {
		t.Fatalf("finish valid lease after rejected cuts: %v", err)
	}
}

func TestReplicatedReadReuseLayoutReplacementDoesNotReuseStaleSlot(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	options := replicatedReadReuseOptions()

	warm := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := queryReplicatedReadReuseData(t, warm, values, f.rows); err != nil {
		_ = warm.Abort(err)
		t.Fatal(err)
	}
	warmSlot := warm.slot
	if err := warm.Finish(nil); err != nil {
		t.Fatalf("finish warm layout lease: %v", err)
	}

	core := f.claim.database
	core.mu.Lock()
	oldTables := core.tables
	oldEpoch := core.layoutEpoch
	oldTable := oldTables["docs"]
	if oldTable == nil || oldEpoch == nil {
		core.mu.Unlock()
		t.Fatal("replicated fixture has no live docs layout")
	}
	// A new incarnation shares immutable/storage handles, never its mutex.
	replacement := table{
		meta: oldTable.meta, schema: oldTable.schema, primary: oldTable.primary,
		file: oldTable.file, collection: oldTable.collection,
	}
	replacedTables := make(map[string]*table, len(oldTables))
	for name, table := range oldTables {
		replacedTables[name] = table
	}
	replacedTables["docs"] = &replacement
	core.tables = replacedTables
	core.layoutEpoch = newCatalogLayoutEpoch(replacedTables, core.catalog.Views)
	core.mu.Unlock()

	next := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if next.slot == warmSlot {
		_ = next.Abort(ErrReplicatedReadLeaseClosed)
		core.mu.Lock()
		core.tables, core.layoutEpoch = oldTables, oldEpoch
		core.mu.Unlock()
		t.Fatal("replaced table incarnation reused the stale prepared slot")
	}
	if err := queryReplicatedReadReuseData(t, next, values, f.rows); err != nil {
		_ = next.Abort(err)
		core.mu.Lock()
		core.tables, core.layoutEpoch = oldTables, oldEpoch
		core.mu.Unlock()
		t.Fatal(err)
	}
	if err := next.Finish(nil); err != nil {
		core.mu.Lock()
		core.tables, core.layoutEpoch = oldTables, oldEpoch
		core.mu.Unlock()
		t.Fatalf("finish replacement-layout lease: %v", err)
	}
	core.mu.Lock()
	core.tables, core.layoutEpoch = oldTables, oldEpoch
	core.mu.Unlock()
}

func assertReplicatedReadReuseIdleState(t *testing.T, claim *ReplicatedApply) {
	t.Helper()
	cache := claim.readReuse
	if cache == nil {
		t.Fatal("read-reuse cache was not created")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.retained > replicatedReadReuseMaxBytes {
		t.Fatalf("idle retained bytes=%d, want <=%d", cache.retained, replicatedReadReuseMaxBytes)
	}
	for i := range cache.slots {
		slot := &cache.slots[i]
		if slot.active || slot.lease != nil {
			t.Fatalf("idle slot %d remains active: %+v", i, slot)
		}
		if slot.reader == nil {
			continue
		}
		reader := slot.reader
		if reader.session.current != nil || reader.session.state != SessionIdle ||
			reader.conn.open || reader.conn.tx != nil {
			t.Fatalf("idle slot %d retains live session state", i)
		}
		for _, arg := range reader.conn.args[:cap(reader.conn.args)] {
			if arg != nil {
				t.Fatalf("idle slot %d retains argument backing", i)
			}
		}
		if len(reader.conn.args) != 0 || reader.conn.pointRaw != nil ||
			reader.conn.pointKeyRaw != nil || reader.conn.pointKeys != nil ||
			reader.conn.matchKeys != nil || reader.conn.joinCatalog != nil ||
			reader.conn.insertSeeds != nil || reader.conn.insertKeyRaw != nil ||
			reader.conn.insertSeen != nil || reader.conn.insertTape != nil {
			t.Fatalf("idle slot %d retains request/point argument buffers", i)
		}
		if reader.conn.pointDocs.Len() != 0 || reader.conn.joinSnapshot.Len() != 0 ||
			reader.conn.insertSnapshot.Len() != 0 ||
			!reflect.DeepEqual(reader.conn.fileRange, query.FileRangeSource{}) {
			t.Fatalf("idle slot %d retains point/cut source state", i)
		}
		if reader.conn.exec.Options.Cancel != nil ||
			reader.conn.exec.Stats != (query.ExecStats{}) ||
			reader.conn.exec.Result.RowCount != 0 ||
			len(reader.conn.exec.Result.Columns) != 0 {
			t.Fatalf("idle slot %d retains execution request state", i)
		}
	}
}

func TestReplicatedReadReuseApplyCloseRejectsAndRetiresLeases(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	options := replicatedReadReuseOptions()
	lease := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, options,
	)
	if err := f.claim.Close(); err != nil {
		t.Fatalf("close apply with outstanding lease: %v", err)
	}
	if _, err := f.claim.AcquireReplicatedDataRead(
		context.Background(), &f.cut, replicatedReadReuseDataSQL,
		params, false, options,
	); !errors.Is(err, ErrReplicatedApplyClosed) {
		t.Fatalf("acquire after apply close=%v, want ErrReplicatedApplyClosed", err)
	}
	if err := lease.Finish(nil); err != nil {
		t.Fatalf("finish lease after apply close: %v", err)
	}
	cache := f.claim.readReuse
	cache.mu.Lock()
	if cache.retained != 0 {
		cache.mu.Unlock()
		t.Fatalf("closed apply retained %d idle bytes", cache.retained)
	}
	for i := range cache.slots {
		if cache.slots[i].reader != nil || cache.slots[i].active {
			cache.mu.Unlock()
			t.Fatalf("closed apply retained slot %d: %+v", i, cache.slots[i])
		}
	}
	cache.mu.Unlock()
}

func TestReplicatedReadReuseIdleStateDropsRequestReferences(t *testing.T) {
	f := newReplicatedReadReuseFixture(t)
	params := []ParamType{ParamTypeText, ParamTypeText}
	values := []any{"row-00000000", "row-00000064"}
	lease := acquireReplicatedReadReuseData(
		t, f, &f.cut, replicatedReadReuseDataSQL, params, replicatedReadReuseOptions(),
	)
	if err := queryReplicatedReadReuseData(t, lease, values, f.rows); err != nil {
		_ = lease.Abort(err)
		t.Fatal(err)
	}
	if err := lease.Finish(nil); err != nil {
		t.Fatalf("finish idle-state data lease: %v", err)
	}
	assertReplicatedReadReuseIdleState(t, f.claim)
}
