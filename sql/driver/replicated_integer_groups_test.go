package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
)

const replicatedIntegerGroupRows = 8192

const replicatedIntegerGroupMutationBatch = 64

type replicatedIntegerGroupAggregate struct {
	count int64
	sum   int64
}

func replicatedIntegerGroupDocument(row, score int) []byte {
	return fmt.Appendf(nil,
		`{"id":"row-%08d","bucket":%d,"score":%d,"payload":"payload-%08d"}`,
		row, row%16, score, row,
	)
}

func replicatedIntegerGroupWant(score func(int) int) map[int64]replicatedIntegerGroupAggregate {
	want := make(map[int64]replicatedIntegerGroupAggregate, 16)
	for row := 0; row < replicatedIntegerGroupRows; row++ {
		bucket := int64(row % 16)
		aggregate := want[bucket]
		aggregate.count++
		aggregate.sum += int64(score(row))
		want[bucket] = aggregate
	}
	return want
}

func queryReplicatedIntegerGroups(
	t *testing.T,
	reader *ReplicatedReadSession,
	ctx context.Context,
	want map[int64]replicatedIntegerGroupAggregate,
) {
	t.Helper()
	countStatement, err := reader.Prepare(ctx, `SELECT COUNT(*) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	var countCursor Cursor
	if err := countStatement.QueryInto(ctx, nil, &countCursor); err != nil {
		_ = countStatement.Close()
		t.Fatal(err)
	}
	if !countCursor.Next() {
		_ = countCursor.Close()
		_ = countStatement.Close()
		t.Fatal("count query returned no row")
	}
	count, ok := countCursor.Cell(0).Int64()
	if !ok || count != replicatedIntegerGroupRows || countCursor.Next() {
		_ = countCursor.Close()
		_ = countStatement.Close()
		t.Fatalf("count=%d (%t), want exactly %d rows", count, ok, replicatedIntegerGroupRows)
	}
	if err := countCursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := countStatement.Close(); err != nil {
		t.Fatal(err)
	}
	prepared, err := reader.Prepare(ctx,
		`SELECT bucket, COUNT(*), SUM(score) FROM docs GROUP BY bucket ORDER BY bucket`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Errorf("close grouped statement: %v", err)
		}
	}()
	var cursor Cursor
	if err := prepared.QueryInto(ctx, nil, &cursor); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cursor.Close(); err != nil {
			t.Errorf("close grouped cursor: %v", err)
		}
	}()

	seen := make(map[int64]replicatedIntegerGroupAggregate, len(want))
	nextBucket := int64(0)
	for cursor.Next() {
		bucket, bucketOK := cursor.Cell(0).Int64()
		count, countOK := cursor.Cell(1).Int64()
		sum, sumOK := cursor.Cell(2).Int64()
		if !bucketOK || !countOK || !sumOK {
			t.Fatalf("group row types = (%s,%s,%s), want integer cells",
				cursor.Cell(0).JSON(), cursor.Cell(1).JSON(), cursor.Cell(2).JSON())
		}
		if bucket != nextBucket {
			t.Fatalf("group bucket order=%d at position %d, want %d", bucket, len(seen), nextBucket)
		}
		nextBucket++
		if _, exists := want[bucket]; exists {
			if _, duplicate := seen[bucket]; duplicate {
				t.Fatalf("duplicate bucket %d", bucket)
			}
			seen[bucket] = replicatedIntegerGroupAggregate{count: count, sum: sum}
			continue
		}
		t.Fatalf("unexpected bucket %d", bucket)
	}
	if len(seen) != len(want) {
		t.Fatalf("group keys=%v, want all %d buckets", seen, len(want))
	}
	for bucket, expected := range want {
		if got := seen[bucket]; got != expected {
			t.Fatalf("bucket %d aggregate=%+v, want %+v", bucket, got, expected)
		}
	}
	if got := reader.conn.exec.Stats.GroupedIntegerRows; got != replicatedIntegerGroupRows {
		t.Fatalf("grouped integer rows=%d, want %d", got, replicatedIntegerGroupRows)
	}
}

func TestReplicatedReadSessionUsesTypedIntegerGroupLane(t *testing.T) {
	_, database, binding, _ := prepareReplicatedTestRoot(t, "sql-replicated-integer-groups", false)
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	base := requireReplicatedShardStoreBind(t, database, binding, "docs")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := claim.Close(); err != nil {
			t.Errorf("close replicated apply: %v", err)
		}
	}()
	if _, err = claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)

	initialScore := func(row int) int { return row % 100 }
	// The canonical replicated relation admits 64 distinct mutations per
	// command. Keep the fixture in those bounded batches so the test exercises
	// the real apply path without paying one durable publication per row.
	initialApplied := uint64(2)
	for start := 0; start < replicatedIntegerGroupRows; start += replicatedIntegerGroupMutationBatch {
		batch := start / replicatedIntegerGroupMutationBatch
		end := min(start+replicatedIntegerGroupMutationBatch, replicatedIntegerGroupRows)
		mutations := make([]replication.Mutation, 0, end-start)
		for row := start; row < end; row++ {
			document := replicatedIntegerGroupDocument(row, initialScore(row))
			mutations = append(mutations, replication.Mutation{
				Kind:  replication.MutationPut,
				Key:   testReplicatedApplyKey(t, database, document),
				Value: document,
			})
		}
		index := uint64(3 + batch)
		publication, err := claim.ApplyNormal(
			testReplicatedApplyMeta(index),
			testReplicatedApplyCommand(base, epoch, uint64(batch)+2, mutations...),
		)
		if err != nil || publication.Applied != index {
			t.Fatalf("apply initial benchmark batch %d = %+v, %v; want applied %d",
				batch, publication, err, index)
		}
		initialApplied = index
	}

	var oldCut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, initialApplied, &oldCut); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := oldCut.Close(); err != nil {
			t.Errorf("close old data cut: %v", err)
		}
	}()

	ctx := context.Background()
	var oldCancel query.CancelFlag
	oldReader, err := claim.NewDataReadSession(ctx, &oldCut, query.ExecOptions{
		Workers: 1,
		Cancel:  &oldCancel,
	})
	if err != nil {
		t.Fatal(err)
	}
	if oldReader.conn.exec.Options.Cancel != &oldCancel {
		t.Fatal("old read session lost its nonnil CancelFlag")
	}
	defer func() {
		if err := oldReader.Close(); err != nil {
			t.Errorf("close old read session: %v", err)
		}
	}()
	queryReplicatedIntegerGroups(t, oldReader, ctx, replicatedIntegerGroupWant(initialScore))
	if err := oldReader.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot, ok := oldCut.Relation(1); !ok || snapshot == nil {
		t.Fatal("closing old reader closed its caller-owned data cut")
	}

	updatedScore := func(row int) int {
		if row == 16 {
			return 92
		}
		return initialScore(row)
	}
	updated := replicatedIntegerGroupDocument(16, updatedScore(16))
	updateIndex := initialApplied + 1
	if _, err := claim.ApplyNormal(
		testReplicatedApplyMeta(updateIndex),
		testReplicatedApplyCommand(base, epoch, initialApplied, replication.Mutation{
			Kind:  replication.MutationPut,
			Key:   testReplicatedApplyKey(t, database, updated),
			Value: updated,
		}),
	); err != nil {
		t.Fatalf("apply replicated update: %v", err)
	}

	// The borrowed old cut must continue to expose the immutable pre-update
	// publication while a fresh cut observes the replicated update.
	var newCut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, updateIndex, &newCut); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := newCut.Close(); err != nil {
			t.Errorf("close new data cut: %v", err)
		}
	}()
	var newCancel query.CancelFlag
	newReader, err := claim.NewDataReadSession(ctx, &newCut, query.ExecOptions{
		Workers: 1,
		Cancel:  &newCancel,
	})
	if err != nil {
		t.Fatal(err)
	}
	if newReader.conn.exec.Options.Cancel != &newCancel {
		t.Fatal("new read session lost its nonnil CancelFlag")
	}
	defer func() {
		if err := newReader.Close(); err != nil {
			t.Errorf("close new read session: %v", err)
		}
	}()
	queryReplicatedIntegerGroups(t, newReader, ctx, replicatedIntegerGroupWant(updatedScore))
	if err := newReader.Close(); err != nil {
		t.Fatal(err)
	}

	var oldAgainCancel query.CancelFlag
	oldAgainReader, err := claim.NewDataReadSession(ctx, &oldCut, query.ExecOptions{
		Workers: 1,
		Cancel:  &oldAgainCancel,
	})
	if err != nil {
		t.Fatal(err)
	}
	if oldAgainReader.conn.exec.Options.Cancel != &oldAgainCancel {
		t.Fatal("reused old read session lost its nonnil CancelFlag")
	}
	defer func() {
		if err := oldAgainReader.Close(); err != nil {
			t.Errorf("close reused old read session: %v", err)
		}
	}()
	queryReplicatedIntegerGroups(t, oldAgainReader, ctx, replicatedIntegerGroupWant(initialScore))
	if err := oldAgainReader.Close(); err != nil {
		t.Fatal(err)
	}
}
