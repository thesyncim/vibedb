package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
)

const replicatedProjectedFieldRows = 8192

type replicatedProjectedFieldRow struct {
	id      string
	doc     []byte
	values  []string
	deleted bool
}

// replicatedProjectedFieldDocument retains the RF3 benchmark shape while
// adding scalar values that catch lossy projection: the logical id is the
// source JSON field, while the mutation key is derived by the normal driver
// helper. The payload includes escaped JSON text but remains inline.
func replicatedProjectedFieldDocument(row int, updated bool) []byte {
	id := fmt.Sprintf("row-%08d", row)
	bucket := row % 16
	score := row%100 + 7
	if updated {
		score += 1000
	}
	name := fmt.Sprintf("name-%08d", row)
	if row&3 == 1 {
		name = fmt.Sprintf("escaped-%08d \" / \\\\ \\n", row)
	}
	wide := int64(1<<53) + int64(row)
	if row&1 != 0 {
		wide = -wide
	}
	decimal := []string{"1.50", "-0.25", "9007199254740993.000", "3e-2"}[row&3]
	active := row&1 == 0
	return fmt.Appendf(nil,
		`{"id":%q,"bucket":%d,"score":%d,"payload":%q,"name":%q,"wide":%d,"decimal":%s,"active":%t,"nullable":null}`,
		id, bucket, score, fmt.Sprintf("payload-%08d", row), name, wide, decimal, active,
	)
}

func replicatedProjectedFieldRowsWant() []replicatedProjectedFieldRow {
	want := make([]replicatedProjectedFieldRow, replicatedProjectedFieldRows)
	for row := range want {
		id := fmt.Sprintf("row-%08d", row)
		want[row] = replicatedProjectedFieldRow{
			id:  id,
			doc: replicatedProjectedFieldDocument(row, false),
			values: []string{
				fmt.Sprintf(`"%s"`, id),
				fmt.Sprintf("%d", row%16),
				fmt.Sprintf("%d", row%100+7),
				fmt.Sprintf(`"payload-%08d"`, row),
			},
		}
	}
	return want
}

// applyReplicatedProjectedFieldRows uses the real bounded RF3 apply path. A
// command may carry at most 64 distinct mutations in this relation, so each
// batch is published with the same shape as the production fixture rather
// than issuing one durable commit per row.
func applyReplicatedProjectedFieldRows(
	t *testing.T,
	database *Database,
	claim *ReplicatedApply,
	base ReplicatedShardStoreIdentity,
	epoch uint64,
	startIndex uint64,
	want []replicatedProjectedFieldRow,
) uint64 {
	t.Helper()
	const batchSize = 64
	last := startIndex
	for start := 0; start < len(want); start += batchSize {
		end := min(start+batchSize, len(want))
		mutations := make([]replication.Mutation, 0, end-start)
		for row := start; row < end; row++ {
			doc := want[row].doc
			mutations = append(mutations, replication.Mutation{
				Kind:  replication.MutationPut,
				Key:   testReplicatedApplyKey(t, database, doc),
				Value: doc,
			})
		}
		index := startIndex + uint64(start/batchSize) + 1
		publication, err := claim.ApplyNormal(
			testReplicatedApplyMeta(index),
			testReplicatedApplyCommand(base, epoch, uint64(start/batchSize)+2, mutations...),
		)
		if err != nil || publication.Applied != index {
			t.Fatalf("apply projection batch %d = %+v, %v; want applied %d",
				start/batchSize, publication, err, index)
		}
		last = index
	}
	return last
}

func newReplicatedProjectedFieldReader(
	t *testing.T,
	ctx context.Context,
	claim *ReplicatedApply,
	cut *replicatedstate.DataReadCut,
	cancel *query.CancelFlag,
) *ReplicatedReadSession {
	t.Helper()
	reader, err := claim.NewDataReadSession(ctx, cut, query.ExecOptions{
		Workers: 1,
		Cancel:  cancel,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reader.conn.exec.Options.Cancel != cancel {
		t.Fatal("projection reader lost its nonnil CancelFlag")
	}
	return reader
}

func replicatedProjectedFieldExpectedAfterUpdate(
	want []replicatedProjectedFieldRow,
	row int,
	updated bool,
) []string {
	values := append([]string(nil), want[row].values...)
	if updated {
		values[2] = fmt.Sprintf("%d", row%100+1007)
	}
	return values
}

// queryReplicatedProjectedFields exercises the SQL boundary over a real
// borrowed RF3 cut. The range and ORDER BY are both on the declared primary
// JSON field, so the query layer can hand the immutable snapshot to the
// storage projection lane without reconstructing each document.
func queryReplicatedProjectedFields(
	t *testing.T,
	reader *ReplicatedReadSession,
	ctx context.Context,
	want []replicatedProjectedFieldRow,
) {
	t.Helper()
	upperID := fmt.Sprintf("row-%08d", len(want))
	statement, err := reader.Prepare(ctx, fmt.Sprintf(`
		SELECT id, bucket, score, payload
		FROM docs
		WHERE id >= 'row-00000000' AND id < '%s'
		ORDER BY id
		LIMIT %d`, upperID, len(want)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := statement.Close(); err != nil {
			t.Errorf("close projected statement: %v", err)
		}
	}()
	var cursor Cursor
	if err := statement.QueryInto(ctx, nil, &cursor); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cursor.Close(); err != nil {
			t.Errorf("close projected cursor: %v", err)
		}
	}()

	if !reader.conn.exec.Stats.PrimaryRangeBounded {
		t.Fatalf("projected SQL did not use a bounded primary range: %+v", reader.conn.exec.Stats)
	}
	if got := uint64(reader.conn.exec.Stats.ProjectedRows); got != uint64(len(want)) {
		t.Fatalf("projected rows=%d, want %d; stats=%+v", got, len(want), reader.conn.exec.Stats)
	}
	row := 0
	for cursor.Next() {
		if row >= len(want) {
			t.Fatalf("projected cursor returned more than %d rows", len(want))
		}
		expected := want[row]
		id, idOK := cursor.Cell(0).Text()
		bucket, bucketOK := cursor.Cell(1).Int64()
		score, scoreOK := cursor.Cell(2).Int64()
		payload, payloadOK := cursor.Cell(3).Text()
		scoreJSON := fmt.Sprintf("%d", score)
		if !idOK || !bucketOK || !scoreOK || !payloadOK {
			t.Fatalf("row %d cell types=(%s,%s,%s,%s), want id/bucket/score/payload",
				row, cursor.Cell(0).JSON(), cursor.Cell(1).JSON(),
				cursor.Cell(2).JSON(), cursor.Cell(3).JSON())
		}
		if id != expected.id || bucket != int64(row%16) ||
			scoreJSON != expected.values[2] ||
			payload != fmt.Sprintf("payload-%08d", row) {
			t.Fatalf("projected row %d=(%q,%d,%d,%q), want id=%q bucket=%s score=%s payload=%q",
				row, id, bucket, score, payload, expected.id,
				expected.values[1], expected.values[2], expected.values[3])
		}
		row++
	}
	if row != len(want) {
		t.Fatalf("projected rows=%d, want %d", row, len(want))
	}
}

func TestReplicatedReadSessionUsesPrimaryProjectionLane(t *testing.T) {
	_, database, binding, _ := prepareReplicatedTestRoot(t, "sql-replicated-projected-fields", false)
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
	allWant := replicatedProjectedFieldRowsWant()
	want := allWant[:64]
	initialApplied := applyReplicatedProjectedFieldRows(t, database, claim, base, epoch, 2, allWant)

	ctx := context.Background()
	var oldCut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, initialApplied, &oldCut); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := oldCut.Close(); err != nil {
			t.Errorf("close old data cut: %v", err)
		}
	}()
	var oldCancel query.CancelFlag
	oldReader := newReplicatedProjectedFieldReader(t, ctx, claim, &oldCut, &oldCancel)
	queryReplicatedProjectedFields(t, oldReader, ctx, want)
	if err := oldReader.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot, ok := oldCut.Relation(1); !ok || snapshot == nil {
		t.Fatal("closing old projected reader closed its caller-owned cut")
	}

	updatedRow := 16
	updatedDoc := replicatedProjectedFieldDocument(updatedRow, true)
	updateIndex := initialApplied + 1
	if _, err := claim.ApplyNormal(
		testReplicatedApplyMeta(updateIndex),
		testReplicatedApplyCommand(base, epoch, initialApplied, replication.Mutation{
			Kind:  replication.MutationPut,
			Key:   testReplicatedApplyKey(t, database, updatedDoc),
			Value: updatedDoc,
		}),
	); err != nil {
		t.Fatalf("apply projected update: %v", err)
	}

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
	newReader := newReplicatedProjectedFieldReader(t, ctx, claim, &newCut, &newCancel)
	freshWant := append([]replicatedProjectedFieldRow(nil), want...)
	freshWant[updatedRow].values = replicatedProjectedFieldExpectedAfterUpdate(want, updatedRow, true)
	queryReplicatedProjectedFields(t, newReader, ctx, freshWant)
	if err := newReader.Close(); err != nil {
		t.Fatal(err)
	}

	var oldAgainCancel query.CancelFlag
	oldAgainReader := newReplicatedProjectedFieldReader(t, ctx, claim, &oldCut, &oldAgainCancel)
	queryReplicatedProjectedFields(t, oldAgainReader, ctx, want)
	if err := oldAgainReader.Close(); err != nil {
		t.Fatal(err)
	}
}
