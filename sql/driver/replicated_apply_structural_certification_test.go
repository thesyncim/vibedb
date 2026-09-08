package driver

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

// TestReplicatedApplyBatch64StructuralCertification exercises the split through
// the production one-entry batch-completion path. Four 64-row entries fill the
// user leaf; the fifth entry crosses its structural boundary while the real
// system, user, and transition-capture members remain group-owned. The held
// user snapshot is captured after the first committed batch, before any
// uncertified batch, and keeps nonempty old leaf bytes across the split. The
// complete prospective graph publishes at one logical generation after the
// preceding cut is certified; its physical root may remain at the prior
// durable cut until the explicit final checkpoint.
func TestReplicatedApplyBatch64StructuralCertification(t *testing.T) {
	database, claim, identity, group := newReplicatedApplyBatch64Fixture(t)
	core := database.connector.db
	core.mu.RLock()
	system := core.replicatedApplyCollection
	capture := core.replicatedCaptureCollection
	userTable := core.tables[identity.UserTable]
	if userTable == nil {
		core.mu.RUnlock()
		t.Fatal("replicated user table is unavailable")
	}
	user := userTable.collection
	core.mu.RUnlock()
	if system == nil || capture == nil || user == nil {
		t.Fatal("replicated checkpoint members are unavailable")
	}
	members := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: system},
		{Name: identity.UserTable, Collection: user},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: capture},
	}
	if !group.Owns(members) {
		t.Fatal("replicated checkpoint group does not own all three members")
	}
	var snapshot *durable.Snapshot
	var err error
	snapshotClosed := false
	defer func() {
		if !snapshotClosed {
			if snapshot != nil {
				_ = snapshot.Close()
			}
		}
	}()

	keys := make([][]byte, batch64Rows*5)
	values := make([][]byte, batch64Rows*5)
	lanes := replicatedApplyBatch64Lanes()
	var completions raftmodel.NormalApplyBatchCompletions

	applyBatch := func(batch int) {
		t.Helper()
		start := batch * batch64Rows
		batchKeys := make([][]byte, batch64Rows)
		batchValues := make([][]byte, batch64Rows)
		if err := fillReplicatedApplyBatch64Rows(database, start, batchKeys, batchValues); err != nil {
			t.Fatalf("fill batch %d: %v", batch, err)
		}
		mutations := make([]replication.Mutation, batch64Rows)
		for row := range mutations {
			keys[start+row] = bytes.Clone(batchKeys[row])
			canonical, err := vibejson.AppendCanonicalize(nil, batchValues[row])
			if err != nil {
				t.Fatalf("canonical batch %d row %d: %v", batch, row, err)
			}
			values[start+row] = canonical
			mutations[row] = replication.Mutation{
				Kind: replication.MutationPutAbsent,
				Key:  batchKeys[row], Value: batchValues[row],
			}
		}
		batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}
		command, err := appendReplicatedApplyBatch64Command(
			nil, identity, lanes[batch%len(lanes)], uint64(batch/len(lanes))+1, batches,
		)
		if err != nil {
			t.Fatalf("encode batch %d: %v", batch, err)
		}
		entries := []raftmodel.NormalApply{{
			Meta: replicatedApplyBatch64Meta(uint64(batch + 2)), Data: command,
		}}
		witnesses := make([][32]byte, 1)
		applied, publication, err := claim.ApplyNormalBatchWithCompletions(
			entries, witnesses, &completions,
		)
		if err != nil || applied != 1 || publication.Applied != uint64(batch+2) ||
			witnesses[0] == ([32]byte{}) || publication.DataChainDigest != witnesses[0] {
			t.Fatalf("batch %d apply count=%d publication=%+v witness=%x err=%v",
				batch, applied, publication, witnesses[0], err)
		}
		completion, present := completions.Completion(0)
		if !present || len(completion) == 0 {
			t.Fatalf("batch %d returned no completion", batch)
		}
		view, err := replication.OpenCompletion(completion)
		if err != nil {
			t.Fatalf("batch %d open completion: %v", batch, err)
		}
		result, err := replicatedstate.OpenTransactionCompletionResult(
			view.ResultCode, view.InlineResult,
		)
		if err != nil || view.ResultCode != replicatedstate.ResultApplied ||
			view.AppliedSequence != uint64(batch+2) ||
			!result.AffectedRowsValid || result.AffectedRows != batch64Rows {
			t.Fatalf("batch %d completion=%+v result=%+v err=%v", batch, view, result, err)
		}
	}

	applyBatch(0)
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint first batch before snapshot: %v", err)
	}
	beforeSnapshot := group.Stats()
	snapshot, err = user.Snapshot()
	if err != nil {
		t.Fatalf("post-first-batch user snapshot: %v", err)
	}
	if snapshot.Len() != batch64Rows {
		t.Fatalf("held snapshot rows = %d, want %d", snapshot.Len(), batch64Rows)
	}
	if afterSnapshot := group.Stats(); afterSnapshot != beforeSnapshot {
		t.Fatalf("snapshot changed group state: before=%+v after=%+v",
			beforeSnapshot, afterSnapshot)
	}
	for row := 0; row < batch64Rows; row++ {
		got, found, readErr := snapshot.AppendRaw(nil, keys[row])
		if readErr != nil || !found || !bytes.Equal(got, values[row]) {
			t.Fatalf("held snapshot row %d = %q/%v/%v", row, got, found, readErr)
		}
	}
	for row := batch64Rows; row < batch64Rows*5; row++ {
		got, found, readErr := snapshot.AppendRaw(nil, keys[row])
		if readErr != nil || found {
			t.Fatalf("held snapshot future row %d = %q/%v/%v, want absent", row, got, found, readErr)
		}
	}
	for batch := 1; batch < 4; batch++ {
		applyBatch(batch)
	}
	beforeSplit := group.Stats()
	if beforeSplit.AppliedIndex == 0 || beforeSplit.CheckpointAppliedIndex >= beforeSplit.AppliedIndex ||
		beforeSplit.PhysicalCheckpoints == 0 {
		t.Fatalf("pre-split group state = %+v", beforeSplit)
	}
	physicalBefore := [3]uint64{
		system.DurableGeneration(), user.DurableGeneration(), capture.DurableGeneration(),
	}
	logicalUserBefore := user.Generation()
	userSplitsBefore := user.Stats().PrimaryLeafSplits

	applyBatch(4)
	afterSplit := group.Stats()
	if afterSplit.AppliedIndex != beforeSplit.AppliedIndex+1 ||
		afterSplit.CheckpointAppliedIndex != beforeSplit.AppliedIndex ||
		afterSplit.CheckpointTransactions != beforeSplit.TransactionHighWater ||
		afterSplit.Checkpoints != beforeSplit.Checkpoints+1 ||
		afterSplit.PhysicalCheckpoints != beforeSplit.PhysicalCheckpoints {
		t.Fatalf("structural split group state before=%+v after=%+v", beforeSplit, afterSplit)
	}
	physicalAfter := [3]uint64{
		system.DurableGeneration(), user.DurableGeneration(), capture.DurableGeneration(),
	}
	if physicalAfter[0] != physicalBefore[0] || physicalAfter[2] != physicalBefore[2] ||
		physicalAfter[1] <= physicalBefore[1] {
		t.Fatalf("structural split member roots before=%v after=%v", physicalBefore, physicalAfter)
	}
	if got := user.Generation(); got != logicalUserBefore+1 {
		t.Fatalf("structural split logical generation=%d, want %d", got, logicalUserBefore+1)
	}
	if got := user.Stats().PrimaryLeafSplits; got != userSplitsBefore+1 {
		t.Fatalf("structural split count=%d, want %d", got, userSplitsBefore+1)
	}
	for row := range keys {
		got, found, readErr := user.AppendRaw(nil, keys[row])
		if readErr != nil || !found || !bytes.Equal(got, values[row]) {
			t.Fatalf("current row %d = %q/%v/%v", row, got, found, readErr)
		}
	}
	if snapshot.Len() != batch64Rows {
		t.Fatalf("held snapshot rows after split = %d, want %d", snapshot.Len(), batch64Rows)
	}
	for row := 0; row < batch64Rows; row++ {
		got, found, readErr := snapshot.AppendRaw(nil, keys[row])
		if readErr != nil || !found || !bytes.Equal(got, values[row]) {
			t.Fatalf("held snapshot row %d = %q/%v/%v", row, got, found, readErr)
		}
	}
	for row := batch64Rows; row < len(keys); row++ {
		got, found, readErr := snapshot.AppendRaw(nil, keys[row])
		if readErr != nil || found {
			t.Fatalf("held snapshot future row %d = %q/%v/%v, want absent", row, got, found, readErr)
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close held snapshot: %v", err)
	}
	snapshotClosed = true

	completionStats := claim.BatchCompletionStats()
	if completionStats.Batches != 5 || completionStats.Entries != 5 ||
		completionStats.CompleteBatches != 5 {
		t.Fatalf("batch completion stats = %+v, want five complete one-entry batches", completionStats)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("final all-member checkpoint: %v", err)
	}
	finalStats := group.Stats()
	if finalStats.CheckpointAppliedIndex != afterSplit.AppliedIndex ||
		finalStats.PhysicalCheckpoints != afterSplit.PhysicalCheckpoints+3 {
		t.Fatalf("final checkpoint state before=%+v after=%+v", afterSplit, finalStats)
	}
	finalPhysical := [3]uint64{
		system.DurableGeneration(), user.DurableGeneration(), capture.DurableGeneration(),
	}
	for index, before := range physicalAfter {
		if index == 2 {
			if finalPhysical[index] != before {
				t.Fatalf("idle capture durable generation=%d, before=%d", finalPhysical[index], before)
			}
			continue
		}
		if finalPhysical[index] <= before {
			t.Fatalf("member %d final durable generation=%d, before=%d", index, finalPhysical[index], before)
		}
	}
}
