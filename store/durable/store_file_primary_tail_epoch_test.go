package durable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestCheckpointGroupTailSplitRetainsVolatileFrameForDirectEpoch(t *testing.T) {
	options := txnTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 4 << 10
	options.MaxDocumentBytes = 4 << 10
	options.ResidentBytes = 64 << 20
	if _, err := options.normalized(); err != nil {
		t.Fatalf("tail epoch options: %v", err)
	}

	dir := t.TempDir()
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", options),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: 4096,
	})
	if err != nil {
		_ = log.Close()
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() {
		_ = group.Close()
		_ = log.Close()
	})
	user := members[1].Collection

	writeRows := func(start int) error {
		first := uint64(start + 1)
		last := first + structuralCertificationBatchRows - 1
		return group.UpdateConsecutive(
			first, last, members[1:], defaultTxnLimits(),
			func(batch *DatabaseBatch) error {
				write, err := batch.Collection("user")
				if err != nil {
					return err
				}
				for row := start; row < start+structuralCertificationBatchRows; row++ {
					key := []byte(fmt.Sprintf("epoch-tail-row-%08d", row))
					if err := write.Put(key, tailEpochQualificationValue(row)); err != nil {
						return err
					}
				}
				return nil
			},
		)
	}
	if err := writeRows(0); err != nil {
		t.Fatalf("write initial rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint initial rows: %v", err)
	}

	next := structuralCertificationBatchRows
	for attempts := 0; attempts < 4 && user.primaryTailSplit == nil; attempts++ {
		if err := writeRows(next); err != nil {
			t.Fatalf("build tail lineage at row %d: %v", next, err)
		}
		next += structuralCertificationBatchRows
	}
	if user.primaryTailSplit == nil || len(user.primaryTailSplit.leaves) < 2 {
		t.Fatalf("tail lineage after warm batches = %+v", user.primaryTailSplit)
	}
	if len(user.primaryVolatileRetired) != 0 ||
		user.primaryTailRetiredRouterBytes.Load() != 0 {
		t.Fatalf("retired state before held epoch = refs %v router bytes %d",
			user.primaryVolatileRetired,
			user.primaryTailRetiredRouterBytes.Load())
	}

	view, epoch, ok := user.enterReadEpoch()
	if !ok {
		t.Fatal("enter direct read epoch")
	}
	epochActive := true
	defer func() {
		if epochActive {
			epoch.Exit()
		}
	}()
	oldRouter := user.primaryRouter.Load()
	if oldRouter == nil {
		t.Fatal("missing current resident router")
	}
	if oldRouter.Generation() != view.state.root.Generation {
		t.Fatalf("epoch/router generation = %d/%d", view.state.root.Generation,
			oldRouter.Generation())
	}
	oldKey := []byte(fmt.Sprintf("epoch-tail-row-%08d", next-1))
	oldRoute, ok := oldRouter.Route(oldKey)
	if !ok {
		t.Fatalf("route current trailing key %q", oldKey)
	}
	oldLineage := user.primaryTailSplit
	oldTail := oldLineage.leaves[len(oldLineage.leaves)-1]
	if oldRoute.Ref != oldTail.volatile {
		t.Fatalf("captured trailing route ref=%+v lineage=%+v",
			oldRoute.Ref, oldTail.volatile)
	}
	oldRef := oldRoute.Ref
	oldRouterGeneration := oldRouter.Generation()
	oldRouterBytes := uint64(oldRouter.ResidentBytes())

	if err := writeRows(next); err != nil {
		t.Fatalf("force later tail split at row %d: %v", next, err)
	}
	newRouter := user.primaryRouter.Load()
	if newRouter == oldRouter || newRouter.Len() <= oldRouter.Len() ||
		oldRouter.Generation() != oldRouterGeneration {
		t.Fatalf("later split did not retain old router: old=%p/%d gen=%d new=%p/%d",
			oldRouter, oldRouter.Len(), oldRouter.Generation(), newRouter, newRouter.Len())
	}
	newRoute, ok := newRouter.Route(oldKey)
	if !ok || newRoute.Ref == oldRef {
		t.Fatalf("old key route after later split = %+v/%v, old ref %+v",
			newRoute, ok, oldRef)
	}
	if len(user.primaryVolatileRetired) != 1 ||
		user.primaryVolatileRetired[0] != oldRef {
		t.Fatalf("retired volatile refs after split = %v, want [%+v]",
			user.primaryVolatileRetired, oldRef)
	}
	historicalRouterBytes := user.primaryTailRetiredRouterBytes.Load()
	if historicalRouterBytes == 0 || historicalRouterBytes != oldRouterBytes ||
		user.primaryTailCurrentChargeBytes.Load() <= historicalRouterBytes {
		t.Fatalf("split historical/current router charge = %d/%d want historical %d",
			historicalRouterBytes, user.primaryTailCurrentChargeBytes.Load(),
			oldRouterBytes)
	}

	oldLease, err := oldRouter.AcquireLeaf(user.cache, oldRoute)
	if err != nil {
		t.Fatalf("acquire old route while direct epoch is held: %v", err)
	}
	oldLeaf, admitted := storeio.AdmittedCompactPrimaryStripe(
		oldLease.Page(), user.storeID, oldRoute.Bucket,
	)
	if !admitted {
		oldLease.Release()
		t.Fatal("old route frame is not an admitted compact primary leaf")
	}
	rank, found := oldLeaf.FindKey(oldKey)
	if !found {
		oldLease.Release()
		t.Fatalf("old route leaf lacks captured key %q", oldKey)
	}
	oldValue, decoded := oldLeaf.AppendValue(nil, rank)
	oldLease.Release()
	wantValue := tailEpochQualificationValue(next - 1)
	if !found || !decoded || !bytes.Equal(oldValue, wantValue) {
		t.Fatalf("old route value after split = %q/%v/%v, want %q",
			oldValue, found, decoded, wantValue)
	}

	cacheBeforeSweep := user.cache.Stats()
	currentChargeBeforeSweep := user.primaryTailCurrentChargeBytes.Load()
	epoch.Exit()
	epochActive = false
	user.writer.Lock()
	user.clearPrimaryVolatileRetiredLocked()
	user.writer.Unlock()
	cacheAfterSweep := user.cache.Stats()
	if len(user.primaryVolatileRetired) != 0 ||
		user.primaryTailRetiredRouterBytes.Load() != 0 {
		t.Fatalf("retired state after direct-reader sweep = refs %v router bytes %d",
			user.primaryVolatileRetired,
			user.primaryTailRetiredRouterBytes.Load())
	}
	if cacheBeforeSweep.ResidentBytes < uint64(oldRef.Length) ||
		cacheBeforeSweep.ResidentBytes-cacheAfterSweep.ResidentBytes != uint64(oldRef.Length) {
		t.Fatalf("retired frame residency before/after sweep = %d/%d, want delta %d",
			cacheBeforeSweep.ResidentBytes, cacheAfterSweep.ResidentBytes,
			oldRef.Length)
	}
	currentChargeAfterSweep := user.primaryTailCurrentChargeBytes.Load()
	if currentChargeBeforeSweep < historicalRouterBytes ||
		currentChargeBeforeSweep-currentChargeAfterSweep != historicalRouterBytes {
		t.Fatalf("historical router charge drain before/after=%d/%d historical=%d",
			currentChargeBeforeSweep, currentChargeAfterSweep, historicalRouterBytes)
	}
}

func tailEpochQualificationValue(row int) []byte {
	return fmt.Appendf(
		nil, `{"n":%d,"payload":%q}`,
		row, structuralCertificationPayload(row, 2400),
	)
}
