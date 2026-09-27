package durable

import (
	"errors"
	"testing"
)

func TestPrimaryTailRouterHistoryChargeAndReaderFence(t *testing.T) {
	t.Run("metadata-only history sweep", func(t *testing.T) {
		_, members, group := newTailSplitFixture(t, 4096)
		user := members[1].Collection
		if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
			t.Fatalf("seed rows: %v", err)
		}
		if err := group.Checkpoint(); err != nil {
			t.Fatalf("checkpoint seed: %v", err)
		}
		snapshot, err := user.Snapshot()
		if err != nil {
			t.Fatalf("pin pre-split snapshot: %v", err)
		}
		t.Cleanup(func() { _ = snapshot.Close() })
		if err := writeTailSplitRows(group, members[1:], "user", 64); err != nil {
			t.Fatalf("split tail: %v", err)
		}
		if user.primaryTailSplit == nil ||
			user.primaryTailRetiredRouterBytes.Load() == 0 {
			t.Fatalf("split did not retain its old router: lineage=%v history=%d",
				user.primaryTailSplit != nil,
				user.primaryTailRetiredRouterBytes.Load())
		}
		if len(user.primaryVolatileRetired) != 0 {
			t.Fatalf("initial physical-source split retired volatile refs: %d",
				len(user.primaryVolatileRetired))
		}
		minimumPeak := user.cache.DirtyReservedBytes() +
			user.primaryTailCurrentChargeBytes.Load()
		if got := user.Stats().PrimaryTailSplitPeakChargeBytes; got < minimumPeak {
			t.Fatalf("peak charge %d omits live dirty+tail charge %d", got, minimumPeak)
		}
		if err := snapshot.Close(); err != nil {
			t.Fatalf("release pre-split snapshot: %v", err)
		}
		user.writer.Lock()
		user.clearPrimaryVolatileRetiredLocked()
		user.writer.Unlock()
		if got := user.primaryTailRetiredRouterBytes.Load(); got != 0 {
			t.Fatalf("metadata-only history sweep left %d bytes", got)
		}
		if len(user.primaryVolatileRetired) != 0 {
			t.Fatalf("metadata-only sweep unexpectedly left %d retired refs",
				len(user.primaryVolatileRetired))
		}
		lineage := user.primaryTailSplit
		if lineage == nil {
			t.Fatal("history sweep unexpectedly cleared active tail lineage")
		}
		wantCurrent := lineage.chargedBytes + lineage.routerRetainedBytes
		if got := user.primaryTailCurrentChargeBytes.Load(); got != wantCurrent {
			t.Fatalf("current tail charge after history sweep = %d, want %d",
				got, wantCurrent)
		}
	})

	t.Run("fold keeps reader history in later admission", func(t *testing.T) {
		_, members, group := newTailSplitFixture(t, 4096)
		user := members[1].Collection
		if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
			t.Fatalf("seed rows: %v", err)
		}
		if err := group.Checkpoint(); err != nil {
			t.Fatalf("checkpoint seed: %v", err)
		}
		snapshot, err := user.Snapshot()
		if err != nil {
			t.Fatalf("pin pre-split snapshot: %v", err)
		}
		t.Cleanup(func() { _ = snapshot.Close() })
		if err := writeTailSplitRows(group, members[1:], "user", 64); err != nil {
			t.Fatalf("split tail: %v", err)
		}
		if user.primaryTailRetiredRouterBytes.Load() == 0 {
			t.Fatal("tail split did not retain a reader-visible router")
		}
		if err := group.Checkpoint(); err != nil {
			t.Fatalf("fold tail lineage while reader is held: %v", err)
		}
		if user.primaryTailSplit != nil {
			t.Fatal("checkpoint left tail lineage published")
		}
		history := user.primaryTailRetiredRouterBytes.Load()
		if history == 0 || user.primaryTailCurrentChargeBytes.Load() != history {
			t.Fatalf("fold did not preserve metadata-only history charge: history=%d current=%d",
				history, user.primaryTailCurrentChargeBytes.Load())
		}
		if got := user.Stats().PrimaryTailSplitPeakChargeBytes; got <
			user.cache.DirtyReservedBytes()+history {
			t.Fatalf("peak charge %d omits folded dirty+history charge %d",
				got, user.cache.DirtyReservedBytes()+history)
		}
		// The preceding committed batch leaves scratch entries in the reusable
		// planner slices. They are not live work, and would add an unrelated
		// dirty-frame reservation to this metadata-only admission probe.
		user.writer.Lock()
		user.batchPrimaryLeaves = user.batchPrimaryLeaves[:0]
		user.batchPrimaryOverflowDirty = 0
		user.batchPrimaryOverflowPages = 0
		user.batchPrimaryOverflowVolatile = user.batchPrimaryOverflowVolatile[:0]
		user.batchPrimaryOverflowDurable = user.batchPrimaryOverflowDurable[:0]
		user.batchPrimaryAdmitted = user.batchPrimaryAdmitted[:0]
		user.writer.Unlock()

		available := user.cache.DirtyCapacityAvailable()
		if available < history {
			t.Fatalf("pre-release dirty capacity %d is below history charge %d",
				available, history)
		}
		extra := available - history + 1
		user.writer.Lock()
		_, admissionErr := user.ensurePrimaryBatchCapacityWithPolicy(
			false, extra, false,
		)
		user.writer.Unlock()
		if !errors.Is(admissionErr, ErrCheckpointGroupPressure) {
			t.Fatalf("ordinary admission ignored reader-retained history: %v", admissionErr)
		}
		if got := user.primaryTailRetiredRouterBytes.Load(); got != history {
			t.Fatalf("failed admission changed history charge: %d -> %d", history, got)
		}

		if err := snapshot.Close(); err != nil {
			t.Fatalf("release pre-split snapshot: %v", err)
		}
		user.writer.Lock()
		user.clearPrimaryVolatileRetiredLocked()
		user.writer.Unlock()
		if got := user.primaryTailRetiredRouterBytes.Load(); got != 0 {
			t.Fatalf("reader-fence sweep left historical router charge %d", got)
		}
		if got := user.primaryTailCurrentChargeBytes.Load(); got != 0 {
			t.Fatalf("reader-fence sweep left current charge %d", got)
		}
		if len(user.primaryVolatileRetired) != 0 {
			t.Fatalf("reader-fence sweep left %d volatile refs",
				len(user.primaryVolatileRetired))
		}
		user.writer.Lock()
		_, noHistoryErr := user.ensurePrimaryBatchCapacityWithPolicy(
			false, extra, false,
		)
		user.writer.Unlock()
		if noHistoryErr != nil {
			t.Fatalf("same admission failed after reader-retained history drained: %v", noHistoryErr)
		}
	})
}
