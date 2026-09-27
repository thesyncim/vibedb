package durable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestCheckpointGroupTailFittingAppendsPreserveTopologyAndCheckpointCut(t *testing.T) {
	dir, members, group := newTailSplitFixture(t, 4096)
	user := members[1].Collection
	startTailFittingLineage(t, members, group)
	if got := len(user.primaryTailSplit.leaves); got < 2 {
		t.Fatalf("initial tail lineage leaves = %d, want at least 2", got)
	}

	for _, fit := range []struct {
		start int
		count int
	}{
		{start: 128, count: 1},
		{start: 129, count: 8},
	} {
		beforeStats := group.Stats()
		beforeUserStats := user.Stats()
		beforeGeneration := user.Generation()
		beforeBuckets := tailFittingRouteBuckets(t, user)
		beforeLocals := tailFittingLineageLocalIDs(user)
		if err := appendTailFittingRows(group, members, fit.start, fit.count); err != nil {
			t.Fatalf("append fitting rows [%d,%d): %v", fit.start,
				fit.start+fit.count, err)
		}
		if got := user.Generation(); got != beforeGeneration+1 {
			t.Fatalf("fitting append generation %d -> %d, want exactly one", beforeGeneration, got)
		}
		afterStats := group.Stats()
		afterUserStats := user.Stats()
		if afterStats.CheckpointAppliedIndex != beforeStats.CheckpointAppliedIndex ||
			afterStats.CheckpointTransactions != beforeStats.CheckpointTransactions ||
			afterStats.CertificateSyncs != beforeStats.CertificateSyncs ||
			afterStats.PhysicalCheckpoints != beforeStats.PhysicalCheckpoints {
			t.Fatalf("fitting append forced a checkpoint: before=%+v after=%+v",
				beforeStats, afterStats)
		}
		if afterUserStats.DeviceCommits != beforeUserStats.DeviceCommits {
			t.Fatalf("fitting append issued device commits: before=%d after=%d",
				beforeUserStats.DeviceCommits, afterUserStats.DeviceCommits)
		}
		if got := tailFittingRouteBuckets(t, user); !equalTailFittingBuckets(beforeBuckets, got) {
			t.Fatalf("fitting append changed route topology: before=%v after=%v",
				beforeBuckets, got)
		}
		if got := tailFittingLineageLocalIDs(user); !equalTailFittingLocals(beforeLocals, got) {
			t.Fatalf("fitting append changed descendant identities: before=%v after=%v",
				beforeLocals, got)
		}
		for row := 0; row < fit.start+fit.count; row++ {
			requireTailSplitRow(t, user, row)
		}
	}

	const rowCount = 137
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint fitting tail rows: %v", err)
	}
	assertTailSplitPersistedVerify(t, user, rowCount)
	image := copyCheckpointGroupDirectory(t, dir)
	reopened, reopenedLog, reopenedGroup, files := openTailCheckpointGroupCopy(
		t, image, tailFittingTestOptions(),
	)
	t.Cleanup(func() {
		_ = reopenedGroup.Close()
		for _, collection := range reopened {
			_ = collection.Close()
		}
		_ = reopenedLog.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
	assertTailSplitPersistedVerify(t, reopened[1], rowCount)
	for row := 0; row < rowCount; row++ {
		requireTailSplitRow(t, reopened[1], row)
	}
}

func TestCheckpointGroupTailFitQualificationDoesNotLeakAcrossBatches(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, []NamedCollection, *CheckpointGroup) (map[string][]byte, uint64)
	}{
		{
			name: "same-tail-replacement",
			mutate: func(t *testing.T, members []NamedCollection, group *CheckpointGroup) (map[string][]byte, uint64) {
				const row = 128
				key := []byte(fmt.Sprintf("tail-row-%08d", row))
				value := []byte(`{"n":128,"payload":"replacement"}`)
				if err := updateTailFittingBatch(group, members, 1, func(write *WriteBatch) error {
					return write.Put(key, value)
				}); err != nil {
					t.Fatalf("replace tail row: %v", err)
				}
				want := tailFittingRows(129)
				want[string(key)] = value
				return want, 1
			},
		},
		{
			name: "mixed-replacement-and-append",
			mutate: func(t *testing.T, members []NamedCollection, group *CheckpointGroup) (map[string][]byte, uint64) {
				const replacementRow = 128
				replacementKey := []byte(fmt.Sprintf("tail-row-%08d", replacementRow))
				replacementValue := []byte(`{"n":128,"payload":"mixed replacement"}`)
				appendKey := []byte(fmt.Sprintf("tail-row-%08d", replacementRow+1))
				appendValue := tailSplitQualificationValue(replacementRow + 1)
				if err := updateTailFittingBatch(group, members, 2, func(write *WriteBatch) error {
					if err := write.Put(replacementKey, replacementValue); err != nil {
						return err
					}
					return write.Put(appendKey, appendValue)
				}); err != nil {
					t.Fatalf("replace and append tail rows: %v", err)
				}
				want := tailFittingRows(130)
				want[string(replacementKey)] = replacementValue
				return want, 2
			},
		},
		{
			name: "absent-interior-key-insert",
			mutate: func(t *testing.T, members []NamedCollection, group *CheckpointGroup) (map[string][]byte, uint64) {
				user := members[1].Collection
				lower, interior := tailFittingInteriorKey(t, user, 129)
				value := []byte(`{"n":129,"payload":"interior"}`)
				if err := updateTailFittingBatch(group, members, 1, func(write *WriteBatch) error {
					return write.Put(interior, value)
				}); err != nil {
					t.Fatalf("insert absent interior key after %q: %v", lower, err)
				}
				want := tailFittingRows(129)
				want[string(interior)] = value
				return want, 1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, members, group := newTailSplitFixture(t, 4096)
			user := members[1].Collection
			startTailFittingLineage(t, members, group)
			fitBefore := group.Stats()
			fitBeforeUser := user.Stats()
			fitGeneration := user.Generation()
			fitBuckets := tailFittingRouteBuckets(t, user)
			if err := appendTailFittingRows(group, members, 128, 1); err != nil {
				t.Fatalf("valid fitting append before invalidating batch: %v", err)
			}
			if user.Generation() != fitGeneration+1 ||
				!equalTailFittingBuckets(fitBuckets, tailFittingRouteBuckets(t, user)) {
				t.Fatalf("valid fit did not stay within current topology: generation=%d->%d buckets=%v->%v",
					fitGeneration, user.Generation(), fitBuckets,
					tailFittingRouteBuckets(t, user))
			}
			fitAfter := group.Stats()
			fitAfterUser := user.Stats()
			if fitAfter.CheckpointAppliedIndex != fitBefore.CheckpointAppliedIndex ||
				fitAfter.CheckpointTransactions != fitBefore.CheckpointTransactions ||
				fitAfter.CertificateSyncs != fitBefore.CertificateSyncs ||
				fitAfter.PhysicalCheckpoints != fitBefore.PhysicalCheckpoints {
				t.Fatalf("valid fitting append forced a checkpoint: before=%+v after=%+v",
					fitBefore, fitAfter)
			}
			if fitAfterUser.DeviceCommits != fitBeforeUser.DeviceCommits {
				t.Fatalf("valid fitting append issued device commits: before=%d after=%d",
					fitBeforeUser.DeviceCommits, fitAfterUser.DeviceCommits)
			}
			baseline := group.Stats()
			wantRows, span := test.mutate(t, members, group)
			after := group.Stats()
			wantApplied := baseline.AppliedIndex + span
			if after.PressureCheckpoints != baseline.PressureCheckpoints+1 ||
				after.CheckpointAppliedIndex != baseline.AppliedIndex ||
				after.AppliedIndex != wantApplied || user.primaryTailSplit != nil {
				t.Fatalf("invalidating batch bypassed prior pressure/fallback: before=%+v after=%+v lineage=%+v",
					baseline, after, user.primaryTailSplit)
			}
			assertTailFittingPointRows(t, user, wantRows)
			if err := group.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after invalidating batch: %v", err)
			}
			assertTailSplitPersistedVerify(t, user, len(wantRows))
			assertTailFittingRows(t, user, wantRows)
		})
	}
}

func startTailFittingLineage(
	t *testing.T, members []NamedCollection, group *CheckpointGroup,
) {
	t.Helper()
	if err := writeTailSplitRows(group, members[1:], "user", 0); err != nil {
		t.Fatalf("seed tail-fitting rows: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatalf("checkpoint tail-fitting seed: %v", err)
	}
	if err := writeTailSplitRows(group, members[1:], "user", 64); err != nil {
		t.Fatalf("split tail-fitting rows: %v", err)
	}
	if user := members[1].Collection; user.primaryTailSplit == nil {
		t.Fatal("first rightmost append did not establish a volatile tail lineage")
	}
}

func appendTailFittingRows(
	group *CheckpointGroup, members []NamedCollection, start, count int,
) error {
	return updateTailFittingBatch(group, members, uint64(count), func(write *WriteBatch) error {
		for row := start; row < start+count; row++ {
			if err := write.Put(
				[]byte(fmt.Sprintf("tail-row-%08d", row)),
				tailSplitQualificationValue(row),
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func updateTailFittingBatch(
	group *CheckpointGroup,
	members []NamedCollection,
	span uint64,
	mutate func(*WriteBatch) error,
) error {
	first := group.AppliedIndex() + 1
	return group.UpdateConsecutive(
		first, first+span-1, members[1:], defaultTxnLimits(),
		func(batch *DatabaseBatch) error {
			write, err := batch.Collection("user")
			if err != nil {
				return err
			}
			return mutate(write)
		},
	)
}

func tailFittingTestOptions() Options {
	options := txnTestOptions()
	options.MaxBatchDocuments = structuralCertificationBatchRows
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 2048
	options.MaxDocumentBytes = 2048
	options.ResidentBytes = 64 << 20
	return options
}

func tailFittingRows(count int) map[string][]byte {
	rows := make(map[string][]byte, count)
	for row := 0; row < count; row++ {
		rows[fmt.Sprintf("tail-row-%08d", row)] = tailSplitQualificationValue(row)
	}
	return rows
}

func assertTailFittingRows(t *testing.T, collection *Collection, want map[string][]byte) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatalf("snapshot fitting-tail oracle: %v", err)
	}
	defer snapshot.Close()
	got := make(map[string][]byte, len(want))
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		got[string(key)] = append([]byte(nil), value...)
		return nil
	}); err != nil {
		t.Fatalf("range fitting-tail oracle: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("fitting-tail row count = %d, want %d", len(got), len(want))
	}
	for key, expected := range want {
		if value, ok := got[key]; !ok || !bytes.Equal(value, expected) {
			t.Fatalf("fitting-tail row %q = %q/%v, want %q", key, value, ok, expected)
		}
	}
}

func assertTailFittingPointRows(t *testing.T, collection *Collection, want map[string][]byte) {
	t.Helper()
	for key, expected := range want {
		value, found, err := collection.AppendRaw(nil, []byte(key))
		if err != nil || !found || !bytes.Equal(value, expected) {
			t.Fatalf("fitting-tail point read %q = %q/%v/%v, want %q",
				key, value, found, err, expected)
		}
	}
}

func tailFittingRouteBuckets(t *testing.T, collection *Collection) []storeio.BucketID {
	t.Helper()
	router := collection.primaryRouter.Load()
	if router == nil {
		t.Fatal("missing resident primary router")
	}
	buckets := make([]storeio.BucketID, router.Len())
	for rank := range buckets {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("resident route rank %d is missing", rank)
		}
		buckets[rank] = route.Bucket
	}
	return buckets
}

func tailFittingLineageLocalIDs(collection *Collection) []uint16 {
	if collection.primaryTailSplit == nil {
		return nil
	}
	localIDs := make([]uint16, len(collection.primaryTailSplit.leaves))
	for index := range collection.primaryTailSplit.leaves {
		localIDs[index] = collection.primaryTailSplit.leaves[index].localID
	}
	return localIDs
}

func equalTailFittingBuckets(left, right []storeio.BucketID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalTailFittingLocals(left, right []uint16) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func tailFittingInteriorKey(
	t *testing.T, collection *Collection, rowCount int,
) ([]byte, []byte) {
	t.Helper()
	lineage := collection.primaryTailSplit
	if lineage == nil || len(lineage.leaves) < 2 {
		t.Fatalf("interior-key fixture has no active split lineage: %+v", lineage)
	}
	bucket, ok := storeio.MakeTabletLocalIdentityBucket(
		lineage.tabletID, uint32(lineage.leaves[len(lineage.leaves)-1].localID),
	)
	if !ok {
		t.Fatal("invalid trailing descendant bucket")
	}
	var prior int = -2
	var lower []byte
	for row := 0; row < rowCount; row++ {
		key := []byte(fmt.Sprintf("tail-row-%08d", row))
		route, routeOK := collection.primaryRouter.Load().Route(key)
		if !routeOK || route.Bucket != storeio.BucketID(bucket) {
			continue
		}
		if prior+1 == row {
			interior := append(append([]byte(nil), lower...), 'a')
			middleRoute, middleOK := collection.primaryRouter.Load().Route(interior)
			if !middleOK || middleRoute.Bucket != storeio.BucketID(bucket) {
				t.Fatalf("interior key %q route=%v/%v, want trailing bucket %d",
					interior, middleRoute, middleOK, bucket)
			}
			if bytes.Compare(lower, interior) >= 0 || bytes.Compare(interior, key) >= 0 {
				t.Fatalf("candidate key %q is not strictly between %q and %q", interior, lower, key)
			}
			return lower, interior
		}
		prior = row
		lower = key
	}
	t.Fatal("no adjacent existing rows in trailing descendant for interior insertion")
	return nil, nil
}
