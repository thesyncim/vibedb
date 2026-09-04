package durable

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestCheckpointGroupReplacementBatchUsesRowOverlay(t *testing.T) {
	dir, members, log, group := newCheckpointGroupOverlayTestStore(t, 8)
	const rows = 24
	checkpointGroupOverlayReplace(t, group, 1, members, rows, `{"v":1,"w":10}`)
	for _, member := range members {
		for row := 0; row < rows; row++ {
			got, found, err := member.Collection.AppendRaw(
				nil, []byte(testCheckpointOverlayKey(row)),
			)
			if err != nil || !found || string(got) != `{"v":1,"w":10}` {
				t.Fatalf("member %q seeded routed row %d = %q/%v/%v",
					member.Name, row, got, found, err)
			}
		}
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := members[0].Collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	checkpointGroupOverlayReplace(t, group, 2, members, rows, `{"v":2,"w":10}`)
	type scalarCertificate struct {
		base       storeio.PageRef
		shape      uint16
		hole       uint16
		fixedBytes uint32
	}
	certificates := make(map[string]scalarCertificate, len(members))
	for _, member := range members {
		route, routeErr := member.Collection.currentPrimaryResidentRoute(
			member.Collection.state.Load(), []byte(testCheckpointOverlayKey(0)),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		bucketSlot, found := member.Collection.primaryUnifiedOverlay.bucketSlot(route.Bucket)
		if !found {
			t.Fatalf("member %q lost first overlay bucket", member.Name)
		}
		bucket := &member.Collection.primaryUnifiedOverlay.buckets[bucketSlot]
		if !bucket.scalarSet || bucket.scalarBase != route.Ref ||
			bucket.scalarFixedBytes == 0 {
			t.Fatalf("member %q lost first scalar fold certificate", member.Name)
		}
		certificates[member.Name] = scalarCertificate{
			base: bucket.scalarBase, shape: bucket.scalarShape,
			hole: bucket.scalarHole, fixedBytes: bucket.scalarFixedBytes,
		}
	}

	checkpointGroupOverlayReplace(t, group, 3, members, rows, `{"v":3,"w":10}`)
	for _, member := range members {
		if got := member.Collection.primaryUnifiedOverlay.count.Load(); got != 2*rows {
			t.Fatalf("member %q overlay records = %d, want %d",
				member.Name, got, 2*rows)
		}
		for row := 0; row < rows; row++ {
			got, found, readErr := member.Collection.AppendRaw(
				nil, []byte(testCheckpointOverlayKey(row)),
			)
			if readErr != nil || !found || string(got) != `{"v":3,"w":10}` {
				t.Fatalf("member %q overlay row %d = %q/%v/%v",
					member.Name, row, got, found, readErr)
			}
		}
		route, routeErr := member.Collection.currentPrimaryResidentRoute(
			member.Collection.state.Load(), []byte(testCheckpointOverlayKey(0)),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		bucketSlot, found := member.Collection.primaryUnifiedOverlay.bucketSlot(
			route.Bucket,
		)
		bucket := &member.Collection.primaryUnifiedOverlay.buckets[bucketSlot]
		want := certificates[member.Name]
		if !found || !bucket.scalarSet || bucket.scalarBase != want.base ||
			bucket.scalarShape != want.shape || bucket.scalarHole != want.hole ||
			bucket.scalarFixedBytes != want.fixedBytes {
			t.Fatalf("member %q did not reuse scalar fold certificate", member.Name)
		}
	}
	if got, found, readErr := old.AppendRaw(
		nil, []byte(testCheckpointOverlayKey(0)),
	); readErr != nil || !found || string(got) != `{"v":1,"w":10}` {
		t.Fatalf("old pinned row before fold = %q/%v/%v", got, found, readErr)
	}

	generationBeforeColumnChange := make(map[string]uint64, len(members))
	for _, member := range members {
		generationBeforeColumnChange[member.Name] = member.Collection.Generation()
	}
	checkpointGroupOverlayReplace(t, group, 4, members, rows, `{"v":3,"w":11}`)
	for _, member := range members {
		overlay := member.Collection.primaryUnifiedOverlay
		wantRecords := rows
		if member.Collection == members[0].Collection {
			wantRecords = 3 * rows
		}
		if got := int(overlay.count.Load()); got != wantRecords {
			t.Fatalf("member %q retained overlay records = %d, want %d",
				member.Name, got, wantRecords)
		}
		folded := overlay.folded.Load()
		wantFolded := uint64(0)
		if member.Collection == members[0].Collection {
			wantFolded = generationBeforeColumnChange[member.Name]
		}
		if folded != wantFolded ||
			member.Collection.Generation() !=
				generationBeforeColumnChange[member.Name]+1 ||
			!overlay.hasPending() {
			t.Fatalf("member %q different-column fold = before %d folded %d current %d pending %v",
				member.Name, generationBeforeColumnChange[member.Name], folded,
				member.Collection.Generation(), overlay.hasPending())
		}
		for row := 0; row < rows; row++ {
			got, found, readErr := member.Collection.AppendRaw(
				nil, []byte(testCheckpointOverlayKey(row)),
			)
			if readErr != nil || !found || string(got) != `{"v":3,"w":11}` {
				t.Fatalf("member %q new-column row %d = %q/%v/%v",
					member.Name, row, got, found, readErr)
			}
		}
		route, routeErr := member.Collection.currentPrimaryResidentRoute(
			member.Collection.state.Load(), []byte(testCheckpointOverlayKey(0)),
		)
		if routeErr != nil {
			t.Fatal(routeErr)
		}
		bucketSlot, found := overlay.bucketSlot(route.Bucket)
		bucket := &overlay.buckets[bucketSlot]
		oldCertificate := certificates[member.Name]
		if !found || !bucket.scalarSet || bucket.scalarBase != route.Ref ||
			bucket.scalarBase == oldCertificate.base ||
			bucket.scalarShape != oldCertificate.shape ||
			bucket.scalarHole == oldCertificate.hole {
			t.Fatalf("member %q did not replace the scalar-column certificate", member.Name)
		}
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if member.Collection.primaryUnifiedOverlay.hasPending() {
			t.Fatalf("member %q retained overlay after checkpoint", member.Name)
		}
	}
	if got, found, readErr := old.AppendRaw(
		nil, []byte(testCheckpointOverlayKey(0)),
	); readErr != nil || !found || string(got) != `{"v":1,"w":10}` {
		t.Fatalf("old pinned row after folds = %q/%v/%v", got, found, readErr)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	old = nil

	collections := make([]*Collection, len(members))
	for index := range members {
		collections[index] = members[index].Collection
	}
	closeCheckpointGroupTestHandles(t, collections, log, group)
	reopened, reopenedLog, reopenedGroup := openCheckpointGroupTestCopy(t, dir)
	for index, collection := range reopened {
		for row := 0; row < rows; row++ {
			got, found, readErr := collection.AppendRaw(
				nil, []byte(testCheckpointOverlayKey(row)),
			)
			if readErr != nil || !found || string(got) != `{"v":3,"w":11}` {
				t.Fatalf("reopened member %d row %d = %q/%v/%v",
					index, row, got, found, readErr)
			}
		}
	}
	closeCheckpointGroupTestHandles(t, reopened, reopenedLog, reopenedGroup)
}

func checkpointGroupOverlayReplace(
	t testing.TB, group *CheckpointGroup, applied uint64,
	members []NamedCollection, rows int, value string,
) {
	t.Helper()
	if err := group.Update(applied, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		for _, member := range members {
			write, err := batch.Collection(member.Name)
			if err != nil {
				return err
			}
			for row := 0; row < rows; row++ {
				if err := write.Put(
					[]byte(testCheckpointOverlayKey(row)), []byte(value),
				); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update(%d): %v", applied, err)
	}
}

func TestPrimaryUnifiedOverlayBatchPublishesSameGenerationCollisions(t *testing.T) {
	const (
		maxLeaf = uint32(64 << 10)
		parents = uint32(16 << 10)
	)
	overlay := newPrimaryUnifiedOverlay(
		64<<10, 8, 8*uint64(maxLeaf+parents), maxLeaf, parents,
	)
	bucketA := storeio.BucketID(7)
	bucketB := bucketA + 1
	for candidate := bucketA + 1; candidate < bucketA+100000; candidate++ {
		if primaryUnifiedOverlayBucketHash(candidate)&
			uint32(primaryUnifiedOverlayBucketTable-1) ==
			primaryUnifiedOverlayBucketHash(bucketA)&
				uint32(primaryUnifiedOverlayBucketTable-1) {
			bucketB = candidate
			break
		}
	}
	if primaryUnifiedOverlayBucketHash(bucketA)&
		uint32(primaryUnifiedOverlayBucketTable-1) !=
		primaryUnifiedOverlayBucketHash(bucketB)&
			uint32(primaryUnifiedOverlayBucketTable-1) {
		t.Fatal("failed to find bucket-directory collision")
	}
	const hashA = uint64(11)
	mutations := []primaryUnifiedOverlayBatchMutation{
		{
			bucket: bucketA, hash: hashA, key: []byte("alpha"), value: []byte("new-a"),
			rawDelta: 1, kind: primaryUnifiedOverlayPut, stableSlot: 1,
			fixedLeafBytes: 4 << 10,
		},
		{
			bucket: bucketA, hash: hashA + primaryUnifiedOverlayTable,
			key: []byte("bravo"), value: []byte("new-b"), rawDelta: 1,
			kind: primaryUnifiedOverlayPut, stableSlot: 2, fixedLeafBytes: 4 << 10,
		},
		{
			bucket: bucketB, hash: hashA, key: []byte("charlie"), value: []byte("new-c"),
			rawDelta: 1, kind: primaryUnifiedOverlayPut, stableSlot: 1,
			fixedLeafBytes: 4 << 10,
		},
	}
	prepared, err := overlay.prepareBatch(9, mutations)
	if err != nil {
		t.Fatal(err)
	}
	overlay.publishBatch(&prepared)
	for _, mutation := range mutations {
		value, disposition, _ := overlay.lookup(
			mutation.bucket, mutation.hash, mutation.key, 9,
		)
		if disposition != primaryUnifiedOverlayValue || !bytes.Equal(value, mutation.value) {
			t.Fatalf("lookup bucket=%d hash=%d key=%q = %q/%d", mutation.bucket,
				mutation.hash, mutation.key, value, disposition)
		}
	}
	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	got, err := overlay.latestBucketRecords(&indexes, bucketA, 9)
	if err != nil || got != 2 {
		t.Fatalf("same-generation bucket records = %d,%v, want 2,nil", got, err)
	}
	rows, err := overlay.applyBucket([]storeio.CommonPrimaryLeafRecord{
		{Key: []byte("alpha"), Value: storeio.CommonPrimaryLeafValue{Inline: []byte("old-a")}, Slot: 1},
		{Key: []byte("bravo"), Value: storeio.CommonPrimaryLeafValue{Inline: []byte("old-b")}, Slot: 2},
	}, bucketA, 9)
	if err != nil || len(rows) != 2 || string(rows[0].Value.Inline) != "new-a" ||
		string(rows[1].Value.Inline) != "new-b" {
		t.Fatalf("same-generation fold rows=%+v err=%v", rows, err)
	}
	got, err = overlay.latestBucketRecords(&indexes, bucketB, 9)
	if err != nil || got != 1 {
		t.Fatalf("colliding bucket records = %d,%v, want 1,nil", got, err)
	}
}

func TestPrimaryUnifiedOverlayBatchCheckedDeltasAndAbort(t *testing.T) {
	if got, ok := addPrimaryUnifiedOverlayInt64(17, 5); !ok || got != 22 {
		t.Fatalf("positive int64 delta = %d,%v", got, ok)
	}
	if got, ok := addPrimaryUnifiedOverlayInt64(22, -5); !ok || got != 17 {
		t.Fatalf("negative int64 delta = %d,%v", got, ok)
	}
	if _, ok := addPrimaryUnifiedOverlayInt64(primaryUnifiedOverlayMaxInt64, 1); ok {
		t.Fatal("positive int64 overflow was accepted")
	}
	if _, ok := addPrimaryUnifiedOverlayInt64(primaryUnifiedOverlayMinInt64, -1); ok {
		t.Fatal("negative int64 overflow was accepted")
	}
	if got, ok := addPrimaryUnifiedOverlayInt32(17, 5); !ok || got != 22 {
		t.Fatalf("positive int32 delta = %d,%v", got, ok)
	}
	if got, ok := addPrimaryUnifiedOverlayInt32(22, -5); !ok || got != 17 {
		t.Fatalf("negative int32 delta = %d,%v", got, ok)
	}
	if _, ok := addPrimaryUnifiedOverlayInt32(int32(primaryUnifiedOverlayMaxInt32), 1); ok {
		t.Fatal("positive int32 overflow was accepted")
	}
	if _, ok := addPrimaryUnifiedOverlayInt32(int32(primaryUnifiedOverlayMinInt32), -1); ok {
		t.Fatal("negative int32 overflow was accepted")
	}

	const (
		maxLeaf = uint32(64 << 10)
		parents = uint32(16 << 10)
	)
	overlay := newPrimaryUnifiedOverlay(
		64<<10, 1, uint64(4<<10)+uint64(parents), maxLeaf, parents,
	)
	mutations := []primaryUnifiedOverlayBatchMutation{{
		bucket: 1, hash: 1, key: []byte("one"), value: []byte("value"),
		rawDelta: 7, countDelta: 0, kind: primaryUnifiedOverlayPut,
		stableSlot: 1, fixedLeafBytes: 4 << 10,
	}}
	before := overlay.stats()
	prepared, err := overlay.prepareBatch(1, mutations)
	if err != nil {
		t.Fatal(err)
	}
	privateCount, privateUsed := prepared.countAfter, prepared.usedAfter
	if privateCount != 1 || privateUsed == 0 {
		t.Fatalf("private reservation = count %d used %d", privateCount, privateUsed)
	}
	overlay.abortBatch(&prepared)
	if prepared.live || overlay.stats() != before || overlay.count.Load() != 0 ||
		overlay.used.Load() != 0 || overlay.bucketCount.Load() != 0 {
		t.Fatalf("abort changed published state: stats=%+v before=%+v", overlay.stats(), before)
	}
	retry, err := overlay.prepareBatch(1, mutations)
	if err != nil {
		t.Fatal(err)
	}
	overlay.publishBatch(&retry)
	state := overlay.stats()
	if state.retainedRecords != 1 || state.dirtyBuckets != 1 {
		t.Fatalf("published retry stats = %+v", state)
	}
	failed := []primaryUnifiedOverlayBatchMutation{
		mutations[0],
		{bucket: 2, hash: 2, key: []byte("two"), value: []byte("value"),
			rawDelta: -3, countDelta: 0, kind: primaryUnifiedOverlayPut,
			stableSlot: 2, fixedLeafBytes: 4 << 10},
	}
	before = overlay.stats()
	if _, err := overlay.prepareBatch(2, failed); !errors.Is(err, storeio.ErrPageCachePinned) {
		t.Fatalf("second bucket pressure = %v, want page-cache pressure", err)
	}
	if overlay.stats() != before || overlay.count.Load() != 1 ||
		overlay.used.Load() != retry.usedAfter {
		t.Fatalf("failed growth changed published state: stats=%+v before=%+v", overlay.stats(), before)
	}
	if value, disposition, _ := overlay.lookup(1, 1, []byte("one"), 1); disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(value, []byte("value")) {
		t.Fatalf("published value after failed prepare = %q/%d", value, disposition)
	}
}

func TestPrimaryUnifiedOverlayFoldExtentHonorsConfiguredMaximum(t *testing.T) {
	const quantum = uint32(4 << 10)
	onePagePayload := int(quantum) - storeio.PageHeaderSize - storeio.PageTrailerSize
	if extent, ok := primaryUnifiedOverlayFoldExtent(
		onePagePayload, quantum, quantum,
	); !ok || extent != quantum {
		t.Fatalf("one-page fold extent = %d,%v, want %d,true", extent, ok, quantum)
	}
	if extent, ok := primaryUnifiedOverlayFoldExtent(
		onePagePayload+1, quantum, quantum,
	); ok || extent != 0 {
		t.Fatalf("configured-maximum overflow extent = %d,%v, want 0,false", extent, ok)
	}
	if extent, ok := primaryUnifiedOverlayFoldExtent(
		onePagePayload+1, quantum, 2*quantum,
	); !ok || extent != 2*quantum {
		t.Fatalf("two-page fold extent = %d,%v, want %d,true", extent, ok, 2*quantum)
	}
}

func TestPrimaryUnifiedOverlayBatchRetentionCountsCompleteBucketUnion(t *testing.T) {
	const (
		maxLeaf = uint32(64 << 10)
		parents = uint32(16 << 10)
	)
	overlay := newPrimaryUnifiedOverlay(
		64<<10, 8, 8*uint64(maxLeaf+parents), maxLeaf, parents,
	)
	prepared, err := overlay.prepareBatch(1, []primaryUnifiedOverlayBatchMutation{{
		bucket: 2, hash: 2, key: []byte("overlay"), value: []byte("value"),
		kind: primaryUnifiedOverlayPut, stableSlot: 2, fixedLeafBytes: 4 << 10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	overlay.publishBatch(&prepared)
	collection := &Collection{
		primaryUnifiedOverlay: overlay,
		primaryPendingParents: make([]filePrimaryPendingParent, 1, 2),
	}
	collection.primaryPendingParents[0].leafRoute.Bucket = 1
	leaves := []primaryBatchLeaf{{
		resident: storeio.ResidentPrimaryRoute{Bucket: 3},
	}}
	if err := collection.reservePrimaryUnifiedOverlayRetentionLocked(leaves); !errors.Is(err, ErrCheckpointGroupPressure) {
		t.Fatalf("three-bucket retention union = %v, want checkpoint pressure", err)
	}
}

func TestCheckpointGroupReplacementBatchFallsBackUnsupportedScalar(t *testing.T) {
	_, members, _, group := newCheckpointGroupOverlayTestStore(t, 8)
	const key = "checkpoint-overlay-unsupported"
	if err := group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		for _, member := range members {
			write, err := batch.Collection(member.Name)
			if err != nil {
				return err
			}
			if err := write.Put([]byte(key), []byte(`{"v":1}`)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	baseGeneration := make(map[string]uint64, len(members))
	baseRef := make(map[string]storeio.PageRef, len(members))
	for _, member := range members {
		baseGeneration[member.Name] = member.Collection.Generation()
		route, err := member.Collection.currentPrimaryResidentRoute(
			member.Collection.state.Load(), []byte(key),
		)
		if err != nil {
			t.Fatal(err)
		}
		baseRef[member.Name] = route.Ref
	}
	if err := group.Update(2, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		for _, member := range members {
			write, err := batch.Collection(member.Name)
			if err != nil {
				return err
			}
			if err := write.Put([]byte(key), []byte(`{"v":"text"}`)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if got := member.Collection.primaryUnifiedOverlay.count.Load(); got != 0 {
			t.Fatalf("member %q unsupported scalar entered row overlay: %d", member.Name, got)
		}
		if got := member.Collection.Generation(); got != baseGeneration[member.Name]+1 {
			t.Fatalf("member %q fallback generation = %d, want %d",
				member.Name, got, baseGeneration[member.Name]+1)
		}
		route, err := member.Collection.currentPrimaryResidentRoute(
			member.Collection.state.Load(), []byte(key),
		)
		if err != nil {
			t.Fatal(err)
		}
		if route.Ref == baseRef[member.Name] ||
			route.Ref.Generation != member.Collection.Generation() {
			t.Fatalf("member %q fallback ref = %+v, base %+v generation %d",
				member.Name, route.Ref, baseRef[member.Name], member.Collection.Generation())
		}
		if got, found, readErr := member.Collection.AppendRaw(nil, []byte(key)); readErr != nil || !found || string(got) != `{"v":"text"}` {
			t.Fatalf("member %q fallback value = %q/%v/%v",
				member.Name, got, found, readErr)
		}
	}
}

func newCheckpointGroupOverlayTestStore(
	t testing.TB, checkpointEvery uint64,
) (string, []NamedCollection, *TxnLog, *CheckpointGroup) {
	t.Helper()
	dir := t.TempDir()
	options := txnTestOptions()
	// The normal test extent is exactly the compact codec ceiling. Use a larger
	// resident route so row-overlay admission must rely on its eventual-fold
	// envelope rather than the maximum-extent fallback.
	options.MaxPageSize = 64 << 10
	members := []NamedCollection{
		openTxnNamedCollection(t, dir, "system", options),
		openTxnNamedCollection(t, dir, "user", options),
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: checkpointEvery,
	})
	if err != nil {
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return dir, members, log, group
}

func TestPrimaryUnifiedOverlayBatchAbortLeavesPublishedState(t *testing.T) {
	const (
		maxLeaf = uint32(64 << 10)
		parents = uint32(16 << 10)
	)
	overlay := newPrimaryUnifiedOverlay(
		64<<10, 2, uint64(maxLeaf)+uint64(parents), maxLeaf, parents,
	)
	mutations := []primaryUnifiedOverlayBatchMutation{
		{
			bucket: 1, hash: 1, key: []byte("one"), value: []byte("value"),
			rawDelta: 5, countDelta: 0, kind: primaryUnifiedOverlayPut,
			stableSlot: 1, fixedLeafBytes: maxLeaf, reservationWide: true,
		},
		{
			bucket: 2, hash: 2, key: []byte("two"), value: []byte("value"),
			rawDelta: 5, countDelta: 0, kind: primaryUnifiedOverlayPut,
			stableSlot: 2, fixedLeafBytes: maxLeaf, reservationWide: true,
		},
	}
	before := overlay.stats()
	beforeCount, beforeUsed := overlay.count.Load(), overlay.used.Load()
	beforeBucketCount, beforeDirty := overlay.bucketCount.Load(), overlay.dirtyBytes.Load()
	if _, err := overlay.prepareBatch(1, mutations); err == nil {
		t.Fatal("expected batch pressure")
	}
	if overlay.count.Load() != beforeCount || overlay.used.Load() != beforeUsed ||
		overlay.bucketCount.Load() != beforeBucketCount ||
		overlay.dirtyBytes.Load() != beforeDirty || overlay.stats() != before {
		t.Fatalf("failed prepare changed published state: before=%+v after=%+v",
			before, overlay.stats())
	}
	if _, disposition, _ := overlay.lookup(1, 1, []byte("one"), 1); disposition != primaryUnifiedOverlayMissing {
		t.Fatalf("failed prepare published a row: disposition=%d", disposition)
	}

	retry, err := overlay.prepareBatch(1, mutations[:1])
	if err != nil {
		t.Fatal(err)
	}
	overlay.publishBatch(&retry)
	value, disposition, _ := overlay.lookup(1, 1, []byte("one"), 1)
	if disposition != primaryUnifiedOverlayValue || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("retry lookup = %q/%d", value, disposition)
	}
}

func testCheckpointOverlayKey(row int) string {
	var suffix [8]byte
	at := len(suffix)
	for {
		at--
		suffix[at] = byte('a' + row%26)
		row = row/26 - 1
		if row < 0 {
			break
		}
	}
	return "checkpoint-overlay-" + string(suffix[at:])
}
