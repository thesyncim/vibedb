package durable

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func newTestPrimaryUnifiedOverlay(
	bytes, bucketLimit int,
) *primaryUnifiedOverlay {
	const (
		maxLeafBytes = uint32(64 << 10)
		parentBytes  = uint32(4 * 4096)
	)
	return newPrimaryUnifiedOverlay(
		bytes, bucketLimit,
		uint64(bucketLimit)*uint64(maxLeafBytes+parentBytes),
		maxLeafBytes, parentBytes,
	)
}

func TestPrimaryUnifiedOverlayLazyBacking(t *testing.T) {
	const (
		arenaBytes  = 64 << 10
		maxLeaf     = uint32(64 << 10)
		parentBytes = uint32(4 * 4096)
	)
	overlay := newLazyPrimaryUnifiedOverlay(
		arenaBytes, primaryUnifiedOverlayBuckets,
		uint64(primaryUnifiedOverlayBuckets)*uint64(maxLeaf+parentBytes),
		maxLeaf, parentBytes,
	)
	if overlay == nil {
		t.Fatal("lazy overlay disabled")
	}
	if len(overlay.records) != 0 || len(overlay.heads) != 0 ||
		len(overlay.arena) != 0 {
		t.Fatal("lazy overlay allocated backing before first mutation")
	}
	if stats := overlay.stats(); stats.capacityBytes != arenaBytes ||
		stats.arenaBytes != 0 || stats.logicalDirtyBytes != 0 {
		t.Fatalf(
			"lazy stats = (%d, %d, %d), want (%d, 0, 0)",
			stats.capacityBytes, stats.arenaBytes,
			stats.logicalDirtyBytes, arenaBytes,
		)
	}
	if _, disposition, _ := overlay.lookup(1, 7, []byte("key"), 1); disposition != primaryUnifiedOverlayMissing {
		t.Fatalf("empty lazy lookup disposition = %d", disposition)
	}
	prepared, err := overlay.prepare(
		1, 7, 1, []byte("key"), []byte("value"),
		0, 0, primaryUnifiedOverlayPut, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	overlay.publish(prepared)
	if len(overlay.records) != primaryUnifiedOverlayRecords ||
		len(overlay.heads) != primaryUnifiedOverlayTable ||
		len(overlay.arena) != arenaBytes {
		t.Fatal("first mutation did not allocate complete bounded backing")
	}
	value, disposition, _ := overlay.lookup(1, 7, []byte("key"), 1)
	if disposition != primaryUnifiedOverlayValue || string(value) != "value" {
		t.Fatalf("lazy lookup = (%q, %d)", value, disposition)
	}
}

func TestPrimaryUnifiedOverlayStatsSeparateRetainedOccupancyFromDebt(t *testing.T) {
	const (
		maxLeaf = uint32(64 << 10)
		parents = uint32(16 << 10)
	)
	overlay := newPrimaryUnifiedOverlay(
		64<<10, 2, 2*uint64(maxLeaf+parents), maxLeaf, parents,
	)
	publish := func(bucket storeio.BucketID, generation uint64, key string) {
		t.Helper()
		prepared, err := overlay.prepareWithLeafReservation(
			bucket, uint64(bucket), generation, []byte(key), []byte(`{"v":1}`),
			0, 0, primaryUnifiedOverlayPut, 1, 4<<10, false,
			storeio.CommonPrimaryUnifiedScalarPatch{},
		)
		if err != nil {
			t.Fatal(err)
		}
		overlay.publish(prepared)
	}

	publish(1, 1, "first")
	before := overlay.stats()
	if before.retainedRecords != 1 || before.arenaBytes == 0 ||
		before.dirtyBuckets != 1 || before.reservedFoldBytes == 0 {
		t.Fatalf("pending stats = %+v", before)
	}
	overlay.markFolded(1, false)
	pinned := overlay.stats()
	if pinned.retainedRecords != before.retainedRecords ||
		pinned.arenaBytes != before.arenaBytes || pinned.dirtyBuckets != 0 ||
		pinned.reservedFoldBytes != 0 || pinned.logicalDirtyBytes != 0 {
		t.Fatalf("reader-pinned folded stats = %+v; before = %+v", pinned, before)
	}

	publish(2, 2, "second")
	after := overlay.stats()
	if after.retainedRecords != 2 || after.arenaBytes <= pinned.arenaBytes ||
		after.dirtyBuckets != 1 || after.reservedFoldBytes == 0 ||
		after.logicalDirtyBytes == 0 {
		t.Fatalf("new-window stats = %+v; pinned = %+v", after, pinned)
	}
}

func TestPrimaryUnifiedOverlaySparseRecycleClearsPublishedIndexes(t *testing.T) {
	overlay := newTestPrimaryUnifiedOverlay(
		64<<10, primaryUnifiedOverlayBuckets,
	)
	publish := func(
		bucket storeio.BucketID,
		hash, generation uint64,
		key, value string,
	) {
		t.Helper()
		prepared, err := overlay.prepareWithLeafBytes(
			bucket, hash, generation, []byte(key), []byte(value),
			0, 0, primaryUnifiedOverlayPut, uint8(generation), 4<<10,
		)
		if err != nil {
			t.Fatal(err)
		}
		overlay.publish(prepared)
	}

	// Exercise both duplicate hash slots and distinct bucket-directory entries.
	// A sparse recycle must clear every published lookup root without depending
	// on a full-table sweep.
	const firstHash = uint64(17)
	publish(1, firstHash, 1, "first", "v1")
	publish(2, firstHash+primaryUnifiedOverlayTable, 2, "second", "v2")
	publish(1, firstHash, 3, "first", "v3")
	retained := overlay.records[0]
	overlay.markFolded(3, true)

	if overlay.count.Load() != 0 || overlay.used.Load() != 0 ||
		overlay.folded.Load() != 0 || overlay.bucketCount.Load() != 0 ||
		overlay.dirtyBytes.Load() != 0 {
		t.Fatalf("sparse recycle retained published state: %+v", overlay.stats())
	}
	if got := overlay.heads[firstHash&(primaryUnifiedOverlayTable-1)].Load(); got != 0 {
		t.Fatalf("collision hash head = %d, want zero", got)
	}
	if overlay.records[0] != retained {
		t.Fatal("sparse recycle cleared pointer-free record backing")
	}
	for i := range overlay.buckets {
		bucket := &overlay.buckets[i]
		if bucket.head.Load() != 0 || bucket.reservedBytes.Load() != 0 ||
			bucket.fixedBytes.Load() != 0 || bucket.wideKeys.Load() != 0 ||
			bucket.rawBytes.Load() != 0 || bucket.rows.Load() != 0 {
			t.Fatalf("bucket %d retained sparse recycle state", i)
		}
		for slot := range bucket.insertSlots {
			if bucket.insertSlots[slot].Load() != 0 {
				t.Fatalf("bucket %d slot word %d retained state", i, slot)
			}
		}
	}

	// Reusing record zero and the same hash slot must not reconnect the stale
	// collision chain or expose either old key.
	publish(3, firstHash, 4, "third", "v4")
	if value, disposition, _ := overlay.lookup(3, firstHash, []byte("third"), 4); disposition != primaryUnifiedOverlayValue || string(value) != "v4" {
		t.Fatalf("post-recycle lookup = %q,%d", value, disposition)
	}
	for bucket, key := range map[storeio.BucketID][]byte{
		1: []byte("first"), 2: []byte("second"),
	} {
		if _, disposition, _ := overlay.lookup(bucket, firstHash, key, 4); disposition != primaryUnifiedOverlayMissing {
			t.Fatalf("stale lookup bucket %d = disposition %d", bucket, disposition)
		}
	}
	overlay.markFolded(4, true)
	key, value := []byte("allocation"), []byte("stable")
	if allocs := testing.AllocsPerRun(100, func() {
		prepared, err := overlay.prepareWithLeafBytes(
			4, 29, 1, key, value, 0, 0,
			primaryUnifiedOverlayPut, 1, 4<<10,
		)
		if err != nil {
			panic(err)
		}
		overlay.publish(prepared)
		overlay.markFolded(1, true)
	}); allocs != 0 {
		t.Fatalf("publish + sparse recycle allocated %.2f times", allocs)
	}
}

func TestPrimaryUnifiedOverlayDenseRecycleClearsPublishedIndexes(t *testing.T) {
	const records = primaryUnifiedOverlayRecords/4 + 1
	overlay := newTestPrimaryUnifiedOverlay(
		primaryUnifiedOverlayMax, primaryUnifiedOverlayBuckets,
	)
	key, value := []byte("k"), []byte("v")
	var hashes [records]uint64
	for index := range records {
		hash := uint64(index+1) * 0x9e3779b97f4a7c15
		hashes[index] = hash
		prepared, err := overlay.prepareWithLeafBytes(
			1, hash, uint64(index+1), key, value,
			0, 0, primaryUnifiedOverlayPut, uint8(index), 4<<10,
		)
		if err != nil {
			t.Fatalf("prepare record %d: %v", index, err)
		}
		overlay.publish(prepared)
	}
	retained := overlay.records[records/2]
	overlay.markFolded(records, true)
	if overlay.count.Load() != 0 || overlay.used.Load() != 0 ||
		overlay.folded.Load() != 0 || overlay.bucketCount.Load() != 0 ||
		overlay.dirtyBytes.Load() != 0 {
		t.Fatalf("dense recycle retained published state: %+v", overlay.stats())
	}
	if overlay.records[records/2] != retained {
		t.Fatal("dense recycle cleared pointer-free record backing")
	}
	for index, hash := range hashes {
		if got := overlay.heads[hash&(primaryUnifiedOverlayTable-1)].Load(); got != 0 {
			t.Fatalf("record %d hash head = %d, want zero", index, got)
		}
	}
	prepared, err := overlay.prepareWithLeafBytes(
		2, hashes[0], records+1, []byte("fresh"), value,
		0, 0, primaryUnifiedOverlayPut, 1, 4<<10,
	)
	if err != nil {
		t.Fatal(err)
	}
	overlay.publish(prepared)
	got, disposition, _ := overlay.lookup(
		2, hashes[0], []byte("fresh"), records+1,
	)
	if disposition != primaryUnifiedOverlayValue || !bytes.Equal(got, value) {
		t.Fatalf("post-dense-recycle lookup = %q,%d", got, disposition)
	}
}

// legacyLatestBucketRecordsKeyOracle retains the former key-deduplication walk
// as a test-only differential oracle. It intentionally pays O(history*256);
// production must use the slot-indexed implementation.
func legacyLatestBucketRecordsKeyOracle(
	o *primaryUnifiedOverlay,
	dst *[storeio.CommonPrimaryLeafWideSlots]uint16,
	bucket storeio.BucketID,
	generation uint64,
) (int, error) {
	if o == nil || dst == nil {
		return 0, nil
	}
	folded := o.folded.Load()
	count := int(o.count.Load())
	if count > len(o.records) {
		return 0, storeio.ErrCommonPrimaryLeafCorrupt
	}
	used := o.used.Load()
	if used > uint32(len(o.arena)) {
		return 0, storeio.ErrCommonPrimaryLeafCorrupt
	}
	n := 0
	newerGeneration := uint64(0)
	bucketSlot, found := o.bucketSlot(bucket)
	if !found {
		return 0, nil
	}
	head := o.buckets[bucketSlot].head.Load()
	for head != 0 {
		if head > uint32(len(o.records)) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		index := head - 1
		record := &o.records[index]
		if record.bucket != uint32(bucket) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		if record.generation == 0 ||
			newerGeneration != 0 && record.generation >= newerGeneration {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		newerGeneration = record.generation
		if record.generation <= folded {
			break
		}
		next := record.bucketPrevious
		if next >= head && next != 0 {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		head = next
		keyEnd := uint64(record.keyOffset) + uint64(record.keyLen)
		valueEnd := uint64(record.valueOff) + uint64(record.valueLen)
		if record.keyLen == 0 || keyEnd > uint64(len(o.arena)) ||
			uint64(record.valueOff) != keyEnd ||
			valueEnd > uint64(len(o.arena)) ||
			record.kind == primaryUnifiedOverlayPut && record.valueLen == 0 ||
			record.kind == primaryUnifiedOverlayDelete && record.valueLen != 0 ||
			record.kind != primaryUnifiedOverlayPut &&
				record.kind != primaryUnifiedOverlayDelete {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		if record.generation > generation {
			continue
		}
		if index >= uint32(count) || valueEnd > uint64(used) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		key := o.arena[record.keyOffset:uint32(keyEnd):uint32(keyEnd)]
		duplicate := false
		for at := 0; at < n; at++ {
			other := &o.records[dst[at]]
			otherEnd := uint64(other.keyOffset) + uint64(other.keyLen)
			if other.keyLen == 0 || otherEnd > uint64(used) {
				return 0, storeio.ErrCommonPrimaryLeafCorrupt
			}
			if bytes.Equal(
				key,
				o.arena[other.keyOffset:uint32(otherEnd):uint32(otherEnd)],
			) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		if n == len(dst) {
			return 0, storeio.ErrCommonPrimaryLeafCorrupt
		}
		dst[n] = uint16(index)
		n++
	}
	for i := 1; i < n; i++ {
		index := dst[i]
		record := &o.records[index]
		end := record.keyOffset + uint32(record.keyLen)
		key := o.arena[record.keyOffset:end:end]
		at := i
		for at > 0 {
			previous := &o.records[dst[at-1]]
			previousEnd := previous.keyOffset + uint32(previous.keyLen)
			previousKey := o.arena[previous.keyOffset:previousEnd:previousEnd]
			if bytes.Compare(previousKey, key) <= 0 {
				break
			}
			dst[at] = dst[at-1]
			at--
		}
		dst[at] = index
	}
	return n, nil
}

func TestPrimaryUnifiedOverlayBucketDirectory(t *testing.T) {
	// Find two identities with the same initial directory slot so the test
	// exercises probing rather than relying only on the no-collision case.
	var first, second storeio.BucketID
	var seen [primaryUnifiedOverlayBucketTable]storeio.BucketID
	for candidate := storeio.BucketID(1); second == 0; candidate++ {
		slot := primaryUnifiedOverlayBucketHash(candidate) &
			(primaryUnifiedOverlayBucketTable - 1)
		if seen[slot] != 0 {
			first, second = seen[slot], candidate
			break
		}
		seen[slot] = candidate
	}

	overlay := newTestPrimaryUnifiedOverlay(
		16<<10, primaryUnifiedOverlayBuckets,
	)
	publish := func(
		bucket storeio.BucketID,
		hash, generation uint64,
		key, value string,
		rawDelta, countDelta int,
		slot uint8,
	) {
		t.Helper()
		prepared, err := overlay.prepare(
			bucket, hash, generation, []byte(key), []byte(value),
			rawDelta, countDelta, primaryUnifiedOverlayPut, slot,
		)
		if err != nil {
			t.Fatalf("prepare generation %d: %v", generation, err)
		}
		overlay.publish(prepared)
	}

	publish(first, 11, 1, "first", "v1", 10, 1, 5)
	publish(first, 11, 2, "first", "v2", 3, 0, 5)
	publish(second, 22, 3, "second", "other", 12, 1, 67)

	if gotRaw, gotRows := overlay.pendingBucketDeltas(first); gotRaw != 13 ||
		gotRows != 1 {
		t.Fatalf("first deltas = %d,%d, want 13,1", gotRaw, gotRows)
	}
	if gotRaw, gotRows := overlay.pendingBucketDeltas(second); gotRaw != 12 ||
		gotRows != 1 {
		t.Fatalf("second deltas = %d,%d, want 12,1", gotRaw, gotRows)
	}
	firstSlots := overlay.pendingInsertSlots(first)
	secondSlots := overlay.pendingInsertSlots(second)
	if firstSlots[0] != uint64(1)<<5 ||
		secondSlots[1] != uint64(1)<<3 {
		t.Fatalf(
			"insert slots = %#x/%#x, want bit 5 / bit 67",
			firstSlots, secondSlots,
		)
	}
	if got := overlay.bucketVersion(first, 3); got != 2 {
		t.Fatalf("first bucket version = %d, want 2", got)
	}

	var buckets [primaryUnifiedOverlayBuckets]storeio.BucketID
	var keys [primaryUnifiedOverlayBuckets][]byte
	n, err := overlay.pendingBuckets(&buckets, &keys)
	if err != nil || n != 2 {
		t.Fatalf("pending buckets = %d,%v, want 2,nil", n, err)
	}
	representatives := make(map[storeio.BucketID]string, n)
	for i := range n {
		representatives[buckets[i]] = string(keys[i])
	}
	if representatives[first] != "first" ||
		representatives[second] != "second" {
		t.Fatalf("representatives = %#v", representatives)
	}

	var recordStorage [2]storeio.CommonPrimaryLeafRecord
	records, err := overlay.applyBucket(
		recordStorage[:0], first, 3,
	)
	if err != nil || len(records) != 1 ||
		!bytes.Equal(records[0].Key, []byte("first")) ||
		!bytes.Equal(records[0].Value.Inline, []byte("v2")) {
		t.Fatalf("applied records = %#v,%v", records, err)
	}

	// A non-recycling fold clears only the pending-bucket directory. The global
	// hash chains remain available to the old generation, while a new window
	// starts with exact zeroed deltas and no bucket-chain link into stale rows.
	overlay.markFolded(3, false)
	if overlay.pendingBucket(first) || overlay.bucketCount.Load() != 0 {
		t.Fatal("fold retained pending directory state")
	}
	if raw, rows := overlay.pendingBucketDeltas(first); raw != 0 || rows != 0 {
		t.Fatalf("post-fold deltas = %d,%d, want 0,0", raw, rows)
	}
	old, disposition, _ := overlay.lookup(first, 11, []byte("first"), 3)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(old, []byte("v2")) {
		t.Fatalf("old-generation lookup = %q,%d", old, disposition)
	}
	publish(first, 11, 4, "first", "v4", 0, 0, 5)
	current, disposition, _ :=
		overlay.lookup(first, 11, []byte("first"), 4)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(current, []byte("v4")) {
		t.Fatalf("new-window lookup = %q,%d", current, disposition)
	}
	old, disposition, _ = overlay.lookup(first, 11, []byte("first"), 3)
	if disposition != primaryUnifiedOverlayValue ||
		!bytes.Equal(old, []byte("v2")) {
		t.Fatalf("pinned old-generation lookup = %q,%d", old, disposition)
	}
}

func TestPrimaryUnifiedOverlayLatestBucketRecordsDifferential(t *testing.T) {
	const (
		bucket      = storeio.BucketID(37)
		generations = primaryUnifiedOverlayRecords
	)
	overlay := newTestPrimaryUnifiedOverlay(
		1<<20, primaryUnifiedOverlayBuckets,
	)
	var keys [storeio.CommonPrimaryLeafWideSlots][]byte
	for index := range keys {
		keys[index] = fmt.Appendf(nil, "k%03d", index)
	}
	var baseStorage [storeio.CommonPrimaryLeafWideSlots]storeio.CommonPrimaryLeafRecord
	base := baseStorage[:storeio.CommonPrimaryLeafWideSlots/2]
	for index := range base {
		base[index] = storeio.CommonPrimaryLeafRecord{
			Slot: uint8(index), Key: keys[index],
			Value: storeio.CommonPrimaryLeafValue{
				Inline: fmt.Appendf(nil, "base-%03d", index),
			},
		}
	}
	live := [storeio.CommonPrimaryLeafWideSlots]bool{}
	for index := range base {
		live[index] = true
	}
	seed := uint64(0x9e3779b97f4a7c15)
	for generation := 1; generation <= generations; generation++ {
		seed ^= seed << 7
		seed ^= seed >> 9
		seed ^= seed << 8
		index := int(seed % storeio.CommonPrimaryLeafWideSlots)
		kind := uint8(primaryUnifiedOverlayPut)
		countDelta := 0
		value := fmt.Appendf(nil, "value-%04d-%03d", generation, index)
		if live[index] && seed>>17&3 == 0 {
			kind = primaryUnifiedOverlayDelete
			countDelta = -1
			value = nil
			live[index] = false
		} else {
			if !live[index] {
				countDelta = 1
			}
			live[index] = true
		}
		prepared, err := overlay.prepare(
			bucket, uint64(index+1), uint64(generation),
			keys[index], value, countDelta, countDelta,
			kind, uint8(index),
		)
		if err != nil {
			t.Fatalf("prepare generation %d: %v", generation, err)
		}
		overlay.publish(prepared)
	}

	for _, target := range [...]uint64{
		1, 31, 257, 2048, generations / 2, generations,
	} {
		var gotStorage [storeio.CommonPrimaryLeafWideSlots]storeio.CommonPrimaryLeafRecord
		copy(gotStorage[:], base)
		got, err := overlay.applyBucket(
			gotStorage[:len(base)], bucket, target,
		)
		if err != nil {
			t.Fatalf("apply target %d: %v", target, err)
		}

		present := [storeio.CommonPrimaryLeafWideSlots]bool{}
		var values [storeio.CommonPrimaryLeafWideSlots][]byte
		for index, row := range base {
			present[index] = true
			values[index] = row.Value.Inline
		}
		var newest [storeio.CommonPrimaryLeafWideSlots]uint64
		for index := 0; index < int(overlay.count.Load()); index++ {
			record := &overlay.records[index]
			if record.generation > target {
				break
			}
			keyIndex := int(record.slot)
			newest[keyIndex] = record.generation
			if record.kind == primaryUnifiedOverlayDelete {
				present[keyIndex] = false
				values[keyIndex] = nil
				continue
			}
			present[keyIndex] = true
			end := record.valueOff + record.valueLen
			values[keyIndex] = overlay.arena[record.valueOff:end:end]
		}
		var wantStorage [storeio.CommonPrimaryLeafWideSlots]storeio.CommonPrimaryLeafRecord
		want := wantStorage[:0]
		for index := range present {
			if present[index] {
				want = append(want, storeio.CommonPrimaryLeafRecord{
					Slot: uint8(index), Key: keys[index],
					Value: storeio.CommonPrimaryLeafValue{
						Inline: values[index],
					},
				})
			}
		}
		if len(got) != len(want) {
			t.Fatalf("target %d rows = %d, want %d", target, len(got), len(want))
		}
		for index := range want {
			if got[index].Slot != want[index].Slot ||
				!bytes.Equal(got[index].Key, want[index].Key) ||
				!bytes.Equal(got[index].Value.Inline, want[index].Value.Inline) {
				t.Fatalf("target %d row %d = %#v, want %#v",
					target, index, got[index], want[index])
			}
		}

		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, target)
		if err != nil {
			t.Fatalf("latest target %d: %v", target, err)
		}
		previousKey := []byte(nil)
		seen := [storeio.CommonPrimaryLeafWideSlots]bool{}
		for at, recordIndex := range indexes[:count] {
			record := &overlay.records[recordIndex]
			keyIndex := int(record.slot)
			end := record.keyOffset + uint32(record.keyLen)
			key := overlay.arena[record.keyOffset:end:end]
			if at != 0 && bytes.Compare(previousKey, key) >= 0 {
				t.Fatalf("target %d latest keys are not lexical at %q/%q",
					target, previousKey, key)
			}
			if seen[keyIndex] || record.generation != newest[keyIndex] {
				t.Fatalf("target %d slot %d generation = %d, newest %d seen=%v",
					target, keyIndex, record.generation, newest[keyIndex], seen[keyIndex])
			}
			seen[keyIndex] = true
			previousKey = key
		}
		var oracleIndexes [storeio.CommonPrimaryLeafWideSlots]uint16
		oracleCount, oracleErr := legacyLatestBucketRecordsKeyOracle(
			overlay, &oracleIndexes, bucket, target,
		)
		if oracleErr != nil || oracleCount != count {
			t.Fatalf(
				"target %d slot-indexed records = %v,%v; legacy oracle = %v,%v",
				target, indexes[:count], err,
				oracleIndexes[:oracleCount], oracleErr,
			)
		}
		for at := range count {
			if oracleIndexes[at] != indexes[at] {
				t.Fatalf(
					"target %d record %d = %d, legacy oracle %d",
					target, at, indexes[at], oracleIndexes[at],
				)
			}
		}
	}
}

func TestPrimaryUnifiedOverlayLatestBucketRecordsFullHotSlotHistory(
	t *testing.T,
) {
	const (
		bucket = storeio.BucketID(39)
		slot   = uint8(173)
	)
	overlay := newTestPrimaryUnifiedOverlay(
		1<<20, primaryUnifiedOverlayBuckets,
	)
	key := []byte("one-hot-key")
	value := []byte("v")
	for generation := 1; generation <= primaryUnifiedOverlayRecords; generation++ {
		prepared, err := overlay.prepare(
			bucket, 1, uint64(generation), key, value,
			0, 0, primaryUnifiedOverlayPut, slot,
		)
		if err != nil {
			t.Fatalf("prepare generation %d: %v", generation, err)
		}
		overlay.publish(prepared)
	}
	for _, target := range [...]uint64{
		1, primaryUnifiedOverlayRecords / 2, primaryUnifiedOverlayRecords,
	} {
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, target)
		if err != nil || count != 1 || indexes[0] != uint16(target-1) {
			t.Fatalf(
				"target %d latest = %v,%d,%v, want record %d",
				target, indexes[:count], count, err, target-1,
			)
		}
	}
}

func BenchmarkPrimaryUnifiedOverlayLatestBucketRecordsFullHistory(
	b *testing.B,
) {
	const bucket = storeio.BucketID(43)
	overlay := newTestPrimaryUnifiedOverlay(
		1<<20, primaryUnifiedOverlayBuckets,
	)
	var keys [storeio.CommonPrimaryLeafWideSlots][]byte
	for slot := range keys {
		// Reverse the lexical spelling relative to slot order so both
		// implementations still pay the final bounded lexical sort.
		keys[slot] = fmt.Appendf(nil, "k%03d", len(keys)-1-slot)
	}
	for generation := 1; generation <= primaryUnifiedOverlayRecords; generation++ {
		slot := uint8((generation * 73) & 255)
		prepared, err := overlay.prepare(
			bucket, uint64(slot)+1, uint64(generation),
			keys[slot], []byte("v"), 0, 0,
			primaryUnifiedOverlayPut, slot,
		)
		if err != nil {
			b.Fatalf("prepare generation %d: %v", generation, err)
		}
		overlay.publish(prepared)
	}
	target := uint64(primaryUnifiedOverlayRecords)
	b.Run("slot-indexed", func(b *testing.B) {
		b.ReportAllocs()
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		for range b.N {
			count, err := overlay.latestBucketRecords(&indexes, bucket, target)
			if err != nil || count != len(indexes) {
				b.Fatalf("latest = %d,%v", count, err)
			}
		}
	})
	b.Run("legacy-key-oracle", func(b *testing.B) {
		b.ReportAllocs()
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		for range b.N {
			count, err := legacyLatestBucketRecordsKeyOracle(
				overlay, &indexes, bucket, target,
			)
			if err != nil || count != len(indexes) {
				b.Fatalf("legacy latest = %d,%v", count, err)
			}
		}
	})
}

func TestPrimaryUnifiedOverlayLatestBucketRecordsFailsClosed(t *testing.T) {
	const bucket = storeio.BucketID(41)
	t.Run("same-slot-different-key", func(t *testing.T) {
		overlay := newTestPrimaryUnifiedOverlay(
			64<<10, primaryUnifiedOverlayBuckets,
		)
		for generation, key := range [...]string{"first", "second"} {
			prepared, err := overlay.prepare(
				bucket, uint64(generation+1), uint64(generation+1),
				[]byte(key), []byte("v"), 0, 0,
				primaryUnifiedOverlayPut, 7,
			)
			if err != nil {
				t.Fatal(err)
			}
			overlay.publish(prepared)
		}
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, 2)
		if count != 0 || !errors.Is(err, storeio.ErrCommonPrimaryLeafCorrupt) {
			t.Fatalf("same-slot mismatch = %d,%v, want 0,corrupt", count, err)
		}
	})

	t.Run("same-key-different-slots", func(t *testing.T) {
		overlay := newTestPrimaryUnifiedOverlay(
			64<<10, primaryUnifiedOverlayBuckets,
		)
		for generation, slot := range [...]uint8{7, 19} {
			prepared, err := overlay.prepare(
				bucket, 1, uint64(generation+1),
				[]byte("duplicate"), []byte("v"), 0, 0,
				primaryUnifiedOverlayPut, slot,
			)
			if err != nil {
				t.Fatal(err)
			}
			overlay.publish(prepared)
		}
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, 2)
		if count != 0 || !errors.Is(err, storeio.ErrCommonPrimaryLeafCorrupt) {
			t.Fatalf("duplicate key slots = %d,%v, want 0,corrupt", count, err)
		}
	})

	t.Run("too-many-final-keys", func(t *testing.T) {
		overlay := newTestPrimaryUnifiedOverlay(
			64<<10, primaryUnifiedOverlayBuckets,
		)
		for index := 0; index <= storeio.CommonPrimaryLeafWideSlots; index++ {
			key := fmt.Appendf(nil, "key-%03d", index)
			prepared, err := overlay.prepare(
				bucket, uint64(index+1), uint64(index+1), key, []byte("v"),
				1, 1, primaryUnifiedOverlayPut, uint8(index),
			)
			if err != nil {
				t.Fatalf("prepare %d: %v", index, err)
			}
			overlay.publish(prepared)
		}
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(
			&indexes, bucket, storeio.CommonPrimaryLeafWideSlots+1,
		)
		if count != 0 || !errors.Is(err, storeio.ErrCommonPrimaryLeafCorrupt) {
			t.Fatalf("latest overflow = %d,%v, want 0,corrupt", count, err)
		}
	})

	t.Run("corrupt-chain", func(t *testing.T) {
		overlay := newTestPrimaryUnifiedOverlay(
			64<<10, primaryUnifiedOverlayBuckets,
		)
		prepared, err := overlay.prepare(
			bucket, 1, 1, []byte("key"), []byte("value"),
			1, 1, primaryUnifiedOverlayPut, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		overlay.publish(prepared)
		overlay.records[0].bucketPrevious = 1
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, 1)
		if count != 0 || !errors.Is(err, storeio.ErrCommonPrimaryLeafCorrupt) {
			t.Fatalf("latest corrupt chain = %d,%v, want 0,corrupt", count, err)
		}
	})

	t.Run("nonmonotonic-generation", func(t *testing.T) {
		overlay := newTestPrimaryUnifiedOverlay(
			64<<10, primaryUnifiedOverlayBuckets,
		)
		for generation := uint64(1); generation <= 2; generation++ {
			prepared, err := overlay.prepare(
				bucket, 1, generation, []byte("key"), []byte("value"),
				0, 0, primaryUnifiedOverlayPut, 1,
			)
			if err != nil {
				t.Fatal(err)
			}
			overlay.publish(prepared)
		}
		overlay.records[0].generation = 2
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, 2)
		if count != 0 || !errors.Is(err, storeio.ErrCommonPrimaryLeafCorrupt) {
			t.Fatalf("latest nonmonotonic chain = %d,%v, want 0,corrupt", count, err)
		}
	})

	t.Run("future-head-before-watermarks", func(t *testing.T) {
		overlay := newTestPrimaryUnifiedOverlay(
			64<<10, primaryUnifiedOverlayBuckets,
		)
		first, err := overlay.prepare(
			bucket, 1, 1, []byte("key"), []byte("first"),
			1, 1, primaryUnifiedOverlayPut, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		overlay.publish(first)
		future, err := overlay.prepare(
			bucket, 1, 2, []byte("key"), []byte("second"),
			0, 0, primaryUnifiedOverlayPut, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		// Match publish's first visibility word while count and used still name
		// generation 1. An old snapshot must skip the initialized future record.
		overlay.buckets[future.bucketSlot].head.Store(future.index + 1)
		var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
		count, err := overlay.latestBucketRecords(&indexes, bucket, 1)
		if err != nil || count != 1 || indexes[0] != 0 {
			t.Fatalf("latest across future head = %v,%d,%v, want [0]",
				indexes[:count], count, err)
		}
	})
}

func TestPrimaryUnifiedOverlaySameSizeArenaReuseAppendsRecord(t *testing.T) {
	overlay := newTestPrimaryUnifiedOverlay(
		64<<10, primaryUnifiedOverlayBuckets,
	)
	key := []byte("same-size-key")
	first := []byte("first-value")
	second := []byte("other-value")
	const (
		bucket = storeio.BucketID(19)
		hash   = uint64(0x1234)
		slot   = uint8(7)
	)
	prepared, err := overlay.prepare(
		bucket, hash, 2, key, first, 3, 0,
		primaryUnifiedOverlayPut, slot,
	)
	if err != nil {
		t.Fatal(err)
	}
	overlay.publish(prepared)
	used := overlay.used.Load()
	oldKeyOffset := overlay.records[0].keyOffset
	oldValueOffset := overlay.records[0].valueOff
	if overlay.canReuseSameSizeArena(
		bucket, hash, key, second, slot, 1,
	) {
		t.Fatal("record newer than the journal watermark was reusable")
	}
	if !overlay.canReuseSameSizeArena(
		bucket, hash, key, second, slot, 2,
	) {
		t.Fatal("journal-covered same-size replacement was not reusable")
	}

	reused, ok := overlay.prepareSameSizeArenaReuse(
		bucket, hash, 3, key, second, slot, 2,
		overlay.maxLeafBytes, storeio.CommonPrimaryUnifiedScalarPatch{},
	)
	if !ok {
		t.Fatal("prepareSameSizeArenaReuse declined an eligible record")
	}
	overlay.publish(reused)
	if got := overlay.count.Load(); got != 2 {
		t.Fatalf("record count = %d, want two distinct metadata records", got)
	}
	if got := overlay.used.Load(); got != used {
		t.Fatalf("arena used = %d, want unchanged %d", got, used)
	}
	latest := &overlay.records[1]
	if latest.keyOffset != oldKeyOffset ||
		latest.valueOff != oldValueOffset {
		t.Fatalf("reused offsets = %d/%d, want %d/%d",
			latest.keyOffset, latest.valueOff,
			oldKeyOffset, oldValueOffset)
	}
	value, disposition, gotSlot := overlay.lookup(bucket, hash, key, 3)
	if disposition != primaryUnifiedOverlayValue || gotSlot != slot ||
		!bytes.Equal(value, second) {
		t.Fatalf("latest lookup = %q,%d,%d", value, disposition, gotSlot)
	}

	entries, complete, err := overlay.checkpointEntries(
		make([]storeio.RecoveryBatchEntry, 0, 1), 2, 3,
	)
	if err != nil || !complete || len(entries) != 1 ||
		!bytes.Equal(entries[0].Key, key) ||
		!bytes.Equal(entries[0].Value, second) {
		t.Fatalf("checkpoint entries = %#v complete=%v err=%v",
			entries, complete, err)
	}
	if overlay.canReuseSameSizeArena(
		bucket, hash, key, []byte("different-size"), slot, 3,
	) {
		t.Fatal("different-size replacement was reusable")
	}

	insert := newTestPrimaryUnifiedOverlay(
		64<<10, primaryUnifiedOverlayBuckets,
	)
	inserted, err := insert.prepare(
		bucket, hash, 2, key, first, len(first), 1,
		primaryUnifiedOverlayPut, slot,
	)
	if err != nil {
		t.Fatal(err)
	}
	insert.publish(inserted)
	if insert.canReuseSameSizeArena(
		bucket, hash, key, second, slot, 2,
	) {
		t.Fatal("insert record arena was reusable")
	}
}

func TestPrimaryUnifiedOverlayBucketDirectoryPressure(t *testing.T) {
	overlay := newTestPrimaryUnifiedOverlay(
		64<<10, primaryUnifiedOverlayBuckets,
	)
	for index := range primaryUnifiedOverlayBuckets {
		key := fmt.Appendf(nil, "k%d", index)
		prepared, err := overlay.prepare(
			storeio.BucketID(index+1), uint64(index+1), uint64(index+1),
			key, []byte("v"), 1, 1, primaryUnifiedOverlayPut, uint8(index),
		)
		if err != nil {
			t.Fatalf("prepare bucket %d: %v", index, err)
		}
		overlay.publish(prepared)
	}
	if got := overlay.bucketCount.Load(); got != primaryUnifiedOverlayBuckets {
		t.Fatalf("bucket count = %d, want %d", got, primaryUnifiedOverlayBuckets)
	}
	_, err := overlay.prepare(
		storeio.BucketID(primaryUnifiedOverlayBuckets+1),
		primaryUnifiedOverlayBuckets+1,
		primaryUnifiedOverlayBuckets+1,
		[]byte("overflow"), []byte("v"), 1, 1,
		primaryUnifiedOverlayPut, 1,
	)
	if !errors.Is(err, storeio.ErrPageCachePinned) {
		t.Fatalf("overflow distinct bucket error = %v", err)
	}
	// Reusing one of the admitted buckets remains legal at the exact limit.
	if _, err := overlay.prepare(
		1, 1, primaryUnifiedOverlayBuckets+1,
		[]byte("k0"), []byte("new"), 2, 0,
		primaryUnifiedOverlayPut, 0,
	); err != nil {
		t.Fatalf("existing bucket at directory limit: %v", err)
	}
}

func TestPrimaryUnifiedOverlayDirtyByteAdmission(t *testing.T) {
	const (
		maxLeaf = uint32(64 << 10)
		parents = uint32(4 * 4096)
	)
	budget := uint64(4<<10+parents) + uint64(8<<10+parents)
	overlay := newPrimaryUnifiedOverlay(
		64<<10, 3, budget, maxLeaf, parents,
	)
	publishExact := func(bucket storeio.BucketID, leafBytes uint32) {
		t.Helper()
		key := fmt.Appendf(nil, "exact-%d", bucket)
		prepared, err := overlay.prepareWithLeafBytes(
			bucket, uint64(bucket), uint64(bucket), key, []byte("v"),
			0, 0, primaryUnifiedOverlayPut, 0, leafBytes,
		)
		if err != nil {
			t.Fatalf("prepare exact bucket %d: %v", bucket, err)
		}
		overlay.publish(prepared)
	}
	publishExact(1, 4<<10)
	if got, want := overlay.dirtyBytes.Load(), uint64(4<<10+parents); got != want {
		t.Fatalf("one exact bucket bytes = %d, want %d", got, want)
	}
	// A non-certified mutation in the same bucket must upgrade to MaxPageSize;
	// this budget cannot cover it, so admission fails before publication.
	if _, err := overlay.prepare(
		1, 1, 2, []byte("exact-1"), []byte("other"),
		4, 0, primaryUnifiedOverlayPut, 0,
	); !errors.Is(err, storeio.ErrPageCachePinned) {
		t.Fatalf("uncertified reservation upgrade = %v, want pressure", err)
	}
	if got, want := overlay.dirtyBytes.Load(), uint64(4<<10+parents); got != want {
		t.Fatalf("failed upgrade changed dirty bytes = %d, want %d", got, want)
	}
	publishExact(2, 8<<10)
	if got := overlay.dirtyBytes.Load(); got != budget {
		t.Fatalf("complete exact budget = %d, want %d", got, budget)
	}
	if _, err := overlay.prepareWithLeafBytes(
		3, 3, 3, []byte("exact-3"), []byte("v"),
		0, 0, primaryUnifiedOverlayPut, 0, 4<<10,
	); !errors.Is(err, storeio.ErrPageCachePinned) {
		t.Fatalf("dirty-byte overflow = %v, want pressure", err)
	}
	overlay.markFolded(2, true)
	if got := overlay.dirtyBytes.Load(); got != 0 {
		t.Fatalf("recycled dirty bytes = %d, want zero", got)
	}

	upgrade := newPrimaryUnifiedOverlay(
		64<<10, 1, uint64(maxLeaf+parents), maxLeaf, parents,
	)
	prepared, err := upgrade.prepareWithLeafBytes(
		1, 1, 1, []byte("upgrade"), []byte("first"),
		0, 0, primaryUnifiedOverlayPut, 0, 4<<10,
	)
	if err != nil {
		t.Fatal(err)
	}
	upgrade.publish(prepared)
	prepared, err = upgrade.prepare(
		1, 1, 2, []byte("upgrade"), []byte("other"),
		0, 0, primaryUnifiedOverlayPut, 0,
	)
	if err != nil {
		t.Fatalf("covered max-leaf upgrade: %v", err)
	}
	upgrade.publish(prepared)
	if got, want := upgrade.dirtyBytes.Load(), uint64(maxLeaf+parents); got != want {
		t.Fatalf("upgraded dirty bytes = %d, want %d", got, want)
	}

	downgrade := newPrimaryUnifiedOverlay(
		64<<10, 1, uint64(maxLeaf+parents), maxLeaf, parents,
	)
	prepared, err = downgrade.prepareWithLeafReservation(
		1, 1, 1, []byte("restore"), nil,
		-16, -1, primaryUnifiedOverlayDelete, 7,
		4<<10, true, storeio.CommonPrimaryUnifiedScalarPatch{},
	)
	if err != nil {
		t.Fatalf("prepare wide tombstone: %v", err)
	}
	downgrade.publish(prepared)
	slot, found := downgrade.bucketSlot(1)
	if !found {
		t.Fatal("wide tombstone bucket missing")
	}
	if got := downgrade.buckets[slot].wideKeys.Load(); got != 1 {
		t.Fatalf("wide tombstone keys = %d, want 1", got)
	}
	if got, want := downgrade.dirtyBytes.Load(), uint64(maxLeaf+parents); got != want {
		t.Fatalf("wide tombstone bytes = %d, want %d", got, want)
	}
	prepared, err = downgrade.prepareWithLeafReservation(
		1, 1, 2, []byte("restore"), []byte("fixed"),
		16, 1, primaryUnifiedOverlayPut, 7,
		4<<10, false, storeio.CommonPrimaryUnifiedScalarPatch{},
	)
	if err != nil {
		t.Fatalf("prepare certified restore: %v", err)
	}
	downgrade.publish(prepared)
	if got := downgrade.buckets[slot].wideKeys.Load(); got != 0 {
		t.Fatalf("restored wide keys = %d, want 0", got)
	}
	if got, want := downgrade.buckets[slot].reservedBytes.Load(), uint32(4<<10+parents); got != want {
		t.Fatalf("restored reservation = %d, want %d", got, want)
	}
	if got, want := downgrade.dirtyBytes.Load(), uint64(4<<10+parents); got != want {
		t.Fatalf("restored dirty bytes = %d, want %d", got, want)
	}
	if got := downgrade.records[0].reservationWide; got != 1 {
		t.Fatalf("old tombstone reservation metadata = %d, want immutable wide", got)
	}
}

func TestPrimaryUnifiedOverlayBucketDirectoryPinnedSnapshotIsGenerationSafe(t *testing.T) {
	built, keys, _ := buildFilePrimaryCorpus(t, 1_000)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	file := createPrimaryPointFile(
		t, built, options, "primary-overlay-directory-pinned.vibe",
	)
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	key := []byte(keys[500])
	first := []byte(`{"window":"first","id":500}`)
	second := []byte(`{"window":"other","id":500}`)
	third := []byte(`{"window":"third","id":500}`)
	canonical := canonicalDocs(t, [][]byte{first, second, third})
	first, second, third = canonical[0], canonical[1], canonical[2]
	if _, err := collection.Put(key, first); err != nil {
		t.Fatal(err)
	}
	// The public snapshot contract first materializes the overlay and then
	// leases that rooted generation, so its point reads and ordered scans name
	// the same base.
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(key, second); err != nil {
		t.Fatal(err)
	}
	// An active generation lease does not veto append-only overlay publication.
	// The immutable generation-stamped record remains invisible to the older
	// snapshot while current readers use it directly.
	overlay := collection.primaryUnifiedOverlay
	if !overlay.hasPending() || overlay.bucketCount.Load() != 1 ||
		overlay.count.Load() != 1 {
		t.Fatalf(
			"snapshot overlay = pending %v, buckets %d, records %d; want true/1/1",
			overlay.hasPending(), overlay.bucketCount.Load(), overlay.count.Load(),
		)
	}
	got, ok, err := snapshot.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, first) {
		t.Fatalf("pinned point read = %q,%v,%v, want %q", got, ok, err, first)
	}
	var (
		scanned int
		scanGot []byte
	)
	if err := snapshot.RangeRaw(func(gotKey, value []byte) error {
		scanned++
		if bytes.Equal(gotKey, key) {
			scanGot = append(scanGot[:0], value...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if scanned != 1_000 || !bytes.Equal(scanGot, first) {
		t.Fatalf(
			"pinned ordered scan = %d rows, target %q, want 1000/%q",
			scanned, scanGot, first,
		)
	}
	got, ok, err = collection.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, second) {
		t.Fatalf("live point read = %q,%v,%v, want %q", got, ok, err, second)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	// Once the lease exits, the next replacement appends to the same exact
	// one-bucket directory without requiring a materialization boundary.
	if _, err := collection.Put(key, third); err != nil {
		t.Fatal(err)
	}
	if !overlay.hasPending() || overlay.bucketCount.Load() != 1 ||
		overlay.count.Load() != 2 {
		t.Fatalf(
			"continued overlay = pending %v, buckets %d, records %d; want true/1/2",
			overlay.hasPending(), overlay.bucketCount.Load(), overlay.count.Load(),
		)
	}
	got, ok, err = collection.AppendRaw(nil, key)
	if err != nil || !ok || !bytes.Equal(got, third) {
		t.Fatalf("resumed point read = %q,%v,%v, want %q", got, ok, err, third)
	}
}
