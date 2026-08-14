package durable

import (
	"bytes"
	"fmt"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryUnifiedCheckpointEntriesKeepFullRedoForBaseRelativeCertificate(t *testing.T) {
	t.Run("tiny top-level scalar falls back to full value", func(t *testing.T) {
		docs := make(map[string][]byte, 256)
		for i := range 256 {
			docs[fmt.Sprintf("scalar-%03d", i)] = []byte(`{"v":0}`)
		}
		coll := buildIndexedPrimaryFile(
			t, t.TempDir(), "scalar-wire-cost-*", docs,
			concurrentPrimaryTestOptions(),
		)
		key := []byte("scalar-128")
		baseGeneration := coll.Generation()
		created, err := coll.Put(key, []byte(`{"v":1}`))
		if err != nil || created {
			t.Fatalf("Put = %v,%v", created, err)
		}
		if coll.primaryUnifiedOverlay.count.Load() != 1 ||
			coll.primaryUnifiedOverlay.records[0].scalarPatch ==
				(storeio.CommonPrimaryUnifiedScalarPatch{}) {
			t.Fatal("compact replacement dropped its scalar-patch certificate")
		}
		entries, complete, err :=
			coll.primaryUnifiedOverlay.checkpointEntries(
				make([]storeio.RecoveryBatchEntry, 0, 1),
				baseGeneration, coll.Generation(),
			)
		if err != nil || !complete || len(entries) != 1 ||
			entries[0].Kind != storeio.RecoveryRecordKindPut ||
			!bytes.Equal(entries[0].Value, []byte(`{"v":1}`)) {
			t.Fatalf("checkpoint entries = %#v complete=%v err=%v",
				entries, complete, err)
		}
	})

	t.Run("document scalar emits compact patch", func(t *testing.T) {
		fixture := openConcurrentPrimaryTestFixture(
			t, 512, concurrentPrimaryTestOptions(),
		)
		same, _ := concurrentPrimaryTestTargets(t, fixture)
		key := []byte(fixture.keys[same[0]])
		base := canonicalConcurrentPrimaryValue(t, fixture.values[same[0]])
		start := bytes.Index(base, []byte(`"group":`))
		if start < 0 {
			t.Fatalf("group scalar missing from %q", base)
		}
		start += len(`"group":`)
		end := start
		for end < len(base) && base[end] >= '0' && base[end] <= '9' {
			end++
		}
		newScalar := []byte("999999999999999999")
		updated := make([]byte, 0, len(base)-(end-start)+len(newScalar))
		updated = append(updated, base[:start]...)
		updated = append(updated, newScalar...)
		updated = append(updated, base[end:]...)
		baseGeneration := fixture.collection.Generation()
		created, err := fixture.collection.Put(key, updated)
		if err != nil || created {
			t.Fatalf("Put = %v,%v", created, err)
		}
		if fixture.collection.primaryUnifiedOverlay.records[0].scalarPatch ==
			(storeio.CommonPrimaryUnifiedScalarPatch{}) {
			t.Fatal("document scalar dropped its compact admission certificate")
		}
		entries, complete, err :=
			fixture.collection.primaryUnifiedOverlay.checkpointEntries(
				make([]storeio.RecoveryBatchEntry, 0, 1),
				baseGeneration, fixture.collection.Generation(),
			)
		if err != nil || !complete || len(entries) != 1 ||
			entries[0].Kind != storeio.RecoveryRecordKindPut ||
			!bytes.Equal(entries[0].Value, updated) {
			t.Fatalf("checkpoint entries = %#v complete=%v err=%v",
				entries, complete, err)
		}
	})
}

func TestPrimaryUnifiedCheckpointEntriesDoNotReplayBaseRelativePatchOnPredecessor(
	t *testing.T,
) {
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	same, _ := concurrentPrimaryTestTargets(t, fixture)
	key := []byte(fixture.keys[same[0]])
	base := canonicalConcurrentPrimaryValue(t, fixture.values[same[0]])
	first := append([]byte(nil), base...)
	group := bytes.Index(first, []byte(`"group":`)) + len(`"group":`)
	if group < len(`"group":`) || group >= len(first) {
		t.Fatal("group scalar missing")
	}
	if first[group] == '9' {
		first[group] = '8'
	} else {
		first[group]++
	}
	second := append([]byte(nil), base...)
	id := bytes.Index(second, []byte(`"id":`)) + len(`"id":`)
	if id < len(`"id":`) || id >= len(second) {
		t.Fatal("id scalar missing")
	}
	if second[id] == '9' {
		second[id] = '8'
	} else {
		second[id]++
	}

	baseGeneration := fixture.collection.Generation()
	for index, value := range [][]byte{first, second} {
		created, err := fixture.collection.Put(key, value)
		if err != nil || created {
			t.Fatalf("Put %d = %v,%v", index, created, err)
		}
	}
	for index := 0; index < 2; index++ {
		if fixture.collection.primaryUnifiedOverlay.records[index].scalarPatch ==
			(storeio.CommonPrimaryUnifiedScalarPatch{}) {
			t.Fatalf("record %d dropped compact fold certificate", index)
		}
	}
	entries, complete, err :=
		fixture.collection.primaryUnifiedOverlay.checkpointEntries(
			make([]storeio.RecoveryBatchEntry, 0, 2),
			baseGeneration, fixture.collection.Generation(),
		)
	if err != nil || !complete || len(entries) != 2 {
		t.Fatalf("checkpoint entries = %d complete=%v err=%v", len(entries), complete, err)
	}
	for index, want := range [][]byte{first, second} {
		if entries[index].Kind != storeio.RecoveryRecordKindPut ||
			!bytes.Equal(entries[index].Value, want) {
			t.Fatalf("entry %d = %#v, want full Put %q", index, entries[index], want)
		}
	}
}

// TestConcurrentPrimaryScalarPatchRequestReuse pins the context-reuse boundary:
// a certified Put must persist its certificate in that record, the same
// writer-private request reused for Delete must explicitly clear it, and a
// resurrection must replace it with a certificate independently derived
// against the admitted base. Otherwise a stale six-byte certificate can name
// an unrelated value even though the fold-side verifier would reject it.
func TestConcurrentPrimaryScalarPatchRequestReuse(t *testing.T) {
	if got := unsafe.Sizeof(primaryUnifiedOverlayRecord{}); got != 56 {
		t.Fatalf("overlay record size = %d, want 56", got)
	}
	fixture := openConcurrentPrimaryTestFixture(
		t, 512, concurrentPrimaryTestOptions(),
	)
	same, _ := concurrentPrimaryTestTargets(t, fixture)
	index := same[0]
	key := []byte(fixture.keys[index])
	base := canonicalConcurrentPrimaryValue(t, fixture.values[index])
	start := bytes.Index(base, []byte(`"group":`))
	if start < 0 {
		t.Fatalf("group scalar missing from %q", base)
	}
	start += len(`"group":`)
	end := start
	for end < len(base) && base[end] >= '0' && base[end] <= '9' {
		end++
	}
	updated := make([]byte, 0, len(base)+20)
	updated = append(updated, base[:start]...)
	updated = append(updated, "999999999999999999"...)
	updated = append(updated, base[end:]...)

	created, err := fixture.collection.Put(key, updated)
	if err != nil || created {
		t.Fatalf("certified Put = %v,%v", created, err)
	}
	overlay := fixture.collection.primaryUnifiedOverlay
	if got := overlay.count.Load(); got != 1 {
		t.Fatalf("overlay count after Put = %d, want 1", got)
	}
	zero := storeio.CommonPrimaryUnifiedScalarPatch{}
	if overlay.records[0].scalarPatch == zero {
		t.Fatal("existing-key compact Put dropped its certificate")
	}

	deleted, err := fixture.collection.Delete(key)
	if err != nil || !deleted {
		t.Fatalf("Delete = %v,%v", deleted, err)
	}
	if got := overlay.count.Load(); got != 2 {
		t.Fatalf("overlay count after Delete = %d, want 2", got)
	}
	if overlay.records[1].scalarPatch != zero {
		t.Fatal("Delete retained the preceding Put certificate")
	}

	created, err = fixture.collection.Put(key, base)
	if err != nil || !created {
		t.Fatalf("resurrection Put = %v,%v", created, err)
	}
	if got := overlay.count.Load(); got != 3 {
		t.Fatalf("overlay count after resurrection = %d, want 3", got)
	}
	if overlay.records[2].scalarPatch != zero {
		t.Fatal("compact resurrection retained a legacy certificate")
	}
	for i := range fixture.collection.primaryConcurrentContexts.contexts {
		if fixture.collection.primaryConcurrentContexts.contexts[i].publish.scalarPatch != zero {
			t.Fatalf("context %d retained a certificate after submit cleanup", i)
		}
	}
}

func TestPrimaryUnifiedFixedReplacementsKeepsArbitrarySlotOrder(t *testing.T) {
	const bucket = storeio.BucketID(77)
	overlay := newTestPrimaryUnifiedOverlay(
		64<<10, primaryUnifiedOverlayBuckets,
	)
	for generation, row := range []struct {
		key  string
		slot uint8
		hash uint64
	}{
		{key: "z-last-lexically", slot: 2, hash: 2},
		{key: "a-first-lexically", slot: 201, hash: 201},
	} {
		prepared, err := overlay.prepare(
			bucket, row.hash, uint64(generation+1),
			[]byte(row.key), []byte(`{"v":1}`), 0, 0,
			primaryUnifiedOverlayPut, row.slot,
		)
		if err != nil {
			t.Fatal(err)
		}
		overlay.publish(prepared)
	}
	replacements, allPuts, err := overlay.primaryUnifiedFixedReplacements(
		make([]storeio.CommonPrimaryUnifiedReplacement, 0,
			storeio.CommonPrimaryLeafWideSlots),
		bucket, 2,
	)
	if err != nil || !allPuts || len(replacements) != 2 {
		t.Fatalf("fixed replacements = %d,%v,%v", len(replacements), allPuts, err)
	}
	if string(replacements[0].Key) != "z-last-lexically" ||
		string(replacements[1].Key) != "a-first-lexically" {
		t.Fatalf("native replacements unexpectedly sorted: %q, %q",
			replacements[0].Key, replacements[1].Key)
	}

	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	count, err := overlay.latestBucketRecords(&indexes, bucket, 2)
	if err != nil || count != 2 {
		t.Fatalf("structural latest = %d,%v", count, err)
	}
	first := &overlay.records[indexes[0]]
	second := &overlay.records[indexes[1]]
	firstKey := overlay.arena[first.keyOffset : first.keyOffset+uint32(first.keyLen)]
	secondKey := overlay.arena[second.keyOffset : second.keyOffset+uint32(second.keyLen)]
	if bytes.Compare(firstKey, secondKey) >= 0 {
		t.Fatalf("structural latest is not lexical: %q, %q", firstKey, secondKey)
	}
}
