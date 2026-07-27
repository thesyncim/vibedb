package storeio

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"
)

// batchMutate applies one sorted edit batch as a single published generation
// and returns the pages the descent retired.
func (h *keyTreeHarness) batchMutate(edits []KeyTreeEdit, maxPages int) KeyTreeBatchMutation {
	h.t.Helper()
	h.generation++
	tx, err := BeginWriteTransaction(h.committer, h.cache, maxPages, WriteTransactionOptions{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		FileEnd: h.fileEnd, NextLogicalID: h.nextID,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.bounds.FileEnd = h.fileEnd
	h.bounds.NextLogicalID = h.nextID
	mutation, err := MutateKeyTreeBatch(h.cache, tx, h.root, edits, h.bounds, nil)
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	statePage, err := tx.Allocate(PageStateRoot, testSuperblockPageSize, StateRootLogicalID)
	if err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	state := StateRoot{
		StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
		MaxPageSize: 64 << 10, NextLogicalID: tx.NextLogicalID(), ChunkDocuments: 64,
	}
	if _, err := EncodeStateRootPage(statePage.Bytes(), state, tx.FileEnd()); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := statePage.Stage(); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := tx.Publish(statePage.Ref(), PageChecksum(statePage.Bytes()), 0, 0, 0); err != nil {
		_ = tx.Abort()
		h.t.Fatal(err)
	}
	if err := h.committer.Wait(h.generation); err != nil {
		h.t.Fatal(err)
	}
	h.cache.MarkDurable(h.generation)
	h.root = mutation.Root
	h.fileEnd = tx.FileEnd()
	h.nextID = tx.NextLogicalID()
	return mutation
}

// TestKeyTreeBatchMatchesSequentialUpserts is the differential oracle for the
// batch descent: two harnesses receive the same edits, one key at a time and
// one batch at a time, and must agree on every lookup afterwards. Random keys
// with varied lengths force leaf and branch splits at both ends of the key
// space, which is where a batch's partition boundaries differ from a
// single-key descent's.
func TestKeyTreeBatchMatchesSequentialUpserts(t *testing.T) {
	random := rand.New(rand.NewSource(20260725))
	sequential := newKeyTreeHarnessPages(t, 256, 512)
	batched := newKeyTreeHarnessPages(t, 256, 512)
	live := map[string]KeyLocation{}
	for round := range 12 {
		count := 1 + random.Intn(24)
		edits := make([]KeyTreeEdit, 0, count)
		seen := map[string]struct{}{}
		existing := make([]string, 0, len(live))
		for key := range live {
			existing = append(existing, key)
		}
		sort.Strings(existing)
		for range count {
			// Half of every round after the first revisits a key the tree
			// already holds. Drawing fresh random spellings only would make
			// replacement and deletion statistically unreachable, and those are
			// exactly the merge branches a batch descent gets wrong.
			key := fmt.Sprintf("%03d-%s", random.Intn(400),
				strings.Repeat(string(rune('a'+random.Intn(26))), 1+random.Intn(600)))
			revisit := len(existing) != 0 && random.Intn(2) == 0
			if revisit {
				key = existing[random.Intn(len(existing))]
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			edit := KeyTreeEdit{Key: []byte(key)}
			if revisit && random.Intn(2) == 0 {
				edit.Delete = true
			} else {
				edit.Location = KeyLocation{
					Chunk: uint32(random.Intn(1024)), Slot: uint8(random.Intn(64)),
				}
			}
			edits = append(edits, edit)
		}
		sort.Slice(edits, func(i, j int) bool {
			return string(edits[i].Key) < string(edits[j].Key)
		})
		for _, edit := range edits {
			sequential.mutate(string(edit.Key), edit.Location, edit.Delete)
			if edit.Delete {
				delete(live, string(edit.Key))
			} else {
				live[string(edit.Key)] = edit.Location
			}
		}
		batched.batchMutate(edits, 256)
		for key, want := range live {
			got, ok := batched.lookup(key)
			if !ok || got != want {
				t.Fatalf("round %d batched lookup %q = %+v,%v want %+v", round, key, got, ok, want)
			}
			reference, referenceOK := sequential.lookup(key)
			if !referenceOK || reference != want {
				t.Fatalf("round %d sequential lookup %q = %+v,%v want %+v", round, key, reference, referenceOK, want)
			}
		}
		for key := range seen {
			if _, present := live[key]; present {
				continue
			}
			if _, ok := batched.lookup(key); ok {
				t.Fatalf("round %d deleted key %q still present", round, key)
			}
		}
	}
}

// TestKeyTreeBatchWritesFewerPagesThanSequentialUpserts is the reason the
// descent exists. A batch that rewrites each visited page once must retire far
// fewer pages than one root-to-leaf copy per key, and the gap is what makes a
// bounded single-transaction batch possible at all.
func TestKeyTreeBatchWritesFewerPagesThanSequentialUpserts(t *testing.T) {
	const count = 64
	edits := make([]KeyTreeEdit, count)
	for i := range edits {
		edits[i] = KeyTreeEdit{
			Key:      fmt.Appendf(nil, "%04d-%s", i, strings.Repeat("k", 200)),
			Location: KeyLocation{Chunk: uint32(i), Slot: uint8(i % 64)},
		}
	}
	seed := newKeyTreeHarnessPages(t, 256, 512)
	for i := range 96 {
		seed.mutate(fmt.Sprintf("seed-%04d-%s", i, strings.Repeat("s", 200)),
			KeyLocation{Chunk: uint32(i), Slot: 0}, false)
	}
	sequential := 0
	for _, edit := range edits {
		sequential += int(seed.mutate(string(edit.Key), edit.Location, false).RetiredCount)
	}

	batched := newKeyTreeHarnessPages(t, 256, 512)
	for i := range 96 {
		batched.mutate(fmt.Sprintf("seed-%04d-%s", i, strings.Repeat("s", 200)),
			KeyLocation{Chunk: uint32(i), Slot: 0}, false)
	}
	mutation := batched.batchMutate(edits, 256)
	if len(mutation.Retired) >= sequential/4 {
		t.Fatalf("batch retired %d pages, sequential retired %d", len(mutation.Retired), sequential)
	}
	for _, edit := range edits {
		got, ok := batched.lookup(string(edit.Key))
		if !ok || got != edit.Location {
			t.Fatalf("batched lookup %q = %+v,%v", edit.Key, got, ok)
		}
	}
	if len(mutation.Retired) != len(slices.Compact(slices.Clone(mutation.Retired))) {
		t.Fatalf("batch retired a page twice: %+v", mutation.Retired)
	}
}

// TestKeyTreeBatchRejectsUnsortedAndOutOfRangeEdits keeps the descent's
// precondition enforced rather than assumed: an unsorted batch would silently
// build a directory whose binary search cannot find its own keys.
func TestKeyTreeBatchRejectsUnsortedAndOutOfRangeEdits(t *testing.T) {
	h := newKeyTreeHarnessPages(t, 256, 512)
	h.mutate("a", KeyLocation{Chunk: 1, Slot: 1}, false)
	cases := map[string][]KeyTreeEdit{
		"unsorted": {
			{Key: []byte("b"), Location: KeyLocation{Chunk: 1}},
			{Key: []byte("a"), Location: KeyLocation{Chunk: 1}},
		},
		"duplicate": {
			{Key: []byte("b"), Location: KeyLocation{Chunk: 1}},
			{Key: []byte("b"), Location: KeyLocation{Chunk: 2}},
		},
		"chunk out of range": {
			{Key: []byte("b"), Location: KeyLocation{Chunk: 4096}},
		},
		"slot out of range": {
			{Key: []byte("b"), Location: KeyLocation{Chunk: 1, Slot: 64}},
		},
	}
	for name, edits := range cases {
		t.Run(name, func(t *testing.T) {
			h.generation++
			tx, err := BeginWriteTransaction(h.committer, h.cache, 32, WriteTransactionOptions{
				StoreID: testStoreID, Generation: h.generation, PageSize: testSuperblockPageSize,
				FileEnd: h.fileEnd, NextLogicalID: h.nextID,
			})
			if err != nil {
				t.Fatal(err)
			}
			h.bounds.FileEnd = h.fileEnd
			h.bounds.NextLogicalID = h.nextID
			_, err = MutateKeyTreeBatch(h.cache, tx, h.root, edits, h.bounds, nil)
			_ = tx.Abort()
			h.generation--
			if err == nil {
				t.Fatalf("%s batch accepted", name)
			}
		})
	}
}
