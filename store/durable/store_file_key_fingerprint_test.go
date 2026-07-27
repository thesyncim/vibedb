package durable

import (
	"bytes"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestFileFingerprintPointUpdateKeepsDirectoryRoot(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "fingerprint-point-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	if created, err := collection.Put("key", []byte(`{"version":1}`)); err != nil || !created {
		t.Fatalf("insert = (%v,%v)", created, err)
	}
	before := collection.state.Load().keyRoot
	if before.Kind != storeio.PageFingerprintDirectory {
		t.Fatalf("point root kind = %v, want fingerprint directory", before.Kind)
	}
	if created, err := collection.Put("key", []byte(`{"version":2}`)); err != nil || created {
		t.Fatalf("replacement = (%v,%v)", created, err)
	}
	after := collection.state.Load().keyRoot
	if after != before {
		t.Fatalf("unchanged key rewrote fingerprint root: before=%+v after=%+v", before, after)
	}
	raw, ok, err := collection.AppendRaw(nil, "key")
	if err != nil || !ok || string(raw) != `{"version":2}` {
		t.Fatalf("replacement read = (%q,%v,%v)", raw, ok, err)
	}
}

func TestFileFingerprintResolverCarriesVerifiedDocument(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "fingerprint-resolver-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	want := []byte(`{"carried":"once"}`)
	if _, err := collection.Put("key", want); err != nil {
		t.Fatal(err)
	}

	state := collection.state.Load()
	match, ok, err := collection.resolveFileFingerprint(state, []byte("key"))
	if err != nil || !ok {
		t.Fatalf("resolve = (%v,%v)", ok, err)
	}
	defer match.Release()
	reads := collection.Stats().PageReads
	got, ok := match.view.appendJSON(make([]byte, 0, len(want)), match.value)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("carried document = (%q,%v)", got, ok)
	}
	if after := collection.Stats().PageReads; after != reads {
		t.Fatalf("consuming verified match performed another read: before=%d after=%d", reads, after)
	}
}

func TestFileFingerprintResolverDisambiguatesForcedCollision(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "fingerprint-collision-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, testFileStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	values := [2][]byte{[]byte(`{"id":1}`), []byte(`{"id":2}`)}
	keys := [2][]byte{[]byte("alpha"), []byte("beta")}
	for index := range keys {
		if _, err := collection.Put(string(keys[index]), values[index]); err != nil {
			t.Fatal(err)
		}
	}

	state := collection.state.Load()
	var locations [2]storeio.PageKeyLocation
	for index := range keys {
		match, ok, resolveErr := collection.resolveFileFingerprint(state, keys[index])
		if resolveErr != nil || !ok {
			t.Fatalf("resolve %q = (%v,%v)", keys[index], ok, resolveErr)
		}
		locations[index] = match.location
		match.Release()
	}
	const collisionHash = uint64(0x9b6d_4f2a_1107_ee31)
	for index := range locations {
		locations[index].Hash = collisionHash
	}
	if locations[1].Chunk < locations[0].Chunk ||
		locations[1].Chunk == locations[0].Chunk && locations[1].Slot < locations[0].Slot {
		locations[0], locations[1] = locations[1], locations[0]
		keys[0], keys[1] = keys[1], keys[0]
		values[0], values[1] = values[1], values[0]
	}

	tx, err := storeio.BeginWriteTransaction(
		collection.committer, collection.cache, collection.options.maxTransactionPages,
		storeio.WriteTransactionOptions{
			StoreID: collection.storeID, Generation: state.root.Generation + 1,
			PageSize: uint32(collection.options.PageSize),
			FileEnd:  state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	edits := []storeio.PageKeyTreeEdit{
		{Location: locations[0], Operation: storeio.PageKeyTreeInsert},
		{Location: locations[1], Operation: storeio.PageKeyTreeInsert},
	}
	mutation, err := storeio.MutatePageKeyTreeBatch(
		collection.cache, tx, storeio.PageRef{}, edits,
		storeio.PageKeyTreeBounds{
			FileEnd: state.super.FileEnd, NextLogicalID: state.root.NextLogicalID,
			ChunkHighWater: state.root.ChunkHighWater,
			ChunkDocuments: state.root.ChunkDocuments,
		}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	synthetic := *state
	synthetic.keyRoot = mutation.Root
	synthetic.root.KeyDirectory = mutation.Root
	synthetic.root.NextLogicalID = tx.NextLogicalID()
	synthetic.super.FileEnd = tx.FileEnd()
	for index := range keys {
		match, ok, resolveErr := collection.resolveFileFingerprintHash(
			&synthetic, keys[index], collisionHash,
		)
		if resolveErr != nil || !ok {
			t.Fatalf("collision resolve %q = (%v,%v)", keys[index], ok, resolveErr)
		}
		got, appendOK := match.view.appendJSON(nil, match.value)
		match.Release()
		if !appendOK || !bytes.Equal(got, values[index]) {
			t.Fatalf("collision resolve %q returned %q, want %q", keys[index], got, values[index])
		}
	}
}
