package durable

import (
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestSingleDocumentTransactionFitsExactReservation leaves exactly the point
// transaction reservation free in the manual committer, then writes the
// largest accepted document and key with index maintenance enabled. Any missed
// overflow, directory, free-log, or retirement consumer fails admission or
// exhausts the transaction before publication.
// Under frame-native staging, data and directory pages are encoded in their
// cache frames and never consume committer buffers; a single-document
// transaction reserves exactly one committer buffer for the alternate root.
// This test pins that contract at both edges: with every other buffer held,
// a maximum indexed Put still succeeds without an automatic checkpoint, and
// with every buffer held its preflight takes the documented pressure path
// instead of deadlocking or corrupting state.
func TestSingleDocumentTransactionFitsExactReservation(t *testing.T) {
	options := Options{
		Collection: store.Options{ChunkDocuments: 64},
		Indexes: []store.IndexDefinition{
			{Name: "indexed", Paths: []string{"/indexed"}},
		},
		PageSize: 4096, MaxPageSize: 64 << 10,
		ResidentBytes:            64 << 20,
		MaxKeyBytes:              128,
		InlineValueBytes:         512,
		MaxDocumentBytes:         64 << 10,
		MaxBatchDocuments:        64,
		Backend:                  BackendPortable,
		Durability:               DurabilityBufferedVisible,
	}
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.singleDocumentTransactionPages >= normalized.maxTransactionPages {
		t.Fatalf(
			"single-document pages = %d, batch pages = %d; test needs a narrower point reservation",
			normalized.singleDocumentTransactionPages,
			normalized.maxTransactionPages,
		)
	}

	file, err := os.CreateTemp(t.TempDir(), "single-document-bound-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	bufferCount := int(collection.Stats().CommitCapacityBytes /
		uint64(collection.options.MaxPageSize))
	held := make([]*storeio.Batch, 0, bufferCount)
	releaseHeld := func() {
		t.Helper()
		for i := len(held) - 1; i >= 0; i-- {
			if abortErr := held[i].Abort(); abortErr != nil {
				t.Fatalf("release held reservation %d: %v", i, abortErr)
			}
		}
		held = held[:0]
	}
	defer func() { releaseHeld() }()
	for len(held) < bufferCount-1 {
		batch, beginErr := collection.committer.Begin(0)
		if beginErr != nil {
			t.Fatalf("hold root buffer %d of %d: %v", len(held)+1, bufferCount-1, beginErr)
		}
		held = append(held, batch)
	}

	prefix := `{"indexed":"maximum","padding":"`
	suffix := `"}`
	document := []byte(prefix + strings.Repeat(
		"x", collection.options.MaxDocumentBytes-len(prefix)-len(suffix),
	) + suffix)
	key := strings.Repeat("k", collection.options.MaxKeyBytes)
	checkpoints := collection.automaticCheckpoints.Load()
	created, err := collection.Put(key, document)
	if err != nil || !created {
		t.Fatalf("maximum indexed Put = (%v,%v), want (true,nil)", created, err)
	}
	if got := collection.automaticCheckpoints.Load(); got != checkpoints {
		t.Fatalf(
			"point preflight checkpointed with its exact reservation free: %d -> %d",
			checkpoints, got,
		)
	}
	releaseHeld()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
}
