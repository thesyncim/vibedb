package durable

import (
	"os"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestZZProbeBounds(t *testing.T) {
	options := Options{
		Collection: store.Options{ChunkDocuments: 64},
		Indexes: []store.IndexDefinition{
			{Name: "indexed", Paths: []string{"/indexed"}},
		},
		PageSize: 4096, MaxPageSize: 64 << 10,
		ResidentBytes: 64 << 20, MaxKeyBytes: 128,
		InlineValueBytes: 512, MaxDocumentBytes: 64 << 10,
		MaxBatchDocuments: 64, Backend: BackendPortable,
		Durability:               DurabilityBufferedVisible,
		DisableMutationCombining: true,
	}
	n, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("maxTx=%d singleDoc=%d BufferCount=%d MaxRetired=%d",
		n.maxTransactionPages, n.singleDocumentTransactionPages,
		n.BufferCount, n.MaxRetiredExtents)
	file, _ := os.CreateTemp(t.TempDir(), "probe-*")
	defer file.Close()
	c, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.Stats()
	t.Logf("committer capacity bytes=%d (pages=%d)", s.CommitCapacityBytes,
		s.CommitCapacityBytes/uint64(n.MaxPageSize))
	b, err := c.committer.Begin(n.maxTransactionPages)
	if err != nil {
		t.Logf("Begin(maxTx)=%v", err)
	} else {
		t.Logf("Begin(maxTx) OK")
		b.Abort()
	}
}
