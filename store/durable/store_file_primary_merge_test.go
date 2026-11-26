package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibejson"
)

func TestFileStoreInlineSiblingLeafMergeOnUnderfillAndCrashReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "merge-underfill.vibe")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	options := testFileStoreOptions()
	options.Durability = DurabilitySync
	options.ResidentBytes = 16 << 20
	options.BufferCount = 2048
	options.MaxRetiredExtents = 4096
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}

	// Insert enough documents with incompressible payloads in scattered key order
	// to force multiple regular leaf splits across the tablet.
	const totalKeys = 1600
	oracle := make(map[string][]byte, totalKeys)
	for n := range totalKeys {
		i := (n * 7919) % totalKeys
		key := fmt.Sprintf("k-%05d", i)
		var padBuf [220]byte
		state := uint64(i)*0x9e3779b97f4a7c15 + 1
		for b := range padBuf {
			state ^= state >> 12
			state ^= state << 25
			state ^= state >> 27
			padBuf[b] = byte('a' + (state % 26))
		}
		doc := []byte(fmt.Sprintf(`{"id":%d,"group":%d,"pad":%q}`, i, i%10, string(padBuf[:])))
		canonical, err := vibejson.AppendCanonicalize(nil, doc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := collection.Put([]byte(key), doc); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		oracle[key] = canonical
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("initial Flush: %v", err)
	}

	splitsBefore := collection.Stats().PrimaryLeafSplits
	if splitsBefore < 4 {
		t.Fatalf("expected multiple leaf splits before delete, got %d", splitsBefore)
	}

	// Delete 98% of keys across all leaves, leaving each leaf non-empty (~4 rows,
	// ~1 KiB payload in a 4 KiB extent = ~25% full), then Flush in small waves
	// so the bounded per-fold merge budget (2 merges/fold) reclaims sparse leaves.
	for i := range totalKeys {
		if i%50 == 0 {
			continue
		}
		key := fmt.Sprintf("k-%05d", i)
		if _, err := collection.Delete([]byte(key)); err != nil {
			t.Fatalf("Delete %s: %v", key, err)
		}
		delete(oracle, key)
		if i%200 == 199 {
			if err := collection.Flush(); err != nil {
				t.Fatalf("delete wave Flush at %d: %v", i, err)
			}
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("final Flush: %v", err)
	}

	stats := collection.Stats()
	if stats.PrimaryLeafMerges == 0 {
		t.Fatalf("expected inline sibling leaf merges after 98%% delete, got PrimaryLeafMerges=0 (splits=%d emptyReclaims=%d)",
			stats.PrimaryLeafSplits, stats.PrimaryEmptyReclaims)
	}
	t.Logf("inline sibling leaf merges succeeded: splits=%d merges=%d emptyReclaims=%d",
		stats.PrimaryLeafSplits, stats.PrimaryLeafMerges, stats.PrimaryEmptyReclaims)

	// Capture a crash image right after merges and verify full scan parity.
	crashPath := filepath.Join(dir, "merge-underfill-crash.vibe")
	copyFileForCrash(t, path, crashPath)
	copyFileForCrash(t, path+".rjournal", crashPath+".rjournal")
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	crashFile, err := os.OpenFile(crashPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer crashFile.Close()
	reopened, err := Open(crashFile, options)
	if err != nil {
		t.Fatalf("reopen merged crash image: %v", err)
	}
	defer reopened.Close()

	snap, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	seen := make(map[string][]byte, len(oracle))
	if err := snap.RangeRaw(func(key, value []byte) error {
		seen[string(key)] = append([]byte(nil), value...)
		return nil
	}); err != nil {
		t.Fatalf("RangeRaw after reopen: %v", err)
	}
	if len(seen) != len(oracle) {
		t.Fatalf("reopened row count = %d, want %d", len(seen), len(oracle))
	}
	for k, want := range oracle {
		if got, ok := seen[k]; !ok || !bytes.Equal(got, want) {
			t.Fatalf("key %q mismatch after merge+reopen: got (%q, %v) want %q", k, got, ok, want)
		}
	}
}
