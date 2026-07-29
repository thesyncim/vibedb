package durable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func createUnifiedPrimary(t *testing.T, path string, keys []string, docs [][]byte) *Collection {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		ResidentBytes: 16 << 20, Backend: BackendPortable, UnifiedLeaves: true,
	}
	if _, err := CreateFromPrimary(compactPrimarySource(t, keys, docs), file, options); err != nil {
		t.Fatalf("CreateFromPrimary(unified): %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(mustOpen(t, path))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("Verify findings: %+v", report.Findings)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen without the option: the reader dispatches on the class byte and
	// must never need to know which representation wrote the file.
	collection, err := Open(reopened, Options{ResidentBytes: 16 << 20, Backend: BackendPortable})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = collection.Close()
		_ = reopened.Close()
	})
	return collection
}

func canonicalDocs(t *testing.T, docs [][]byte) [][]byte {
	t.Helper()
	out := make([][]byte, len(docs))
	for i := range docs {
		canonical, err := vibejson.AppendCanonicalize(nil, docs[i])
		if err != nil {
			t.Fatalf("AppendCanonicalize: %v", err)
		}
		out[i] = canonical
	}
	return out
}

// TestUnifiedPrimaryCanonicalReads pins the whole unified read surface: a
// bulk-built unified store returns exactly the canonical spelling of every
// document (unified-leaf design §3.2 — canonicalization changes returned
// bytes by ruling) on point reads and ordered scans, and the build stages
// only class-5 leaves.
func TestUnifiedPrimaryCanonicalReads(t *testing.T) {
	for _, high := range []bool{false, true} {
		name := "low"
		if high {
			name = "high"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			keys, docs := compactPrimaryCorpus(2500, high)
			want := canonicalDocs(t, docs)
			unified := createUnifiedPrimary(t, filepath.Join(dir, "unified.vibe"), keys, docs)

			counts := primaryLeafClassCounts(t, unified)
			if counts[storeio.CommonPrimaryLeafUnified] == 0 {
				t.Fatalf("unified build produced no class-5 leaves: %v", counts)
			}
			for class, n := range counts {
				if class != storeio.CommonPrimaryLeafUnified && n != 0 {
					t.Fatalf("unified build staged class %d leaves: %v", class, counts)
				}
			}
			t.Logf("[%s] unified leaf classes: %v; disk=%d",
				name, counts, fileSize(t, filepath.Join(dir, "unified.vibe")))

			snapshot, err := unified.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			for i := range keys {
				got, ok, err := snapshot.AppendRaw(nil, []byte(keys[i]))
				if err != nil || !ok {
					t.Fatalf("AppendRaw(%q) = (%v,%v)", keys[i], ok, err)
				}
				if !bytes.Equal(got, want[i]) {
					t.Fatalf("point read %q:\n got %q\nwant %q", keys[i], got, want[i])
				}
			}
			rows := 0
			previous := ""
			if err := snapshot.RangeRaw(func(k, v []byte) error {
				if string(k) <= previous {
					return fmt.Errorf("scan order violation at %q", k)
				}
				previous = string(k)
				i := 0
				if _, err := fmt.Sscanf(string(k), "doc:%08d", &i); err != nil {
					return err
				}
				if !bytes.Equal(v, want[i]) {
					return fmt.Errorf("scan value mismatch for %q", k)
				}
				rows++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if rows != len(keys) {
				t.Fatalf("scanned %d rows want %d", rows, len(keys))
			}
		})
	}
}

// TestUnifiedPrimaryDeterminism pins that the unified bulk build is
// byte-for-byte reproducible from the same source (INVARIANT 2, the U1
// -count=2 gate at file strength).
func TestUnifiedPrimaryDeterminism(t *testing.T) {
	dir := t.TempDir()
	keys, docs := compactPrimaryCorpus(1500, false)
	build := func(name string) []byte {
		path := filepath.Join(dir, name)
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		// AsyncVisible mints no recovery journal, so the file carries no random
		// journal identity and a reproducible build is byte-for-byte identical.
		if _, err := CreateFromPrimary(compactPrimarySource(t, keys, docs), file,
			Options{
				ResidentBytes: 16 << 20, Backend: BackendPortable,
				UnifiedLeaves: true, Durability: DurabilityAsyncVisible,
			}); err != nil {
			t.Fatalf("CreateFromPrimary: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	a, b := build("a.vibe"), build("b.vibe")
	if !bytes.Equal(a, b) {
		t.Fatalf("unified bulk build is not deterministic: %d vs %d bytes", len(a), len(b))
	}
}

// TestUnifiedPrimaryMutation drives Put/Delete against a unified collection.
// In this phase a mutated class-5 leaf routes through the existing structural
// rewrite (unified-leaf design §11, U1 row): its rows render to canonical
// bytes and re-encode as raw leaves, and every read must remain exact against
// the mutating reference — canonical spellings for untouched bulk rows,
// verbatim Put bytes for mutated ones.
func TestUnifiedPrimaryMutation(t *testing.T) {
	dir := t.TempDir()
	keys, docs := compactPrimaryCorpus(1200, false)
	canonical := canonicalDocs(t, docs)
	unified := createUnifiedPrimary(t, filepath.Join(dir, "unified.vibe"), keys, docs)
	want := make(map[string][]byte, len(keys))
	for i := range keys {
		want[keys[i]] = canonical[i]
	}

	origN := len(keys)
	rng := rand.New(rand.NewPCG(0x99, 0x11))
	for round := range 400 {
		i := rng.IntN(origN)
		key := keys[i]
		switch round % 4 {
		case 0, 1: // update
			val := fmt.Appendf(nil, `{"rev":%d,"key":"%s","note":"mutated"}`, round, key)
			if _, err := unified.Put([]byte(key), val); err != nil {
				t.Fatalf("put %q: %v", key, err)
			}
			want[key] = val
		case 2: // delete then reinsert the canonical original
			if _, err := unified.Delete([]byte(key)); err != nil {
				t.Fatalf("delete %q: %v", key, err)
			}
			if _, err := unified.Put([]byte(key), canonical[i]); err != nil {
				t.Fatalf("reinsert %q: %v", key, err)
			}
			want[key] = canonical[i]
		case 3: // insert a fresh key
			nk := fmt.Sprintf("new:%08d", round)
			val := fmt.Appendf(nil, `{"fresh":%d}`, round)
			if _, err := unified.Put([]byte(nk), val); err != nil {
				t.Fatalf("insert %q: %v", nk, err)
			}
			keys = append(keys, nk)
			want[nk] = val
		}
	}
	snapshot, err := unified.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	for _, key := range keys {
		got, ok, err := snapshot.AppendRaw(nil, []byte(key))
		if err != nil || !ok {
			t.Fatalf("AppendRaw(%q) = (%v,%v)", key, ok, err)
		}
		if !bytes.Equal(got, want[key]) {
			t.Fatalf("read %q:\n got %q\nwant %q", key, got, want[key])
		}
	}
}

// TestUnifiedPrimaryWithIndexes pins that exact indexes built beside a
// unified graph stay consistent with the leaves' stable hash slots — the
// §3.1 one-slot-discipline invariant (posting slot == read slot), exercised
// through the indexed scan path.
func TestUnifiedPrimaryWithIndexes(t *testing.T) {
	dir := t.TempDir()
	keys, docs := compactPrimaryCorpus(2000, false)
	want := canonicalDocs(t, docs)
	path := filepath.Join(dir, "unified-indexed.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		ResidentBytes: 16 << 20, Backend: BackendPortable, UnifiedLeaves: true,
		Indexes: []store.IndexDefinition{{Name: "country", Paths: []string{"/country"}}},
	}
	if _, err := CreateFromPrimary(compactPrimarySource(t, keys, docs), file, options); err != nil {
		t.Fatalf("CreateFromPrimary(unified, indexed): %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(mustOpen(t, path))
	if err != nil || !report.OK() {
		t.Fatalf("Verify: %v %+v", err, report.Findings)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := Open(reopened, options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		_ = collection.Close()
		_ = reopened.Close()
	}()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	expected := map[string][]byte{}
	for i := range keys {
		if bytes.Contains(docs[i], []byte(`"country":"PT"`)) {
			expected[keys[i]] = want[i]
		}
	}
	needle, err := vibejson.BuildIndex([]byte(`"PT"`), make([]vibejson.IndexEntry, 4))
	if err != nil {
		t.Fatal(err)
	}
	masks, err := snapshot.AppendIndexMasks(nil, "country", needle)
	if err != nil {
		t.Fatalf("AppendIndexMasks: %v", err)
	}
	got := map[string][]byte{}
	if err := snapshot.RangeMasksRaw(masks, func(k, v []byte) error {
		got[string(k)] = append([]byte(nil), v...)
		return nil
	}); err != nil {
		t.Fatalf("RangeMasksRaw: %v", err)
	}
	if len(got) != len(expected) || len(expected) == 0 {
		t.Fatalf("index scan matched %d rows want %d", len(got), len(expected))
	}
	for key, value := range expected {
		if !bytes.Equal(got[key], value) {
			t.Fatalf("indexed row %q mismatch:\n got %q\nwant %q", key, got[key], value)
		}
	}
}

// TestUnifiedOptionValidation pins the option contract: unified leaves are
// mutually exclusive with the compact document format.
func TestUnifiedOptionValidation(t *testing.T) {
	invalid := Options{UnifiedLeaves: true, DocumentFormat: DocumentFormatCompact}
	if _, err := invalid.normalized(); err == nil {
		t.Fatal("UnifiedLeaves+DocumentFormatCompact must be rejected")
	}
}
