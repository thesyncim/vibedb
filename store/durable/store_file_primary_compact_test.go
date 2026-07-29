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
)

// compactPrimaryCorpus builds a representative nested-document corpus. Most
// fields are drawn from small categorical pools (so a per-leaf value dictionary
// pays) with one per-row varying number and name (so literals exercise the token
// path), and the tags array has a variable length (so several shape templates
// coexist in one leaf). highCardinality substitutes same-length random strings
// for the categorical values, collapsing the dictionary win but keeping the
// template win — the two settings a compact writer's space is measured against.
func compactPrimaryCorpus(n int, highCardinality bool) ([]string, [][]byte) {
	rng := rand.New(rand.NewPCG(0x1234, 0x5678))
	fill := rand.New(rand.NewPCG(0xABCD, 0xEF01))
	countries := []string{"PT", "US", "DE", "FR", "GB", "JP", "BR", "IN"}
	tiers := []string{"free", "pro", "team", "enterprise"}
	notes := []string{
		"steady state, no anomalies observed",
		"migrated from the legacy pipeline",
		"flagged for review after a breach",
	}
	uniq := func(sample string) string {
		if !highCardinality {
			return sample
		}
		out := make([]byte, len(sample))
		for i := range out {
			out[i] = byte('a' + fill.IntN(26))
		}
		return string(out)
	}
	keys := make([]string, n)
	docs := make([][]byte, n)
	for i := range n {
		country := countries[rng.IntN(len(countries))]
		tier := uniq(tiers[rng.IntN(len(tiers))])
		note := uniq(notes[rng.IntN(len(notes))])
		nTags := 2 + rng.IntN(3)
		var b bytes.Buffer
		fmt.Fprintf(&b, `{"id":%d,"name":"user-%d","country":"%s","score":%d,"active":%t,"profile":{"tier":"%s"},"tags":[`,
			i, i, country, rng.IntN(1000), i%3 == 0, tier)
		for t := range nTags {
			if t > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"%s"`, uniq(tiers[(i+t)%len(tiers)]))
		}
		fmt.Fprintf(&b, `],"note":"%s"}`, note)
		keys[i] = fmt.Sprintf("doc:%08d", i)
		docs[i] = append([]byte(nil), b.Bytes()...)
	}
	return keys, docs
}

func compactPrimarySource(t *testing.T, keys []string, docs [][]byte) *store.Collection {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range keys {
		if err := builder.Append(keys[i], docs[i]); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func createFormattedPrimary(t *testing.T, path string, format DocumentFormat, source *store.Collection) *Collection {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{ResidentBytes: 16 << 20, Backend: BackendPortable, DocumentFormat: format}
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		t.Fatalf("CreateFromPrimary(format=%d): %v", format, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Verify(mustOpen(t, path))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.OK() {
		t.Fatalf("Verify findings for format %d: %+v", format, report.Findings)
	}
	reopened, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen with the default (verbatim) format: the reader must never need to
	// know which format wrote the file.
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

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func primaryLeafClassCounts(t *testing.T, c *Collection) map[storeio.CommonPrimaryLeafClass]int {
	t.Helper()
	router := c.primaryRouter.Load()
	if router == nil {
		t.Fatal("no primary router")
	}
	counts := map[storeio.CommonPrimaryLeafClass]int{}
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("route at rank %d", rank)
		}
		lease, err := router.AcquireLeaf(c.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		counts[storeio.PrimaryLeafClass(lease.Page())]++
		lease.Release()
	}
	return counts
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// TestCompactPrimaryByteExactness is the byte-exactness differential: a compact
// ordered-primary collection and a verbatim one, built from the same corpus,
// must agree with each other and with the original bytes on every read surface.
func TestCompactPrimaryByteExactness(t *testing.T) {
	for _, high := range []bool{false, true} {
		high := high
		name := "low"
		if high {
			name = "high"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			keys, docs := compactPrimaryCorpus(2500, high)
			want := make(map[string][]byte, len(keys))
			for i := range keys {
				want[keys[i]] = docs[i]
			}

			verbatim := createFormattedPrimary(t, filepath.Join(dir, "verbatim.vibe"),
				DocumentFormatVerbatim, compactPrimarySource(t, keys, docs))
			compact := createFormattedPrimary(t, filepath.Join(dir, "compact.vibe"),
				DocumentFormatCompact, compactPrimarySource(t, keys, docs))

			// The compact build must actually stage compact leaves.
			counts := primaryLeafClassCounts(t, compact)
			if counts[storeio.CommonPrimaryLeafCompact] == 0 {
				t.Fatalf("compact build produced no compact leaves: %v", counts)
			}
			if v := primaryLeafClassCounts(t, verbatim); v[storeio.CommonPrimaryLeafCompact] != 0 {
				t.Fatalf("verbatim build produced compact leaves: %v", v)
			}
			t.Logf("[%s] compact leaf classes: %v; disk compact=%d verbatim=%d",
				name, counts,
				fileSize(t, filepath.Join(dir, "compact.vibe")),
				fileSize(t, filepath.Join(dir, "verbatim.vibe")))

			verbatimSnap, err := verbatim.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer verbatimSnap.Close()
			compactSnap, err := compact.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer compactSnap.Close()

			// Point reads: every doc, both files, against the original bytes.
			for i := range keys {
				gotV, okV, err := verbatimSnap.AppendRaw(nil, []byte(keys[i]))
				if err != nil || !okV {
					t.Fatalf("verbatim AppendRaw(%q) = (%v,%v)", keys[i], okV, err)
				}
				gotC, okC, err := compactSnap.AppendRaw(nil, []byte(keys[i]))
				if err != nil || !okC {
					t.Fatalf("compact AppendRaw(%q) = (%v,%v)", keys[i], okC, err)
				}
				if !bytes.Equal(gotV, want[keys[i]]) {
					t.Fatalf("verbatim %q mismatch:\n got %q\nwant %q", keys[i], gotV, want[keys[i]])
				}
				if !bytes.Equal(gotC, want[keys[i]]) {
					t.Fatalf("compact %q mismatch:\n got %q\nwant %q", keys[i], gotC, want[keys[i]])
				}
			}

			// Ordered scans: both files agree row-for-row with the sorted corpus.
			collect := func(s *Snapshot) [][2]string {
				var rows [][2]string
				if err := s.RangeRaw(func(k, v []byte) error {
					rows = append(rows, [2]string{string(k), string(v)})
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				return rows
			}
			vRows, cRows := collect(verbatimSnap), collect(compactSnap)
			if len(vRows) != len(keys) || len(cRows) != len(keys) {
				t.Fatalf("scanned %d verbatim, %d compact, want %d", len(vRows), len(cRows), len(keys))
			}
			for i := range vRows {
				if vRows[i] != cRows[i] {
					t.Fatalf("scan row %d: verbatim %v compact %v", i, vRows[i], cRows[i])
				}
				if []byte(cRows[i][1]) == nil || cRows[i][1] != string(want[cRows[i][0]]) {
					t.Fatalf("scan row %d value mismatch for %q", i, cRows[i][0])
				}
			}
		})
	}
}

// TestCompactPrimaryDeterminism pins that the compact bulk build is byte-for-byte
// reproducible from the same source.
func TestCompactPrimaryDeterminism(t *testing.T) {
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
				DocumentFormat: DocumentFormatCompact, Durability: DurabilityAsyncVisible,
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
		t.Fatalf("compact bulk build is not deterministic: %d vs %d bytes", len(a), len(b))
	}
}

// TestCompactPrimaryMutation drives Put/Delete against a compact collection: a
// mutated compact leaf de-compacts into a raw leaf, and every read must remain
// byte-exact against the mutating reference.
func TestCompactPrimaryMutation(t *testing.T) {
	dir := t.TempDir()
	keys, docs := compactPrimaryCorpus(1200, false)
	compact := createFormattedPrimary(t, filepath.Join(dir, "compact.vibe"),
		DocumentFormatCompact, compactPrimarySource(t, keys, docs))
	want := make(map[string][]byte, len(keys))
	for i := range keys {
		want[keys[i]] = docs[i]
	}

	origN := len(keys)
	rng := rand.New(rand.NewPCG(0x99, 0x11))
	for round := range 400 {
		i := rng.IntN(origN)
		key := keys[i]
		switch round % 4 {
		case 0, 1: // update in place
			val := fmt.Appendf(nil, `{"rev":%d,"key":"%s","note":"mutated"}`, round, key)
			if _, err := compact.Put([]byte(key), val); err != nil {
				t.Fatalf("put %q: %v", key, err)
			}
			want[key] = val
		case 2: // delete then reinsert original
			if _, err := compact.Delete([]byte(key)); err != nil {
				t.Fatalf("delete %q: %v", key, err)
			}
			if _, err := compact.Put([]byte(key), docs[i]); err != nil {
				t.Fatalf("reinsert %q: %v", key, err)
			}
			want[key] = docs[i]
		case 3: // insert a fresh key
			nk := fmt.Sprintf("new:%08d", round)
			val := fmt.Appendf(nil, `{"fresh":%d}`, round)
			if _, err := compact.Put([]byte(nk), val); err != nil {
				t.Fatalf("insert %q: %v", nk, err)
			}
			keys = append(keys, nk)
			want[nk] = val
		}
	}
	if err := compact.Flush(); err != nil {
		t.Fatal(err)
	}
	snap, err := compact.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	for k, v := range want {
		got, ok, err := snap.AppendRaw(nil, []byte(k))
		if err != nil || !ok {
			t.Fatalf("post-mutation AppendRaw(%q) = (%v,%v)", k, ok, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("post-mutation %q mismatch:\n got %q\nwant %q", k, got, v)
		}
	}
	// A full scan must still be ordered and complete.
	seen := 0
	var prev string
	if err := snap.RangeRaw(func(k, val []byte) error {
		if prev != "" && string(k) <= prev {
			t.Fatalf("scan out of order at %q after %q", k, prev)
		}
		prev = string(k)
		if !bytes.Equal(val, want[string(k)]) {
			t.Fatalf("scan value mismatch for %q", k)
		}
		seen++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(want) {
		t.Fatalf("scanned %d rows, want %d", seen, len(want))
	}
}
