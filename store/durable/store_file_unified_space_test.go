package durable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/benchcorpus"
	"github.com/thesyncim/vibedb/internal/storeio"
)

// unifiedCompetitiveCorpus adapts the single shared deterministic corpus to
// the parallel key/document slices used by the durable tests. The nested
// competitive module imports the same dependency-free generator, so neither
// benchmark can silently drift away from the bytes reported by the other.
func unifiedCompetitiveCorpus(n int, high bool) ([]string, [][]byte) {
	corpus := benchcorpus.Corpus(n, high)
	keys := make([]string, n)
	docs := make([][]byte, n)
	for i := range corpus {
		keys[i] = corpus[i].Key
		docs[i] = corpus[i].JSON
	}
	return keys, docs
}

// unifiedLeafCensus walks every primary leaf through the resident router and
// aggregates leaves and rows per extent, shapes
// per leaf, dictionary entries per leaf, and the trivial-row fraction.
type unifiedLeafCensus struct {
	leaves         int
	rows           int
	templates      int
	dictionary     int
	trivial        int
	payloadBytes   int
	extentBytes    int
	leavesByExtent map[int]int
	rowsByExtent   map[int]int
}

func collectUnifiedLeafCensus(t *testing.T, c *Collection) unifiedLeafCensus {
	t.Helper()
	census := unifiedLeafCensus{
		leavesByExtent: map[int]int{}, rowsByExtent: map[int]int{},
	}
	router := c.primaryRouter.Load()
	if router == nil {
		t.Fatal("no primary router")
	}
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("route at rank %d", rank)
		}
		lease, err := router.AcquireLeaf(c.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		page := lease.Page()
		if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
			lease.Release()
			t.Fatalf("leaf %d is class %d", rank, storeio.PrimaryLeafClass(page))
		}
		state := c.state.Load()
		view, ok := storeio.AdmittedCompactPrimaryStripe(
			page, state.root.StoreID, route.Bucket,
		)
		if !ok {
			lease.Release()
			t.Fatalf("leaf %d does not admit as unified", rank)
		}
		extent := len(page)
		census.leaves++
		census.rows += view.Len()
		census.templates += view.ShapeCount()
		census.payloadBytes += view.EncodedPayloadBytes()
		census.extentBytes += extent
		census.leavesByExtent[extent]++
		census.rowsByExtent[extent] += view.Len()
		lease.Release()
	}
	return census
}

// TestUnifiedSpaceCompetitiveCorpus is the unified space and census test:
// deliverable: both competitive corpora (100k ~249 B documents, low and high
// cardinality) built through the unified path must land at or below the
// compact complete-file baselines of 10.3 / 70.0 B/doc, and the template
// census reports shapes per leaf and the trivial-row fraction. Skipped under
// -short: it builds 200k documents.
func TestUnifiedSpaceCompetitiveCorpus(t *testing.T) {
	if testing.Short() {
		t.Skip("space gate builds two 100k-document stores")
	}
	const n = 100_000
	gates := map[string]float64{"low": 10.3, "high": 70.0}
	for _, name := range []string{"low", "high"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			keys, docs := unifiedCompetitiveCorpus(n, name == "high")
			rawBytes := 0
			for i := range docs {
				rawBytes += len(docs[i])
			}
			build := func(file string, options Options) int64 {
				path := filepath.Join(dir, file)
				f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				source := unifiedPrimarySource(t, keys, docs)
				fileEnd, err := CreateFromPrimary(source, f, options)
				if err != nil {
					t.Fatalf("CreateFromPrimary(%s): %v", file, err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				return fileEnd
			}
			unifiedEnd := build("unified.vibe", Options{
				ResidentBytes: 256 << 20, Backend: BackendPortable,
				Durability: DurabilityAsyncVisible,
			})
			unifiedPerDoc := float64(unifiedEnd) / n

			reopened, err := os.OpenFile(filepath.Join(dir, "unified.vibe"), os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			collection, err := Open(reopened, Options{
				ResidentBytes: 256 << 20, Backend: BackendPortable,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = collection.Close()
				_ = reopened.Close()
			}()
			census := collectUnifiedLeafCensus(t, collection)
			if census.rows != n {
				t.Fatalf("census rows %d want %d", census.rows, n)
			}
			t.Logf("[%s] raw=%.1f B/doc unified=%d bytes %.2f B/doc (gate ≤ %.1f)",
				name, float64(rawBytes)/n, unifiedEnd, unifiedPerDoc, gates[name])
			t.Logf("[%s] census: leaves=%d rows=%d shapes/leaf=%.2f dict/leaf=%.1f trivial=%d (%.4f%%) leavesByExtent=%v rowsByExtent=%v",
				name, census.leaves, census.rows,
				float64(census.templates)/float64(census.leaves),
				float64(census.dictionary)/float64(census.leaves),
				census.trivial,
				100*float64(census.trivial)/float64(census.rows),
				census.leavesByExtent, census.rowsByExtent)
			t.Logf("[%s] compact payload=%.2f B/doc leaf extents=%.2f B/doc extent slack=%.2f B/doc",
				name,
				float64(census.payloadBytes)/n,
				float64(census.extentBytes)/n,
				float64(census.extentBytes-census.payloadBytes)/n,
			)
			if unifiedPerDoc > gates[name] {
				t.Fatalf("[%s] unified space %.2f B/doc exceeds the gate %.1f",
					name, unifiedPerDoc, gates[name])
			}
		})
	}
}
