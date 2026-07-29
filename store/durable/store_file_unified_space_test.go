package durable

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// unifiedCompetitiveCorpus reproduces bench/competitive/corpus.go byte for
// byte (same PCG seeds, same draw order, same substitution scheme) so the U1
// space gate measures exactly the corpus the design's §9 arithmetic and the
// compact baselines (81.8 / 184.5 B/doc) were measured on. The generator is
// copied rather than imported because bench/competitive is a separate module
// by design (its competitor dependencies must not leak into the root module).
func unifiedCompetitiveCorpus(n int, high bool) ([]string, [][]byte) {
	countries := make([]string, 0, 100)
	const letters = "ABCDEFGHIJ"
	for i := range len(letters) {
		for j := range len(letters) {
			countries = append(countries, string(letters[i])+string(letters[j]))
		}
	}
	countries[26] = "PT"
	tiers := []string{"free", "pro", "team", "enterprise"}
	regions := []string{"eu-west-1", "eu-central-1", "us-east-1", "us-west-2", "ap-south-1"}
	tagPool := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	notes := []string{
		"steady state, no anomalies observed in the last reporting window",
		"migrated from the legacy pipeline during the maintenance window",
		"flagged for review after a threshold breach on the ingest path",
		"nominal; retention policy applied and checkpoint acknowledged",
	}
	rng := rand.New(rand.NewPCG(0x5DEECE66D, 0xB16B00B5))
	fill := rand.New(rand.NewPCG(0xC0FFEE, 0x1234567))
	uniq := func(sample string) string {
		if !high {
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
	buf := make([]byte, 0, 512)
	for i := range n {
		country := countries[rng.IntN(len(countries))]
		tier := uniq(tiers[rng.IntN(len(tiers))])
		region := uniq(regions[rng.IntN(len(regions))])
		note := uniq(notes[rng.IntN(len(notes))])
		score := rng.IntN(1000)
		joinedY := 2018 + rng.IntN(7)
		joinedM := 1 + rng.IntN(12)
		joinedD := 1 + rng.IntN(28)
		nTags := 2 + rng.IntN(3)

		buf = buf[:0]
		buf = append(buf, `{"id":`...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, `,"name":"user-`...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		buf = append(buf, `","country":"`...)
		buf = append(buf, country...)
		buf = append(buf, `","score":`...)
		buf = strconv.AppendInt(buf, int64(score), 10)
		buf = append(buf, `,"active":`...)
		if i%3 == 0 {
			buf = append(buf, "false"...)
		} else {
			buf = append(buf, "true"...)
		}
		buf = append(buf, `,"profile":{"tier":"`...)
		buf = append(buf, tier...)
		buf = append(buf, `","region":"`...)
		buf = append(buf, region...)
		buf = append(buf, `","joined":"`...)
		buf = append(buf, fmt.Sprintf("%04d-%02d-%02d", joinedY, joinedM, joinedD)...)
		buf = append(buf, `"},"tags":[`...)
		for t := range nTags {
			if t > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '"')
			buf = append(buf, uniq(tagPool[(i+t*3)%len(tagPool)])...)
			buf = append(buf, '"')
		}
		buf = append(buf, `],"note":"`...)
		buf = append(buf, note...)
		buf = append(buf, `"}`...)

		keys[i] = fmt.Sprintf("doc:%08d", i)
		docs[i] = append([]byte(nil), buf...)
	}
	return keys, docs
}

// unifiedLeafCensus walks every primary leaf through the resident router and
// aggregates the §11 census deliverable: leaves and rows per extent, shapes
// per leaf, dictionary entries per leaf, and the trivial-row fraction.
type unifiedLeafCensus struct {
	leaves         int
	rows           int
	templates      int
	dictionary     int
	trivial        int
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
		if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
			lease.Release()
			t.Fatalf("leaf %d is class %d", rank, storeio.PrimaryLeafClass(page))
		}
		state := c.state.Load()
		bounds := storeio.CommonPrimaryLeafBounds{
			FileEnd:           state.super.FileEnd,
			NextLogicalID:     state.root.NextLogicalID,
			AllocationQuantum: uint32(4096),
		}
		view, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
			page, state.root.StoreID, route.Bucket, bounds,
		)
		if !ok {
			lease.Release()
			t.Fatalf("leaf %d does not admit as unified", rank)
		}
		extent := len(page)
		census.leaves++
		census.rows += view.Len()
		census.templates += view.TemplateCount()
		census.dictionary += view.DictionaryCount()
		census.trivial += view.TrivialRowCount()
		census.leavesByExtent[extent]++
		census.rowsByExtent[extent] += view.Len()
		lease.Release()
	}
	return census
}

// TestUnifiedSpaceCompetitiveCorpus is the U1 space gate and census
// deliverable: both competitive corpora (100k ~249 B documents, low and high
// cardinality) built through the unified path must land at or below the
// compact baselines of 81.8 / 184.5 B/doc (§1.1, §9; projection 70–75 /
// 164–171), and the template census (shapes per leaf, trivial fraction) is
// reported for U2/U3. Skipped under -short: it builds 200k documents.
func TestUnifiedSpaceCompetitiveCorpus(t *testing.T) {
	if testing.Short() {
		t.Skip("space gate builds two 100k-document stores")
	}
	const n = 100_000
	gates := map[string]float64{"low": 81.8, "high": 184.5}
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
				source := compactPrimarySource(t, keys, docs)
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
				UnifiedLeaves: true, Durability: DurabilityAsyncVisible,
			})
			compactEnd := build("compact.vibe", Options{
				ResidentBytes: 256 << 20, Backend: BackendPortable,
				DocumentFormat: DocumentFormatCompact,
				Durability:     DurabilityAsyncVisible,
			})
			unifiedPerDoc := float64(unifiedEnd) / n
			compactPerDoc := float64(compactEnd) / n

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
			t.Logf("[%s] raw=%.1f B/doc unified=%.2f B/doc compact=%.2f B/doc (gate ≤ %.1f)",
				name, float64(rawBytes)/n, unifiedPerDoc, compactPerDoc, gates[name])
			t.Logf("[%s] census: leaves=%d rows=%d shapes/leaf=%.2f dict/leaf=%.1f trivial=%d (%.4f%%) leavesByExtent=%v rowsByExtent=%v",
				name, census.leaves, census.rows,
				float64(census.templates)/float64(census.leaves),
				float64(census.dictionary)/float64(census.leaves),
				census.trivial,
				100*float64(census.trivial)/float64(census.rows),
				census.leavesByExtent, census.rowsByExtent)
			if unifiedPerDoc > gates[name] {
				t.Fatalf("[%s] unified space %.2f B/doc exceeds the gate %.1f",
					name, unifiedPerDoc, gates[name])
			}
			if unifiedPerDoc > compactPerDoc {
				t.Fatalf("[%s] unified space %.2f B/doc exceeds compact %.2f on the same corpus",
					name, unifiedPerDoc, compactPerDoc)
			}
		})
	}
}
