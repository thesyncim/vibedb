package durable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// unifiedPrimaryCorpus builds the representative low/high-cardinality corpus
// used by the unified codec's correctness, space, and mutation gates.
func unifiedPrimaryCorpus(n int, highCardinality bool) ([]string, [][]byte) {
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
		fmt.Fprintf(
			&b,
			`{"id":%d,"name":"user-%d","country":"%s","score":%d,"active":%t,"profile":{"tier":"%s"},"tags":[`,
			i, i, country, rng.IntN(1000), i%3 == 0, tier,
		)
		for tag := range nTags {
			if tag > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `"%s"`, uniq(tiers[(i+tag)%len(tiers)]))
		}
		fmt.Fprintf(&b, `],"note":"%s"}`, note)
		keys[i] = fmt.Sprintf("doc:%08d", i)
		docs[i] = append([]byte(nil), b.Bytes()...)
	}
	return keys, docs
}

func unifiedPrimarySource(
	t *testing.T, keys []string, docs [][]byte,
) *store.Collection {
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

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func primaryLeafClassCounts(
	t *testing.T, collection *Collection,
) map[storeio.CommonPrimaryLeafClass]int {
	t.Helper()
	router := collection.primaryRouter.Load()
	if router == nil {
		t.Fatal("no primary router")
	}
	counts := map[storeio.CommonPrimaryLeafClass]int{}
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("route at rank %d", rank)
		}
		lease, err := router.AcquireLeaf(collection.cache, route)
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
