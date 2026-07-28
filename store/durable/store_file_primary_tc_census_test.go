package durable

import (
	"fmt"
	randv2 "math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestPhase0TCCensus builds the exact competitive low-cardinality 10k corpus as
// an ordered primary graph and reports the leaf-class split plus docs-per-leaf,
// answering directly whether bulk build adopts template-columnar leaves for the
// benchmark corpus. Diagnostic; run with PHASE0=1.
func TestPhase0TCCensus(t *testing.T) {
	if os.Getenv("PHASE0") == "" {
		t.Skip("set PHASE0=1 to run the phase-0 TC census")
	}
	const count = 10_000
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, doc := range phase0Corpus(count) {
		if err := builder.Append(phase0Key(i), doc); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Backend: BackendPortable, ResidentBytes: 64 << 20}
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "tc.vibe"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		t.Fatal(err)
	}
	coll, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer coll.Close()
	root := coll.state.Load().root
	counts := filePrimaryLeafClassCounts(t, file, root)
	total := counts[1] + counts[2] + counts[3]
	info, _ := file.Stat()
	fmt.Printf("\n[TC census] docs=%d leaves=%d (narrow=%d wide=%d template=%d) file=%d docsPerLeaf=%.1f bytesPerDoc=%.1f\n",
		count, total, counts[1], counts[2], counts[3], info.Size(),
		float64(count)/float64(max(total, 1)), float64(info.Size())/float64(count))

	// Direct plan probe: for the first ~28 docs (one wide leaf's worth), report
	// the template-vs-raw byte decision and the per-doc metadata cost, which is
	// why TC loses on this corpus (small leaves, near-unique per-doc values, and
	// the fixed row+column/zone directory overhead dominate).
	docs := phase0Corpus(64)
	for _, n := range []int{14, 19, 24, 28} {
		rows := make([]storeio.TemplateColumnarLeafRow, n)
		for at := range rows {
			rows[at] = storeio.TemplateColumnarLeafRow{
				Slot: uint8(at), Key: []byte(phase0Key(at)), JSON: docs[at],
			}
		}
		plan, image, planErr := storeio.PlanTemplateColumnarLeaf(rows)
		if planErr != nil {
			fmt.Printf("[TC plan] n=%d err=%v\n", n, planErr)
			continue
		}
		fmt.Printf("[TC plan] n=%d rawBytes=%d templateBytes=%d useTemplate=%v tcBytesPerDoc=%.1f (4KB payload cap ~4030)\n",
			n, plan.RawBytes, plan.TemplateBytes, plan.UseTemplate, float64(len(image))/float64(n))
	}
}

func phase0Key(i int) string { return fmt.Sprintf("doc:%08d", i) }

// phase0Corpus replicates bench/competitive CorpusOf(n, LowCardinality) byte for
// byte so the census measures the exact benchmark corpus.
func phase0Corpus(n int) [][]byte {
	countries := func() []string {
		const letters = "ABCDEFGHIJ"
		out := make([]string, 0, 100)
		for i := range len(letters) {
			for j := range len(letters) {
				out = append(out, string(letters[i])+string(letters[j]))
			}
		}
		out[26] = "PT"
		return out
	}()
	tiers := []string{"free", "pro", "team", "enterprise"}
	regions := []string{"eu-west-1", "eu-central-1", "us-east-1", "us-west-2", "ap-south-1"}
	tagPool := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	notes := []string{
		"steady state, no anomalies observed in the last reporting window",
		"migrated from the legacy pipeline during the maintenance window",
		"flagged for review after a threshold breach on the ingest path",
		"nominal; retention policy applied and checkpoint acknowledged",
	}
	rng := randv2.New(randv2.NewPCG(0x5DEECE66D, 0xB16B00B5))
	docs := make([][]byte, n)
	buf := make([]byte, 0, 512)
	for i := range n {
		country := countries[rng.IntN(len(countries))]
		tier := tiers[rng.IntN(len(tiers))]
		region := regions[rng.IntN(len(regions))]
		note := notes[rng.IntN(len(notes))]
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
		for ti := range nTags {
			if ti > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, '"')
			buf = append(buf, tagPool[(i+ti*3)%len(tagPool)]...)
			buf = append(buf, '"')
		}
		buf = append(buf, `],"note":"`...)
		buf = append(buf, note...)
		buf = append(buf, `"}`...)
		docs[i] = append([]byte(nil), buf...)
	}
	return docs
}
