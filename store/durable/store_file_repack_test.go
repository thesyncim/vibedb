package durable

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// TestFilePrimaryRepackRoundTrip is the repack (vacuum-into) gate: a churned
// store whose lexical-adjacency locality has collapsed is rewritten through the
// ordinary snapshot/read path into a fresh store, and the result must be
//
//   - clean under the offline verify walker,
//   - byte-exact in contents against every key of the churned store, and
//   - back at ~1.00x leaf adjacency, having reclaimed the free space churn left.
func TestFilePrimaryRepackRoundTrip(t *testing.T) {
	present := 20_000
	budget := 40_000
	if testing.Short() {
		present = 4_000
		budget = 8_000
	}
	universe := present * 2

	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	oracle := make(map[int][]byte, present)
	for id := 0; id < universe; id += 2 {
		value := primaryChurnValue(nil, 0)
		if err := builder.Append(primaryChurnKey(id), value); err != nil {
			t.Fatalf("append id %d: %v", id, err)
		}
		oracle[id] = value
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	options := Options{
		Backend:       BackendPortable,
		ResidentBytes: 64 << 20,
		Durability:    DurabilityBufferedVisible,
	}
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "churn.vibe")
	srcFile, err := os.OpenFile(srcPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, srcFile, options); err != nil {
		t.Fatal(err)
	}

	collection, err := Open(srcFile, options)
	if err != nil {
		t.Fatal(err)
	}

	// Scatter the store with steady mixed churn: 70% same-size replace, 30%
	// delete+reinsert, matching the churn qualification workload.
	part := newPrimaryChurnPartition(universe)
	rng := rand.New(rand.NewPCG(0x6d75726d75726174, 0x9e3779b97f4a7c15))
	valueScratch := make([]byte, 0, 64)
	revision, fallbacks, applied := 0, 0, 0
	for applied < budget {
		if rng.Float64() < 0.70 {
			applied += primaryChurnReplace(t, collection, oracle, part, rng, &revision, &valueScratch, &fallbacks)
		} else {
			applied += primaryChurnMove(t, collection, oracle, part, rng, &revision, &valueScratch, &fallbacks)
		}
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}

	churnedAdjacency, churnedFileEnd := repackTestAdjacency(t, collection)
	t.Logf(
		"churned store: adjacency %.2fx extents, file_end %d, live keys %d",
		churnedAdjacency, churnedFileEnd, len(oracle),
	)
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	// Repack through the ordinary read path into a fresh store.
	srcReopen, err := os.OpenFile(srcPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer srcReopen.Close()
	outPath := filepath.Join(dir, "repacked.vibe")
	outFile, err := os.OpenFile(outPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()

	report, err := Repack(srcReopen, outFile, options)
	if err != nil {
		t.Fatalf("repack: %v", err)
	}
	if report.Documents != len(oracle) {
		t.Fatalf("repack documents = %d, want %d", report.Documents, len(oracle))
	}
	if report.OutputFileEnd > report.SourceFileEnd {
		t.Fatalf(
			"repack grew the file: output_file_end %d > source_file_end %d",
			report.OutputFileEnd, report.SourceFileEnd,
		)
	}
	t.Logf(
		"repacked store: documents %d, source_file_end %d -> output_file_end %d (%.1f%% of source)",
		report.Documents, report.SourceFileEnd, report.OutputFileEnd,
		100*float64(report.OutputFileEnd)/float64(report.SourceFileEnd),
	)

	// The repacked store must verify clean under the offline walker.
	verifyReport, err := Verify(outFile)
	if err != nil {
		t.Fatalf("verify repacked: %v", err)
	}
	if !verifyReport.OK() {
		for _, finding := range verifyReport.Findings {
			t.Errorf("verify finding: %+v", finding)
		}
		t.Fatalf("repacked store failed verify with %d findings", len(verifyReport.Findings))
	}

	// Reopen the repacked store and compare contents byte-exact against every
	// live key, and confirm every absent key still misses.
	repacked, err := Open(outFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer repacked.Close()
	if got := int(repacked.Len()); got != len(oracle) {
		t.Fatalf("repacked Len = %d, want %d", got, len(oracle))
	}
	buffer := make([]byte, 0, 64)
	for id := 0; id < universe; id++ {
		want, wantOK := oracle[id]
		got, ok, err := repacked.AppendRaw(buffer[:0], primaryChurnKey(id))
		if err != nil || ok != wantOK || !bytes.Equal(got, want) {
			t.Fatalf(
				"contents drift id %d: got %q,%v,%v want %q,%v",
				id, got, ok, err, want, wantOK,
			)
		}
		buffer = got
	}

	// Adjacency must be restored to the pristine bulk-build value of ~1.00x.
	repackedAdjacency, repackedFileEnd := repackTestAdjacency(t, repacked)
	t.Logf(
		"repacked adjacency %.2fx extents, file_end %d",
		repackedAdjacency, repackedFileEnd,
	)
	if repackedAdjacency > 1.01 {
		t.Fatalf(
			"repack did not restore locality: adjacency %.2fx, want <= 1.01x",
			repackedAdjacency,
		)
	}
}

// repackTestAdjacency reports the mean |Δoffset| between lexically adjacent
// leaves divided by the mean extent size (the locality gate metric) for the
// collection's current durable graph, plus its file end.
func repackTestAdjacency(t *testing.T, collection *Collection) (float64, uint64) {
	t.Helper()
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	leaves, _, err := primaryChurnWalkGraph(collection)
	if err != nil {
		t.Fatalf("graph walk: %v", err)
	}
	if len(leaves) == 0 {
		t.Fatal("graph walk returned no leaves")
	}
	var adjSum, extentSum float64
	for i := range leaves {
		extentSum += float64(leaves[i].extent)
		if i == 0 {
			continue
		}
		delta := int64(leaves[i].offset) - int64(leaves[i-1].offset)
		if delta < 0 {
			delta = -delta
		}
		adjSum += float64(delta)
	}
	meanExtent := extentSum / float64(len(leaves))
	meanAdj := adjSum / float64(max(len(leaves)-1, 1))
	ratio := 0.0
	if meanExtent != 0 {
		ratio = meanAdj / meanExtent
	}
	return ratio, collection.state.Load().super.FileEnd
}
