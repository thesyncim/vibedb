package query

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	rankAffineQueryBenchRows   = 8192
	rankAffineQueryBenchGroups = 16
	rankAffineQueryBenchPage   = fileSmallMaxRows
)

type rankAffineQueryBenchCase struct {
	name  string
	query *Query
}

// rankAffineQueryBenchScore is shared by the fixture builder and the query
// bounds so every query's expected result is derived from one source of truth.
func rankAffineQueryBenchScore(row int) int64 {
	return int64(10000 + 7*row)
}

// rankAffineQueryBenchDocument makes four sparse JSON shapes when sparse is
// true. The score and name values are affine in the physical row, while the
// optional fields create gaps in every shape's local ordinal sequence. The
// ordinary control carries both optional fields on every row, so its score and
// name remain local ordinal-affine with the same logical values.
func rankAffineQueryBenchDocument(row int, sparse bool) []byte {
	score := rankAffineQueryBenchScore(row)
	doc := fmt.Appendf(nil,
		`{"score":%d,"name":"item-%08d-end","bucket":%d`,
		score, score, row%rankAffineQueryBenchGroups,
	)
	if sparse {
		if row%7 < 3 {
			doc = fmt.Appendf(doc, `,"only":%d`, score)
		}
		if row%11 < 4 {
			doc = append(doc, `,"extra":true`...)
		}
	} else {
		doc = fmt.Appendf(doc, `,"only":%d,"extra":true`, score)
	}
	return append(doc, '}')
}

func rankAffineQueryBenchCases(rows int) []rankAffineQueryBenchCase {
	middle := rankAffineQueryBenchScore(rows / 2)
	quarter := rankAffineQueryBenchScore(rows / 4)
	threeQuarter := rankAffineQueryBenchScore(3 * rows / 4)
	return []rankAffineQueryBenchCase{
		{
			name: "projection-mixed",
			query: Select(
				Path("/score"), Path("/name"), Path("/only"), Path("/extra"),
			).OrderBy("/score", Asc).Limit(rankAffineQueryBenchPage),
		},
		{
			name: "group-sum",
			query: Select(Path("bucket"), Count(), Sum("score")).
				GroupBy("bucket").OrderBy("bucket", Asc),
		},
		{
			name: "numeric-eq",
			query: Select(Count()).Where(
				Cmp("score", Eq, rankAffineQueryBenchScore(17)),
			),
		},
		{
			name: "spelling-eq",
			query: Select(Count()).Where(
				Cmp("name", Eq, fmt.Sprintf("item-%08d-end", rankAffineQueryBenchScore(17))),
			),
		},
		{
			name:  "ordered-half",
			query: Select(Count()).Where(Cmp("score", Ge, middle)),
		},
		{
			name: "interval-middle",
			query: Select(Count()).Where(And(
				Cmp("score", Ge, quarter),
				Cmp("score", Lt, threeQuarter),
			)),
		},
		{
			name:  "min-max",
			query: Select(Min("score"), Max("score")),
		},
	}
}

type rankAffineQueryBenchFixture struct {
	sparse   bool
	snapshot *durable.Snapshot
	heap     store.Snapshot
	disk     rankAffineQueryBenchDiskProof
}

type rankAffineQueryBenchDiskProof struct {
	primaryLeaves   int
	compactLeaves   int
	scoreRankLeaves int
	nameRankLeaves  int
	scoreStreams    int
	nameStreams     int
}

func rankAffineQueryBenchSnapshot(
	tb testing.TB, sparse bool,
) rankAffineQueryBenchFixture {
	tb.Helper()
	file, err := os.CreateTemp(tb.TempDir(), "query-rank-affine-format-pair-*")
	if err != nil {
		tb.Fatalf("CreateTemp: %v", err)
	}
	tb.Cleanup(func() { _ = file.Close() })
	source := &store.Collection{}
	for row := range rankAffineQueryBenchRows {
		if _, err := source.Put(fmt.Sprintf("row-%07d", row), rankAffineQueryBenchDocument(row, sparse)); err != nil {
			tb.Fatalf("source row %d: %v", row, err)
		}
	}
	options := durable.Options{Collection: store.Options{ChunkDocuments: 64}}
	if _, err := durable.CreateFromPrimary(source, file, options); err != nil {
		tb.Fatalf("CreateFromPrimary: %v", err)
	}
	collection, err := durable.Open(file, options)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = collection.Close() })
	snapshot, err := collection.Snapshot()
	if err != nil {
		tb.Fatalf("Snapshot: %v", err)
	}
	tb.Cleanup(func() { _ = snapshot.Close() })
	heap, err := source.Snapshot()
	if err != nil {
		tb.Fatalf("heap Snapshot: %v", err)
	}
	return rankAffineQueryBenchFixture{
		sparse: sparse, snapshot: snapshot, heap: heap,
		disk: rankAffineQueryBenchDiskProofFromFile(tb, file),
	}
}

// rankAffineQueryBenchDiskProofFromFile examines only compact leaves reachable
// from the selected primary root. The score and name hole ordinals are resolved
// against each canonical shape before parsing its streams. A sparse shape has a
// physical-rank stream when its stream envelope count is the whole leaf row
// count rather than its own shape ordinal count. The parser deliberately does
// not depend on the candidate-only stream kind enum, so the same proof runs
// against the fallback.
func rankAffineQueryBenchDiskProofFromFile(
	tb testing.TB, file *os.File,
) rankAffineQueryBenchDiskProof {
	tb.Helper()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		tb.Fatalf("DiscoverMutableInlineBootstrap: %v", err)
	}
	scratch := make([]byte, bootstrap.MaxPageSize)
	inline, root, _, _, err := storeio.RecoverInlineStateRootWithFallback(
		file, bootstrap.PageSize, scratch,
	)
	if err != nil {
		tb.Fatalf("RecoverInlineStateRootWithFallback: %v", err)
	}
	cache, err := storeio.NewPageCache(file, storeio.PageCacheOptions{
		PageSize:        int(root.PageSize),
		MaxPageSize:     int(bootstrap.MaxPageSize),
		ResidentBytes:   max(int64(bootstrap.MaxPageSize)*64, 8<<20),
		StoreID:         root.StoreID,
		Backend:         storeio.BackendPortable,
		ReadConcurrency: 1,
	})
	if err != nil {
		tb.Fatalf("NewPageCache: %v", err)
	}
	tb.Cleanup(func() { _ = cache.Close() })
	bounds := storeio.GlobalTabletCatalogBounds{
		StoreID:                root.StoreID,
		SelectedRootGeneration: root.Generation,
		FileEnd:                inline.FileEnd,
		NextLogicalID:          root.NextLogicalID,
	}
	leafBounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           inline.FileEnd,
		NextLogicalID:     root.NextLogicalID,
		AllocationQuantum: root.PageSize,
	}
	var proof rankAffineQueryBenchDiskProof
	err = storeio.VisitPrimaryGraphRefs(cache, root.PrimaryRoot, bounds, func(ref storeio.PageRef) error {
		if ref.Kind != storeio.PagePrimaryLeaf {
			return nil
		}
		proof.primaryLeaves++
		page := make([]byte, int(ref.Length))
		n, readErr := file.ReadAt(page, int64(ref.Offset))
		if readErr != nil {
			return fmt.Errorf("read primary leaf at %d: %w", ref.Offset, readErr)
		}
		if n != len(page) {
			return fmt.Errorf("short primary leaf at %d: read %d of %d bytes", ref.Offset, n, len(page))
		}
		header, payload, err := storeio.OpenPage(page)
		if err != nil {
			return fmt.Errorf("open primary leaf at %d: %w", ref.Offset, err)
		}
		if header.Kind != storeio.PagePrimaryLeaf ||
			storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
			return nil
		}
		bucket := storeio.BucketID(header.LogicalID - storeio.PrimaryLeafLogicalIDBase)
		expected := storeio.PageRef{
			Offset: ref.Offset, Length: ref.Length, LogicalID: header.LogicalID,
			Generation: header.Generation, Kind: storeio.PagePrimaryLeaf,
		}
		stripe, err := storeio.OpenCompactPrimaryStripe(
			page, root.StoreID, bucket, expected, root.Generation, leafBounds,
		)
		if err != nil {
			return fmt.Errorf("open compact primary leaf at %d: %w", ref.Offset, err)
		}
		proof.compactLeaves++
		if stripe.Len() == 0 {
			return nil
		}
		var scoreResolver, nameResolver storeio.UnifiedHoleResolver
		if err := scoreResolver.SetPath([]byte("/score")); err != nil {
			return fmt.Errorf("set score resolver: %w", err)
		}
		if err := nameResolver.SetPath([]byte("/name")); err != nil {
			return fmt.Errorf("set name resolver: %w", err)
		}
		scoreHoles, ok := stripe.ResolveHoles(nil, &scoreResolver)
		if !ok {
			return fmt.Errorf("resolve score holes")
		}
		nameHoles, ok := stripe.ResolveHoles(nil, &nameResolver)
		if !ok {
			return fmt.Errorf("resolve name holes")
		}
		score, name, err := rankAffineQueryBenchCompactRankDomainStreams(
			payload, scoreHoles, nameHoles,
		)
		if err != nil {
			return fmt.Errorf("parse compact primary leaf at %d: %w", ref.Offset, err)
		}
		proof.scoreStreams += score
		proof.nameStreams += name
		if score > 0 {
			proof.scoreRankLeaves++
		}
		if name > 0 {
			proof.nameRankLeaves++
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("VisitPrimaryGraphRefs: %v", err)
	}
	if err := cache.Close(); err != nil {
		tb.Fatalf("close graph proof cache: %v", err)
	}
	return proof
}

func rankAffineQueryBenchCompactRankDomainStreams(
	payload []byte, scoreHoles, nameHoles []int,
) (scoreStreams, nameStreams int, err error) {
	const (
		headerBytes        = 40
		shapeHeaderBytes   = 16
		streamHeaderBytes  = 12
		compactHasOverflow = byte(1 << 0)
	)
	bad := func(what string) (int, int, error) {
		return 0, 0, fmt.Errorf("%s", what)
	}
	if len(payload) < headerBytes || string(payload[:4]) != "VCS1" {
		return bad("invalid compact stripe header")
	}
	rows := int(binary.LittleEndian.Uint32(payload[4:8]))
	shapeCount := int(binary.LittleEndian.Uint16(payload[8:10]))
	flags := payload[11]
	if flags&^compactHasOverflow != 0 {
		return bad("unknown compact stripe flags")
	}
	keyBytes := uint64(binary.LittleEndian.Uint32(payload[12:16]))
	shapeCodeBytes := uint64(binary.LittleEndian.Uint32(payload[16:20]))
	rankBytes := uint64(binary.LittleEndian.Uint32(payload[20:24]))
	shapeBytes := uint64(binary.LittleEndian.Uint32(payload[24:28]))
	slotBytes := uint64(binary.LittleEndian.Uint32(payload[28:32]))
	summaryBytes := uint64(binary.LittleEndian.Uint32(payload[36:40]))
	if rows <= 0 || shapeCount <= 0 {
		return bad("empty compact stripe geometry")
	}
	dirBytes := uint64(4 * shapeCount)
	prefixBeforeOverflow := uint64(headerBytes) + dirBytes + keyBytes + slotBytes
	overflowBitmapBytes := uint64(0)
	if flags&compactHasOverflow != 0 {
		overflowBitmapBytes = uint64((rows + 7) / 8)
	}
	bitmapEnd := prefixBeforeOverflow + overflowBitmapBytes
	if prefixBeforeOverflow > uint64(len(payload)) ||
		bitmapEnd > uint64(len(payload)) {
		return bad("compact stripe fixed sections out of bounds")
	}
	overflowCount := 0
	for _, value := range payload[prefixBeforeOverflow:bitmapEnd] {
		overflowCount += bits.OnesCount8(value)
	}
	shapeDataStart := bitmapEnd + uint64(overflowCount*storeio.PageRefSize) +
		shapeCodeBytes + rankBytes
	shapeDataEnd := shapeDataStart + shapeBytes
	if shapeDataStart > uint64(len(payload)) || shapeDataEnd > uint64(len(payload)) ||
		shapeDataEnd+summaryBytes != uint64(len(payload)) {
		return bad("compact stripe shape data out of bounds")
	}
	shapeData := payload[int(shapeDataStart):int(shapeDataEnd)]
	shapeDir := payload[headerBytes : headerBytes+int(dirBytes)]
	previousEnd := uint32(0)
	if len(scoreHoles) != shapeCount || len(nameHoles) != shapeCount {
		return bad("compact stripe resolver shape count")
	}
	for shape := 0; shape < shapeCount; shape++ {
		end := binary.LittleEndian.Uint32(shapeDir[shape*4:])
		if end <= previousEnd || uint64(end) > shapeBytes {
			return bad("compact stripe shape directory")
		}
		raw := shapeData[int(previousEnd):int(end)]
		previousEnd = end
		if len(raw) < shapeHeaderBytes {
			return bad("compact stripe shape header")
		}
		shapeRows := int(binary.LittleEndian.Uint32(raw[0:4]))
		holes := int(binary.LittleEndian.Uint16(raw[4:6]))
		templateBytes := int(binary.LittleEndian.Uint32(raw[8:12]))
		streamBytes := int(binary.LittleEndian.Uint32(raw[12:16]))
		if holes < 2 || templateBytes < 8+(holes+1)*4 ||
			shapeHeaderBytes+templateBytes+streamBytes != len(raw) {
			return bad("compact stripe shape geometry")
		}
		streams := raw[shapeHeaderBytes+templateBytes:]
		if scoreHoles[shape] < 0 || scoreHoles[shape] >= holes ||
			nameHoles[shape] < 0 || nameHoles[shape] >= holes {
			return bad("compact stripe resolver hole")
		}
		cursor := 0
		for hole := 0; hole < holes; hole++ {
			if cursor < 0 || len(streams)-cursor < streamHeaderBytes {
				return bad("compact stripe stream header")
			}
			stream := streams[cursor:]
			dictCount := uint64(binary.LittleEndian.Uint16(stream[2:4]))
			count := uint64(binary.LittleEndian.Uint32(stream[4:8]))
			dictBytes := uint64(binary.LittleEndian.Uint16(stream[8:10]))
			dataBytes := uint64(binary.LittleEndian.Uint16(stream[10:12]))
			encoded := uint64(streamHeaderBytes) + 2*dictCount + dictBytes + dataBytes
			if encoded > uint64(len(streams)-cursor) {
				return bad("compact stripe stream bounds")
			}
			if shapeRows < rows && count == uint64(rows) {
				if hole == scoreHoles[shape] {
					scoreStreams++
				}
				if hole == nameHoles[shape] {
					nameStreams++
				}
			}
			cursor += int(encoded)
		}
		if cursor != len(streams) {
			return bad("compact stripe stream tail")
		}
	}
	if uint64(previousEnd) != shapeBytes {
		return bad("compact stripe shape tail")
	}
	return scoreStreams, nameStreams, nil
}

// rankAffineQueryBenchExpected runs the generic heap executor before timing.
// Only the canonical result keys are retained, so the durable result checks do
// not share mutable Result or Exec storage with the oracle.
func rankAffineQueryBenchExpected(
	tb testing.TB,
	heap store.Snapshot,
	cases []rankAffineQueryBenchCase,
) []string {
	tb.Helper()
	want := make([]string, len(cases))
	for i, tc := range cases {
		result, err := tc.query.Run(FromSnapshot(heap))
		if err != nil {
			tb.Fatalf("generic oracle %s: %v", tc.name, err)
		}
		want[i] = resultKey(result)
		result.Release()
	}
	return want
}

func assertRankAffineQueryBenchResult(
	tb testing.TB, execution *Exec, want string,
) {
	tb.Helper()
	if got := resultKey(execution.Result); got != want {
		tb.Fatalf("query result differs from heap oracle: got %d bytes, want %d bytes", len(got), len(want))
	}
}

// The environment gate lets one benchmark source run on both the historical
// fallback and the RankAffine candidate. Candidate runs set the gate to prove
// that the sparse fixture entered the real native query lanes; no unsupported
// candidate-only API is timed or called.
func assertRankAffineQueryBenchNative(
	tb testing.TB,
	fixture rankAffineQueryBenchFixture,
	name string,
	stats ExecStats,
) {
	tb.Helper()
	rows := uint64(rankAffineQueryBenchRows)
	if name == "projection-mixed" {
		page := uint64(rankAffineQueryBenchPage)
		if stats.Workers != 1 || stats.RowsTotal != rows ||
			stats.RowsScanned != page || stats.ProjectedRows != page ||
			stats.Batches != 1 || !stats.PrimaryRangeBounded {
			tb.Fatalf("projection query did not use the native bounded lane: %+v", stats)
		}
		return
	}
	if os.Getenv("VIBEDB_EXPECT_RANK_AFFINE_QUERY") != "1" {
		return
	}
	if name == "numeric-eq" {
		if stats.Workers != 1 || stats.RowsTotal != rows ||
			stats.RowsScanned != rows || stats.TokenFilterRows != rows ||
			stats.TokenFilterFallbackRows != 0 || stats.Batches != 0 {
			lane := "ordinary"
			if fixture.sparse {
				lane = "sparse"
			}
			tb.Fatalf("%s numeric equality did not use native token lane: %+v",
				lane, stats)
		}
		return
	}
	if !fixture.sparse {
		switch name {
		case "ordered-half", "interval-middle":
			if stats.Workers != 1 || stats.RowsTotal != rows ||
				stats.RowsScanned != rows || stats.TokenFilterRows != rows ||
				stats.TokenFilterFallbackRows != 0 || stats.Batches != 0 {
				tb.Fatalf("ordinary %s query did not use native token lane: %+v", name, stats)
			}
		case "min-max":
			if stats.Workers != 1 || stats.RowsTotal != rows ||
				stats.RowsScanned != rows || stats.CoveringColumns != 1 ||
				stats.Batches != 0 {
				tb.Fatalf("ordinary extrema query did not use native covering lane: %+v", stats)
			}
		}
		return
	}
	switch name {
	case "group-sum":
		if stats.Workers != 1 || stats.RowsTotal != rows ||
			stats.RowsScanned != rows || stats.GroupedIntegerRows != rows ||
			stats.Batches != 0 {
			tb.Fatalf("sparse group query did not use native integer lane: %+v", stats)
		}
	case "spelling-eq", "ordered-half", "interval-middle":
		if stats.Workers != 1 || stats.RowsTotal != rows ||
			stats.RowsScanned != rows || stats.TokenFilterRows != rows ||
			stats.TokenFilterFallbackRows != 0 || stats.Batches != 0 {
			tb.Fatalf("sparse %s query did not use native token lane: %+v", name, stats)
		}
	case "min-max":
		if stats.Workers != 1 || stats.RowsTotal != rows ||
			stats.RowsScanned != rows || stats.CoveringColumns != 1 ||
			stats.Batches != 0 {
			tb.Fatalf("sparse extrema query did not use native covering lane: %+v", stats)
		}
	}
}

func assertRankAffineQueryBenchDiskProof(tb testing.TB, fixture rankAffineQueryBenchFixture) {
	tb.Helper()
	if !fixture.sparse {
		if fixture.disk.scoreStreams != 0 || fixture.disk.nameStreams != 0 {
			tb.Fatalf("ordinary control unexpectedly has rank-domain streams: %+v", fixture.disk)
		}
		return
	}
	if os.Getenv("VIBEDB_EXPECT_RANK_AFFINE_QUERY") == "1" &&
		(fixture.disk.primaryLeaves == 0 ||
			fixture.disk.compactLeaves != fixture.disk.primaryLeaves ||
			fixture.disk.scoreRankLeaves != fixture.disk.primaryLeaves ||
			fixture.disk.nameRankLeaves != fixture.disk.primaryLeaves) {
		tb.Fatalf("sparse fixture has no persisted score/name rank-domain proof: %+v", fixture.disk)
	}
}

func TestRankAffineQueryOrdinaryDecimalHeapOracle(t *testing.T) {
	fixture := rankAffineQueryBenchSnapshot(t, false)
	const rows = uint64(rankAffineQueryBenchRows)
	cases := []struct {
		name          string
		query         *Query
		wantTokenRows uint64
	}{
		{
			name: "fractional-ordered",
			query: Select(Count()).Where(
				Cmp("score", Lt, Number("10000.5")),
			),
		},
		{
			name: "fractional-interval",
			query: Select(Count()).Where(And(
				Cmp("score", Ge, Number("10000.5")),
				Cmp("score", Lt, Number("10014.5")),
			)),
		},
		{
			name:          "negative-zero-ordered",
			query:         Select(Count()).Where(Cmp("score", Ge, Number("-0"))),
			wantTokenRows: rows,
		},
		{
			name: "negative-zero-interval",
			query: Select(Count()).Where(And(
				Cmp("score", Ge, Number("-0")),
				Cmp("score", Lt, Number("10007")),
			)),
			wantTokenRows: rows,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oracle := Exec{Options: ExecOptions{Workers: 1}}
			defer oracle.Release()
			if err := tc.query.RunInto(&oracle, FromSnapshot(fixture.heap)); err != nil {
				t.Fatalf("heap oracle: %v", err)
			}
			file := Exec{Options: ExecOptions{Workers: 1}}
			defer file.Release()
			if err := tc.query.RunInto(&file, FromFile(fixture.snapshot)); err != nil {
				t.Fatalf("file query: %v", err)
			}
			if got, want := resultKey(file.Result), resultKey(oracle.Result); got != want {
				t.Fatalf("file result differs from heap oracle: got %d bytes, want %d bytes", len(got), len(want))
			}
			stats := file.Stats
			if stats.Workers != 1 || stats.RowsTotal != rows || stats.RowsScanned != rows ||
				stats.TokenFilterRows != tc.wantTokenRows ||
				stats.TokenFilterFallbackRows != 0 ||
				tc.wantTokenRows != 0 && stats.Batches != 0 {
				t.Fatalf("predicate stats=%+v want token rows=%d", stats, tc.wantTokenRows)
			}
		})
	}
}

func benchmarkRankAffineQueryBenchCases(
	b *testing.B,
	fixture rankAffineQueryBenchFixture,
	cases []rankAffineQueryBenchCase,
	want []string,
) {
	b.Helper()
	assertRankAffineQueryBenchDiskProof(b, fixture)
	for i, tc := range cases {
		tc, expected := tc, want[i]
		b.Run(tc.name, func(b *testing.B) {
			execution := Exec{Options: ExecOptions{Workers: 1}}
			defer execution.Release()
			source := FromFile(fixture.snapshot)
			var span FileRangeSource
			if tc.name == "projection-mixed" {
				span = NewFileRangeSource(
					[]byte("row-0000000"), []byte("row-0008192"), false,
				)
				span.BindPrimaryOrder("/score")
				source = FromFileRange(fixture.snapshot, &span)
			}
			if err := tc.query.RunInto(&execution, source); err != nil {
				b.Fatal(err)
			}
			assertRankAffineQueryBenchResult(b, &execution, expected)
			assertRankAffineQueryBenchNative(b, fixture, tc.name, execution.Stats)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := tc.query.RunInto(&execution, source); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			assertRankAffineQueryBenchResult(b, &execution, expected)
			assertRankAffineQueryBenchNative(b, fixture, tc.name, execution.Stats)
			b.ReportMetric(float64(rankAffineQueryBenchRows), "rows")
			b.ReportMetric(float64(execution.Result.RowCount), "result-rows")
			b.ReportMetric(float64(execution.Stats.TokenFilterRows), "token-rows")
			b.ReportMetric(float64(execution.Stats.GroupedIntegerRows), "grouped-rows")
			b.ReportMetric(float64(execution.Stats.CoveringColumns), "covered-columns")
			b.ReportMetric(float64(fixture.disk.scoreStreams), "persisted-score-rank-streams")
			b.ReportMetric(float64(fixture.disk.nameStreams), "persisted-name-rank-streams")
			b.ReportMetric(float64(execution.Stats.ProjectedRows), "projected-rows")
		})
	}
}

// BenchmarkRankAffineQueryFormat measures complete query execution over the
// same durable snapshot shape used by the candidate and the fallback. The
// sparse arm contains nonlocal affine values that can only use physical-rank
// streams; the ordinary arm has one shape and remains the control. Snapshot
// construction, heap oracle execution, one warm RunInto, and result checks are
// outside the timed loops.
func BenchmarkRankAffineQueryFormat(b *testing.B) {
	cases := rankAffineQueryBenchCases(rankAffineQueryBenchRows)
	for _, setup := range []struct {
		name   string
		sparse bool
	}{
		{name: "rank-affine-sparse", sparse: true},
		{name: "ordinary-fixed-shape", sparse: false},
	} {
		setup := setup
		b.Run(setup.name, func(b *testing.B) {
			fixture := rankAffineQueryBenchSnapshot(b, setup.sparse)
			want := rankAffineQueryBenchExpected(b, fixture.heap, cases)
			benchmarkRankAffineQueryBenchCases(b, fixture, cases, want)
		})
	}
}
