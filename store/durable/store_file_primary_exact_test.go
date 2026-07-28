package durable

import (
	"fmt"
	"math/bits"
	"os"
	"slices"
	"testing"

	vibejson "github.com/thesyncim/vibejson"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestCreateFromPrimaryExactIndexDifferential builds the same indexed corpus on
// the legacy chunk store and the ordered primary graph and proves the exact
// index answers identically, the posting recheck fails closed on a dead slot,
// and the lookup is allocation-free.
func TestCreateFromPrimaryExactIndexDifferential(t *testing.T) {
	const documents = 10_000
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range documents {
		raw := fmt.Appendf(
			nil, `{"country":"c%03d","active":%t,"row":%d}`,
			row%100, row&1 == 0, row,
		)
		if err := builder.Append(fmt.Sprintf("k%05d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
			{Name: "country_alias", Paths: []string{"/country"}},
			{Name: "country_active", Paths: []string{"/country", "/active"}},
		},
	}
	legacyFile, err := os.CreateTemp(t.TempDir(), "legacy-exact-*")
	if err != nil {
		t.Fatal(err)
	}
	defer legacyFile.Close()
	primaryFile, err := os.CreateTemp(t.TempDir(), "primary-exact-*")
	if err != nil {
		t.Fatal(err)
	}
	defer primaryFile.Close()
	if _, err := CreateFrom(source, legacyFile, options); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(source, primaryFile, options); err != nil {
		t.Fatal(err)
	}
	legacy, err := Open(legacyFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	primary, err := Open(primaryFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	root := primary.state.Load().root
	if root.PrimaryRoot == (storeio.PageRef{}) ||
		root.ExactIndexRoot == (storeio.PageRef{}) ||
		root.IndexDirectory != (storeio.PageRef{}) ||
		root.IndexCount != 2 {
		t.Fatalf("primary exact roots/count = %+v %+v %+v/%d",
			root.PrimaryRoot, root.ExactIndexRoot, root.IndexDirectory,
			root.IndexCount)
	}

	country := primaryExactTestNeedle(t, `"c042"`)
	active := primaryExactTestNeedle(t, `true`)
	for _, probe := range []struct {
		name   string
		values []vibejson.Index
	}{
		{name: "country", values: []vibejson.Index{country}},
		{name: "country_alias", values: []vibejson.Index{country}},
		{name: "country_active", values: []vibejson.Index{country, active}},
	} {
		want := primaryExactTestKeys(t, legacy, probe.name, probe.values...)
		got := primaryExactTestKeys(t, primary, probe.name, probe.values...)
		if !slices.Equal(got, want) {
			t.Fatalf("%s primary keys differ: got %d want %d",
				probe.name, len(got), len(want))
		}
	}

	snapshot, err := primary.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var workspace IndexWorkspace
	dst := make([]store.Mask, 0, 128)
	dst, err = snapshot.AppendIndexMasksInto(
		dst, &workspace, "country", country,
	)
	if err != nil || primaryExactMaskRows(dst) != documents/100 {
		t.Fatalf("primary exact masks = %d rows, %v",
			primaryExactMaskRows(dst), err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var runErr error
		dst, runErr = snapshot.AppendIndexMasksInto(
			dst[:0], &workspace, "country", country,
		)
		if runErr != nil {
			panic(runErr)
		}
	})
	if allocs != 0 {
		t.Fatalf("primary exact lookup allocated %.2f times", allocs)
	}
	corruptTile := uint32(0)
	corruptBit := uint64(0)
	for tileID, live := range primary.primaryLive {
		if dead := ^live[0]; dead != 0 {
			corruptTile = tileID
			corruptBit = dead & -dead
			break
		}
	}
	if corruptBit == 0 {
		t.Fatal("test corpus unexpectedly has no dead primary slot")
	}
	if err := snapshot.RangeMasksRaw(
		[]store.Mask{{Chunk: corruptTile, Bits: corruptBit}},
		func(_, _ []byte) error { return nil },
	); err != store.ErrMaskChunk {
		t.Fatalf("outside-live primary mask = %v, want %v",
			err, store.ErrMaskChunk)
	}
}

// TestPrimaryExactIndexRejectsMutation pins the build-and-read contract: a
// mutation of an indexed ordered-primary collection fails closed rather than
// silently leaving a stale posting index, and reopening still succeeds because
// no divergent state was ever published.
func TestPrimaryExactIndexRejectsMutation(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 512 {
		raw := fmt.Appendf(nil, `{"country":"c%02d","row":%d}`, row%20, row)
		if err := builder.Append(fmt.Sprintf("k%04d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	file, err := os.CreateTemp(t.TempDir(), "primary-exact-readonly-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("k0000", []byte(`{"country":"cXX","row":0}`)); err !=
		ErrPrimaryExactIndexReadOnly {
		collection.Close()
		t.Fatalf("indexed primary Put = %v, want %v",
			err, ErrPrimaryExactIndexReadOnly)
	}
	if _, err := collection.Delete("k0000"); err != ErrPrimaryExactIndexReadOnly {
		collection.Close()
		t.Fatalf("indexed primary Delete = %v, want %v",
			err, ErrPrimaryExactIndexReadOnly)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	// The refused mutations published nothing, so the exact index still opens and
	// answers, proving nothing diverged.
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	needle := primaryExactTestNeedle(t, `"c07"`)
	keys := primaryExactTestKeys(t, reopened, "country", needle)
	if len(keys) != 512/20 && len(keys) != 512/20+1 {
		t.Fatalf("reopened indexed primary lookup rows = %d", len(keys))
	}
}

// TestCreateFromPrimaryExactIndexDeterministic pins byte-identical rebuilds of
// an indexed ordered-primary file: the graph, the exact posting tiles, and the
// reference catalog all encode to the same bytes on every build of the same
// input.
func TestCreateFromPrimaryExactIndexDeterministic(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 3000 {
		raw := fmt.Appendf(
			nil, `{"country":"c%03d","active":%t,"row":%d}`,
			row%100, row&1 == 0, row,
		)
		if err := builder.Append(fmt.Sprintf("k%05d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		// Byte determinism holds only without a recovery journal: the sync
		// lane mints a random per-store JournalID into the state root.
		Durability: DurabilityAsyncVisible,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
			{Name: "country_active", Paths: []string{"/country", "/active"}},
		},
	}
	build := func(name string) []byte {
		file, err := os.CreateTemp(t.TempDir(), name)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if _, err := CreateFromPrimary(source, file, options); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	first := build("determinism-first-*")
	second := build("determinism-second-*")
	if !slices.Equal(first, second) {
		t.Fatalf(
			"same indexed primary input produced different files (%d and %d bytes)",
			len(first), len(second),
		)
	}
}

// TestPrimaryExactIndexTemplateLeaves exercises the template-columnar leaf class
// on the exact-index path. The redundant corpus template-compresses, so its
// leaves are PagePrimaryLeaf template images with no hash directory; the posting
// slot is the lexical rank at both build and read. The test proves at least one
// template leaf exists and that the exact index answers identically to the
// legacy chunk store on that corpus.
func TestPrimaryExactIndexTemplateLeaves(t *testing.T) {
	const documents = 4000
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for at := range documents {
		raw := fmt.Appendf(nil,
			`{"id":%d,"kind":"document","group":%d,"active":%t,`+
				`"tier":"standard","region":"eu-west-1","name":"row %d"}`,
			at, at%997, at%3 == 0, at)
		if err := builder.Append(
			fmt.Sprintf("primary-key-%09d", at), raw,
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Indexes: []store.IndexDefinition{
			{Name: "active", Paths: []string{"/active"}},
			{Name: "group", Paths: []string{"/group"}},
		},
	}
	legacyFile, err := os.CreateTemp(t.TempDir(), "legacy-tmpl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer legacyFile.Close()
	primaryFile, err := os.CreateTemp(t.TempDir(), "primary-tmpl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer primaryFile.Close()
	if _, err := CreateFrom(source, legacyFile, options); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(source, primaryFile, options); err != nil {
		t.Fatal(err)
	}
	legacy, err := Open(legacyFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	primary, err := Open(primaryFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()

	// Prove the corpus actually placed template leaves, so this test genuinely
	// covers the template slot model rather than the succinct one.
	router := primary.primaryRouter.Load()
	templates := 0
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatal("router row missing")
		}
		lease, err := router.AcquireLeaf(primary.cache, route)
		if err != nil {
			t.Fatal(err)
		}
		if storeio.PrimaryLeafClass(lease.Page()) ==
			storeio.CommonPrimaryLeafTemplate {
			templates++
		}
		lease.Release()
	}
	if templates == 0 {
		t.Fatal("redundant corpus produced no template leaves; test would not cover the template posting path")
	}

	for _, probe := range []struct {
		name string
		raw  string
	}{
		{name: "active", raw: `true`},
		{name: "active", raw: `false`},
		{name: "group", raw: `42`},
		{name: "group", raw: `500`},
	} {
		needle := primaryExactTestNeedle(t, probe.raw)
		want := primaryExactTestKeys(t, legacy, probe.name, needle)
		got := primaryExactTestKeys(t, primary, probe.name, needle)
		if !slices.Equal(got, want) {
			t.Fatalf("%s=%s template primary keys differ: got %d want %d",
				probe.name, probe.raw, len(got), len(want))
		}
	}
}

func primaryExactTestNeedle(t testing.TB, raw string) vibejson.Index {
	t.Helper()
	need, err := vibejson.RequiredIndexEntries([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	index, err := vibejson.BuildIndex(
		[]byte(raw), make([]vibejson.IndexEntry, need),
	)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func primaryExactTestKeys(
	t testing.TB, collection *Collection,
	name string, values ...vibejson.Index,
) []string {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	masks, err := snapshot.AppendIndexMasks(nil, name, values...)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, primaryExactMaskRows(masks))
	if err := snapshot.RangeMasksRaw(
		masks, func(key, _ []byte) error {
			keys = append(keys, string(key))
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	return keys
}

func primaryExactMaskRows(masks []store.Mask) int {
	rows := 0
	for _, mask := range masks {
		rows += bits.OnesCount64(mask.Bits)
	}
	return rows
}

var primaryExactBenchmarkRows int

func BenchmarkPrimaryExactLookupIteration(b *testing.B) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	const documents = 10_000
	for row := range documents {
		raw := fmt.Appendf(nil, `{"country":"c%03d","row":%d}`, row%100, row)
		if err := builder.Append(fmt.Sprintf("k%05d", row), raw); err != nil {
			b.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Indexes: []store.IndexDefinition{
			{Name: "country", Paths: []string{"/country"}},
		},
	}
	file, err := os.CreateTemp(b.TempDir(), "primary-exact-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromPrimary(source, file, options); err != nil {
		b.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		b.Fatal(err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	needle := primaryExactTestNeedle(b, `"c042"`)
	var workspace IndexWorkspace
	masks := make([]store.Mask, 0, 128)
	masks, err = snapshot.AppendIndexMasksInto(
		masks, &workspace, "country", needle,
	)
	if err != nil {
		b.Fatal(err)
	}
	byteCount, postings := 0, 0
	for _, resident := range collection.primaryExact {
		if resident.present {
			byteCount += len(resident.encoded)
			postings += resident.view.PostingLen()
		}
	}
	bytesPerPosting := float64(byteCount) / float64(postings)

	b.Run("lookup", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(bytesPerPosting, "exact-B/posting")
		for range b.N {
			masks, err = snapshot.AppendIndexMasksInto(
				masks[:0], &workspace, "country", needle,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
		primaryExactBenchmarkRows = primaryExactMaskRows(masks)
	})
	b.Run("lookup_iteration", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(bytesPerPosting, "exact-B/posting")
		for range b.N {
			masks, err = snapshot.AppendIndexMasksInto(
				masks[:0], &workspace, "country", needle,
			)
			if err != nil {
				b.Fatal(err)
			}
			rows := 0
			if err := snapshot.RangeMasksRaw(
				masks, func(_, _ []byte) error {
					rows++
					return nil
				},
			); err != nil {
				b.Fatal(err)
			}
			primaryExactBenchmarkRows = rows
		}
	})
}
