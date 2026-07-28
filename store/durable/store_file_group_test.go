package durable

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/document"
)

func TestFileSnapshotIndexScalarGroupsAndResidual(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "file-index-groups-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fs, err := Create(file, Options{
		Collection: store.Options{ChunkDocuments: 4},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	documents := []string{
		`{"kind":"a"}`,
		`{"kind":"b"}`,
		`{"kind":"a"}`,
		`{"missing":true}`,
		`{"kind":null}`,
		`{"kind":{"nested":true}}`,
		`{"kind":1}`,
		`{"kind":1.0}`,
	}
	for row, document := range documents {
		if _, err := fs.Put(fmt.Sprintf("k%d", row), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	var workspace IndexWorkspace
	groups, residual, ok, err := snapshot.AppendIndexScalarGroupsInto(
		nil, nil, &workspace, "kind",
	)
	if err != nil || !ok {
		t.Fatalf("index groups = ok %v err %v", ok, err)
	}
	if got := workspace.LastProbeStats(); got.CertificateRows != 6 ||
		got.CandidateRows != 6 || got.MatchedRows != 6 ||
		got.DocumentRecheckRows != 0 || got.PostingPages == 0 {
		t.Fatalf("index group stats = %+v", got)
	}
	counts := make(map[string]uint64, len(groups))
	for _, group := range groups {
		key := string(group.Value.Bytes())
		if _, numeric := group.Value.NumberBytes(); numeric {
			key = "number"
		}
		counts[key] += group.Count
	}
	want := map[string]uint64{`"a"`: 2, `"b"`: 1, "null": 1, "number": 2}
	if len(counts) != len(want) {
		t.Fatalf("certified groups = %v, want %v", counts, want)
	}
	for key, count := range want {
		if counts[key] != count {
			t.Fatalf("certified groups = %v, want %v", counts, want)
		}
	}
	var rows []store.Location
	var scratch []byte
	scratch, err = snapshot.RangeMasksRawRowsBuffer(
		residual, scratch,
		func(row store.Location, _, _ []byte) error {
			rows = append(rows, row)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(rows) != fmt.Sprint([]store.Location{{Chunk: 0, Slot: 3}, {Chunk: 1, Slot: 1}}) {
		t.Fatalf("residual rows = %v", rows)
	}
	rowAllocs := testing.AllocsPerRun(100, func() {
		var callErr error
		scratch, callErr = snapshot.RangeMasksRawRowsBuffer(
			residual, scratch[:0],
			func(store.Location, []byte, []byte) error { return nil },
		)
		if callErr != nil {
			panic(callErr)
		}
	})
	if rowAllocs != 0 {
		t.Fatalf("warmed row-address scan allocated %.2f times", rowAllocs)
	}

	reuseGroups := make([]IndexScalarGroup, 0, len(groups))
	reuseResidual := make([]store.Mask, 0, len(residual))
	reuseGroups, reuseResidual, _, err = snapshot.AppendIndexScalarGroupsInto(
		reuseGroups, reuseResidual, &workspace, "kind",
	)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var callErr error
		reuseGroups, reuseResidual, _, callErr =
			snapshot.AppendIndexScalarGroupsInto(
				reuseGroups[:0], reuseResidual[:0], &workspace, "kind",
			)
		if callErr != nil {
			panic(callErr)
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed index group scan allocated %.2f times", allocs)
	}
}

func TestFileSnapshotIndexGroupCatalogRejectsInvalidScalar(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Append("k0", []byte(`{"kind":"a"}`)); err != nil {
		t.Fatal(err)
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "file-index-group-corrupt-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Collection: store.Options{ChunkDocuments: 4},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 64 << 10,
	}
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	head := fs.state.Load().root.IndexGroupHead
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	page := make([]byte, head.Length)
	if n, err := file.ReadAt(page, int64(head.Offset)); err != nil || n != len(page) {
		t.Fatalf("read catalog = (%d,%v)", n, err)
	}
	valueAt := storeio.PageHeaderSize +
		storeio.IndexGroupCatalogPayloadHeaderSize +
		storeio.IndexGroupCatalogEntryHeaderSize
	page[valueAt] = '{'
	if _, err := storeio.SealPage(page); err != nil {
		t.Fatal(err)
	}
	if n, err := file.WriteAt(page, int64(head.Offset)); err != nil || n != len(page) {
		t.Fatalf("write catalog = (%d,%v)", n, err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if _, _, ok, err := snapshot.AppendIndexScalarGroupsInto(
		nil, nil, nil, "kind",
	); !ok || !errors.Is(err, storeio.ErrIndexGroupCatalogCorrupt) {
		t.Fatalf("corrupt catalog = ok %v err %v", ok, err)
	}
}

func TestFileSnapshotBulkIndexScalarGroupCatalog(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{ChunkDocuments: 4, ShapeTapes: true})
	if err != nil {
		t.Fatal(err)
	}
	documents := []string{
		`{"kind":"a"}`,
		`{"kind":"\u0061"}`,
		`{"missing":true}`,
		`{"kind":null}`,
		`{"kind":1}`,
		`{"kind":1.0}`,
	}
	for row, document := range documents {
		if err := builder.Append(fmt.Sprintf("k%d", row), []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "file-index-group-catalog-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Collection: store.Options{ChunkDocuments: 4, ShapeTapes: true},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 64 << 10,
	}
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	head := fs.state.Load().root.IndexGroupHead
	if head.Kind != storeio.PageIndexGroupCatalog {
		t.Fatalf("index group head = %+v", head)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var workspace IndexWorkspace
	groups, residual, ok, err := snapshot.AppendIndexScalarGroupsInto(
		nil, nil, &workspace, "kind",
	)
	if err != nil || !ok {
		t.Fatalf("catalog groups = ok %v err %v", ok, err)
	}
	if len(residual) != 0 || len(groups) != 3 {
		t.Fatalf("catalog groups = %d residual = %v", len(groups), residual)
	}
	if got := workspace.LastProbeStats(); got.CertificateRows != 6 ||
		got.MatchedRows != 6 || got.CandidateRows != 0 || got.PostingPages != 0 {
		t.Fatalf("catalog stats = %+v", got)
	}
	counts := make(map[string]uint64, len(groups))
	for _, group := range groups {
		key := string(group.Value.Bytes())
		if _, numeric := group.Value.NumberBytes(); numeric {
			key = "number"
		}
		if group.Value.Kind() == document.String {
			key = "string"
		}
		counts[key] += group.Count
	}
	if len(counts) != 3 || counts["string"] != 2 ||
		counts["null"] != 2 || counts["number"] != 2 {
		t.Fatalf("catalog counts = %v", counts)
	}
	reuseGroups := make([]IndexScalarGroup, 0, len(groups))
	reuseGroups, _, _, err = snapshot.AppendIndexScalarGroupsInto(
		reuseGroups, nil, &workspace, "kind",
	)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var callErr error
		reuseGroups, _, _, callErr = snapshot.AppendIndexScalarGroupsInto(
			reuseGroups[:0], nil, &workspace, "kind",
		)
		if callErr != nil {
			panic(callErr)
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed catalog scan allocated %.2f times", allocs)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	if created, err := fs.Put("k0", []byte(`{"kind":"b"}`)); err != nil || created {
		t.Fatalf("Put = (%v,%v)", created, err)
	}
	if got := fs.state.Load().root.IndexGroupHead; got != (storeio.PageRef{}) {
		t.Fatalf("document mutation retained index group head %+v", got)
	}
	retired := false
	for _, extent := range fs.retireScratch {
		if extent.Offset == head.Offset && extent.Length == uint64(head.Length) {
			retired = true
			break
		}
	}
	if !retired {
		t.Fatalf("document mutation did not retire index group head %+v", head)
	}
}

func TestFileSnapshotIndexScalarGroupCatalogSurvivesOrdinaryChurn(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 4, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	documents := []string{
		`{"kind":"a","version":0}`,
		`{"kind":"a","version":0}`,
		`{"kind":"b","version":0}`,
		`{"version":0}`,
	}
	for row, document := range documents {
		if err := builder.Append(
			fmt.Sprintf("k%d", row), []byte(document),
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "file-index-group-churn-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Collection: store.Options{ChunkDocuments: 4, ShapeTapes: true},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 64 << 10,
	}
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldHead := fs.state.Load().root.IndexGroupHead

	if created, err := fs.Put(
		"k1", []byte(`{"kind":"c","version":1}`),
	); err != nil || created {
		t.Fatalf("indexed update = (%v,%v)", created, err)
	}
	firstHead := fs.state.Load().root.IndexGroupHead
	if firstHead == (storeio.PageRef{}) || firstHead == oldHead {
		t.Fatalf("incremental catalog head = %+v, old %+v", firstHead, oldHead)
	}
	if created, err := fs.Put(
		"k4", []byte(`{"kind":"c","version":0}`),
	); err != nil || !created {
		t.Fatalf("indexed insert = (%v,%v)", created, err)
	}
	secondHead := fs.state.Load().root.IndexGroupHead
	if secondHead == (storeio.PageRef{}) || secondHead == firstHead {
		t.Fatalf("insert catalog head = %+v, previous %+v", secondHead, firstHead)
	}
	if deleted, err := fs.Delete("k4"); err != nil || !deleted {
		t.Fatalf("indexed delete = (%v,%v)", deleted, err)
	}
	thirdHead := fs.state.Load().root.IndexGroupHead
	if thirdHead == (storeio.PageRef{}) || thirdHead == secondHead {
		t.Fatalf("delete catalog head = %+v, previous %+v", thirdHead, secondHead)
	}
	// A document-only change with an equivalent indexed scalar can reuse the
	// immutable catalog byte-for-byte.
	if created, err := fs.Put(
		"k0", []byte(`{"kind":"a","version":2}`),
	); err != nil || created {
		t.Fatalf("non-indexed update = (%v,%v)", created, err)
	}
	if got := fs.state.Load().root.IndexGroupHead; got != thirdHead {
		t.Fatalf("unchanged tuple rewrote catalog: got %+v want %+v", got, thirdHead)
	}

	assertGroups := func(
		snapshot *Snapshot,
		want map[string]uint64,
	) {
		t.Helper()
		var workspace IndexWorkspace
		groups, residual, covered, err :=
			snapshot.AppendIndexScalarGroupsInto(
				nil, nil, &workspace, "kind",
			)
		if err != nil || !covered || len(residual) != 0 {
			t.Fatalf(
				"catalog groups = covered %v residual %v err %v",
				covered, residual, err,
			)
		}
		got := make(map[string]uint64, len(groups))
		for _, group := range groups {
			if group.Value.Kind() == document.String {
				text, ok := group.Value.StringBytes()
				if !ok {
					t.Fatal("invalid catalog string")
				}
				got[string(text)] += group.Count
			} else {
				got[string(group.Value.Bytes())] += group.Count
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("catalog groups = %v, want %v", got, want)
		}
		if stats := workspace.LastProbeStats(); stats.PostingPages != 0 ||
			stats.CertificateRows != snapshot.Len() {
			t.Fatalf("catalog probe stats = %+v", stats)
		}
	}
	assertGroups(oldSnapshot, map[string]uint64{
		"a": 2, "b": 1, "null": 1,
	})
	current, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertGroups(current, map[string]uint64{
		"a": 1, "b": 1, "c": 1, "null": 1,
	})
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	assertGroups(recovered, map[string]uint64{
		"a": 1, "b": 1, "c": 1, "null": 1,
	})
}

func TestFileSnapshotSegmentedIndexScalarGroupCatalog(t *testing.T) {
	const documents = 512
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 8, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for row := range documents {
		if err := builder.Append(
			fmt.Sprintf("k%03d", row),
			fmt.Appendf(nil, `{"kind":"value-%03d"}`, row),
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "file-index-group-segments-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Collection: store.Options{ChunkDocuments: 8, ShapeTapes: true},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 4096,
		MaxKeyBytes: 32, InlineValueBytes: 128,
		MaxDocumentBytes: 1024,
	}
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	state := fs.state.Load()
	var catalogRefs []storeio.PageRef
	for ref := state.root.IndexGroupHead; ref != (storeio.PageRef{}); {
		catalogRefs = append(catalogRefs, ref)
		lease, acquireErr := fs.cache.Acquire(ref)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		catalog := storeio.AdmittedIndexGroupCatalog(lease.Page())
		if !catalog.Segmented() {
			lease.Release()
			t.Fatalf("catalog page %+v is not segmented", ref)
		}
		ref = catalog.Header().Next
		lease.Release()
	}
	if len(catalogRefs) < 2 {
		t.Fatalf(
			"high-cardinality catalog pages = %d, want multiple",
			len(catalogRefs),
		)
	}

	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var workspace IndexWorkspace
	groups, residual, covered, err :=
		snapshot.AppendIndexScalarGroupsInto(
			nil, nil, &workspace, "kind",
		)
	if err != nil || !covered || len(residual) != 0 ||
		len(groups) != documents {
		t.Fatalf(
			"segmented groups = (%d,%d,%v,%v)",
			len(groups), len(residual), covered, err,
		)
	}
	if stats := workspace.LastProbeStats(); stats.PostingPages != 0 ||
		stats.CertificateRows != documents {
		t.Fatalf("segmented group stats = %+v", stats)
	}
	reuse := make([]IndexScalarGroup, 0, documents)
	reuse, _, _, err = snapshot.AppendIndexScalarGroupsInto(
		reuse[:0], nil, &workspace, "kind",
	)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		var callErr error
		reuse, _, _, callErr =
			snapshot.AppendIndexScalarGroupsInto(
				reuse[:0], nil, &workspace, "kind",
			)
		if callErr != nil || len(reuse) != documents {
			panic("segmented catalog reuse")
		}
	})
	if allocs != 0 {
		t.Fatalf(
			"segmented catalog warm allocations = %.2f, want zero",
			allocs,
		)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}

	if created, err := fs.Put(
		"k001", []byte(`{"kind":"changed"}`),
	); err != nil || created {
		t.Fatalf("segmented catalog mutation = (%v,%v)", created, err)
	}
	if got := fs.state.Load().root.IndexGroupHead; got != (storeio.PageRef{}) {
		t.Fatalf("segmented mutation retained catalog head %+v", got)
	}
	for _, ref := range catalogRefs {
		retired := false
		for _, extent := range fs.retireScratch {
			if extent.Offset <= ref.Offset &&
				uint64(ref.Length) <= extent.Length &&
				ref.Offset-extent.Offset <=
					extent.Length-uint64(ref.Length) {
				retired = true
				break
			}
		}
		if !retired {
			t.Fatalf("segmented catalog ref not retired %+v", ref)
		}
	}
	current, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	groups, residual, covered, err =
		current.AppendIndexScalarGroupsInto(
			groups[:0], residual[:0], &workspace, "kind",
		)
	if err != nil || !covered || len(residual) != 0 ||
		len(groups) != documents {
		t.Fatalf(
			"post-mutation posting groups = (%d,%d,%v,%v)",
			len(groups), len(residual), covered, err,
		)
	}
	if stats := workspace.LastProbeStats(); stats.PostingPages == 0 ||
		stats.CertificateRows != documents {
		t.Fatalf("post-mutation group stats = %+v", stats)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileSnapshotPackedIndexScalarGroupCatalogSurvivesMutation(t *testing.T) {
	const documents = 128
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 8, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for row := range documents {
		if err := builder.Append(
			fmt.Sprintf("k%03d", row),
			fmt.Appendf(
				nil, `{"kind":"categorical-value-%03d"}`, row,
			),
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(
		t.TempDir(), "file-index-group-packed-*",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := Options{
		Collection: store.Options{ChunkDocuments: 8, ShapeTapes: true},
		Indexes: []store.IndexDefinition{{
			Name: "kind", Paths: []string{"/kind"},
		}},
		PageSize: 4096, MaxPageSize: 64 << 10,
		MaxKeyBytes: 32, InlineValueBytes: 128,
		MaxDocumentBytes: 1024,
	}
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}
	fs, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	head := fs.state.Load().root.IndexGroupHead
	if head.Length <= uint32(options.PageSize) {
		t.Fatalf(
			"packed catalog extent = %d, want larger than %d",
			head.Length, options.PageSize,
		)
	}
	lease, err := fs.cache.Acquire(head)
	if err != nil {
		t.Fatal(err)
	}
	segmented := storeio.AdmittedIndexGroupCatalog(
		lease.Page(),
	).Segmented()
	lease.Release()
	if segmented {
		t.Fatal("single packed catalog unexpectedly segmented")
	}
	if created, err := fs.Put(
		"k001", []byte(`{"kind":"categorical-value-002"}`),
	); err != nil || created {
		t.Fatalf("packed catalog mutation = (%v,%v)", created, err)
	}
	currentHead := fs.state.Load().root.IndexGroupHead
	if currentHead == (storeio.PageRef{}) ||
		currentHead == head ||
		currentHead.Length <= uint32(options.PageSize) {
		t.Fatalf(
			"maintained packed catalog = %+v, old %+v",
			currentHead, head,
		)
	}
	snapshot, err := fs.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var workspace IndexWorkspace
	groups, residual, covered, err :=
		snapshot.AppendIndexScalarGroupsInto(
			nil, nil, &workspace, "kind",
		)
	if err != nil || !covered || len(residual) != 0 ||
		len(groups) != documents-1 {
		t.Fatalf(
			"maintained packed groups = (%d,%d,%v,%v)",
			len(groups), len(residual), covered, err,
		)
	}
	var merged uint64
	for _, group := range groups {
		merged += group.Count
	}
	if merged != documents {
		t.Fatalf("maintained packed group rows = %d", merged)
	}
}
