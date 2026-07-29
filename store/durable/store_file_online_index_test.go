package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestOnlineCreateIndexPublishesAtomicallyAndReopens(t *testing.T) {
	path := t.TempDir() + "/online.vjc"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
	}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	const documents = 2_000
	for row := range documents {
		raw := fmt.Appendf(
			nil, `{"id":%d,"group":"g%02d"}`, row, row%20,
		)
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("k%05d", row)), raw,
		); err != nil {
			t.Fatal(err)
		}
	}
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	info, err := collection.CreateIndex(store.IndexDefinition{
		Name: "by_group", Paths: []string{"/group"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.State != store.IndexReady || info.CoveredChunks != 1 {
		t.Fatalf("CreateIndex info = %+v", info)
	}
	needle := primaryExactTestNeedle(t, `"g07"`)
	if indexes := before.AppendIndexes(nil); len(indexes) != 0 {
		t.Fatalf("pre-publication snapshot saw indexes: %+v", indexes)
	}
	if _, err := before.AppendIndexMasks(nil, "by_group", needle); !errors.Is(
		err, store.ErrIndexNotFound,
	) {
		t.Fatalf("pre-publication snapshot probe = %v, want index not found", err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	got := primaryExactTestKeys(t, collection, "by_group", needle)
	if len(got) != documents/20 {
		t.Fatalf("indexed rows = %d, want %d", len(got), documents/20)
	}
	if _, err := collection.Put(
		[]byte("k00007"), []byte(`{"id":7,"group":"after"}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(
		[]byte("new"), []byte(`{"id":2000,"group":"after"}`),
	); err != nil {
		t.Fatal(err)
	}
	afterNeedle := primaryExactTestNeedle(t, `"after"`)
	if after := primaryExactTestKeys(
		t, collection, "by_group", afterNeedle,
	); len(after) != 2 {
		t.Fatalf("post-cutover indexed rows = %d, want 2", len(after))
	}
	got = primaryExactTestKeys(t, collection, "by_group", needle)
	if len(got) != documents/20-1 {
		t.Fatalf("post-cutover old-term rows = %d, want %d",
			len(got), documents/20-1)
	}
	root := collection.state.Load().root
	if root.IndexCount != 1 || root.ExactIndexRoot == (storeio.PageRef{}) {
		t.Fatalf("published index root/count = %+v/%d",
			root.ExactIndexRoot, root.IndexCount)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err = Open(file, Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	reopened := primaryExactTestKeys(t, collection, "by_group", needle)
	if !slices.Equal(reopened, got) {
		t.Fatalf("reopened keys differ: got %d want %d",
			len(reopened), len(got))
	}
	if after := primaryExactTestKeys(
		t, collection, "by_group", afterNeedle,
	); len(after) != 2 {
		t.Fatalf("reopened post-cutover rows = %d, want 2", len(after))
	}
}

func TestOnlineCreateIndexReconcilesConcurrentLeafRewrites(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-concurrent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		Backend: BackendPortable, ResidentBytes: 64 << 20,
		Indexes: []store.IndexDefinition{
			{Name: "by_seed", Paths: []string{"/seed"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const documents = 6_000
	for row := range documents {
		raw := fmt.Appendf(
			nil, `{"id":%d,"group":"g%02d"}`, row, row%100,
		)
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("k%05d", row)), raw,
		); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, buildErr := collection.CreateIndex(store.IndexDefinition{
			Name: "by_group", Paths: []string{"/group"},
		})
		done <- buildErr
	}()
	stopCapabilityProbe := make(chan struct{})
	capabilityProbeDone := make(chan struct{})
	go func() {
		defer close(capabilityProbeDone)
		for {
			select {
			case <-stopCapabilityProbe:
				return
			default:
				_ = collection.SupportsUpdate()
				runtime.Gosched()
			}
		}
	}()
	for spins := 0; !collection.onlineIndexBuilding(); spins++ {
		if spins == 10_000 {
			t.Fatal("online index build did not start")
		}
		runtime.Gosched()
	}
	if _, err := collection.CreateIndex(store.IndexDefinition{
		Name: "by_id", Paths: []string{"/id"},
	}); !errors.Is(err, ErrIndexBuildInProgress) {
		t.Fatalf("overlapping online index build = %v, want %v",
			err, ErrIndexBuildInProgress)
	}
	const rewrites = 256
	for row := range rewrites {
		raw := fmt.Appendf(
			nil, `{"id":%d,"group":"after"}`, row,
		)
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("k%05d", row)), raw,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		close(stopCapabilityProbe)
		<-capabilityProbeDone
		t.Fatal(err)
	}
	close(stopCapabilityProbe)
	<-capabilityProbeDone
	needle := primaryExactTestNeedle(t, `"after"`)
	if got := len(primaryExactTestKeys(
		t, collection, "by_group", needle,
	)); got != rewrites {
		t.Fatalf("concurrent rewrite rows = %d, want %d", got, rewrites)
	}
}

func TestOnlineCreateIndexReusesImmutableLeavesAndAliases(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-reuse-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		Backend: BackendPortable, ResidentBytes: 16 << 20,
		Indexes: []store.IndexDefinition{
			{Name: "by_z", Paths: []string{"/z"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	for row := range 400 {
		raw := fmt.Appendf(
			nil, `{"id":%d,"a":"a%02d","z":"z%02d"}`,
			row, row%17, row%13,
		)
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("k%04d", row)), raw,
		); err != nil {
			t.Fatal(err)
		}
	}

	collection.writer.Lock()
	err = collection.flushPendingForStructural()
	collection.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	oldState := collection.state.Load()
	oldLease, err := collection.cache.Acquire(oldState.root.ExactIndexRoot)
	if err != nil {
		t.Fatal(err)
	}
	oldRoot, err := storeio.OpenPrimaryExactRootPage(
		oldLease.Page(), oldState.root.ExactIndexRoot,
		collection.primaryExactBounds(oldState),
	)
	if err != nil {
		oldLease.Release()
		t.Fatal(err)
	}
	oldEntry, ok := oldRoot.Entry(collection.options.indexNameIDs["by_z"])
	oldLease.Release()
	if !ok || oldEntry.Catalog == (storeio.PageRef{}) {
		t.Fatalf("old by_z catalog = %+v ok=%v", oldEntry.Catalog, ok)
	}

	if _, err := collection.CreateIndex(store.IndexDefinition{
		Name: "by_a", Paths: []string{"/a"},
	}); err != nil {
		t.Fatal(err)
	}
	newState := collection.state.Load()
	newLease, err := collection.cache.Acquire(newState.root.ExactIndexRoot)
	if err != nil {
		t.Fatal(err)
	}
	newRoot, err := storeio.OpenPrimaryExactRootPage(
		newLease.Page(), newState.root.ExactIndexRoot,
		collection.primaryExactBounds(newState),
	)
	if err != nil {
		newLease.Release()
		t.Fatal(err)
	}
	reused, ok := newRoot.Entry(collection.options.indexNameIDs["by_z"])
	newLease.Release()
	if !ok || reused != oldEntry {
		t.Fatalf("by_z catalog rewritten: got %+v want %+v", reused, oldEntry)
	}

	rootBeforeAlias := newState.root.ExactIndexRoot
	if _, err := collection.CreateIndex(store.IndexDefinition{
		Name: "by_z_alias", Paths: []string{"/z"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := collection.state.Load().root.ExactIndexRoot; got != rootBeforeAlias {
		t.Fatalf("alias rewrote exact root: got %+v want %+v",
			got, rootBeforeAlias)
	}
	needle := primaryExactTestNeedle(t, `"z03"`)
	if got, want := len(primaryExactTestKeys(
		t, collection, "by_z_alias", needle,
	)), 31; got != want {
		t.Fatalf("alias rows = %d, want %d", got, want)
	}
	if _, err := collection.Put(
		[]byte("k0000"),
		[]byte(`{"id":0,"a":"a-after","z":"z-after"}`),
	); err != nil {
		t.Fatal(err)
	}
	for _, probe := range []struct {
		index string
		value string
	}{
		{index: "by_a", value: `"a-after"`},
		{index: "by_z", value: `"z-after"`},
		{index: "by_z_alias", value: `"z-after"`},
	} {
		if got := len(primaryExactTestKeys(
			t, collection, probe.index,
			primaryExactTestNeedle(t, probe.value),
		)); got != 1 {
			t.Fatalf("%s post-cutover rows = %d, want 1", probe.index, got)
		}
	}
}

func TestOnlineCreateIndexDatabaseSnapshotsPinCatalogEpoch(t *testing.T) {
	options := testDatabaseOptions()
	// Leave the immutable transaction arena the same headroom production SQL
	// defaults carry. The generic database tests deliberately set a one-document
	// ceiling to exercise the opposite, tight-capacity boundary.
	options.MaxBatchDocuments = 4
	db, err := OpenDatabase(
		t.TempDir(), DatabaseOptions{Options: options},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.CreateCollection("orders", options); err != nil {
		t.Fatal(err)
	}
	orders, ok := db.Collection("orders")
	if !ok {
		t.Fatal("orders collection is absent")
	}
	mustPut(t, orders, "o1", `{"status":"ready"}`)

	before, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if _, err := orders.CreateIndex(store.IndexDefinition{
		Name: "by_status", Paths: []string{"/status"},
	}); err != nil {
		t.Fatal(err)
	}
	oldOrders, ok := before.Collection("orders")
	if !ok {
		t.Fatal("orders is absent from old database snapshot")
	}
	if indexes := oldOrders.AppendIndexes(nil); len(indexes) != 0 {
		t.Fatalf("old database snapshot saw new index: %+v", indexes)
	}

	after, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	newOrders, ok := after.Collection("orders")
	if !ok {
		t.Fatal("orders is absent from new database snapshot")
	}
	indexes := newOrders.AppendIndexes(nil)
	if len(indexes) != 1 || indexes[0].Name != "by_status" {
		t.Fatalf("new database snapshot indexes = %+v", indexes)
	}
	masks, err := newOrders.AppendIndexMasks(
		nil, "by_status", primaryExactTestNeedle(t, `"ready"`),
	)
	if err != nil || len(masks) != 1 || masks[0].Bits == 0 {
		t.Fatalf("new database snapshot masks = %+v, %v", masks, err)
	}
}

func onlineIndexCrashSeed(t *testing.T) *store.Collection {
	t.Helper()
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 200 {
		if err := builder.Append(
			fmt.Sprintf("k%04d", row),
			fmt.Appendf(nil, `{"group":"g%02d","row":%d}`, row%10, row),
		); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func runOnlineIndexFaultPass(
	t *testing.T,
	built *store.Collection,
	options Options,
	controller *faultController,
	plan storeio.FaultPlan,
) (image []byte, records []storeio.FaultCommitRecord, faulted bool) {
	t.Helper()
	previousFactory := storeCommitterFactory
	storeCommitterFactory = controller.factory()
	defer func() { storeCommitterFactory = previousFactory }()
	controller.plan = plan
	controller.device = nil

	path := filepath.Join(t.TempDir(), "online-index-crash.vibe")
	file, err := os.OpenFile(
		path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateFromPrimary(built, file, options); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	device := controller.device
	if device == nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatal("fault device was not installed")
	}
	_, buildErr := collection.CreateIndex(store.IndexDefinition{
		Name: "by_group", Paths: []string{"/group"},
	})
	if !device.Faulted() && buildErr != nil {
		_ = collection.Close()
		_ = file.Close()
		t.Fatalf("clean online index build: %v", buildErr)
	}
	records = device.Records()
	faulted = device.Faulted()
	image, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = collection.Close()
	_ = file.Close()
	return image, records, faulted
}

func assertOnlineIndexCrashImage(
	t *testing.T,
	options Options,
	image []byte,
	label string,
) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "online-index-recover.vibe")
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options.Indexes = nil
	collection, err := Open(file, options)
	if err != nil {
		t.Fatalf("%s: atomic online-index image did not reopen: %v", label, err)
	}
	defer collection.Close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	indexes := snapshot.AppendIndexes(nil)
	_ = snapshot.Close()
	if len(indexes) == 0 {
		return // old root and old catalog won the crash
	}
	if len(indexes) != 1 || indexes[0].Name != "by_group" ||
		indexes[0].State != store.IndexReady {
		t.Fatalf("%s: recovered partial catalog = %+v", label, indexes)
	}
	needle := primaryExactTestNeedle(t, `"g03"`)
	if got, want := len(primaryExactTestKeys(
		t, collection, "by_group", needle,
	)), 20; got != want {
		t.Fatalf("%s: recovered index rows = %d, want %d",
			label, got, want)
	}
}

func TestOnlineCreateIndexAtomicCrashBoundary(t *testing.T) {
	built := onlineIndexCrashSeed(t)
	options := Options{
		Backend: BackendPortable, ResidentBytes: 16 << 20,
		Durability:         DurabilityBufferedVisible,
		CheckpointStrength: CheckpointFilesystem,
		GroupLimit:         1, CommitCoalesce: 0,
	}
	controller := &faultController{}
	_, records, faulted := runOnlineIndexFaultPass(
		t, built, options, controller,
		storeio.FaultPlan{Phase: storeio.FaultNone},
	)
	if faulted || len(records) == 0 {
		t.Fatalf("clean online build faulted=%v commits=%d",
			faulted, len(records))
	}
	phases := []storeio.FaultPhase{
		storeio.FaultAfterDataWrite,
		storeio.FaultAfterBarrier,
		storeio.FaultAfterRootWrite,
		storeio.FaultAfterFinalSync,
		storeio.FaultTornRoot,
	}
	exercised := 0
	for commit, record := range records {
		for _, phase := range phases {
			indexes := []int{0}
			if phase == storeio.FaultAfterDataWrite {
				indexes = make([]int, len(record.DataPages))
				for i := range indexes {
					indexes[i] = i
				}
			}
			for _, dataIndex := range indexes {
				plan := storeio.FaultPlan{
					Commit: commit, Phase: phase, DataIndex: dataIndex,
				}
				image, _, didFault := runOnlineIndexFaultPass(
					t, built, options, controller, plan,
				)
				if !didFault {
					continue
				}
				exercised++
				assertOnlineIndexCrashImage(
					t, options, image,
					fmt.Sprintf(
						"commit=%d phase=%d data=%d",
						commit, phase, dataIndex,
					),
				)
			}
		}
	}
	if exercised == 0 {
		t.Fatal("online index crash sweep exercised no fault point")
	}
}

func TestOnlineCreateIndexMatchesCanonicalAggregation(t *testing.T) {
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for row := range 900 {
		var raw []byte
		if row%7 == 0 {
			raw = fmt.Appendf(nil, `{"row":%d}`, row)
		} else {
			raw = fmt.Appendf(nil, `{"row":%d,"unique":%d}`, row, row)
		}
		if err := builder.Append(fmt.Sprintf("k%04d", row), raw); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	definition := store.IndexDefinition{
		Name: "by_unique", Paths: []string{"/unique"},
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 16 << 20,
	}

	onlineFile, err := os.CreateTemp(t.TempDir(), "online-differential-*")
	if err != nil {
		t.Fatal(err)
	}
	defer onlineFile.Close()
	if _, err := CreateFromPrimary(source, onlineFile, options); err != nil {
		t.Fatal(err)
	}
	online, err := Open(onlineFile, options)
	if err != nil {
		t.Fatal(err)
	}
	defer online.Close()
	exact, err := store.CompileExactIndex(definition)
	if err != nil {
		t.Fatal(err)
	}
	build := onlineIndexBuild{
		exact: exact, buckets: make(map[storeio.BucketID]onlineIndexBucket),
	}
	for {
		online.writer.Lock()
		complete, reconcileErr := build.reconcileOneLocked(online)
		online.writer.Unlock()
		if reconcileErr != nil {
			t.Fatal(reconcileErr)
		}
		if complete {
			break
		}
	}
	live := make(
		map[uint32]*[storeio.TermPostingTileChunks]uint64,
		len(build.buckets)*4,
	)
	terms := make(map[string]map[uint32]uint64)
	for bucketID, bucket := range build.buckets {
		for quadrant, bits := range bucket.live {
			if bits == 0 {
				continue
			}
			tileID := uint32(bucketID)<<2 | uint32(quadrant)
			mask := new([storeio.TermPostingTileChunks]uint64)
			mask[0] = bits
			live[tileID] = mask
		}
		for key, termID := range bucket.terms {
			if int(termID) >= len(bucket.termMasks) {
				t.Fatal("bucket term mask index is out of bounds")
			}
			masks := bucket.termMasks[termID]
			tiles := terms[key]
			if tiles == nil {
				tiles = make(map[uint32]uint64, 4)
				terms[key] = tiles
			}
			for quadrant, bits := range masks {
				if bits != 0 {
					tiles[uint32(bucketID)<<2|uint32(quadrant)] = bits
				}
			}
		}
	}
	ordered, err := buildPrimaryExactTerms(online.storeID, terms, live)
	if err != nil {
		t.Fatal(err)
	}
	want, err := storeio.AppendIndexTermLeaf(nil, online.storeID, ordered)
	if err != nil {
		t.Fatal(err)
	}
	got, err := online.encodeOnlineIndexBuckets(
		build.buckets,
		func(tileID uint32) *[storeio.TermPostingTileChunks]uint64 {
			return live[tileID]
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf(
			"merged canonical index differs from aggregation: got=%d want=%d",
			len(got), len(want),
		)
	}
	if _, err := online.CreateIndex(definition); err != nil {
		t.Fatal(err)
	}
	var wantLeaves [][]byte
	err = storeio.CutIndexTermLeaves(
		ordered,
		storeio.IndexTermLeafCutBudget(uint32(online.options.MaxPageSize)),
		func(leafTerms []storeio.IndexTermLeafTerm, _ bool) error {
			encoded, encodeErr := storeio.AppendIndexTermLeaf(
				nil, online.storeID, leafTerms,
			)
			if encodeErr == nil {
				wantLeaves = append(wantLeaves, encoded)
			}
			return encodeErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if online.primaryEpoch == nil || len(online.primaryEpoch.exact) != 1 ||
		len(online.primaryEpoch.exact[0].leaves) != len(wantLeaves) {
		t.Fatal("published online index differs from canonical aggregation")
	}
	for leafAt := range wantLeaves {
		if !slices.Equal(
			online.primaryEpoch.exact[0].leaves[leafAt].encoded,
			wantLeaves[leafAt],
		) {
			t.Fatalf("published online leaf %d differs from canonical aggregation",
				leafAt)
		}
	}
}
