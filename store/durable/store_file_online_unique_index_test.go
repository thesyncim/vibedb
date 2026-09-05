package durable

import (
	"errors"
	"fmt"
	"math/bits"
	"os"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestOnlineCreateUniqueIndexRejectsCanonicalDuplicatesAndValidatesAlias(
	t *testing.T,
) {
	file, err := os.CreateTemp(t.TempDir(), "online-unique-duplicate-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		MaxBatchDocuments: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	// The first and last ordered primary rows land in different leaves after
	// online-index repartitioning. Their number spellings still have one
	// canonical exact term.
	const documents = 300
	for row := range documents {
		score := fmt.Sprintf("%d", row+1000)
		switch row {
		case 0:
			score = "1"
		case documents - 1:
			score = "1.0"
		}
		raw := fmt.Appendf(nil, `{"score":%s,"row":%d}`, score, row)
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("k%04d", row)), raw,
		); err != nil {
			t.Fatal(err)
		}
	}

	unique := store.IndexDefinition{
		Name: "score_unique", Paths: []string{"/score"},
	}
	if _, err := collection.CreateUniqueIndex(unique); !errors.Is(
		err, store.ErrUniqueIndexViolation,
	) {
		t.Fatalf("CreateUniqueIndex duplicate error = %v, want %v",
			err, store.ErrUniqueIndexViolation)
	}
	assertOnlineIndexNames(t, collection, nil)

	ordinary := store.IndexDefinition{
		Name: "score", Paths: []string{"/score"},
	}
	if _, err := collection.CreateIndex(ordinary); err != nil {
		t.Fatalf("ordinary CreateIndex rejected duplicates: %v", err)
	}
	needle := primaryExactTestNeedle(t, "1")
	if got := primaryExactTestKeys(
		t, collection, "score", needle,
	); !slices.Equal(got, []string{"k0000", "k0299"}) {
		t.Fatalf("ordinary index duplicate rows = %v, want [k0000 k0299]", got)
	}
	rootBeforeAlias := collection.state.Load().root.ExactIndexRoot
	generationBeforeAlias := collection.Generation()
	if _, err := collection.CreateUniqueIndex(unique); !errors.Is(
		err, store.ErrUniqueIndexViolation,
	) {
		t.Fatalf("unique alias duplicate error = %v, want %v",
			err, store.ErrUniqueIndexViolation)
	}
	if got := collection.state.Load().root.ExactIndexRoot; got != rootBeforeAlias {
		t.Fatalf("rejected unique alias changed exact root: got %+v want %+v",
			got, rootBeforeAlias)
	}
	if got := collection.Generation(); got != generationBeforeAlias {
		t.Fatalf("rejected unique alias generation = %d, want %d",
			got, generationBeforeAlias)
	}
	assertOnlineIndexNames(t, collection, []string{"score"})

	if created, err := collection.Put(
		[]byte("k0299"), []byte(`{"score":1299,"row":299}`),
	); err != nil || created {
		t.Fatalf("repair duplicate = created %v, err %v", created, err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	rootBeforeAlias = collection.state.Load().root.ExactIndexRoot
	if _, err := collection.CreateUniqueIndex(unique); err != nil {
		t.Fatalf("CreateUniqueIndex repaired alias: %v", err)
	}
	if got := collection.state.Load().root.ExactIndexRoot; got != rootBeforeAlias {
		t.Fatalf("unique alias rewrote exact root: got %+v want %+v",
			got, rootBeforeAlias)
	}
	assertOnlineIndexNames(t, collection, []string{"score", "score_unique"})
	if got := primaryExactTestKeys(
		t, collection, "score_unique", needle,
	); !slices.Equal(got, []string{"k0000"}) {
		t.Fatalf("unique alias rows = %v, want [k0000]", got)
	}
}

// TestOnlineCreateUniqueIndexRejectsFullOverlayPostingCollision fixes the
// store-id-dependent failure mode in a small, deterministic witness. An
// unindexed full compact stripe uses overlay-local posting slots for inserts;
// that slot is allowed to overlap a base slot until the pending overlay is
// materialized. The online scan must not validate the mixed representation as
// if those slots formed one posting namespace.
func TestOnlineCreateUniqueIndexRejectsFullOverlayPostingCollision(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-unique-collision-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		MaxBatchDocuments: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()

	const documents = 257
	for row := range documents {
		score := fmt.Sprintf("%d", row+1000)
		if row == 0 {
			score = "1"
		} else if row == documents-1 {
			score = "1.0"
		}
		raw := fmt.Appendf(nil, `{"score":%s,"row":%d}`, score, row)
		if _, err := collection.Put(
			[]byte(fmt.Sprintf("k%04d", row)), raw,
		); err != nil {
			t.Fatal(err)
		}
	}
	forceOnlineUniqueOverlayBaseSlotCollision(t, collection)

	unique := store.IndexDefinition{
		Name: "score_unique", Paths: []string{"/score"},
	}
	scannedRows, scannedMask, scanErr :=
		onlineUniqueCollisionScanWitness(t, collection, unique)
	if scanErr != nil || scannedRows != 1 {
		t.Fatalf("forced collision scan = (err=%v rows=%d mask=%#v), want one row before materialization",
			scanErr, scannedRows, scannedMask)
	}
	if _, err := collection.CreateUniqueIndex(unique); !errors.Is(
		err, store.ErrUniqueIndexViolation,
	) {
		t.Fatalf("CreateUniqueIndex forced posting collision error = %v, want %v (scanErr=%v scannedTermRows=%d independentTermRows=2 postingMask=%#v)",
			err, store.ErrUniqueIndexViolation, scanErr, scannedRows, scannedMask)
	}
	assertOnlineIndexNames(t, collection, nil)
}

func onlineUniqueCollisionScanWitness(
	t *testing.T, c *Collection, definition store.IndexDefinition,
) (int, [4]uint64, error) {
	t.Helper()
	state := c.state.Load()
	router := c.primaryRouter.Load()
	if state == nil || router == nil || router.Len() != 1 {
		return 0, [4]uint64{}, fmt.Errorf("state/router/routes unavailable")
	}
	route, ok := router.RouteAtRank(0)
	if !ok {
		return 0, [4]uint64{}, fmt.Errorf("route unavailable")
	}
	candidateOptions := c.options.Options
	candidateOptions.Indexes = append(slices.Clone(c.options.Indexes), definition)
	candidate, err := candidateOptions.normalized()
	if err != nil {
		return 0, [4]uint64{}, err
	}
	targetID, ok := candidate.indexNameIDs[definition.Name]
	if !ok || int(targetID) >= len(candidate.indexes) {
		return 0, [4]uint64{}, fmt.Errorf("candidate index unavailable")
	}
	build := &onlineIndexBuild{
		exact: candidate.indexes[targetID], unique: true,
		buckets: make(map[storeio.BucketID]onlineIndexBucket, 1),
	}
	bucket, err := build.scanBucket(c, state, router, route)
	if err != nil {
		return 0, [4]uint64{}, err
	}
	var components [store.MaxIndexColumns]storeio.IndexTermComponent
	needle, present, err := appendOnlineIndexDocumentTerm(
		nil, components[:], build.exact, []byte(`{"score":1,"row":0}`), true,
	)
	if err != nil || !present {
		return 0, [4]uint64{}, fmt.Errorf("canonical needle present=%v: %w", present, err)
	}
	termID, ok := bucket.terms[string(needle)]
	if !ok || int(termID) >= len(bucket.termMasks) {
		return 0, [4]uint64{}, nil
	}
	mask := bucket.termMasks[termID]
	rows := 0
	for _, word := range mask {
		rows += bits.OnesCount64(word)
	}
	return rows, mask, nil
}

func forceOnlineUniqueOverlayBaseSlotCollision(t *testing.T, c *Collection) {
	t.Helper()
	c.writer.Lock()
	defer c.writer.Unlock()
	state := c.state.Load()
	router := c.primaryRouter.Load()
	if state == nil || router == nil || router.Len() != 1 {
		t.Fatalf("collision witness state/router = (%v,%v) routes=%d",
			state != nil, router != nil, router.Len())
	}
	route, ok := router.RouteAtRank(0)
	if !ok {
		t.Fatal("collision witness route unavailable")
	}
	lease, err := router.AcquireLeaf(c.cache, route)
	if err != nil {
		t.Fatal(err)
	}
	stripe, admitted := storeio.AdmittedCompactPrimaryStripe(
		lease.Page(), state.root.StoreID, route.Bucket,
	)
	if !admitted || stripe.Len() != storeio.CommonPrimaryLeafWideSlots {
		lease.Release()
		t.Fatalf("collision witness base stripe = (admitted=%v rows=%d)",
			admitted, stripe.Len())
	}
	baseRank, found := stripe.FindKey([]byte("k0000"))
	baseSlot, slotOK := stripe.PostingSlot(baseRank)
	lease.Release()
	if !found || !slotOK {
		t.Fatalf("collision witness base key = (found=%v slotOK=%v)",
			found, slotOK)
	}
	var indexes [storeio.CommonPrimaryLeafWideSlots]uint16
	count, err := c.primaryUnifiedOverlay.latestBucketRecords(
		&indexes, route.Bucket, state.root.Generation,
	)
	if err != nil || count != 1 {
		t.Fatalf("collision witness overlay = (count=%d err=%v)", count, err)
	}
	record := &c.primaryUnifiedOverlay.records[indexes[0]]
	keyEnd := uint64(record.keyOffset) + uint64(record.keyLen)
	if keyEnd > uint64(len(c.primaryUnifiedOverlay.arena)) ||
		string(c.primaryUnifiedOverlay.arena[record.keyOffset:keyEnd]) != "k0256" {
		t.Fatalf("collision witness overlay key is not k0256")
	}
	bucketSlot, found := c.primaryUnifiedOverlay.bucketSlot(route.Bucket)
	if !found {
		t.Fatalf("collision witness overlay bucket disappeared")
	}
	oldSlot := record.slot
	oldWord, oldBit := oldSlot>>6, uint64(1)<<uint(oldSlot&63)
	entry := &c.primaryUnifiedOverlay.buckets[bucketSlot]
	entry.insertSlots[oldWord].Store(entry.insertSlots[oldWord].Load() &^ oldBit)
	newWord, newBit := baseSlot>>6, uint64(1)<<uint(baseSlot&63)
	entry.insertSlots[newWord].Store(entry.insertSlots[newWord].Load() | newBit)
	record.slot = baseSlot
}

func TestOnlineCreateUniqueIndexAllowsCompoundNullTermsAndReopens(t *testing.T) {
	path := t.TempDir() + "/online-unique-null.vjc"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, ResidentBytes: 16 << 20,
		MaxBatchDocuments: 1,
	}
	collection, err := Create(file, options)
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	for key, raw := range map[string]string{
		"a": `{"tenant":"acme","external":null}`,
		"b": `{"tenant":"acme","external":null}`,
		"c": `{"tenant":"acme","external":"one"}`,
		"d": `{"tenant":"acme","external":"two"}`,
	} {
		if _, err := collection.Put([]byte(key), []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	definition := store.IndexDefinition{
		Name:  "tenant_external_unique",
		Paths: []string{"/tenant", "/external"},
	}
	info, err := collection.CreateUniqueIndex(definition)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != store.IndexReady || info.ColumnCount != 2 || !info.Unique {
		t.Fatalf("CreateUniqueIndex info = %+v", info)
	}
	alias := store.IndexDefinition{
		Name:  "tenant_external_unique_alias",
		Paths: slices.Clone(definition.Paths), Unique: true,
	}
	aliasInfo, err := collection.CreateIndex(alias)
	if err != nil || !aliasInfo.Unique {
		t.Fatalf("CreateIndex Unique alias = (%+v,%v)", aliasInfo, err)
	}
	tenant := primaryExactTestNeedle(t, `"acme"`)
	null := primaryExactTestNeedle(t, "null")
	if got := primaryExactTestKeys(
		t, collection, definition.Name, tenant, null,
	); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("NULL-exempt rows = %v, want [a b]", got)
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
	collection, err = Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if len(collection.options.Indexes) != 2 ||
		!collection.options.Indexes[0].Unique ||
		!collection.options.Indexes[1].Unique {
		t.Fatalf("zero-option reopen lost unique definitions: %+v",
			collection.options.Indexes)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	infos := snapshot.AppendIndexes(nil)
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 || !infos[0].Unique || !infos[1].Unique {
		t.Fatalf("reopened index metadata = %+v, want Unique", infos)
	}
	if got := primaryExactTestKeys(
		t, collection, definition.Name, tenant, null,
	); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("reopened NULL-exempt rows = %v, want [a b]", got)
	}
}

func TestOnlineCreateUniqueIndexRejectsPresentContainers(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-unique-container-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := Create(file, Options{
		Backend: BackendPortable, ResidentBytes: 16 << 20,
		MaxBatchDocuments: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, err := collection.Put(
		[]byte("array"), []byte(`{"value":[1,2]}`),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put(
		[]byte("scalar"), []byte(`{"value":"ok"}`),
	); err != nil {
		t.Fatal(err)
	}
	definition := store.IndexDefinition{
		Name: "value_unique", Paths: []string{"/value"},
	}
	compound := store.IndexDefinition{
		Name: "missing_value_unique", Paths: []string{"/missing", "/value"},
	}
	if _, err := collection.CreateUniqueIndex(compound); !errors.Is(
		err, store.ErrIndexScalar,
	) {
		t.Fatalf("container after missing path error = %v, want %v",
			err, store.ErrIndexScalar)
	}
	if _, err := collection.CreateUniqueIndex(definition); !errors.Is(
		err, store.ErrIndexScalar,
	) {
		t.Fatalf("container unique build error = %v, want %v",
			err, store.ErrIndexScalar)
	}
	assertOnlineIndexNames(t, collection, nil)
	definition.Name = "value_ordinary"
	if _, err := collection.CreateIndex(definition); err != nil {
		t.Fatalf("ordinary index rejected omitted container: %v", err)
	}
}

func assertOnlineIndexNames(
	t *testing.T, collection *Collection, want []string,
) {
	t.Helper()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	infos := snapshot.AppendIndexes(nil)
	got := make([]string, len(infos))
	for i := range infos {
		got[i] = infos[i].Name
	}
	slices.Sort(got)
	want = slices.Clone(want)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("index names = %v, want %v", got, want)
	}
}
