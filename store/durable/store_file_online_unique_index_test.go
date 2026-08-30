package durable

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

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
	needle := primaryExactTestNeedle(t, "1")
	if got := primaryExactTestKeys(
		t, collection, "score_unique", needle,
	); !slices.Equal(got, []string{"k0000"}) {
		t.Fatalf("unique alias rows = %v, want [k0000]", got)
	}
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
