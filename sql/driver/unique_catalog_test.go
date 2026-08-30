package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestUniqueIndexCatalogJSONCanonicalStrictAndLegacyCompatible(t *testing.T) {
	catalog := catalogFile{Version: catalogVersion, Tables: map[string]*tableMeta{
		"docs": {
			PrimaryKey: "/id",
			Indexes: []indexMeta{
				{Name: "by_email", Paths: []string{"/email"}, Unique: true},
				{Name: "by_kind", Paths: []string{"/kind"}},
			},
		},
	}}
	raw, err := catalog.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`{"name":"by_email","paths":["/email"],"unique":true}`)) {
		t.Fatalf("canonical unique index encoding = %s", raw)
	}
	if bytes.Contains(raw, []byte(`{"name":"by_kind","paths":["/kind"],"unique":false}`)) ||
		!bytes.Contains(raw, []byte(`{"name":"by_kind","paths":["/kind"]}`)) {
		t.Fatalf("canonical non-unique index encoding = %s", raw)
	}

	var decoded catalogFileVibe
	if err := decodeCatalogJSON(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	indexes := catalogFile(decoded).Tables["docs"].Indexes
	if len(indexes) != 2 || !indexes[0].Unique || indexes[1].Unique {
		t.Fatalf("decoded index uniqueness = %+v", indexes)
	}

	legacy := []byte(`{"version":0,"tables":{"docs":{"primary_key":"/id","indexes":[{"name":"by_email","paths":["/email"]}]}}}`)
	if err := decodeCatalogJSON(legacy, &decoded); err != nil {
		t.Fatalf("legacy catalog: %v", err)
	}
	if catalogFile(decoded).Tables["docs"].Indexes[0].Unique {
		t.Fatal("legacy index defaulted to unique")
	}

	for _, malformed := range [][]byte{
		[]byte(`{"version":0,"tables":{"docs":{"primary_key":"/id","indexes":[{"name":"by_email","paths":["/email"],"unique":1}]}}}`),
		[]byte(`{"version":0,"tables":{"docs":{"primary_key":"/id","indexes":[{"name":"by_email","paths":["/email"],"unique":true,"unique":true}]}}}`),
		[]byte(`{"version":0,"tables":{"docs":{"primary_key":"/id","indexes":[{"name":"by_email","paths":["/email"],"uniqueness":true}]}}}`),
	} {
		if err := decodeCatalogJSON(malformed, &decoded); err == nil {
			t.Fatalf("accepted malformed unique metadata: %s", malformed)
		}
	}
}

func TestUniqueIndexMetadataCloneLayoutAndDurableProjection(t *testing.T) {
	source := &tableMeta{PrimaryKey: "/id", Indexes: []indexMeta{
		{Name: "by_email", Paths: []string{"/email"}, Unique: true},
		{Name: "by_kind", Paths: []string{"/kind"}},
	}}
	clone := cloneTableMeta(source)
	indexes := cloneIndexMeta(source.Indexes)
	epoch := newCatalogLayoutEpoch(map[string]*table{
		"docs": {meta: source},
	}, nil)

	source.Indexes[0].Paths[0] = "/changed"
	source.Indexes[0].Unique = false
	if !clone.Indexes[0].Unique || clone.Indexes[0].Paths[0] != "/email" ||
		!indexes[0].Unique || indexes[0].Paths[0] != "/email" {
		t.Fatalf("metadata clones changed with source: table=%+v indexes=%+v", clone.Indexes, indexes)
	}
	layout := epoch.tables["docs"]
	if len(layout.uniqueIndexes) != 1 || !layout.uniqueIndexes[0].Unique ||
		layout.uniqueIndexes[0].Name != "by_email" || layout.uniqueIndexes[0].Paths[0] != "/email" {
		t.Fatalf("immutable transaction unique indexes = %+v", layout.uniqueIndexes)
	}

	uniqueOptions := durableOptions(&table{meta: clone})
	nonUnique := cloneTableMeta(clone)
	nonUnique.Indexes[0].Unique = false
	nonUniqueOptions := durableOptions(&table{meta: nonUnique})
	if len(uniqueOptions.Indexes) != 2 || !uniqueOptions.Indexes[0].Unique ||
		nonUniqueOptions.Indexes[0].Unique {
		t.Fatalf("durable uniqueness projection:\n unique=%+v\nplain=%+v", uniqueOptions.Indexes, nonUniqueOptions.Indexes)
	}

	plainCatalog := catalogFile{Version: catalogVersion, Tables: map[string]*tableMeta{"docs": nonUnique}}
	uniqueCatalog := catalogFile{Version: catalogVersion, Tables: map[string]*tableMeta{"docs": clone}}
	plainBound, err := catalogSizeUpperBound(plainCatalog)
	if err != nil {
		t.Fatal(err)
	}
	uniqueBound, err := catalogSizeUpperBound(uniqueCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if delta := uniqueBound - plainBound; delta != len(`,"unique":true`) {
		t.Fatalf("unique catalog bound delta = %d", delta)
	}
}

func TestUniqueIndexCatalogReopenPreservesSyncAndIntrospection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE docs (id STRING PRIMARY KEY, email STRING)`,
		`INSERT INTO docs VALUES ('{"id":"one","email":"a@example.com"}')`,
	} {
		prepared, prepareErr := session.Prepare(ctx, statement)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		_, execErr := prepared.Exec(ctx, nil)
		closeErr := prepared.Close()
		if execErr != nil || closeErr != nil {
			t.Fatalf("%s: exec=%v close=%v", statement, execErr, closeErr)
		}
	}

	core := database.connector.db
	core.mu.Lock()
	table := core.tables["docs"]
	core.mu.Unlock()
	if _, err := table.collection.CreateUniqueIndex(store.IndexDefinition{
		Name: "by_email", Paths: []string{"/email"}, Unique: true,
	}); err != nil {
		t.Fatal(err)
	}
	core.mu.Lock()
	changed, syncErr := syncTableIndexMeta(table)
	if syncErr != nil || !changed {
		core.mu.Unlock()
		t.Fatalf("sync durable unique index = changed %v, err %v", changed, syncErr)
	}
	core.advanceLayoutEpochLocked()
	published, persistErr := core.persistCatalogLocked()
	core.mu.Unlock()
	if persistErr != nil || !published {
		t.Fatalf("persist unique catalog = published %v, err %v", published, persistErr)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedSession, err := reopened.NewSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSession.Close()
	tables, err := reopenedSession.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || len(tables[0].Indexes) != 1 ||
		tables[0].Indexes[0].Name != "by_email" || !tables[0].Indexes[0].Unique ||
		!reflect.DeepEqual(tables[0].Indexes[0].Paths, []string{"/email"}) {
		t.Fatalf("reopened introspection = %+v", tables)
	}
	if !reopened.connector.db.tables["docs"].meta.Indexes[0].Unique {
		t.Fatal("durable index synchronization erased SQL uniqueness")
	}
}

func TestReplicatedLocalIndexDigestIncludesUniqueness(t *testing.T) {
	plain := []indexMeta{
		{Name: "by_kind", Paths: []string{"/kind"}},
		{Name: "by_email", Paths: []string{"/email"}},
	}
	reordered := []indexMeta{plain[1], plain[0]}
	if replicatedLocalIndexDigest(plain) != replicatedLocalIndexDigest(reordered) {
		t.Fatal("local-index digest depends on catalog creation order")
	}
	if replicatedLocalIndexDigest(plain) != legacyLocalIndexDigestForTest(plain) {
		t.Fatal("non-unique local-index digest broke legacy replicated identity")
	}
	unique := cloneIndexMeta(plain)
	unique[1].Unique = true
	if replicatedLocalIndexDigest(plain) == replicatedLocalIndexDigest(unique) {
		t.Fatal("local-index digest ignores uniqueness")
	}
	if replicatedLocalIndexDigest(nil) != ([32]byte{}) {
		t.Fatal("empty local-index digest changed from the zero sentinel")
	}
}

func legacyLocalIndexDigestForTest(indexes []indexMeta) [sha256.Size]byte {
	canonical := cloneIndexMeta(indexes)
	slices.SortFunc(canonical, func(left, right indexMeta) int {
		return strings.Compare(left.Name, right.Name)
	})
	h := sha256.New()
	_, _ = h.Write(replicatedLocalIndexManifestDomain)
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(canonical)))
	_, _ = h.Write(count[:])
	for i := range canonical {
		writeReplicatedRelationFrame(h, []byte(canonical[i].Name))
		binary.LittleEndian.PutUint64(count[:], uint64(len(canonical[i].Paths)))
		_, _ = h.Write(count[:])
		for _, path := range canonical[i].Paths {
			writeReplicatedRelationFrame(h, []byte(path))
		}
	}
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func TestIndexInfoUniqueDocumentationSurface(t *testing.T) {
	meta := &tableMeta{PrimaryKey: "/id", Indexes: []indexMeta{{
		Name: "by_email", Paths: []string{"/email"}, Unique: true,
	}}}
	info := tableInfoFromMeta("docs", meta)
	if len(info.Indexes) != 1 || !info.Indexes[0].Unique {
		t.Fatalf("index info = %+v", info.Indexes)
	}
	info.Indexes[0].Paths[0] = "/changed"
	if meta.Indexes[0].Paths[0] != "/email" {
		t.Fatal("introspection paths alias catalog storage")
	}
}
