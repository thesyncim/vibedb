package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func schemaShadowApply(t *testing.T, claim *ReplicatedApply, db *Database, base ReplicatedShardStoreIdentity, applied uint64, docs ...string) {
	t.Helper()
	var mutations []replication.Mutation
	for _, doc := range docs {
		mutations = append(mutations, replication.Mutation{Kind: replication.MutationPut,
			Key: testReplicatedApplyKey(t, db, []byte(doc)), Value: []byte(doc)})
	}
	command, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, 2, applied-1, mutations))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(applied), command); err != nil {
		t.Fatal(err)
	}
	if code := completionResultCode(t, claim, command); code != replicatedstate.ResultApplied {
		t.Fatalf("source completion=%d", code)
	}
}

func schemaShadowCollection(t *testing.T, claim *ReplicatedApply, shadow ReplicatedSchemaDDLShadow) (*durable.Collection, func()) {
	t.Helper()
	catalog, _, err := openReplicatedSchemaCatalogImage(shadow.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	relation := catalog.ReplicatedShardStore.Relations[0]
	file, err := os.OpenFile(filepath.Join(claim.database.dataDir, replicatedSchemaTargetsDirectory, relation.Storage+".vjc"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	candidate := &table{meta: catalog.Tables[relation.Table]}
	candidate.schema, err = compileSchemaMeta(candidate.meta.Schema)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Open(file, durableOptions(candidate))
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	return collection, func() {
		if err := errors.Join(collection.Close(), file.Close()); err != nil {
			t.Error(err)
		}
	}
}

func assertSchemaShadowRows(t *testing.T, claim *ReplicatedApply, shadow ReplicatedSchemaDDLShadow, want int, city string, wantMatches int) {
	t.Helper()
	collection, close := schemaShadowCollection(t, claim, shadow)
	defer close()
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	rows := 0
	if err := snapshot.RangeRaw(func(_, _ []byte) error { rows++; return nil }); err != nil || rows != want {
		t.Fatalf("shadow rows=%d want=%d err=%v", rows, want, err)
	}
	if city == "" {
		return
	}
	raw := []byte(fmt.Sprintf("%q", city))
	n, err := vibejson.RequiredIndexEntries(raw)
	if err != nil {
		t.Fatal(err)
	}
	value, err := vibejson.BuildIndex(raw, make([]vibejson.IndexEntry, n))
	if err != nil {
		t.Fatal(err)
	}
	masks, err := snapshot.AppendIndexMasks(nil, "by_city", value)
	count := 0
	for _, mask := range masks {
		count += bits.OnesCount64(mask.Bits)
	}
	if err != nil || count != wantMatches {
		t.Fatalf("city=%s postings=%d want=%d err=%v", city, count, wantMatches, err)
	}
}

func TestReplicatedSchemaDDLShadowCopiesWhileWritesCommitAndReplaysIndexes(t *testing.T) {
	_, db, base, identity, claim := schemaDDLJournalFixture(t)
	op := [32]byte{101}
	// This seam runs with the immutable source cut already pinned, before
	// copying any rows. The synchronous write must commit without that cut
	// changing; capture then brings the actual index up to the newer state.
	schemaDDLShadowFaultHook = func(stage string) error {
		if stage == "copy-reserved" {
			schemaShadowApply(t, claim, db, base, 4, `{"id":"one","city":"Porto"}`, `{"id":"two","city":"Porto"}`)
		}
		return nil
	}
	t.Cleanup(func() { schemaDDLShadowFaultHook = nil })
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	schemaDDLShadowFaultHook = nil
	if err != nil || shadow.Snapshot.Publication.Applied != 3 || claim.Applied() != 4 {
		t.Fatalf("copy=%+v applied=%d err=%v", shadow, claim.Applied(), err)
	}
	if !reflect.DeepEqual(identity, claim.identity) {
		t.Fatal("capture/copy changed durable request identity")
	}
	assertSchemaShadowRows(t, claim, shadow, 1, "Lisbon", 1)
	if _, err := claim.PrepareReplicatedSchemaTarget(shadow.Catalog, 4, [32]byte{7}); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("mutable shadow entered immutable preparation: %v", err)
	}
	if _, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), [32]byte{8}, 4, "TRUNCATE docs"); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("offline build bypassed online ownership: %v", err)
	}
	shadow, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1)
	if err != nil || shadow.Cursor.Publication.Applied != 4 || shadow.Snapshot.Publication.Applied != 3 {
		t.Fatalf("replay=%+v err=%v", shadow, err)
	}
	assertSchemaShadowRows(t, claim, shadow, 2, "Porto", 2)
	assertSchemaShadowRows(t, claim, shadow, 2, "Lisbon", 0)
	if again, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1); err != nil || !reflect.DeepEqual(again, shadow) {
		t.Fatalf("replay changed caught-up receipt: %v", err)
	}
	// Add two publications; a one-entry call must not consume both.
	schemaShadowApply(t, claim, db, base, 5, `{"id":"three","city":"Braga"}`)
	schemaShadowApply(t, claim, db, base, 6, `{"id":"four","city":"Braga"}`)
	shadow, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1)
	if err != nil || shadow.Cursor.Publication.Applied != 5 {
		t.Fatalf("unbounded replay: %+v %v", shadow.Cursor, err)
	}
	assertSchemaShadowRows(t, claim, shadow, 3, "Braga", 1)
}

func TestReplicatedSchemaDDLShadowThousandRowsWithConcurrentInsert(t *testing.T) {
	_, db, base, _, claim := schemaDDLJournalFixture(t)
	applied := uint64(4)
	for first := 1; first < 1000; first += 64 {
		var docs []string
		for i := first; i < min(first+64, 1000); i++ {
			docs = append(docs, fmt.Sprintf(`{"id":"employee-%04d","city":"Lisbon","score":%d}`, i, i))
		}
		schemaShadowApply(t, claim, db, base, applied, docs...)
		applied++
	}
	schemaDDLShadowFaultHook = func(stage string) error {
		if stage == "copy-reserved" {
			schemaShadowApply(t, claim, db, base, applied, `{"id":"new-employee","city":"Porto","score":1000}`)
		}
		return nil
	}
	t.Cleanup(func() { schemaDDLShadowFaultHook = nil })
	op := [32]byte{107}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	schemaDDLShadowFaultHook = nil
	if err != nil || shadow.Snapshot.Publication.Applied != applied-1 || claim.Applied() != applied {
		t.Fatalf("copy did not preserve its cut: %v", err)
	}
	assertSchemaShadowRows(t, claim, shadow, 1000, "Lisbon", 1000)
	shadow, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
	if err != nil || shadow.Cursor.Publication.Applied != applied {
		t.Fatal(err)
	}
	assertSchemaShadowRows(t, claim, shadow, 1001, "Porto", 1)
}

func TestReplicatedSchemaDDLShadowFinalizesOrdinaryBuildReceiptAtFencedCut(t *testing.T) {
	_, db, base, _, claim := schemaDDLJournalFixture(t)
	op := [32]byte{108}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(
		t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20,
	)
	if err != nil || shadow.Cursor.Publication.Applied != 3 {
		t.Fatalf("build shadow=%+v err=%v", shadow, err)
	}
	schemaShadowApply(t, claim, db, base, 4, `{"id":"after-copy","city":"Porto","score":90}`)
	target, err := claim.FinalizeReplicatedSchemaDDLShadow(t.Context(), op, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplicatedSchemaDDLTarget(target, 4, base.Binding.Authority.SchemaGeneration); err != nil {
		t.Fatalf("detached target: %v", err)
	}
	if applied, found, err := claim.ReplicatedSchemaDDLSourceApplied(target.Catalog); err != nil || !found || applied != 4 {
		t.Fatalf("journal applied=%d found=%t err=%v", applied, found, err)
	}
	replay, err := claim.FinalizeReplicatedSchemaDDLShadow(t.Context(), op, 4)
	if err != nil || !reflect.DeepEqual(replay, target) {
		t.Fatalf("exact finalize replay differs: %v", err)
	}
}

func TestReplicatedSchemaDDLShadowReplayRestartAfterDurableEffects(t *testing.T) {
	for _, stage := range []string{"replay-relation", "replay-cursor"} {
		t.Run(stage, func(t *testing.T) {
			path, db, base, identity, claim := schemaDDLJournalFixture(t)
			op := [32]byte{102}
			shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			schemaShadowApply(t, claim, db, base, 4, `{"id":"one","city":"Porto"}`, `{"id":"two","city":"Porto"}`)
			failure := errors.New("injected interruption after target effects")
			schemaDDLShadowFaultHook = func(at string) error {
				if at == stage {
					return failure
				}
				return nil
			}
			t.Cleanup(func() { schemaDDLShadowFaultHook = nil })
			_, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
			schemaDDLShadowFaultHook = nil
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
			assertSchemaShadowRows(t, claim, shadow, 2, "Porto", 2)
			if err := errors.Join(claim.Close(), db.Close()); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			active, afterIdentity, err := reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil || !reflect.DeepEqual(afterIdentity, identity) {
				t.Fatalf("reopen changed source identity: %v", err)
			}
			defer active.Close()
			again, err := active.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
			if err != nil || !reflect.DeepEqual(again, shadow) {
				t.Fatalf("ready copy was rebuilt or cursor advanced before effects: %v", err)
			}
			shadow, err = active.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
			if err != nil || shadow.Cursor.Publication.Applied != 4 {
				t.Fatalf("restarted replay=%+v err=%v", shadow.Cursor, err)
			}
			assertSchemaShadowRows(t, active, shadow, 2, "Porto", 2)
		})
	}
}

func TestReplicatedSchemaDDLShadowInterruptedCopyRestartsAtFreshCut(t *testing.T) {
	path, db, base, identity, claim := schemaDDLJournalFixture(t)
	op := [32]byte{103}
	failure := errors.New("copy receipt interrupted")
	schemaDDLShadowFaultHook = func(stage string) error {
		if stage == "copy-ready" {
			return failure
		}
		return nil
	}
	t.Cleanup(func() { schemaDDLShadowFaultHook = nil })
	_, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	schemaDDLShadowFaultHook = nil
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	root := openSchemaDDLJournalTestRoot(t, claim)
	before, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || !found || before.Ready {
		t.Fatal("missing pending copy ownership")
	}
	schemaShadowApply(t, claim, db, base, 4, `{"id":"two","city":"Porto"}`)
	if err := errors.Join(claim.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	claim, _, err = reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil || shadow.Snapshot.Publication.Applied != 4 || !bytes.Equal(before.Shadow.Catalog, shadow.Catalog) {
		t.Fatalf("retry did not reuse owned images at a fresh cut: %+v %v", shadow, err)
	}
	assertSchemaShadowRows(t, claim, shadow, 2, "Porto", 1)
}

func TestReplicatedSchemaDDLShadowOwnershipNeverLocksSourceAndRejectsForgedCursor(t *testing.T) {
	_, db, base, _, claim := schemaDDLJournalFixture(t)
	op := [32]byte{108}
	_, unlock, err := claim.lockSchemaDDLShadow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20); !errors.Is(err, storeio.ErrWriterLocked) {
		unlock()
		t.Fatalf("DDL ownership did not fail without waiting: %v", err)
	}
	schemaShadowApply(t, claim, db, base, 4, `{"id":"two","city":"Porto"}`)
	unlock()
	if _, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20); err != nil {
		t.Fatal(err)
	}
	schemaShadowApply(t, claim, db, base, 5, `{"id":"three","city":"Porto"}`)
	if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10); err != nil {
		t.Fatal(err)
	}
	root := openSchemaDDLJournalTestRoot(t, claim)
	record, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || !found {
		t.Fatal(err)
	}
	wrongKind := record
	wrongKind.Truncate = true
	if err := writeSchemaDDLShadowRecord(root, wrongKind); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("journal changed index build into truncate: %v", err)
	}
	record.Shadow.Cursor.Digest[0] ^= 1
	if err := writeSchemaDDLShadowRecord(root, record); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("same-applied forged head accepted: %v", err)
	}
}

func TestReplicatedSchemaDDLShadowTruncateDoesNotResurrectEarlierWrites(t *testing.T) {
	_, db, base, _, claim := schemaDDLJournalFixture(t)
	op := [32]byte{104}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "TRUNCATE docs", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	schemaShadowApply(t, claim, db, base, 4, `{"id":"two","city":"Porto"}`)
	shadow, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
	if err != nil || shadow.Cursor.Publication.Applied != 4 {
		t.Fatal(err)
	}
	assertSchemaShadowRows(t, claim, shadow, 0, "", 0)
	row, err := claim.PointReadInto(1, testReplicatedApplyKey(t, db, []byte(`{"id":"two"}`)), 4, base.UserLimits.MaxDocumentBytes, nil)
	if err != nil || !row.Found {
		t.Fatalf("unpublished truncate changed source: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := claim.ReplayReplicatedSchemaDDLShadow(canceled, op, 1); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored")
	}
}

func TestReplicatedSchemaDDLShadowRejectsWrongBeforeImageAndCaptureAbort(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprint(abort), func(t *testing.T) {
			_, db, base, _, claim := schemaDDLJournalFixture(t)
			op := [32]byte{105}
			records := uint64(100)
			if abort {
				records = 1
			}
			shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", records, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			key := testReplicatedApplyKey(t, db, []byte(`{"id":"one"}`))
			if !abort {
				collection, close := schemaShadowCollection(t, claim, shadow)
				_, err := collection.Put(key, []byte(`{"id":"one","city":"Madrid"}`))
				close()
				if err != nil {
					t.Fatal(err)
				}
			}
			schemaShadowApply(t, claim, db, base, 4, `{"id":"one","city":"Porto"}`)
			if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
				t.Fatalf("invalid build replayed: %v", err)
			}
			root := openSchemaDDLJournalTestRoot(t, claim)
			record, found, err := readSchemaDDLShadowRecord(root)
			if err != nil || !found || record.Shadow.Cursor != shadow.Cursor {
				t.Fatal("failed replay advanced durable cursor")
			}
			schemaShadowApply(t, claim, db, base, 5, `{"id":"two","city":"Porto"}`)
			row, err := claim.PointReadInto(1, key, 5, base.UserLimits.MaxDocumentBytes, nil)
			if err != nil || !row.Found || !bytes.Equal(row.Value, []byte(`{"city":"Porto","id":"one"}`)) {
				t.Fatalf("build failure damaged source: %+v %v", row, err)
			}
		})
	}
}

func TestReplicatedSchemaDDLShadowBundleRestartBetweenRelationCommits(t *testing.T) {
	path, db, binding, _ := prepareReplicatedTestRoot(t, "shadow-bundle", false)
	defer db.Close()
	session, err := db.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{"CREATE INDEX by_email ON docs (email)", "CREATE TABLE email_claims (PRIMARY KEY (key))"} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	base, err := db.BindReplicatedShardStoreBundle(binding, "docs", []ReplicatedGlobalIndexRelation{{
		Relation: 2, Table: "email_claims", IndexID: 41, Incarnation: 7, LocatorCount: 1, Unique: true,
		KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion, BucketBits: distribution.DefaultVirtualBucketBits,
	}})
	if err != nil {
		t.Fatal(err)
	}
	claim, identity, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	applyReplicatedApplySessionOpen(t, claim, base, 2)
	baseKey := testReplicatedApplyKey(t, db, []byte(`{"id":"one"}`))
	oldKey := testReplicatedGlobalIndexKey(t, base.Relations[1], "a")
	newKey := testReplicatedGlobalIndexKey(t, base.Relations[1], "b")
	locator := []byte(`["one"]`)
	apply := func(applied uint64, document string, globals []replication.Mutation) {
		t.Helper()
		command := testReplicatedApplyCommandValue(base, 2, applied-1, nil)
		command.Fingerprint = sha256.Sum256([]byte(document))
		command.Batches = []replication.RelationMutationBatch{
			{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: baseKey, Value: []byte(document)}}},
			{Relation: 2, Mutations: globals},
		}
		raw, err := replication.AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(applied), raw); err != nil {
			t.Fatal(err)
		}
		if code := completionResultCode(t, claim, raw); code != replicatedstate.ResultApplied {
			t.Fatalf("bundle completion=%d", code)
		}
	}
	apply(3, `{"id":"one","city":"Lisbon","email":"a"}`, []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual, Key: oldKey, Value: locator}})
	op := [32]byte{106}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "DROP INDEX by_email", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	apply(4, `{"id":"one","city":"Porto","email":"b"}`, []replication.Mutation{
		{Kind: replication.MutationDeleteDigestEqual, Key: oldKey, ExpectedValueLength: uint64(len(locator)), ExpectedValueDigest: sha256.Sum256(locator)},
		{Kind: replication.MutationPutAbsentOrEqual, Key: newKey, Value: locator},
	})
	failure := errors.New("interrupted between base and global relation")
	schemaDDLShadowFaultHook = func(stage string) error {
		if stage == "replay-relation" {
			return failure
		}
		return nil
	}
	t.Cleanup(func() { schemaDDLShadowFaultHook = nil })
	_, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
	schemaDDLShadowFaultHook = nil
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	assertSchemaShadowRows(t, claim, shadow, 1, "", 0)
	if err := errors.Join(claim.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	active, _, err := reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	shadow, err = active.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
	if err != nil || shadow.Cursor.Publication.Applied != 4 {
		t.Fatalf("bundle replay=%+v %v", shadow.Cursor, err)
	}
	assertSchemaShadowRows(t, active, shadow, 1, "", 0)
	baseCollection, closeBase := schemaShadowCollection(t, active, shadow)
	baseSnapshot, err := baseCollection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if indexes := baseSnapshot.AppendIndexes(nil); len(indexes) != 0 {
		t.Fatalf("dropped local index survived in shadow: %+v", indexes)
	}
	if value, found, err := baseSnapshot.AppendRaw(nil, baseKey); err != nil || !found || !bytes.Contains(value, []byte(`"Porto"`)) {
		t.Fatalf("base relation was not replayed: %q %v", value, err)
	}
	if err := baseSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	closeBase()
	catalog, _, err := openReplicatedSchemaCatalogImage(shadow.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	relation := catalog.ReplicatedShardStore.Relations[1]
	file, err := os.OpenFile(filepath.Join(active.database.dataDir, replicatedSchemaTargetsDirectory, relation.Storage+".vjc"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := durable.Open(file, durableOptions(&table{meta: catalog.Tables[relation.Table]}))
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	if _, found, err := collection.AppendRaw(nil, oldKey); err != nil || found {
		t.Fatalf("deleted global locator survived: %v", err)
	}
	if got, found, err := collection.AppendRaw(nil, newKey); err != nil || !found || !bytes.Equal(got, locator) {
		t.Fatalf("new global locator missing: %q %v", got, err)
	}
}
