package driver

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func schemaDDLJournalFixture(t *testing.T) (string, *Database, ReplicatedShardStoreIdentity, ReplicatedApplyIdentity, *ReplicatedApply) {
	t.Helper()
	path, db, base := bindReplicatedApplyTestRoot(t, "ddl-journal")
	t.Cleanup(func() { _ = db.Close() })
	claim, applyID, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = claim.Close() })
	if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	document := []byte(`{"id":"one","city":"Lisbon"}`)
	command, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, epoch, 2,
		[]replication.Mutation{{Kind: replication.MutationPut, Key: testReplicatedApplyKey(t, db, document), Value: document}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
		t.Fatal(err)
	}
	return path, db, base, applyID, claim
}

func openSchemaDDLJournalTestRoot(t *testing.T, claim *ReplicatedApply) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(claim.database.dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

func requireSchemaDDLJournalRecord(t *testing.T, root *os.Root) schemaDDLBuildRecord {
	t.Helper()
	record, found, err := readSchemaDDLBuildRecord(root)
	if err != nil || !found {
		t.Fatalf("retained record: found=%t err=%v", found, err)
	}
	return record
}

func TestReplicatedSchemaDDLJournalReplaysExactReceiptAfterRestart(t *testing.T) {
	path, db, base, applyID, claim := schemaDDLJournalFixture(t)
	operation := [32]byte{1}
	const sql = "CREATE INDEX by_city ON docs (city)"
	target, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql)
	if err != nil || target.Proof.Relations.TotalRows != 1 {
		t.Fatalf("build=%+v err=%v", target, err)
	}
	root := openSchemaDDLJournalTestRoot(t, claim)
	record := requireSchemaDDLJournalRecord(t, root)
	if !record.Ready || !reflect.DeepEqual(record.Target, target) {
		t.Fatal("successful build did not durably retain its exact receipt")
	}
	entries, err := fs.ReadDir(root.FS(), replicatedSchemaTargetsDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err = errors.Join(claim.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, applyID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	active, _, err := reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	replayed, err := active.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql)
	if err != nil || !reflect.DeepEqual(target, replayed) {
		t.Fatalf("replay changed receipt: %v", err)
	}
	after, err := fs.ReadDir(root.FS(), replicatedSchemaTargetsDirectory)
	if err != nil || len(after) != len(entries) {
		t.Fatalf("replay created extra images: before=%d after=%d err=%v", len(entries), len(after), err)
	}
	for _, test := range []struct {
		operation [32]byte
		applied   uint64
		sql       string
	}{
		{operation, 3, "TRUNCATE docs"},
		{operation, 4, sql},
		{[32]byte{2}, 3, sql},
	} {
		if _, err := active.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), test.operation, test.applied, test.sql); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
			t.Fatalf("replaced retained operation: %v", err)
		}
	}
}

func TestReplicatedSchemaDDLJournalRecoversReservedImages(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed-images", true: "partial-image"}[partial], func(t *testing.T) {
			path, db, base, applyID, claim := schemaDDLJournalFixture(t)
			root := openSchemaDDLJournalTestRoot(t, claim)
			operation := [32]byte{1}
			const sql = "CREATE INDEX by_city ON docs (city)"
			target, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql)
			if err != nil {
				t.Fatal(err)
			}
			record := requireSchemaDDLJournalRecord(t, root)
			// Model termination after images were created, before ready was
			// published. Only reserved identities are durable at this cut.
			record.Ready, record.Target.Proof = false, ReplicatedSchemaTargetProof{}
			if err := writeSchemaDDLBuildRecord(root, record); err != nil {
				t.Fatal(err)
			}
			catalog, _, err := openReplicatedSchemaCatalogImage(target.Catalog)
			if err != nil {
				t.Fatal(err)
			}
			if partial {
				name := filepath.Join(replicatedSchemaTargetsDirectory, catalog.ReplicatedShardStore.UserStorage+".vjc")
				file, err := root.OpenFile(name, os.O_WRONLY|os.O_TRUNC, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(claim.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenReplicatedShardStoreWithApply(path, base, applyID)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				claim, _, err = reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer claim.Close()
			}
			replayed, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql)
			if err != nil || !bytes.Equal(target.Catalog, replayed.Catalog) || replayed.Proof.Relations.TotalRows != 1 {
				t.Fatalf("reserved identity recovery: %v", err)
			}
			if _, err := claim.CertifyReplicatedSchemaTarget(replayed.Catalog); err != nil {
				t.Fatalf("recovered images are not valid: %v", err)
			}
			entries, err := fs.ReadDir(root.FS(), replicatedSchemaTargetsDirectory)
			if err != nil || len(entries) != 2*len(catalog.ReplicatedShardStore.Relations) {
				t.Fatalf("orphaned target images: %d err=%v", len(entries), err)
			}
		})
	}
}

func TestReplicatedSchemaDDLJournalRefusesSymlink(t *testing.T) {
	_, _, _, _, claim := schemaDDLJournalFixture(t)
	outside := filepath.Join(t.TempDir(), "unrelated")
	const sentinel = "unrelated data"
	if err := os.WriteFile(outside, []byte(sentinel), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(claim.database.dataDir, schemaDDLJournalName)); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), [32]byte{1}, 3, "TRUNCATE docs"); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("symlink accepted: %v", err)
	}
	raw, err := os.ReadFile(outside)
	if err != nil || string(raw) != sentinel {
		t.Fatalf("unrelated target changed: %q %v", raw, err)
	}
}

func TestReplicatedSchemaDDLJournalCanceledBuildRetainsReservation(t *testing.T) {
	_, _, _, _, claim := schemaDDLJournalFixture(t)
	root := openSchemaDDLJournalTestRoot(t, claim)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The journal wrapper adds one checkpoint before the underlying builder.
	// Its third check is after reservation, before image creation.
	interleave := &schemaDDLInterleaveContext{Context: ctx, apply: cancel}
	operation := [32]byte{1}
	const sql = "CREATE INDEX by_city ON docs (city)"
	if _, err := claim.BuildJournaledReplicatedSchemaDDLTarget(interleave, operation, 3, sql); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	record := requireSchemaDDLJournalRecord(t, root)
	if record.Ready || len(record.Target.Catalog) == 0 {
		t.Fatal("cancellation lost pending identities")
	}
	target, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql)
	if err != nil || !bytes.Equal(record.Target.Catalog, target.Catalog) {
		t.Fatalf("cancellation recovery changed reserved identity: %v", err)
	}
}

func TestReplicatedSchemaDDLJournalNeverDeletesPreparedImages(t *testing.T) {
	_, _, _, _, claim := schemaDDLJournalFixture(t)
	root := openSchemaDDLJournalTestRoot(t, claim)
	operation := [32]byte{1}
	const sql = "CREATE INDEX by_city ON docs (city)"
	target, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.PrepareReplicatedSchemaTarget(target.Catalog, 3, [32]byte{9}); err != nil {
		t.Fatal(err)
	}
	record := requireSchemaDDLJournalRecord(t, root)
	record.Ready, record.Target.Proof = false, ReplicatedSchemaTargetProof{}
	if err := writeSchemaDDLBuildRecord(root, record); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, sql); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("prepared images not protected: %v", err)
	}
	if _, err := claim.CertifyReplicatedSchemaTarget(target.Catalog); err != nil {
		t.Fatalf("prepared images removed: %v", err)
	}
}

func TestReplicatedSchemaDDLJournalNoOpAndCorruption(t *testing.T) {
	_, _, _, _, claim := schemaDDLJournalFixture(t)
	root := openSchemaDDLJournalTestRoot(t, claim)
	for _, operation := range [][32]byte{{1}, {2}} {
		target, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), operation, 3, "DROP INDEX IF EXISTS absent")
		if err != nil || !target.NoOp {
			t.Fatalf("no-op: %+v %v", target, err)
		}
	}
	file, err := root.OpenFile(schemaDDLJournalName, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteAt([]byte("corrupt"), 0)
	if err = errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), [32]byte{2}, 3, "DROP INDEX IF EXISTS absent"); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("corrupt journal accepted: %v", err)
	}
}
