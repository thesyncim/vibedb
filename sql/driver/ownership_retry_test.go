package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

func applyOwnershipRetryPair(
	t *testing.T,
	log *durable.TxnLog,
	limits durable.TxnLimits,
	leftName string,
	left *durable.Collection,
	rightName string,
	right *durable.Collection,
	remove bool,
) {
	t.Helper()
	err := durable.UpdateCollections(
		log,
		[]durable.NamedCollection{
			{Name: leftName, Collection: left},
			{Name: rightName, Collection: right},
		},
		limits,
		func(batch *durable.DatabaseBatch) error {
			leftBatch, err := batch.Collection(leftName)
			if err != nil {
				return err
			}
			rightBatch, err := batch.Collection(rightName)
			if err != nil {
				return err
			}
			key := []byte("ownership-retry")
			if remove {
				if err := leftBatch.Delete(key); err != nil {
					return err
				}
				return rightBatch.Delete(key)
			}
			value := []byte(`{"id":"ownership-retry","n":1}`)
			if err := leftBatch.Put(key, value); err != nil {
				return err
			}
			return rightBatch.Put(key, value)
		},
	)
	if err != nil {
		t.Fatalf("ownership retry transaction remove=%v: %v", remove, err)
	}
}

func addOwnershipRetryAuxTable(
	t *testing.T, database *database,
) *table {
	t.Helper()
	statement, err := query.PrepareDML(`CREATE TABLE aux (PRIMARY KEY (id))`)
	if err != nil {
		t.Fatal(err)
	}
	database.mu.Lock()
	_, err = database.createTableLocked(statement)
	if err == nil {
		_, err = database.materializeLocked("aux", nil)
	}
	aux := database.tables["aux"]
	database.mu.Unlock()
	statement.Release()
	if err != nil {
		t.Fatalf("materialize aux: %v", err)
	}
	if aux == nil || aux.collection == nil || aux.file == nil {
		t.Fatalf("invalid aux table: %+v", aux)
	}
	return aux
}

func ownershipRetryConditionalImage(
	t *testing.T, database *database, leftName string, left *table, rightName string, right *table,
) []byte {
	t.Helper()
	applyOwnershipRetryPair(
		t, database.txnLog, database.txnLimits,
		leftName, left.collection, rightName, right.collection, false,
	)
	applyOwnershipRetryPair(
		t, database.txnLog, database.txnLimits,
		leftName, left.collection, rightName, right.collection, true,
	)
	image, err := os.ReadFile(durable.RecoveryJournalPath(left.file.Name()))
	if err != nil {
		t.Fatalf("read conditional journal: %v", err)
	}
	return image
}

func TestReplicatedApplyRetriesRetainedCandidateDetachBeforeReplacement(t *testing.T) {
	path, db, base := bindReplicatedApplyTestRoot(t, "apply-detach-retry")
	core := db.connector.db
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()
	injected := errors.New("injected definite apply catalog failure")
	var orphan, retainedPath string
	claim, identity, err := db.openReplicatedApply(
		base, bootstrap, options,
		func(current *database) (bool, error) {
			user := current.tables[base.UserTable]
			hidden := &table{
				file:       current.replicatedApplyFile,
				collection: current.replicatedApplyCollection,
			}
			image := ownershipRetryConditionalImage(
				t, current, base.UserTable, user, "hidden", hidden,
			)
			orphan = filepath.Join(
				current.dataDir, strings.Repeat("0123456789abcdef", 4)+".vjc.rjournal",
			)
			if err := os.WriteFile(orphan, image, 0o600); err != nil {
				t.Fatalf("write detach blocker: %v", err)
			}
			retainedPath = current.replicatedApplyFile.Name()
			return false, injected
		},
	)
	if claim != nil || identity != (ReplicatedApplyIdentity{}) ||
		!errors.Is(err, injected) {
		t.Fatalf("blocked activation = %p,%+v,%v", claim, identity, err)
	}
	core.mu.RLock()
	retainedCollection := core.replicatedApplyCollection
	retainedFile := core.replicatedApplyFile
	if core.catalog.ReplicatedApply != nil || retainedCollection == nil || retainedFile == nil {
		core.mu.RUnlock()
		t.Fatal("detach failure did not retain unpublished apply ownership")
	}
	core.mu.RUnlock()
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("retained apply path: %v", err)
	}

	if retry, _, retryErr := db.OpenReplicatedApply(
		base, bootstrap, options,
	); retry != nil || retryErr == nil {
		t.Fatalf("blocked cleanup retry = %p,%v", retry, retryErr)
	}
	core.mu.RLock()
	if core.replicatedApplyCollection != retainedCollection ||
		core.replicatedApplyFile != retainedFile {
		core.mu.RUnlock()
		t.Fatal("blocked cleanup retry replaced retained ownership")
	}
	core.mu.RUnlock()
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("blocked cleanup unlinked retained apply path: %v", err)
	}

	if err := os.Remove(orphan); err != nil {
		t.Fatalf("remove detach blocker: %v", err)
	}
	claim, identity, err = db.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || claim == nil || identity.Storage == "" {
		t.Fatalf("activation after detach retry = %p,%+v,%v", claim, identity, err)
	}
	if _, err := os.Stat(retainedPath); !os.IsNotExist(err) {
		t.Fatalf("settled unpublished apply path = %v, want removed", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatalf("reopen activation after detach retry: %v", err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(
		base, bootstrap, options,
	)
	if err != nil || reopenedClaim == nil || reopenedIdentity != identity {
		t.Fatalf("reacquire activation after detach retry = %p,%+v,%v",
			reopenedClaim, reopenedIdentity, err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementOwnershipFailuresAreRetriedBeforeCleanup(t *testing.T) {
	t.Run("candidate detach quarantine", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "catalog.vdb")
		database, err := openDatabase(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = database.close() })
		prepareIncarnationTable(t, database)
		aux := addOwnershipRetryAuxTable(t, database)
		database.mu.RLock()
		old := database.tables["docs"]
		conditional := ownershipRetryConditionalImage(
			t, database, "docs", old, "aux", aux,
		)
		database.mu.RUnlock()
		orphan := filepath.Join(
			database.dataDir, strings.Repeat("fedcba9876543210", 4)+".vjc.rjournal",
		)
		hookCalls := 0
		setDatabaseTxnBeforeMarkerRecycleHook(t, func(*durable.TxnLog) {
			hookCalls++
			if hookCalls == 1 {
				if err := os.WriteFile(orphan, conditional, 0o600); err != nil {
					t.Fatalf("write replacement detach blocker: %v", err)
				}
			}
		})

		realCatalogPath := database.path
		database.path = filepath.Join(t.TempDir(), "missing", "catalog.vdb")
		database.mu.Lock()
		err = database.truncateTableStorageLockedContext(
			context.Background(), "docs",
		)
		database.path = realCatalogPath
		if !errors.Is(err, os.ErrNotExist) {
			database.mu.Unlock()
			t.Fatalf("definite replacement failure = %v, want not-exist", err)
		}
		if database.tables["docs"] != old || len(database.retired) != 1 {
			database.mu.Unlock()
			t.Fatalf("replacement rollback ownership = old %v retired %+v",
				database.tables["docs"] == old, database.retired)
		}
		quarantined := database.retired[0]
		if quarantined.collection == nil || quarantined.file == nil ||
			!strings.Contains(quarantined.name, "unpublished replacement") {
			database.mu.Unlock()
			t.Fatalf("invalid replacement quarantine: %+v", quarantined)
		}
		database.mu.Unlock()
		if _, err := os.Stat(quarantined.path); err != nil {
			t.Fatalf("quarantined replacement was unlinked: %v", err)
		}

		database.mu.Lock()
		retryErr := database.settleCatalogLocked()
		retained := len(database.retired) == 1 &&
			database.retired[0].collection == quarantined.collection
		database.mu.Unlock()
		if !errors.Is(retryErr, durable.ErrCommitOutcomeUnknown) || !retained {
			t.Fatalf("blocked settlement = %v retained=%v", retryErr, retained)
		}
		if _, err := os.Stat(quarantined.path); err != nil {
			t.Fatalf("blocked settlement unlinked quarantine: %v", err)
		}

		if err := os.Remove(orphan); err != nil {
			t.Fatalf("remove replacement detach blocker: %v", err)
		}
		database.mu.Lock()
		retryErr = database.settleCatalogLocked()
		remaining := len(database.retired)
		database.mu.Unlock()
		if retryErr != nil || remaining != 0 {
			t.Fatalf("settlement after blocker removal = %v retired=%d", retryErr, remaining)
		}
		if _, err := os.Stat(quarantined.path); !os.IsNotExist(err) {
			t.Fatalf("settled quarantine path = %v, want removed", err)
		}
	})

	t.Run("authoritative reattach barrier", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "catalog.vdb")
		database, err := openDatabase(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = database.close() })
		prepareIncarnationTable(t, database)
		aux := addOwnershipRetryAuxTable(t, database)
		database.mu.RLock()
		old := database.tables["docs"]
		_ = ownershipRetryConditionalImage(
			t, database, "docs", old, "aux", aux,
		)
		database.mu.RUnlock()
		hookCalls := 0
		var hookErr error
		setDatabaseTxnBeforeMarkerRecycleHook(t, func(*durable.TxnLog) {
			hookCalls++
			if hookCalls == 2 {
				_, hookErr = old.collection.Put(
					[]byte("reattach-blocker"),
					[]byte(`{"id":"reattach-blocker"}`),
				)
			}
		})

		realCatalogPath := database.path
		database.path = filepath.Join(t.TempDir(), "missing", "catalog.vdb")
		database.mu.Lock()
		err = database.truncateTableStorageLockedContext(
			context.Background(), "docs",
		)
		database.path = realCatalogPath
		pending := len(database.txnReattach)
		retired := len(database.retired)
		database.mu.Unlock()
		if hookErr != nil {
			t.Fatalf("install reattach blocker: %v", hookErr)
		}
		if !errors.Is(err, os.ErrNotExist) || pending != 1 || retired != 0 {
			t.Fatalf(
				"definite replacement/reattach failure = %v pending=%d retired=%d",
				err, pending, retired,
			)
		}

		database.mu.Lock()
		retryErr := database.settleCatalogLocked()
		pending = len(database.txnReattach)
		database.mu.Unlock()
		if !errors.Is(retryErr, durable.ErrCommitOutcomeUnknown) || pending != 1 {
			t.Fatalf("uncleared reattach barrier = %v pending=%d", retryErr, pending)
		}
		if err := old.collection.Flush(); err != nil {
			t.Fatalf("clear authoritative journal blocker: %v", err)
		}
		database.mu.Lock()
		retryErr = database.settleCatalogLocked()
		pending = len(database.txnReattach)
		database.mu.Unlock()
		if retryErr != nil || pending != 0 {
			t.Fatalf("reattach retry = %v pending=%d", retryErr, pending)
		}
	})
}
