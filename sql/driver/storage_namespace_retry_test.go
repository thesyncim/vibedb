package driver

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestDiscardTableStorageRetainsNamespaceAfterRemoveFailure(t *testing.T) {
	directory := t.TempDir()
	meta := &tableMeta{
		PrimaryKey: "/id",
		Storage:    strings.Repeat("a", storageIdentityBytes*2),
	}
	candidate := &table{meta: meta}
	database := &database{dataDir: directory}
	path := database.tablePathForMeta(meta)
	journal := durable.RecoveryJournalPath(path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := durable.Create(file, durableOptions(candidate))
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	candidate.file = file
	candidate.collection = collection
	t.Cleanup(func() {
		if !collection.CloseCompleted() {
			_ = collection.Close()
		}
		_ = file.Close()
		_ = os.RemoveAll(journal)
		_ = os.Remove(path)
	})

	blocker := filepath.Join(journal, "blocker")
	database.closeCollection = func(got *durable.Collection) error {
		if got != collection {
			return errors.New("closed the wrong unpublished collection")
		}
		if err := got.Close(); err != nil {
			return err
		}
		if err := os.Remove(journal); err != nil {
			return err
		}
		if err := os.Mkdir(journal, 0o700); err != nil {
			return err
		}
		return os.WriteFile(blocker, []byte("keep namespace busy"), 0o600)
	}

	err = database.discardTableStorageLocked("docs", candidate)
	if err == nil || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("discard with blocked journal removal = %v", err)
	}
	if !collection.CloseCompleted() {
		t.Fatal("discard did not complete collection close")
	}
	if candidate.collection != nil || candidate.file != nil {
		t.Fatalf(
			"discard retained completed resources on candidate: collection=%p file=%p",
			candidate.collection, candidate.file,
		)
	}
	if len(database.retired) != 1 {
		t.Fatalf("namespace retry entries = %d, want 1", len(database.retired))
	}
	retired := database.retired[0]
	if retired.collection != nil || retired.file != nil || retired.removed ||
		retired.path != path || retired.journal != journal {
		t.Fatalf("namespace retry ownership = %+v", retired)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("discarded primary path = %v, want absent", statErr)
	}
	if _, statErr := os.Stat(blocker); statErr != nil {
		t.Fatalf("blocked journal namespace = %v", statErr)
	}

	database.closeCollection = nil
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := database.settleDroppedTablesLocked(); err != nil {
		t.Fatalf("settle namespace-only retirement: %v", err)
	}
	if len(database.retired) != 0 {
		t.Fatalf("settled namespace retry entries = %+v", database.retired)
	}
	if _, statErr := os.Stat(journal); !os.IsNotExist(statErr) {
		t.Fatalf("settled journal path = %v, want absent", statErr)
	}
}

func TestReplicatedApplyActivationRetainsNamespaceAfterRemoveFailure(t *testing.T) {
	_, db, base := bindReplicatedApplyTestRoot(t, "apply-remove-retry")
	core := db.connector.db
	injected := errors.New("injected definite apply catalog failure")
	var failedPath, failedJournal, blocker string
	var injectedCleanupErr error

	claim, identity, err := db.openReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
		func(current *database) (bool, error) {
			failedPath = current.replicatedApplyFile.Name()
			failedJournal = durable.RecoveryJournalPath(failedPath)
			blocker = filepath.Join(failedJournal, "blocker")
			failedCollection := current.replicatedApplyCollection
			current.closeCollection = func(got *durable.Collection) error {
				if got != failedCollection {
					return errors.New("closed the wrong replicated apply collection")
				}
				if closeErr := got.Close(); closeErr != nil {
					return closeErr
				}
				if removeErr := os.Remove(failedJournal); removeErr != nil {
					injectedCleanupErr = removeErr
					return removeErr
				}
				if mkdirErr := os.Mkdir(failedJournal, 0o700); mkdirErr != nil {
					injectedCleanupErr = mkdirErr
					return mkdirErr
				}
				writeErr := os.WriteFile(
					blocker, []byte("keep apply namespace busy"), 0o600,
				)
				injectedCleanupErr = writeErr
				return writeErr
			}
			return false, injected
		},
	)
	core.mu.Lock()
	core.closeCollection = nil
	core.mu.Unlock()
	if injectedCleanupErr != nil {
		t.Fatalf("install replicated cleanup blocker: %v", injectedCleanupErr)
	}
	if claim != nil || identity != (ReplicatedApplyIdentity{}) ||
		!errors.Is(err, injected) ||
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("failed activation = %p,%+v,%v", claim, identity, err)
	}

	core.mu.RLock()
	applyMeta := core.catalog.ReplicatedApply
	applyCollection := core.replicatedApplyCollection
	applyFile := core.replicatedApplyFile
	retired := append([]retiredTable(nil), core.retired...)
	core.mu.RUnlock()
	if applyMeta != nil || applyCollection != nil || applyFile != nil {
		t.Fatalf(
			"failed activation retained published ownership: meta=%+v collection=%p file=%p",
			applyMeta, applyCollection, applyFile,
		)
	}
	if len(retired) != 1 || retired[0].collection != nil ||
		retired[0].file != nil || retired[0].removed ||
		retired[0].path != failedPath || retired[0].journal != failedJournal {
		t.Fatalf("replicated namespace retry ownership = %+v", retired)
	}
	if _, statErr := os.Stat(failedPath); !os.IsNotExist(statErr) {
		t.Fatalf("failed replicated primary path = %v, want absent", statErr)
	}
	if _, statErr := os.Stat(blocker); statErr != nil {
		t.Fatalf("blocked replicated journal namespace = %v", statErr)
	}

	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	claim, identity, err = db.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil || claim == nil || identity.Storage == "" {
		t.Fatalf("activation after namespace retry = %p,%+v,%v", claim, identity, err)
	}
	if identity.Storage == filepath.Base(strings.TrimSuffix(failedPath, ".vjc")) {
		t.Fatal("activation reused failed storage identity")
	}
	if _, statErr := os.Stat(failedJournal); !os.IsNotExist(statErr) {
		t.Fatalf("settled replicated journal path = %v, want absent", statErr)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
