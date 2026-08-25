package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func newCheckpointGroupTestStore(
	t testing.TB, checkpointEvery uint64,
) (string, []NamedCollection, *TxnLog, *CheckpointGroup) {
	return newCheckpointGroupTestStoreWithNames(
		t, checkpointEvery, "system", "user",
	)
}

func newCheckpointGroupTestResources(
	t testing.TB, names ...string,
) (string, []NamedCollection, *TxnLog) {
	t.Helper()
	dir := t.TempDir()
	members := make([]NamedCollection, len(names))
	for i, name := range names {
		members[i] = openTxnNamedCollection(t, dir, name, txnTestOptions())
	}
	log, err := NewTxnLog(dir, TxnLogOptions{})
	if err != nil {
		t.Fatalf("NewTxnLog: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return dir, members, log
}

func checkpointGroupTestSetMaxSpan(
	certificate *checkpointGroupCertificate,
	span uint8,
	txn, last uint64,
) {
	certificate.maxApplySpan = span
	certificate.maxSpanTxn = 0
	certificate.maxSpanFirst = 0
	certificate.maxSpanLast = 0
	if span > 1 {
		certificate.maxSpanTxn = txn
		certificate.maxSpanLast = last
		width := uint64(span)
		if last >= width {
			certificate.maxSpanFirst = last - width + 1
		}
	}
}

func checkpointGroupTestAllZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func TestCheckpointGroupFileCertificateCheckPinsExactRegularEntry(t *testing.T) {
	t.Run("sibling certificate", func(t *testing.T) {
		dir := t.TempDir()
		file, err := os.OpenFile(
			filepath.Join(dir, "data.vjc"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := rejectCheckpointGroupCertificateForFile(file); err != nil {
			t.Fatalf("certificate-free entry: %v", err)
		}
		certificate, err := os.OpenFile(
			filepath.Join(dir, checkpointGroupFilename),
			os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := certificate.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rejectCheckpointGroupCertificateForFile(file); !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
			t.Fatalf("sibling certificate = %v", err)
		}
	})

	t.Run("leaf symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target.vjc")
		created, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := created.Close(); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(dir, "alias.vjc")
		if err := os.Symlink(target, alias); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		file, err := os.OpenFile(alias, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := rejectCheckpointGroupCertificateForFile(file); !errors.Is(err, ErrTransactionLogDirectoryMismatch) {
			t.Fatalf("leaf symlink = %v", err)
		}
	})

	t.Run("retargeted parent", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, "live")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "data.vjc")
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		if err := os.Rename(dir, filepath.Join(root, "moved")); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		replacement, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := replacement.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rejectCheckpointGroupCertificateForFile(file); !errors.Is(err, ErrTransactionLogDirectoryMismatch) {
			t.Fatalf("retargeted parent = %v", err)
		}
	})
}

func TestCheckpointGroupCertificateMemberCapacity(t *testing.T) {
	names := func(count int) []string {
		result := make([]string, count)
		for i := range result {
			result[i] = "member-" + strconv.Itoa(i)
		}
		return result
	}

	t.Run("maximum", func(t *testing.T) {
		_, members, log := newCheckpointGroupTestResources(
			t, names(checkpointGroupMaxMembers)...,
		)
		group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = group.Close() })
	})

	t.Run("one-over", func(t *testing.T) {
		requests := make([]TransactionCollectionOpen, checkpointGroupMaxMembers+1)
		if _, _, _, err := OpenCollectionsWithCheckpointGroup(
			filepath.Join(t.TempDir(), "must-not-open"), TxnLogOptions{},
			requests, names(checkpointGroupMaxMembers+1), CheckpointGroupOptions{},
		); !errors.Is(err, ErrTxnParticipant) {
			t.Fatalf("oversized recovery admission = %v", err)
		}
		dir, members, log := newCheckpointGroupTestResources(
			t, names(checkpointGroupMaxMembers+1)...,
		)
		group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
		if group != nil || !errors.Is(err, ErrTxnParticipant) {
			t.Fatalf("oversized group=%v err=%v", group, err)
		}
		log.regMu.Lock()
		registered := len(log.registered)
		log.regMu.Unlock()
		if registered != 0 || log.checkpointGroup != nil {
			t.Fatalf("oversized group side effects: registered=%d owner=%v", registered, log.checkpointGroup)
		}
		if _, statErr := os.Stat(filepath.Join(dir, checkpointGroupFilename)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("oversized group certificate = %v", statErr)
		}
		for _, member := range members {
			if member.Collection.checkpointGroup.Load() != nil ||
				member.Collection.checkpointGroupRetired.Load() {
				t.Fatalf("oversized group fenced member %q", member.Name)
			}
		}
	})
}

func newCheckpointGroupTestStoreWithNames(
	t testing.TB, checkpointEvery uint64, names ...string,
) (string, []NamedCollection, *TxnLog, *CheckpointGroup) {
	t.Helper()
	dir, members, log := newCheckpointGroupTestResources(t, names...)
	var err error
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{
		CheckpointEvery: checkpointEvery,
	})
	if err != nil {
		_ = log.Close()
		t.Fatalf("NewCheckpointGroup: %v", err)
	}
	t.Cleanup(func() {
		_ = group.Close()
	})
	return dir, members, log, group
}

func checkpointGroupPut(
	t testing.TB, group *CheckpointGroup, applied uint64,
	members []NamedCollection, key string,
) {
	t.Helper()
	err := group.Update(applied, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
		for _, member := range members {
			write, err := batch.Collection(member.Name)
			if err != nil {
				return err
			}
			if err := write.Put([]byte(key), []byte(`{"n":1}`)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update(%d): %v", applied, err)
	}
}

func TestCheckpointGroupZeroSyncApplyAndExclusiveOwnership(t *testing.T) {
	_, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")

	stats := group.Stats()
	if stats.AppliedIndex != 1 || stats.CheckpointAppliedIndex != 0 ||
		stats.TransactionHighWater != 1 || stats.Updates != 1 ||
		stats.JournalSyncs != 0 || stats.MarkerSyncs != 0 ||
		stats.CertificateSyncs != 0 || stats.BarrierSyncs != 0 {
		t.Fatalf("post-apply stats = %+v", stats)
	}
	for _, member := range members {
		if _, ok := collectionDoc(t, member.Collection, "one"); !ok {
			t.Fatalf("member %q missed atomic publication", member.Name)
		}
		if _, err := member.Collection.Put([]byte("direct"), []byte(`{"n":2}`)); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q direct Put = %v", member.Name, err)
		}
		if err := member.Collection.Flush(); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q Flush = %v", member.Name, err)
		}
		if err := member.Collection.Close(); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q Close = %v", member.Name, err)
		}
	}
	if err := UpdateCollections(log, members, defaultTxnLimits(), func(*DatabaseBatch) error {
		return nil
	}); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("generic UpdateCollections = %v", err)
	}
	if err := log.Close(); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("owned TxnLog.Close = %v", err)
	}

	if err := group.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stats = group.Stats()
	if stats.CheckpointAppliedIndex != 1 || stats.CheckpointTransactions != 1 ||
		stats.JournalSyncs != uint64(len(members)) || stats.MarkerSyncs != 0 ||
		stats.CertificateSyncs != 1 || stats.BarrierSyncs != uint64(len(members)+1) {
		t.Fatalf("post-checkpoint stats = %+v", stats)
	}
}

func TestCheckpointGroupActivationRejectsRecycledGenericMarkerBase(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	retired := [16]byte{0x7f}
	if _, err := log.marker.AppendRetirement(retired); err != nil {
		t.Fatal(err)
	}
	if err := log.marker.Sync(); err != nil {
		t.Fatal(err)
	}
	header := log.marker.Header()
	if err := log.marker.Recycle(header.Epoch + 1); err != nil {
		t.Fatal(err)
	}
	if log.marker.Cursor() != 0 || log.marker.Header().BaseSequence == 0 {
		t.Fatalf("recycled activation marker = %+v/%d", log.marker.Header(), log.marker.Cursor())
	}
	missingImage := copyCheckpointGroupDirectory(t, dir)
	beforeMissing := checkpointGroupDirectoryBytes(t, missingImage)
	requests, files := checkpointGroupTestOpenRequests(t, missingImage)
	collections, recoveredLog, recoveredGroup, missingErr := OpenCollectionsWithCheckpointGroup(
		missingImage,
		TxnLogOptions{},
		requests,
		[]string{"system", "user"},
		CheckpointGroupOptions{CheckpointEvery: 8},
	)
	for _, file := range files {
		_ = file.Close()
	}
	if collections != nil || recoveredLog != nil || recoveredGroup != nil ||
		!errors.Is(missingErr, ErrCheckpointGroupCorrupt) {
		t.Fatalf(
			"missing certificate recycled base = collections %v log %v group %v err %v",
			collections, recoveredLog, recoveredGroup, missingErr,
		)
	}
	requireCheckpointGroupDirectoryBytes(t, missingImage, beforeMissing)
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	group, err := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{CheckpointEvery: 8},
	)
	if group != nil || !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("recycled-base activation = group %v, err %v", group, err)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
}

func TestCheckpointGroupActivationBaselineResetsRecycledGenericMarker(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	for _, member := range members {
		if err := log.AdoptCollection(member.Collection); err != nil {
			t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
		}
	}
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	retired := [16]byte{0x7f}
	if _, err := log.marker.AppendRetirement(retired); err != nil {
		t.Fatal(err)
	}
	if err := log.marker.Sync(); err != nil {
		t.Fatal(err)
	}
	before := log.marker.Header()
	if err := log.marker.Recycle(before.Epoch + 1); err != nil {
		t.Fatal(err)
	}
	before = log.marker.Header()
	if before.BaseSequence == 0 || log.marker.Cursor() != 0 {
		t.Fatalf("generic recycled marker = %+v/%d", before, log.marker.Cursor())
	}

	if err := log.ResetDischargedForCheckpointGroupActivation(); err != nil {
		t.Fatalf("ResetDischargedForCheckpointGroupActivation: %v", err)
	}
	after := log.marker.Header()
	if after.MarkerID == before.MarkerID || after.Epoch != 1 ||
		after.BaseSequence != 0 || after.RecycleCount != 1 ||
		log.marker.Cursor() != 0 || log.marker.NextSequence() != 1 {
		t.Fatalf("activation baseline = %+v cursor=%d next=%d, prior=%+v",
			after, log.marker.Cursor(), log.marker.NextSequence(), before)
	}

	// A crash after the baseline but before descriptor/certificate publication is
	// the canonical missing-certificate seam, never a recycled-owner rollback.
	crashImage := copyCheckpointGroupDirectory(t, dir)
	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	collections, recoveredLog, recoveredGroup, err := OpenCollectionsWithCheckpointGroup(
		crashImage, TxnLogOptions{}, requests, []string{"system", "user"},
		CheckpointGroupOptions{CheckpointEvery: 8},
	)
	if collections != nil || recoveredLog != nil || recoveredGroup != nil ||
		!errors.Is(err, ErrCheckpointGroupMissing) {
		t.Fatalf("pre-certificate crash = %v,%v,%v,%v",
			collections, recoveredLog, recoveredGroup, err)
	}
	collections, recoveredLog, err = OpenCollectionsWithTransactions(
		crashImage, TxnLogOptions{}, requests,
	)
	if err != nil {
		t.Fatalf("generic reopen after pre-certificate crash: %v", err)
	}
	for _, collection := range collections {
		if closeErr := collection.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	if err := recoveredLog.Close(); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	group, err := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{CheckpointEvery: 8},
	)
	if err != nil {
		t.Fatalf("NewCheckpointGroup after baseline: %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
}

func TestCheckpointGroupActivationBaselineFoldsOrdinaryJournals(t *testing.T) {
	tests := []struct {
		name     string
		recycled bool
	}{
		{name: "base-zero"},
		{name: "recycled-marker", recycled: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
			for _, member := range members {
				if err := log.AdoptCollection(member.Collection); err != nil {
					t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
				}
			}
			if err := log.EnsureMinted(); err != nil {
				t.Fatal(err)
			}
			if test.recycled {
				if _, err := log.marker.AppendRetirement([16]byte{0x7f}); err != nil {
					t.Fatal(err)
				}
				if err := log.marker.Sync(); err != nil {
					t.Fatal(err)
				}
				header := log.marker.Header()
				if err := log.marker.Recycle(header.Epoch + 1); err != nil {
					t.Fatal(err)
				}
			}
			before := log.marker.Header()
			for i, member := range members {
				key := []byte("ordinary-" + member.Name)
				document := []byte(`{"n":` + strconv.Itoa(i+1) + `}`)
				if _, err := member.Collection.Put(key, document); err != nil {
					t.Fatalf("Put(%s): %v", member.Name, err)
				}
				if member.Collection.journal.Cursor() == 0 {
					t.Fatalf("%s ordinary write did not retain a journal", member.Name)
				}
			}

			if err := log.ResetDischargedForCheckpointGroupActivation(); err != nil {
				t.Fatalf("ResetDischargedForCheckpointGroupActivation: %v", err)
			}
			after := log.marker.Header()
			if test.recycled {
				if after.MarkerID == before.MarkerID || after.BaseSequence != 0 ||
					after.Epoch != 1 || after.RecycleCount != 1 {
					t.Fatalf("recycled activation marker = %+v, prior %+v", after, before)
				}
			} else if after != before {
				t.Fatalf("base-zero activation changed marker: before=%+v after=%+v",
					before, after)
			}
			for i, member := range members {
				if member.Collection.journal.Cursor() != 0 {
					t.Fatalf("%s activation left journal cursor %d",
						member.Name, member.Collection.journal.Cursor())
				}
				key := "ordinary-" + member.Name
				want := `{"n":` + strconv.Itoa(i+1) + `}`
				if got, ok := collectionDoc(t, member.Collection, key); !ok || got != want {
					t.Fatalf("%s live row = %q,%v, want %q", member.Name, got, ok, want)
				}
			}

			// A crash after the folds but before SQL publishes its descriptor must
			// remain an ordinary generic image with the exact imported rows.
			crashImage := copyCheckpointGroupDirectory(t, dir)
			requests, files := checkpointGroupTestOpenRequests(t, crashImage)
			reopened, recoveredLog, err := OpenCollectionsWithTransactions(
				crashImage, TxnLogOptions{}, requests,
			)
			if err != nil {
				t.Fatalf("generic reopen after activation folds: %v", err)
			}
			for i, collection := range reopened {
				key := "ordinary-" + members[i].Name
				want := `{"n":` + strconv.Itoa(i+1) + `}`
				if got, ok := collectionDoc(t, collection, key); !ok || got != want {
					t.Fatalf("reopened %s row = %q,%v, want %q",
						members[i].Name, got, ok, want)
				}
				if err := collection.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := recoveredLog.Close(); err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCheckpointGroupActivationBaselineConditionalRefusalIsByteStable(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	for _, member := range members {
		if err := log.AdoptCollection(member.Collection); err != nil {
			t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
		}
	}
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	if _, err := members[0].Collection.Put(
		[]byte("ordinary"), []byte(`{"n":1}`),
	); err != nil {
		t.Fatal(err)
	}
	header := log.marker.Header()
	prepareUnpublishedOn(
		t, members[1].Collection, header.MarkerID, header.Epoch, 1,
		"conditional", `{"n":2}`,
	)
	before := checkpointGroupDirectoryBytes(t, dir)
	if err := log.ResetDischargedForCheckpointGroupActivation(); !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("conditional activation baseline = %v, want ErrCheckpointGroupCorrupt", err)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, before)
}

func TestCheckpointGroupActivationBaselineFoldFailureReopensGeneric(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	for _, member := range members {
		if err := log.AdoptCollection(member.Collection); err != nil {
			t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
		}
		if _, err := member.Collection.Put(
			[]byte("ordinary-"+member.Name), []byte(`{"n":1}`),
		); err != nil {
			t.Fatalf("Put(%s): %v", member.Name, err)
		}
	}
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	beforeMarker := log.marker.Header()
	order := []*Collection{members[0].Collection, members[1].Collection}
	sortCollectionSnapshotOrder(order)
	fault := storeio.NewFaultJournal(order[1].journal)
	fault.Program(storeio.JournalFaultPlan{Phase: storeio.JournalFaultENOSPCRecycle})

	err := log.ResetDischargedForCheckpointGroupActivation()
	if err == nil || !fault.Faulted() || order[1].PersistenceError() == nil ||
		!errors.Is(err, order[1].PersistenceError()) {
		t.Fatalf("activation fold fault = %v, faulted=%v", err, fault.Faulted())
	}
	if order[0].journal.Cursor() != 0 || order[1].journal.Cursor() == 0 {
		t.Fatalf("partial activation fold cursors = %d,%d, want clean/live",
			order[0].journal.Cursor(), order[1].journal.Cursor())
	}
	if after := log.marker.Header(); after != beforeMarker {
		t.Fatalf("journal fold failure changed marker: before=%+v after=%+v",
			beforeMarker, after)
	}

	// The first member may already be folded and the second member's root may
	// already be durable when its recycle fails. The unchanged generic
	// marker plus the retained second journal must still recover both rows.
	crashImage := copyCheckpointGroupDirectory(t, dir)
	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	reopened, recoveredLog, openErr := OpenCollectionsWithTransactions(
		crashImage, TxnLogOptions{}, requests,
	)
	if openErr != nil {
		t.Fatalf("generic reopen after activation fold fault: %v", openErr)
	}
	for i, collection := range reopened {
		key := "ordinary-" + members[i].Name
		if got, ok := collectionDoc(t, collection, key); !ok || got != `{"n":1}` {
			t.Fatalf("reopened %s row = %q,%v", members[i].Name, got, ok)
		}
		if err := collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := recoveredLog.Close(); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCheckpointGroupActivationBaselineZeroBaseIsExactNoOp(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	for _, member := range members {
		if err := log.AdoptCollection(member.Collection); err != nil {
			t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
		}
	}
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	beforeHeader := log.marker.Header()
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	if err := log.ResetDischargedForCheckpointGroupActivation(); err != nil {
		t.Fatal(err)
	}
	if after := log.marker.Header(); after != beforeHeader {
		t.Fatalf("zero-base baseline changed marker: before=%+v after=%+v",
			beforeHeader, after)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
}

func TestCheckpointGroupActivationBaselineRefusesExistingCertificate(t *testing.T) {
	dir, _, log := newCheckpointGroupTestResources(t, "system", "user")
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	certificate, err := os.OpenFile(
		filepath.Join(dir, checkpointGroupFilename),
		os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := certificate.Close(); err != nil {
		t.Fatal(err)
	}
	before := checkpointGroupDirectoryBytes(t, dir)
	if err := log.ResetDischargedForCheckpointGroupActivation(); !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("existing-certificate baseline = %v", err)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, before)
}

func TestCheckpointGroupActivationBaselineRefusesLiveMarker(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	for _, member := range members {
		if err := log.AdoptCollection(member.Collection); err != nil {
			t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
		}
	}
	if err := log.EnsureMinted(); err != nil {
		t.Fatal(err)
	}
	if _, err := members[0].Collection.Put(
		[]byte("ordinary"), []byte(`{"n":1}`),
	); err != nil {
		t.Fatal(err)
	}
	if members[0].Collection.journal.Cursor() == 0 {
		t.Fatal("ordinary refusal witness did not retain a journal")
	}
	if _, err := log.marker.AppendRetirement([16]byte{0x7f}); err != nil {
		t.Fatal(err)
	}
	if err := log.marker.Sync(); err != nil {
		t.Fatal(err)
	}
	before := checkpointGroupDirectoryBytes(t, dir)
	if err := log.ResetDischargedForCheckpointGroupActivation(); !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("live-marker baseline = %v, want ErrCheckpointGroupCorrupt", err)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, before)
}

func TestCheckpointGroupActivationBaselineMintFaultsReopenGeneric(t *testing.T) {
	fault := errors.New("injected crash after activation marker removal")
	tests := []struct {
		name  string
		phase storeio.TxnMarkerFaultPhase
		hook  bool
	}{
		{name: "after-remove", hook: true},
		{name: "mint-header", phase: storeio.TxnMarkerFaultCreateHeaderWrite},
		{name: "mint-file-sync", phase: storeio.TxnMarkerFaultCreateFileSync},
		{name: "mint-directory-sync", phase: storeio.TxnMarkerFaultCreateParentDirSync},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
			for _, member := range members {
				if err := log.AdoptCollection(member.Collection); err != nil {
					t.Fatalf("AdoptCollection(%s): %v", member.Name, err)
				}
			}
			if err := log.EnsureMinted(); err != nil {
				t.Fatal(err)
			}
			if _, err := log.marker.AppendRetirement([16]byte{0x7f}); err != nil {
				t.Fatal(err)
			}
			if err := log.marker.Sync(); err != nil {
				t.Fatal(err)
			}
			header := log.marker.Header()
			if err := log.marker.Recycle(header.Epoch + 1); err != nil {
				t.Fatal(err)
			}

			previousHook := checkpointGroupAfterActivationBaselineRemoveHook
			if test.hook {
				checkpointGroupAfterActivationBaselineRemoveHook = func(*TxnLog) error {
					return fault
				}
			} else {
				storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{Phase: test.phase})
			}
			t.Cleanup(func() {
				checkpointGroupAfterActivationBaselineRemoveHook = previousHook
				storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
			})
			err := log.ResetDischargedForCheckpointGroupActivation()
			createFaulted := storeio.TxnMarkerCreateFaulted()
			checkpointGroupAfterActivationBaselineRemoveHook = previousHook
			storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
			if !errors.Is(err, ErrCommitOutcomeUnknown) ||
				(test.hook && !errors.Is(err, fault)) {
				t.Fatalf("activation baseline fault = %v", err)
			}
			if !test.hook && !createFaulted {
				t.Fatal("activation remint fault did not fire")
			}
			if log.marker != nil || !errors.Is(log.poison, ErrCommitOutcomeUnknown) {
				t.Fatalf("faulted baseline owner = marker %v poison %v", log.marker, log.poison)
			}
			for _, member := range members {
				if !errors.Is(member.Collection.PersistenceError(), ErrCommitOutcomeUnknown) {
					t.Fatalf("member %q poison = %v", member.Name, member.Collection.PersistenceError())
				}
			}

			crashImage := copyCheckpointGroupDirectory(t, dir)
			requests, files := checkpointGroupTestOpenRequests(t, crashImage)
			collections, recoveredLog, openErr := OpenCollectionsWithTransactions(
				crashImage, TxnLogOptions{}, requests,
			)
			if openErr != nil {
				t.Fatalf("generic reopen after activation remint fault: %v", openErr)
			}
			for _, collection := range collections {
				if closeErr := collection.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if closeErr := recoveredLog.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			for _, file := range files {
				if closeErr := file.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
		})
	}
}

func TestCheckpointGroupUpdateConsecutiveSharesAtomicUpdateContract(t *testing.T) {
	t.Run("single-entry parity", func(t *testing.T) {
		_, ordinaryMembers, _, ordinary := newCheckpointGroupTestStore(t, 8)
		_, batchMembers, _, batch := newCheckpointGroupTestStore(t, 8)
		write := func(members []NamedCollection) func(*DatabaseBatch) error {
			return func(database *DatabaseBatch) error {
				for _, member := range members {
					collection, err := database.Collection(member.Name)
					if err != nil {
						return err
					}
					if err := collection.Put([]byte("key"), []byte(`{"n":1}`)); err != nil {
						return err
					}
				}
				return nil
			}
		}
		if err := ordinary.Update(
			1, ordinaryMembers, defaultTxnLimits(), write(ordinaryMembers),
		); err != nil {
			t.Fatal(err)
		}
		if err := batch.UpdateConsecutive(
			1, 1, batchMembers, defaultTxnLimits(), write(batchMembers),
		); err != nil {
			t.Fatal(err)
		}
		ordinaryStats, batchStats := ordinary.Stats(), batch.Stats()
		if ordinaryStats != batchStats || ordinaryStats.Updates != 1 ||
			ordinaryStats.TransactionHighWater != 1 || ordinaryStats.LargestUpdateSpan != 1 {
			t.Fatalf("ordinary=%+v consecutive=%+v", ordinaryStats, batchStats)
		}
		for index := range ordinaryMembers {
			left, leftOK := collectionDoc(t, ordinaryMembers[index].Collection, "key")
			right, rightOK := collectionDoc(t, batchMembers[index].Collection, "key")
			if !leftOK || !rightOK || left != right {
				t.Fatalf("member %d parity = %q/%v %q/%v", index, left, leftOK, right, rightOK)
			}
		}
	})

	t.Run("bounded range is one transaction", func(t *testing.T) {
		_, members, _, group := newCheckpointGroupTestStore(t, 8)
		if err := group.UpdateConsecutive(
			1, 3, members, defaultTxnLimits(), func(database *DatabaseBatch) error {
				for _, member := range members {
					collection, err := database.Collection(member.Name)
					if err != nil {
						return err
					}
					if err := collection.Put([]byte("final"), []byte(`{"n":3}`)); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			t.Fatal(err)
		}
		stats := group.Stats()
		if stats.AppliedIndex != 3 || stats.TransactionHighWater != 1 ||
			stats.Updates != 1 || stats.CheckpointAppliedIndex != 0 ||
			stats.BarrierSyncs != 0 || stats.LargestUpdateSpan != 3 {
			t.Fatalf("batch stats = %+v", stats)
		}
		checkpointGroupPut(t, group, 4, members, "tail")
		if stats = group.Stats(); stats.AppliedIndex != 4 ||
			stats.TransactionHighWater != 2 || stats.Updates != 2 {
			t.Fatalf("tail stats = %+v", stats)
		}
	})

	t.Run("callback failure parity", func(t *testing.T) {
		injected := errors.New("injected callback failure")
		for _, consecutive := range []bool{false, true} {
			name := "update"
			if consecutive {
				name = "consecutive"
			}
			t.Run(name, func(t *testing.T) {
				_, members, _, group := newCheckpointGroupTestStore(t, 8)
				write := func(database *DatabaseBatch) error {
					collection, err := database.Collection(members[0].Name)
					if err != nil {
						return err
					}
					if err := collection.Put([]byte("key"), []byte(`{"n":1}`)); err != nil {
						return err
					}
					return injected
				}
				var err error
				if consecutive {
					err = group.UpdateConsecutive(1, 3, members[:1], defaultTxnLimits(), write)
				} else {
					err = group.Update(1, members[:1], defaultTxnLimits(), write)
				}
				if !errors.Is(err, injected) {
					t.Fatalf("callback failure = %v", err)
				}
				if stats := group.Stats(); stats.AppliedIndex != 0 ||
					stats.TransactionHighWater != 0 || stats.Updates != 0 ||
					stats.LargestUpdateSpan != 0 {
					t.Fatalf("callback failure stats = %+v", stats)
				}
				if _, found := collectionDoc(t, members[0].Collection, "key"); found {
					t.Fatal("callback failure published a row")
				}
			})
		}
	})
}

func TestCheckpointGroupUpdateConsecutiveRejectsEverySequenceGapBeforeCallback(t *testing.T) {
	tests := []struct {
		name        string
		first, last uint64
	}{
		{name: "zero first", first: 0, last: 1},
		{name: "skipped first", first: 2, last: 2},
		{name: "reversed", first: 1, last: 0},
		{name: "maximum final", first: 1, last: ^uint64(0)},
		{name: "one over span", first: 1, last: MaxCheckpointGroupUpdateEntries + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, members, _, group := newCheckpointGroupTestStore(t, 8)
			called := false
			err := group.UpdateConsecutive(
				test.first, test.last, members, defaultTxnLimits(),
				func(*DatabaseBatch) error { called = true; return nil },
			)
			if !errors.Is(err, ErrCheckpointGroupSequence) || called {
				t.Fatalf("range [%d,%d] = %v called=%v", test.first, test.last, err, called)
			}
			if stats := group.Stats(); stats.AppliedIndex != 0 ||
				stats.TransactionHighWater != 0 || stats.Updates != 0 {
				t.Fatalf("rejected stats = %+v", stats)
			}
		})
	}
}

func TestCheckpointGroupUpdateConsecutiveCrashRecoveryIsOldOrComplete(t *testing.T) {
	for _, span := range []uint64{1, 2, MaxCheckpointGroupUpdateEntries} {
		for _, checkpoint := range []bool{false, true} {
			name := "uncertified-span-" + strconv.FormatUint(span, 10)
			if checkpoint {
				name = "certified-span-" + strconv.FormatUint(span, 10)
			}
			t.Run(name, func(t *testing.T) {
				dir, members, _, group := newCheckpointGroupTestStore(t, 8)
				if err := group.UpdateConsecutive(
					1, span, members, defaultTxnLimits(), func(database *DatabaseBatch) error {
						for _, member := range members {
							collection, err := database.Collection(member.Name)
							if err != nil {
								return err
							}
							if err := collection.Put([]byte("batch"), []byte(`{"n":5}`)); err != nil {
								return err
							}
						}
						return nil
					},
				); err != nil {
					t.Fatal(err)
				}
				if checkpoint {
					if err := group.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				crashImage := copyCheckpointGroupDirectory(t, dir)
				collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
				wantApplied := uint64(0)
				if checkpoint {
					wantApplied = span
				}
				if reopened.AppliedIndex() != wantApplied ||
					reopened.CheckpointAppliedIndex() != wantApplied {
					t.Fatalf("recovered cut = %d/%d want %d",
						reopened.AppliedIndex(), reopened.CheckpointAppliedIndex(), wantApplied)
				}
				wantStoredSpan := uint8(0)
				if checkpoint && span > 1 {
					wantStoredSpan = uint8(span)
				}
				if reopened.maxApplySpan != wantStoredSpan {
					t.Fatalf("recovered max span = %d, want %d",
						reopened.maxApplySpan, wantStoredSpan)
				}
				if wantStoredSpan > 1 {
					if reopened.maxSpanTxn != 1 || reopened.maxSpanFirst != 1 ||
						reopened.maxSpanLast != span {
						t.Fatalf("recovered max-span witness = txn %d [%d,%d]",
							reopened.maxSpanTxn, reopened.maxSpanFirst, reopened.maxSpanLast)
					}
				} else if reopened.maxSpanTxn != 0 || reopened.maxSpanFirst != 0 ||
					reopened.maxSpanLast != 0 {
					t.Fatalf("legacy span recovered a witness = txn %d [%d,%d]",
						reopened.maxSpanTxn, reopened.maxSpanFirst, reopened.maxSpanLast)
				}
				wantLargestSpan := uint64(0)
				if checkpoint {
					wantLargestSpan = span
				}
				if got := reopened.Stats().LargestUpdateSpan; got != wantLargestSpan {
					t.Fatalf("recovered largest span = %d, want %d", got, wantLargestSpan)
				}
				for index, collection := range collections {
					_, found := collectionDoc(t, collection, "batch")
					if found != checkpoint {
						t.Fatalf("member %d found=%v want=%v", index, found, checkpoint)
					}
				}
			})
		}
	}
}

func TestCheckpointGroupUpdateConsecutiveDecisionFailureMatchesUpdate(t *testing.T) {
	for _, consecutive := range []bool{false, true} {
		name := "update"
		if consecutive {
			name = "consecutive"
		}
		t.Run(name, func(t *testing.T) {
			_, members, _, group := newCheckpointGroupTestStore(t, 8)
			previous := checkpointGroupFaultHook
			t.Cleanup(func() { checkpointGroupFaultHook = previous })
			checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
				if point == checkpointGroupAfterDecisionAppend {
					return ErrCommitOutcomeUnknown
				}
				return nil
			}
			write := func(database *DatabaseBatch) error {
				collection, err := database.Collection(members[0].Name)
				if err != nil {
					return err
				}
				return collection.Put([]byte("uncertain"), []byte(`{"n":1}`))
			}
			var err error
			if consecutive {
				err = group.UpdateConsecutive(1, 3, members[:1], defaultTxnLimits(), write)
			} else {
				err = group.Update(1, members[:1], defaultTxnLimits(), write)
			}
			if !errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("fault = %v", err)
			}
			if stats := group.Stats(); stats.AppliedIndex != 0 ||
				stats.TransactionHighWater != 0 || stats.Updates != 0 {
				t.Fatalf("uncertain stats = %+v", stats)
			}
			if _, found := collectionDoc(t, members[0].Collection, "uncertain"); found {
				t.Fatal("decision-fault suffix became reader-visible")
			}
			if err := group.Update(1, members[:1], defaultTxnLimits(), write); err == nil {
				t.Fatal("poisoned group accepted another update")
			}
		})
	}
}

func TestCheckpointGroupCertificateMaxApplySpanCanonicalGrammar(t *testing.T) {
	_, _, _, group := newCheckpointGroupTestStore(t, 8)
	base := group.certificateLocked()

	t.Run("legacy zero bytes round trip", func(t *testing.T) {
		encoded, err := encodeCheckpointGroupCertificate(base)
		if err != nil {
			t.Fatal(err)
		}
		if got := encoded[checkpointGroupMaxApplySpanOffset]; got != 0 {
			t.Fatalf("legacy reserved bytes = %d", got)
		}
		if !checkpointGroupTestAllZero(encoded[checkpointGroupMaxSpanWitnessTxnOffset:checkpointGroupChecksumOffset]) {
			t.Fatal("legacy format-0 certificate populated max-span witness tail")
		}
		legacyHeader := [16]byte{
			'V', 'I', 'B', 'E', 'C', 'P', 'G', 0,
			0, 0, 168, 0, 2, 0, 0, 0,
		}
		if !bytes.Equal(encoded[:len(legacyHeader)], legacyHeader[:]) {
			t.Fatalf("legacy format-0 header = %x, want %x",
				encoded[:len(legacyHeader)], legacyHeader)
		}
		decoded, err := decodeCheckpointGroupCertificate(encoded)
		if err != nil || decoded.maxApplySpan != 0 {
			t.Fatalf("legacy decode = %+v, %v", decoded, err)
		}
		roundTrip, err := encodeCheckpointGroupCertificate(decoded)
		if err != nil || !bytes.Equal(encoded, roundTrip) {
			t.Fatalf("legacy round trip changed bytes: %v", err)
		}
	})

	t.Run("maximum membership remains disjoint from witness tail", func(t *testing.T) {
		certificate := base
		certificate.members = make([]checkpointGroupMember, checkpointGroupMaxMembers)
		for index := range certificate.members {
			name := sha256.Sum256([]byte("member-" + strconv.Itoa(index)))
			store := sha256.Sum256([]byte("store-" + strconv.Itoa(index)))
			journal := sha256.Sum256([]byte("journal-" + strconv.Itoa(index)))
			certificate.members[index].nameDigest = name
			copy(certificate.members[index].storeID[:], store[:])
			copy(certificate.members[index].journalID[:], journal[:])
		}
		encoded, err := encodeCheckpointGroupCertificate(certificate)
		if err != nil {
			t.Fatal(err)
		}
		memberEnd := checkpointGroupHeaderBytes +
			checkpointGroupMaxMembers*checkpointGroupMemberBytes
		if memberEnd > checkpointGroupMaxSpanWitnessTxnOffset ||
			!checkpointGroupTestAllZero(encoded[memberEnd:checkpointGroupMaxSpanWitnessTxnOffset]) ||
			!checkpointGroupTestAllZero(encoded[checkpointGroupMaxSpanWitnessTxnOffset:checkpointGroupChecksumOffset]) {
			t.Fatalf("max member bank overlaps noncanonical tail: end=%d witness=%d",
				memberEnd, checkpointGroupMaxSpanWitnessTxnOffset)
		}
		decoded, err := decodeCheckpointGroupCertificate(encoded)
		if err != nil || len(decoded.members) != checkpointGroupMaxMembers {
			t.Fatalf("max-member decode = %d, %v", len(decoded.members), err)
		}
	})

	for _, test := range []struct {
		name       string
		span       uint8
		applied    uint64
		txn        uint64
		wantErr    bool
		wantStored uint8
	}{
		{name: "two", span: 2, applied: 2, txn: 1, wantStored: 2},
		{name: "maximum", span: MaxCheckpointGroupUpdateEntries, applied: MaxCheckpointGroupUpdateEntries, txn: 1, wantStored: MaxCheckpointGroupUpdateEntries},
		{name: "explicit one is noncanonical", span: 1, applied: 1, txn: 1, wantErr: true},
		{name: "one over maximum", span: MaxCheckpointGroupUpdateEntries + 1, applied: MaxCheckpointGroupUpdateEntries + 1, txn: 1, wantErr: true},
		{name: "span exceeds history", span: 2, applied: 1, txn: 1, wantErr: true},
		{name: "apply exceeds span envelope", span: 2, applied: 3, txn: 1, wantErr: true},
		{name: "c0 cannot advertise batching", span: 2, applied: 0, txn: 0, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			certificate := base
			checkpointGroupTestSetMaxSpan(
				&certificate, test.span, test.txn, test.applied,
			)
			certificate.applied = test.applied
			certificate.txnHighWater = test.txn
			encoded, err := encodeCheckpointGroupCertificate(certificate)
			if test.wantErr {
				if !errors.Is(err, ErrCheckpointGroupCorrupt) {
					t.Fatalf("encode = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeCheckpointGroupCertificate(encoded)
			if err != nil || decoded.maxApplySpan != test.wantStored ||
				(test.wantStored > 1 &&
					(decoded.maxSpanTxn != test.txn || decoded.maxSpanLast != test.applied ||
						decoded.maxSpanFirst+uint64(test.wantStored)-1 != decoded.maxSpanLast)) {
				t.Fatalf("decode span/witness = %d/%d:[%d,%d], %v",
					decoded.maxApplySpan, decoded.maxSpanTxn,
					decoded.maxSpanFirst, decoded.maxSpanLast, err)
			}
		})
	}

	if !checkpointGroupAppliedWithin(math.MaxUint64-1, math.MaxUint64, 128) ||
		checkpointGroupAppliedWithin(257, 2, 128) {
		t.Fatal("overflow-safe applied/transaction span sanity changed")
	}

	seeded := base
	seeded.seedApplied = 9
	seeded.seedState = sha256.Sum256([]byte("seed-state"))
	seeded.seedMember = sha256.Sum256([]byte("seed-member"))
	seeded.applied = 11
	seeded.txnHighWater = 3
	checkpointGroupTestSetMaxSpan(&seeded, 2, 3, 11)
	if !validCheckpointGroupSeedCertificate(seeded) {
		t.Fatalf("reachable seeded batch rejected: %+v", seeded)
	}
	belowSeed := seeded
	belowSeed.maxSpanFirst = 1
	belowSeed.maxSpanLast = 2
	if validCheckpointGroupSeedCertificate(belowSeed) {
		t.Fatal("seeded certificate accepted a max-span witness below the imported cut")
	}
	baseBinding := seeded
	baseBinding.maxSpanTxn = 2
	if validCheckpointGroupSeedCertificate(baseBinding) {
		t.Fatal("seeded certificate treated the same-index base binding as a batch witness")
	}
	seeded.applied = 0
	seeded.txnHighWater = 0
	if validCheckpointGroupSeedCertificate(seeded) {
		t.Fatal("seed cut zero advertised a batched history")
	}
}

func TestCheckpointGroupCertificateMaxApplySpanTamperIsAuthenticatedCorruption(t *testing.T) {
	_, _, _, group := newCheckpointGroupTestStore(t, 8)
	certificate := group.certificateLocked()
	certificate.applied = 2
	certificate.txnHighWater = 1
	checkpointGroupTestSetMaxSpan(&certificate, 2, 1, 2)
	encoded, err := encodeCheckpointGroupCertificate(certificate)
	if err != nil {
		t.Fatal(err)
	}
	for _, span := range []uint8{0, 1, MaxCheckpointGroupUpdateEntries + 1} {
		tampered := bytes.Clone(encoded)
		tampered[checkpointGroupMaxApplySpanOffset] = span
		h := sha256.New()
		_, _ = h.Write(checkpointGroupDigestDomain)
		_, _ = h.Write(tampered[:checkpointGroupChecksumOffset])
		copy(tampered[checkpointGroupChecksumOffset:], h.Sum(nil))
		if _, err := decodeCheckpointGroupCertificate(tampered); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("checksum-valid malformed span %d = %v", span, err)
		}
	}
	for _, test := range []struct {
		name   string
		offset int
		value  uint64
	}{
		{name: "zero transaction", offset: checkpointGroupMaxSpanWitnessTxnOffset, value: 0},
		{name: "future transaction", offset: checkpointGroupMaxSpanWitnessTxnOffset, value: 2},
		{name: "zero first", offset: checkpointGroupMaxSpanWitnessFirstOffset, value: 0},
		{name: "short range", offset: checkpointGroupMaxSpanWitnessLastOffset, value: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			tampered := bytes.Clone(encoded)
			binary.LittleEndian.PutUint64(tampered[test.offset:test.offset+8], test.value)
			h := sha256.New()
			_, _ = h.Write(checkpointGroupDigestDomain)
			_, _ = h.Write(tampered[:checkpointGroupChecksumOffset])
			copy(tampered[checkpointGroupChecksumOffset:], h.Sum(nil))
			if _, err := decodeCheckpointGroupCertificate(tampered); !errors.Is(err, ErrCheckpointGroupCorrupt) {
				t.Fatalf("checksum-valid malformed witness = %v", err)
			}
		})
	}
}

func TestCheckpointGroupThreeMemberBarrierAndRecovery(t *testing.T) {
	names := []string{"system", "user", "aux"}
	dir, members, _, group := newCheckpointGroupTestStoreWithNames(
		t, 8, names...,
	)
	err := group.Update(
		1, members, defaultTxnLimits(), func(transaction *DatabaseBatch) error {
			for _, name := range names[:2] {
				write, err := transaction.Collection(name)
				if err != nil {
					return err
				}
				if err := write.Put([]byte("one"), []byte(`{"n":1}`)); err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if stats := group.Stats(); stats.BarrierSyncs != 0 || stats.MarkerSyncs != 0 {
		t.Fatalf("per-index path synced: %+v", stats)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	stats := group.Stats()
	if stats.JournalSyncs != 3 || stats.CertificateSyncs != 1 ||
		stats.BarrierSyncs != 4 || stats.MarkerSyncs != 0 {
		t.Fatalf("three-member checkpoint stats = %+v", stats)
	}
	crashImage := copyCheckpointGroupDirectory(t, dir)
	collections, _, reopened := openCheckpointGroupTestCopyNames(
		t, crashImage, names...,
	)
	if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf("three-member recovered cut = %d/%d",
			reopened.AppliedIndex(), reopened.CheckpointAppliedIndex())
	}
	for i, collection := range collections {
		_, ok := collectionDoc(t, collection, "one")
		if want := i < 2; ok != want {
			t.Fatalf("member %q key presence = %v, want %v", names[i], ok, want)
		}
	}
}

func TestCheckpointGroupCertificateCanonicalSlots(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, checkpointGroupFilename))
	if err != nil {
		t.Fatal(err)
	}
	type slotCertificate struct {
		slot        int
		certificate checkpointGroupCertificate
	}
	valid := make([]slotCertificate, 0, checkpointGroupSlots)
	for slot := 0; slot < checkpointGroupSlots; slot++ {
		start := slot * checkpointGroupSlotBytes
		certificate, decodeErr := decodeCheckpointGroupCertificate(
			raw[start : start+checkpointGroupSlotBytes],
		)
		if decodeErr == nil {
			valid = append(valid, slotCertificate{slot: slot, certificate: certificate})
		}
	}
	if len(valid) != 2 {
		t.Fatalf("valid certificate slots = %d, want 2", len(valid))
	}
	if valid[0].certificate.sequence > valid[1].certificate.sequence {
		valid[0], valid[1] = valid[1], valid[0]
	}
	older, newer := valid[0], valid[1]
	if older.certificate.sequence+1 != newer.certificate.sequence {
		t.Fatalf("test certificate history = %d, %d",
			older.certificate.sequence, newer.certificate.sequence)
	}

	resign := func(slot []byte) {
		h := sha256.New()
		_, _ = h.Write(checkpointGroupDigestDomain)
		_, _ = h.Write(slot[:checkpointGroupChecksumOffset])
		copy(slot[checkpointGroupChecksumOffset:], h.Sum(nil))
	}
	openRaw := func(t *testing.T, data []byte) (checkpointGroupCertificate, error) {
		t.Helper()
		openDir := t.TempDir()
		if err := os.WriteFile(
			filepath.Join(openDir, checkpointGroupFilename), data, 0o600,
		); err != nil {
			t.Fatal(err)
		}
		log, err := newTxnLogDirectory(openDir, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer log.Close()
		file, certificate, err := openCheckpointGroupCertificate(log)
		if file != nil {
			_ = file.Close()
		}
		return certificate, err
	}
	newestBytes := func(data []byte) []byte {
		start := newer.slot * checkpointGroupSlotBytes
		return data[start : start+checkpointGroupSlotBytes]
	}
	pairImage := func(
		t *testing.T,
		previous, selected checkpointGroupCertificate,
	) []byte {
		t.Helper()
		image := make([]byte, checkpointGroupFileBytes)
		for _, item := range []struct {
			slot        int
			certificate checkpointGroupCertificate
		}{
			{slot: older.slot, certificate: previous},
			{slot: newer.slot, certificate: selected},
		} {
			encoded, err := encodeCheckpointGroupCertificate(item.certificate)
			if err != nil {
				t.Fatal(err)
			}
			copy(
				image[item.slot*checkpointGroupSlotBytes:(item.slot+1)*checkpointGroupSlotBytes],
				encoded,
			)
		}
		return image
	}

	t.Run("authenticated-reserved", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		slot[14] = 1
		resign(slot)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("reserved-byte certificate = %v", err)
		}
	})
	t.Run("authenticated-reserved-tail", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		slot[15] = 1
		resign(slot)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("reserved-tail certificate = %v", err)
		}
	})
	t.Run("authenticated-padding", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		padding := checkpointGroupHeaderBytes +
			len(newer.certificate.members)*checkpointGroupMemberBytes
		slot[padding] = 1
		resign(slot)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("padding-byte certificate = %v", err)
		}
	})
	t.Run("authenticated-witness-neighbor-padding", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		slot[checkpointGroupMaxSpanWitnessTxnOffset-1] = 1
		resign(slot)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("witness-neighbor padding certificate = %v", err)
		}
	})
	t.Run("authenticated-witness-with-legacy-span", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		binary.LittleEndian.PutUint64(
			slot[checkpointGroupMaxSpanWitnessTxnOffset:checkpointGroupMaxSpanWitnessFirstOffset],
			1,
		)
		resign(slot)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("legacy span with witness = %v", err)
		}
	})
	t.Run("checksum-boundary-is-not-padding", func(t *testing.T) {
		slot := bytes.Clone(newestBytes(raw))
		slot[checkpointGroupChecksumOffset] ^= 1
		if _, err := decodeCheckpointGroupCertificate(slot); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("checksum boundary tamper = %v", err)
		}
	})
	t.Run("slot-parity", func(t *testing.T) {
		image := make([]byte, checkpointGroupFileBytes)
		wrong := (newer.slot + 1) % checkpointGroupSlots
		copy(
			image[wrong*checkpointGroupSlotBytes:(wrong+1)*checkpointGroupSlotBytes],
			newestBytes(raw),
		)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("wrong-slot certificate = %v", err)
		}
	})
	t.Run("nonconsecutive-history", func(t *testing.T) {
		image := bytes.Clone(raw)
		gap := newer.certificate
		gap.sequence = older.certificate.sequence + 3
		encoded, err := encodeCheckpointGroupCertificate(gap)
		if err != nil {
			t.Fatal(err)
		}
		copy(newestBytes(image), encoded)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("gapped certificate history = %v", err)
		}
	})
	for _, test := range []struct {
		name string
		span uint8
	}{
		{name: "two", span: 2},
		{name: "legacy-zero", span: 0},
	} {
		t.Run("max-apply-span-regression-"+test.name, func(t *testing.T) {
			image := make([]byte, checkpointGroupFileBytes)
			historical := older.certificate
			historical.applied = MaxCheckpointGroupUpdateEntries
			historical.txnHighWater = 1
			historical.txnBase = 0
			checkpointGroupTestSetMaxSpan(
				&historical, MaxCheckpointGroupUpdateEntries, 1,
				MaxCheckpointGroupUpdateEntries,
			)
			encoded, err := encodeCheckpointGroupCertificate(historical)
			if err != nil {
				t.Fatal(err)
			}
			copy(
				image[older.slot*checkpointGroupSlotBytes:(older.slot+1)*checkpointGroupSlotBytes],
				encoded,
			)

			regressed := newer.certificate
			regressed.applied = MaxCheckpointGroupUpdateEntries
			regressed.txnHighWater = MaxCheckpointGroupUpdateEntries
			regressed.txnBase = 0
			checkpointGroupTestSetMaxSpan(
				&regressed, test.span, MaxCheckpointGroupUpdateEntries,
				MaxCheckpointGroupUpdateEntries,
			)
			encoded, err = encodeCheckpointGroupCertificate(regressed)
			if err != nil {
				t.Fatal(err)
			}
			copy(
				image[newer.slot*checkpointGroupSlotBytes:(newer.slot+1)*checkpointGroupSlotBytes],
				encoded,
			)
			if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
				t.Fatalf("regressed max apply span %d = %v", test.span, err)
			}
		})
	}
	t.Run("max-apply-span-first-widening-needs-new-range-witness", func(t *testing.T) {
		historical := older.certificate
		historical.applied = 100
		historical.txnHighWater = 100
		historical.txnBase = 0
		selected := newer.certificate
		selected.applied = MaxCheckpointGroupUpdateEntries
		selected.txnHighWater = 101
		selected.txnBase = 0
		checkpointGroupTestSetMaxSpan(
			&selected, MaxCheckpointGroupUpdateEntries, 101,
			MaxCheckpointGroupUpdateEntries,
		)
		if _, err := openRaw(t, pairImage(t, historical, selected)); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("stale max-span witness = %v", err)
		}
	})
	t.Run("max-apply-span-exact-new-range-witness", func(t *testing.T) {
		historical := older.certificate
		historical.applied = 100
		historical.txnHighWater = 100
		historical.txnBase = 0
		selected := newer.certificate
		selected.applied = 100 + MaxCheckpointGroupUpdateEntries
		selected.txnHighWater = 101
		selected.txnBase = 0
		checkpointGroupTestSetMaxSpan(
			&selected, MaxCheckpointGroupUpdateEntries, 101,
			selected.applied,
		)
		certificate, err := openRaw(t, pairImage(t, historical, selected))
		if err != nil || certificate.maxSpanFirst != 101 ||
			certificate.maxSpanLast != selected.applied {
			t.Fatalf("exact max-span witness = %+v, %v", certificate, err)
		}
	})
	t.Run("max-apply-span-witness-respects-transaction-partition", func(t *testing.T) {
		historical := older.certificate
		historical.applied = 100
		historical.txnHighWater = 100
		historical.txnBase = 0
		selected := newer.certificate
		selected.applied = 101 + MaxCheckpointGroupUpdateEntries
		selected.txnHighWater = 102
		selected.txnBase = 0
		checkpointGroupTestSetMaxSpan(
			&selected, MaxCheckpointGroupUpdateEntries, 101,
			selected.applied,
		)
		if _, err := openRaw(t, pairImage(t, historical, selected)); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("chronologically false max-span witness = %v", err)
		}
	})
	t.Run("marker-rollover-cannot-combine-new-transaction", func(t *testing.T) {
		historical := older.certificate
		historical.applied = 10
		historical.txnHighWater = 10
		historical.txnBase = 0
		selected := newer.certificate
		selected.applied = 11
		selected.txnHighWater = 11
		selected.txnBase = 10
		selected.markerEpoch = historical.markerEpoch + 1
		if _, err := openRaw(t, pairImage(t, historical, selected)); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("combined rollover transaction = %v", err)
		}
	})
	t.Run("exact-empty-marker-rollover", func(t *testing.T) {
		historical := older.certificate
		historical.applied = 10
		historical.txnHighWater = 10
		historical.txnBase = 0
		selected := newer.certificate
		selected.applied = historical.applied
		selected.txnHighWater = historical.txnHighWater
		selected.txnBase = historical.txnHighWater
		selected.markerEpoch = historical.markerEpoch + 1
		if _, err := openRaw(t, pairImage(t, historical, selected)); err != nil {
			t.Fatalf("exact rollover = %v", err)
		}
	})
	t.Run("torn-newest-falls-back", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		slot[checkpointGroupHeaderBytes] ^= 0xff
		certificate, err := openRaw(t, image)
		if err != nil {
			t.Fatal(err)
		}
		if certificate.sequence != older.certificate.sequence {
			t.Fatalf("fallback sequence = %d, want %d",
				certificate.sequence, older.certificate.sequence)
		}
	})
}

func TestCheckpointGroupCloseLeavesTerminalGenericMutationFence(t *testing.T) {
	_, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	if err := group.Close(); err != nil {
		t.Fatalf("CheckpointGroup.Close: %v", err)
	}
	stats := group.Stats()
	if stats.CheckpointAppliedIndex != 1 ||
		stats.BarrierSyncs != uint64(len(members)+1) || stats.MarkerSyncs != 0 {
		t.Fatalf("close checkpoint stats = %+v", stats)
	}
	for _, member := range members {
		if _, err := member.Collection.Put(
			[]byte("post-close"), []byte(`{"n":2}`),
		); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q post-close Put = %v", member.Name, err)
		}
		if err := member.Collection.Flush(); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q post-close Flush = %v", member.Name, err)
		}
	}
	if err := UpdateCollections(
		log, members, defaultTxnLimits(), func(*DatabaseBatch) error { return nil },
	); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("post-close UpdateCollections = %v", err)
	}
	if err := log.EnsureMinted(); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("post-close TxnLog.EnsureMinted = %v", err)
	}
	if _, err := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{},
	); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("post-close NewCheckpointGroup = %v", err)
	}
	for _, member := range members {
		if err := member.Collection.Close(); err != nil {
			t.Fatalf("member %q resource Close = %v", member.Name, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("TxnLog resource Close = %v", err)
	}
}

func TestCheckpointGroupCloseRacesGenericMutationFence(t *testing.T) {
	_, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	start := make(chan struct{})
	errorsOut := make(chan error, 64)
	var wait sync.WaitGroup
	for i := 0; i < cap(errorsOut); i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := members[0].Collection.Put(
				[]byte("racing"), []byte(`{"n":2}`),
			)
			errorsOut <- err
		}()
	}
	close(start)
	if err := group.Close(); err != nil {
		t.Fatalf("CheckpointGroup.Close: %v", err)
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("racing generic Put = %v", err)
		}
	}
	for _, member := range members {
		if _, ok := collectionDoc(t, member.Collection, "racing"); ok {
			t.Fatal("racing generic mutation crossed the terminal fence")
		}
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupQualifyMintedRacesClose(t *testing.T) {
	_, _, log, group := newCheckpointGroupTestStore(t, 8)
	const qualifiers = 32
	start := make(chan struct{})
	errorsOut := make(chan error, qualifiers)
	var wait sync.WaitGroup
	for range qualifiers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for attempt := 0; attempt < 64; attempt++ {
				err := log.QualifyMinted()
				if err == nil {
					continue
				}
				errorsOut <- err
				return
			}
			errorsOut <- nil
		}()
	}
	close(start)
	if err := group.Close(); err != nil {
		t.Fatalf("CheckpointGroup.Close: %v", err)
	}
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil && !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("concurrent QualifyMinted = %v", err)
		}
	}
	if err := log.QualifyMinted(); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("post-close QualifyMinted = %v", err)
	}
}

func TestCheckpointGroupStandaloneOpenRechecksAtNamespaceAdmission(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	file, err := os.OpenFile(filepath.Join(dir, "system.vjc"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	prechecked := make(chan struct{})
	resume := make(chan struct{})
	previousHook := checkpointGroupGenericOpenBeforeNamespaceHook
	checkpointGroupGenericOpenBeforeNamespaceHook = func() {
		close(prechecked)
		<-resume
	}
	t.Cleanup(func() { checkpointGroupGenericOpenBeforeNamespaceHook = previousHook })
	type openResult struct {
		collection *Collection
		err        error
	}
	result := make(chan openResult, 1)
	go func() {
		collection, openErr := Open(file, txnTestOptions())
		result <- openResult{collection: collection, err: openErr}
	}()
	<-prechecked

	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	wantBytes := checkpointGroupDirectoryBytes(t, dir)
	close(resume)
	opened := <-result
	if opened.collection != nil {
		_ = opened.collection.Close()
		t.Fatal("standalone Open crossed activation fence")
	}
	if !errors.Is(opened.err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("standalone Open after activation = %v", opened.err)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, wantBytes)
}

func TestCheckpointGroupGenericCatalogRechecksAfterActivation(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	requests, files := checkpointGroupTestOpenRequests(t, dir)
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	prechecked := make(chan struct{})
	resume := make(chan struct{})
	previousHook := checkpointGroupGenericCatalogAfterPrecheckHook
	checkpointGroupGenericCatalogAfterPrecheckHook = func() {
		close(prechecked)
		<-resume
	}
	t.Cleanup(func() { checkpointGroupGenericCatalogAfterPrecheckHook = previousHook })
	type catalogResult struct {
		collections []*Collection
		log         *TxnLog
		err         error
	}
	result := make(chan catalogResult, 1)
	go func() {
		collections, openedLog, openErr := OpenCollectionsWithTransactions(
			dir, TxnLogOptions{}, requests,
		)
		result <- catalogResult{collections: collections, log: openedLog, err: openErr}
	}()
	<-prechecked
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	wantBytes := checkpointGroupDirectoryBytes(t, dir)
	close(resume)
	opened := <-result
	if opened.log != nil {
		_ = opened.log.Close()
	}
	for _, collection := range opened.collections {
		_ = collection.Close()
	}
	if len(opened.collections) != 0 || opened.log != nil ||
		!errors.Is(opened.err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("generic catalog after activation = %+v", opened)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, wantBytes)
}

func TestCheckpointGroupOpenDatabaseRechecksAfterActivation(t *testing.T) {
	db, err := OpenDatabase(
		t.TempDir(), DatabaseOptions{Options: txnTestOptions()},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"system", "user"} {
		if _, err := db.CreateCollection(name, txnTestOptions()); err != nil {
			t.Fatal(err)
		}
	}
	log, err := ensureDatabaseTxnLog(db)
	if err != nil {
		t.Fatal(err)
	}
	members := make([]NamedCollection, 0, 2)
	for _, name := range []string{"system", "user"} {
		collection, ok := db.Collection(name)
		if !ok {
			t.Fatalf("missing collection %q", name)
		}
		members = append(members, NamedCollection{Name: name, Collection: collection})
	}

	prechecked := make(chan struct{})
	resume := make(chan struct{})
	previousHook := checkpointGroupGenericDatabaseAfterPrecheckHook
	checkpointGroupGenericDatabaseAfterPrecheckHook = func() {
		close(prechecked)
		<-resume
	}
	t.Cleanup(func() { checkpointGroupGenericDatabaseAfterPrecheckHook = previousHook })
	type openResult struct {
		database *Database
		err      error
	}
	result := make(chan openResult, 1)
	go func() {
		reopened, openErr := OpenDatabase(
			db.Dir(), DatabaseOptions{Options: txnTestOptions()},
		)
		result <- openResult{database: reopened, err: openErr}
	}()
	<-prechecked
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wantBytes := checkpointGroupDirectoryBytes(t, db.Dir())
	close(resume)
	opened := <-result
	if opened.database != nil {
		_ = opened.database.Close()
		t.Fatal("OpenDatabase crossed activation fence")
	}
	if !errors.Is(opened.err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("OpenDatabase after activation = %v", opened.err)
	}
	requireCheckpointGroupDirectoryBytes(t, db.Dir(), wantBytes)
}

func TestCheckpointGroupActivationRevalidatesExactRegisteredCatalog(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	validated := make(chan struct{})
	resume := make(chan struct{})
	previousHook := checkpointGroupAfterInitialValidationHook
	checkpointGroupAfterInitialValidationHook = func() {
		close(validated)
		<-resume
	}
	t.Cleanup(func() { checkpointGroupAfterInitialValidationHook = previousHook })
	type groupResult struct {
		group *CheckpointGroup
		err   error
	}
	result := make(chan groupResult, 1)
	go func() {
		group, groupErr := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
		result <- groupResult{group: group, err: groupErr}
	}()
	<-validated
	extra := openTxnNamedCollection(t, dir, "extra", txnTestOptions())
	if err := log.AdoptCollection(extra.Collection); err != nil {
		t.Fatal(err)
	}
	close(resume)
	activated := <-result
	if activated.group != nil {
		_ = activated.group.Close()
		t.Fatal("activation accepted an extra registered collection")
	}
	if !errors.Is(activated.err, ErrTxnParticipant) {
		t.Fatalf("activation with extra registration = %v", activated.err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointGroupFilename)); !os.IsNotExist(err) {
		t.Fatalf("definite pre-publication refusal left certificate: %v", err)
	}
	if _, err := members[0].Collection.Put(
		[]byte("ordinary"), []byte(`{"n":1}`),
	); err != nil {
		t.Fatalf("definite refusal terminally fenced ordinary member: %v", err)
	}
}

func TestCheckpointGroupActivationRejectsUnregisteredEngineFile(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	extra := openTxnNamedCollection(t, dir, "unregistered", txnTestOptions())
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if group != nil {
		_ = group.Close()
		t.Fatal("activation accepted an unregistered engine file")
	}
	if !errors.Is(err, ErrTxnParticipant) {
		t.Fatalf("activation with unregistered engine file = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointGroupFilename)); !os.IsNotExist(err) {
		t.Fatalf("unregistered-file refusal left certificate: %v", err)
	}
	if _, err := extra.Collection.Put(
		[]byte("ordinary"), []byte(`{"n":1}`),
	); err != nil {
		t.Fatalf("pre-publication refusal fenced extra handle: %v", err)
	}
}

func TestCheckpointGroupActivationDiscoversArbitraryNamedEngineFile(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	file, err := os.OpenFile(
		filepath.Join(dir, "opaque-engine.data"),
		os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	extra, err := Create(file, txnTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = extra.Close() })
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if group != nil {
		_ = group.Close()
		t.Fatal("activation accepted an arbitrary-named format-0 engine")
	}
	if !errors.Is(err, ErrTxnParticipant) {
		t.Fatalf("activation with arbitrary-named engine = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, checkpointGroupFilename)); !os.IsNotExist(err) {
		t.Fatalf("arbitrary-engine refusal left certificate: %v", err)
	}
}

func TestCheckpointGroupActivationLeaseClosesDirectoryScanRace(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	scanned := make(chan struct{})
	resumeActivation := make(chan struct{})
	previousScanHook := checkpointGroupAfterDirectoryMembershipHook
	checkpointGroupAfterDirectoryMembershipHook = func() {
		close(scanned)
		<-resumeActivation
	}
	t.Cleanup(func() { checkpointGroupAfterDirectoryMembershipHook = previousScanHook })
	type groupResult struct {
		group *CheckpointGroup
		err   error
	}
	activation := make(chan groupResult, 1)
	go func() {
		group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
		activation <- groupResult{group: group, err: err}
	}()
	<-scanned

	extraPath := filepath.Join(dir, "late.vjc")
	extraFile, err := os.OpenFile(
		extraPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	createPrechecked := make(chan struct{})
	previousCreateHook := checkpointGroupGenericCreateBeforeNamespaceHook
	checkpointGroupGenericCreateBeforeNamespaceHook = func() { close(createPrechecked) }
	t.Cleanup(func() { checkpointGroupGenericCreateBeforeNamespaceHook = previousCreateHook })
	type createResult struct {
		collection *Collection
		err        error
	}
	created := make(chan createResult, 1)
	go func() {
		collection, createErr := Create(extraFile, txnTestOptions())
		created <- createResult{collection: collection, err: createErr}
	}()
	<-createPrechecked
	close(resumeActivation)
	activated := <-activation
	if activated.err != nil || activated.group == nil {
		t.Fatalf("activation after directory scan = %v", activated.err)
	}
	late := <-created
	if late.collection != nil {
		_ = late.collection.Close()
		t.Fatal("late Create retained a generic nonmember handle")
	}
	if !errors.Is(late.err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("late Create after activation = %v", late.err)
	}
	if info, err := extraFile.Stat(); err != nil || info.Size() != 0 {
		t.Fatalf("late Create changed extra file: size/info %v, error %v", info, err)
	}
	if err := extraFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(extraPath); err != nil {
		t.Fatal(err)
	}
	if err := activated.group.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupActivationPostRenameFailureSettlesExactCertificate(t *testing.T) {
	db, err := OpenDatabase(
		t.TempDir(), DatabaseOptions{Options: txnTestOptions()},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	names := []string{"system", "user"}
	members := make([]NamedCollection, len(names))
	basenames := make([]string, len(names))
	for i, name := range names {
		collection, createErr := db.CreateCollection(name, txnTestOptions())
		if createErr != nil {
			t.Fatal(createErr)
		}
		members[i] = NamedCollection{Name: name, Collection: collection}
		basenames[i] = filepath.Base(collection.file.Name())
	}
	log, err := ensureDatabaseTxnLog(db)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected checkpoint certificate directory sync")
	previousHook := checkpointGroupFaultHook
	fired := false
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if !fired && point == checkpointGroupAfterCertificateRename {
			fired = true
			return injected
		}
		return nil
	}
	group, activationErr := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{},
	)
	checkpointGroupFaultHook = previousHook
	if !fired || group == nil || activationErr != nil {
		t.Fatalf("post-rename activation = group %v, error %v", group, activationErr)
	}
	t.Cleanup(func() { _ = group.Close() })
	if _, err := os.Stat(filepath.Join(db.Dir(), checkpointGroupFilename)); err != nil {
		t.Fatalf("published certificate is not visible: %v", err)
	}
	for _, member := range members {
		if _, err := member.Collection.Put(
			[]byte("forbidden"), []byte(`{"n":1}`),
		); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q Put after settled activation = %v", member.Name, err)
		}
		if err := member.Collection.Flush(); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q Flush after settled activation = %v", member.Name, err)
		}
	}
	if err := UpdateCollections(
		log, members, defaultTxnLimits(), func(*DatabaseBatch) error { return nil },
	); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("UpdateCollections after settled activation = %v", err)
	}
	if _, err := db.CreateCollection("extra", txnTestOptions()); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("CreateCollection after settled activation = %v", err)
	}
	directPath := filepath.Join(db.Dir(), "direct.vjc")
	direct, err := os.OpenFile(directPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, createErr := Create(direct, txnTestOptions())
	closeErr := direct.Close()
	removeErr := os.Remove(directPath)
	if !errors.Is(createErr, ErrCheckpointGroupRecoveryRequired) || closeErr != nil || removeErr != nil {
		t.Fatalf("direct Create after uncertain activation = create %v close %v remove %v",
			createErr, closeErr, removeErr)
	}
	if err := log.EnsureMinted(); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("EnsureMinted after uncertain activation = %v", err)
	}

	crashPresent := copyCheckpointGroupDirectory(t, db.Dir())
	crashAbsent := copyCheckpointGroupDirectory(t, crashPresent)
	if err := os.Remove(filepath.Join(crashAbsent, checkpointGroupFilename)); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatalf("settled group close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("resource close after settled activation: %v", err)
	}

	openRequests := func(dir string) ([]TransactionCollectionOpen, []*os.File) {
		requests := make([]TransactionCollectionOpen, len(basenames))
		files := make([]*os.File, len(basenames))
		for i, base := range basenames {
			file, openErr := os.OpenFile(filepath.Join(dir, base), os.O_RDWR, 0)
			if openErr != nil {
				t.Fatal(openErr)
			}
			files[i] = file
			requests[i] = TransactionCollectionOpen{File: file, Options: txnTestOptions()}
		}
		return requests, files
	}
	requests, files := openRequests(crashPresent)
	collections, reopenedLog, reopenedGroup, err := OpenCollectionsWithCheckpointGroup(
		crashPresent, TxnLogOptions{}, requests, names, CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatalf("present-certificate crash reopen: %v", err)
	}
	if reopenedGroup.AppliedIndex() != 0 || reopenedGroup.CheckpointAppliedIndex() != 0 {
		t.Fatalf("present-certificate crash cut = %d/%d",
			reopenedGroup.AppliedIndex(), reopenedGroup.CheckpointAppliedIndex())
	}
	presentFiles := files
	t.Cleanup(func() {
		_ = reopenedGroup.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = reopenedLog.Close()
		for _, file := range presentFiles {
			_ = file.Close()
		}
	})

	requests, files = openRequests(crashAbsent)
	_, _, _, err = OpenCollectionsWithCheckpointGroup(
		crashAbsent, TxnLogOptions{}, requests, names, CheckpointGroupOptions{},
	)
	for _, file := range files {
		_ = file.Close()
	}
	if !errors.Is(err, ErrCheckpointGroupMissing) {
		t.Fatalf("absent-certificate crash special reopen = %v", err)
	}
	requests, files = openRequests(crashAbsent)
	generic, genericLog, err := OpenCollectionsWithTransactions(
		crashAbsent, TxnLogOptions{}, requests,
	)
	if err != nil {
		t.Fatalf("absent-certificate generic fallback: %v", err)
	}
	genericMembers := make([]NamedCollection, len(generic))
	for i := range generic {
		genericMembers[i] = NamedCollection{Name: names[i], Collection: generic[i]}
	}
	reactivated, err := NewCheckpointGroup(
		genericLog, genericMembers, CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatalf("absent-certificate reactivation: %v", err)
	}
	t.Cleanup(func() {
		_ = reactivated.Close()
		for _, collection := range generic {
			_ = collection.Close()
		}
		_ = genericLog.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
}

func TestCheckpointGroupActivationCorruptFixedEntryFailsClosed(t *testing.T) {
	dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
	if err := os.WriteFile(
		filepath.Join(dir, checkpointGroupFilename), []byte("torn"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if group != nil || !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("activation beside corrupt fixed entry = group %v, error %v", group, err)
	}
	for _, member := range members {
		if _, err := member.Collection.Put(
			[]byte("forbidden"), []byte(`{"n":1}`),
		); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q mutation beside corrupt fixed entry = %v", member.Name, err)
		}
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupCloseReturnsStickyCertificateCloseError(t *testing.T) {
	_, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	injected := errors.New("injected certificate close")
	previousHook := checkpointGroupCertificateCloseHook
	checkpointGroupCertificateCloseHook = func(file *os.File) error {
		return errors.Join(injected, file.Close())
	}
	t.Cleanup(func() { checkpointGroupCertificateCloseHook = previousHook })
	first := group.Close()
	second := group.Close()
	if !errors.Is(first, injected) || !errors.Is(second, injected) || first.Error() != second.Error() {
		t.Fatalf("Close results = first %v, second %v", first, second)
	}
	stats := group.Stats()
	if stats.BarrierSyncs != uint64(len(members)+1) || stats.MarkerSyncs != 0 {
		t.Fatalf("close-error checkpoint stats = %+v", stats)
	}
	for _, member := range members {
		if _, err := member.Collection.Put(
			[]byte("forbidden"), []byte(`{"n":2}`),
		); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("post-close-error Put = %v", err)
		}
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func copyCheckpointGroupDirectory(t testing.TB, source string) string {
	t.Helper()
	destination := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		input, err := os.Open(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		output, err := os.OpenFile(
			filepath.Join(destination, entry.Name()),
			os.O_CREATE|os.O_EXCL|os.O_RDWR, info.Mode().Perm(),
		)
		if err == nil {
			_, err = io.Copy(output, input)
		}
		err = errors.Join(err, input.Close())
		if output != nil {
			err = errors.Join(err, output.Sync(), output.Close())
		}
		if err != nil {
			t.Fatalf("copy %q: %v", entry.Name(), err)
		}
	}
	return destination
}

func checkpointGroupDirectoryBytes(t testing.TB, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		result[entry.Name()], err = os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func requireCheckpointGroupDirectoryBytes(
	t testing.TB, dir string, want map[string][]byte,
) {
	t.Helper()
	got := checkpointGroupDirectoryBytes(t, dir)
	if len(got) != len(want) {
		t.Fatalf("directory entries changed: got %d, want %d", len(got), len(want))
	}
	for name, wantBytes := range want {
		gotBytes, ok := got[name]
		if !ok {
			t.Fatalf("directory entry %q disappeared", name)
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Fatalf("directory entry %q changed during rejected generic recovery", name)
		}
	}
}

func openCheckpointGroupTestCopy(
	t *testing.T, dir string,
) ([]*Collection, *TxnLog, *CheckpointGroup) {
	return openCheckpointGroupTestCopyNames(t, dir, "system", "user")
}

func openCheckpointGroupTestCopyNames(
	t *testing.T, dir string, names ...string,
) ([]*Collection, *TxnLog, *CheckpointGroup) {
	t.Helper()
	requests := make([]TransactionCollectionOpen, len(names))
	files := make([]*os.File, len(names))
	for i, name := range names {
		file, err := os.OpenFile(filepath.Join(dir, name+".vjc"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = file
		requests[i] = TransactionCollectionOpen{File: file, Options: txnTestOptions()}
	}
	collections, log, group, err := OpenCollectionsWithCheckpointGroup(
		dir, TxnLogOptions{}, requests, names,
		CheckpointGroupOptions{CheckpointEvery: 8},
	)
	if err != nil {
		for _, file := range files {
			_ = file.Close()
		}
		t.Fatalf("OpenCollectionsWithCheckpointGroup: %v", err)
	}
	t.Cleanup(func() {
		_ = group.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
	return collections, log, group
}

func closeCheckpointGroupTestHandles(
	t testing.TB,
	collections []*Collection,
	log *TxnLog,
	group *CheckpointGroup,
) {
	t.Helper()
	var err error
	if group != nil {
		err = errors.Join(err, group.Close())
	}
	for _, collection := range collections {
		if collection != nil {
			err = errors.Join(err, collection.Close())
		}
	}
	if log != nil {
		err = errors.Join(err, log.Close())
	}
	if err != nil {
		t.Fatalf("close checkpoint-group test handles: %v", err)
	}
}

func checkpointGroupTestOpenRequests(
	t *testing.T, dir string,
) ([]TransactionCollectionOpen, []*os.File) {
	t.Helper()
	names := []string{"system", "user"}
	requests := make([]TransactionCollectionOpen, len(names))
	files := make([]*os.File, len(names))
	for i, name := range names {
		file, err := os.OpenFile(filepath.Join(dir, name+".vjc"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = file
		requests[i] = TransactionCollectionOpen{File: file, Options: txnTestOptions()}
	}
	return requests, files
}

func clearCheckpointGroupTestPoison(group *CheckpointGroup) {
	if group == nil {
		return
	}
	group.mu.Lock()
	group.poison = nil
	if group.log != nil {
		group.log.poison = nil
	}
	group.mu.Unlock()
}

func TestCheckpointGroupRecoveryUsesCertificateAndAbortsSuffix(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, members, "suffix-2")
	checkpointGroupPut(t, group, 3, members, "suffix-3")

	crashImage := copyCheckpointGroupDirectory(t, dir)
	collections, recoveredLog, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf("reopened cuts = applied %d checkpoint %d",
			reopened.AppliedIndex(), reopened.CheckpointAppliedIndex())
	}
	for _, collection := range collections {
		if _, ok := collectionDoc(t, collection, "certified"); !ok {
			t.Fatal("certified prefix missing")
		}
		for _, key := range []string{"suffix-2", "suffix-3"} {
			if _, ok := collectionDoc(t, collection, key); ok {
				t.Fatalf("uncertified suffix %q survived recovery", key)
			}
		}
	}
	if base := recoveredLog.marker.Header().BaseSequence; base != reopened.txnBase || base != 1 {
		t.Fatalf("aborted-suffix recovery anchor = marker %d certificate %d", base, reopened.txnBase)
	}
	closeCheckpointGroupTestHandles(t, collections, recoveredLog, reopened)
	secondCollections, secondLog, second := openCheckpointGroupTestCopy(t, crashImage)
	if base := secondLog.marker.Header().BaseSequence; base != second.txnBase || base != 1 {
		t.Fatalf("second aborted-suffix recovery anchor = marker %d certificate %d", base, second.txnBase)
	}
	for _, collection := range secondCollections {
		for _, key := range []string{"suffix-2", "suffix-3"} {
			if _, ok := collectionDoc(t, collection, key); ok {
				t.Fatalf("uncertified suffix %q survived second recovery", key)
			}
		}
	}
	closeCheckpointGroupTestHandles(t, secondCollections, secondLog, second)
}

func TestCheckpointGroupAnchoredRecoveryRecycleCrashCuts(t *testing.T) {
	for _, test := range []struct {
		name        string
		markerPlan  storeio.TxnMarkerFaultPlan
		faultPoint  checkpointGroupFaultPoint
		markerFault bool
	}{
		{
			name: "torn-marker-header",
			markerPlan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultTornRecycle,
			},
			markerFault: true,
		},
		{
			name: "marker-sync",
			markerPlan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultSyncError,
			},
			markerFault: true,
		},
		{name: "after-marker-sync", faultPoint: checkpointGroupAfterMarkerSync},
		{name: "certificate-write", faultPoint: checkpointGroupAfterCertificateWrite},
		{name: "certificate-sync", faultPoint: checkpointGroupAfterCertificateSync},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "certified")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkpointGroupPut(t, group, 2, members[:1], "aborted")
			damaged := copyCheckpointGroupDirectory(t, dir)
			fault := errors.New("stop anchored marker recovery")
			previousRecycleHook := checkpointGroupBeforeMarkerRecoveryRecycleHook
			previousFaultHook := checkpointGroupFaultHook
			t.Cleanup(func() {
				checkpointGroupBeforeMarkerRecoveryRecycleHook = previousRecycleHook
				checkpointGroupFaultHook = previousFaultHook
			})
			var markerFault *storeio.FaultTxnMarker
			checkpointGroupBeforeMarkerRecoveryRecycleHook = func(marker *storeio.TxnMarker) {
				if test.markerFault {
					markerFault = storeio.NewFaultTxnMarker(marker)
					markerFault.Program(test.markerPlan)
				}
			}
			checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
				if test.faultPoint != 0 && point == test.faultPoint {
					return fault
				}
				return nil
			}
			requests, files := checkpointGroupTestOpenRequests(t, damaged)
			collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
				damaged,
				TxnLogOptions{},
				requests,
				[]string{"system", "user"},
				CheckpointGroupOptions{CheckpointEvery: 8},
			)
			checkpointGroupBeforeMarkerRecoveryRecycleHook = previousRecycleHook
			checkpointGroupFaultHook = previousFaultHook
			for _, file := range files {
				_ = file.Close()
			}
			if collections != nil || log != nil || recovered != nil || err == nil {
				t.Fatalf(
					"anchored recovery cut = collections %v log %v group %v err %v",
					collections, log, recovered, err,
				)
			}
			if test.markerFault && (markerFault == nil || !markerFault.Faulted()) {
				t.Fatal("anchored marker fault did not fire")
			}
			if !test.markerFault && !errors.Is(err, fault) {
				t.Fatalf("anchored recovery hook error = %v", err)
			}

			crashImage := copyCheckpointGroupDirectory(t, damaged)
			for attempt := 0; attempt < 2; attempt++ {
				opened, reopenedLog, reopened := openCheckpointGroupTestCopy(t, crashImage)
				if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
					t.Fatalf(
						"anchored retry %d cut = %d/%d",
						attempt, reopened.AppliedIndex(), reopened.CheckpointAppliedIndex(),
					)
				}
				if base := reopenedLog.marker.Header().BaseSequence; base != 1 || base != reopened.txnBase {
					t.Fatalf(
						"anchored retry %d base = marker %d certificate %d",
						attempt, base, reopened.txnBase,
					)
				}
				for _, collection := range opened {
					if _, found := collectionDoc(t, collection, "aborted"); found {
						t.Fatalf("anchored retry %d retained aborted row", attempt)
					}
				}
				closeCheckpointGroupTestHandles(t, opened, reopenedLog, reopened)
			}
		})
	}
}

func TestCheckpointGroupCutZeroRecoveryInvalidatesAbortedMarkerSuffix(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members[:1], "aborted-cut-zero")
	damaged := copyCheckpointGroupDirectory(t, dir)

	firstCollections, firstLog, first := openCheckpointGroupTestCopy(t, damaged)
	if first.AppliedIndex() != 0 || first.CheckpointAppliedIndex() != 0 ||
		firstLog.marker.Header().BaseSequence != 0 ||
		firstLog.marker.Cursor() != 0 || firstLog.marker.NextSequence() != 1 {
		t.Fatalf(
			"first cut-zero recovery = applied %d checkpoint %d base %d cursor %d next %d",
			first.AppliedIndex(), first.CheckpointAppliedIndex(),
			firstLog.marker.Header().BaseSequence, firstLog.marker.Cursor(),
			firstLog.marker.NextSequence(),
		)
	}
	if got := first.Stats().MarkerSyncs; got != 2 {
		t.Fatalf("equal-base recovery marker syncs = %d, want 2", got)
	}
	if _, found := collectionDoc(t, firstCollections[0], "aborted-cut-zero"); found {
		t.Fatal("first cut-zero recovery retained aborted row")
	}
	closeCheckpointGroupTestHandles(t, firstCollections, firstLog, first)

	secondCollections, secondLog, second := openCheckpointGroupTestCopy(t, damaged)
	if secondLog.marker.Cursor() != 0 || secondLog.marker.NextSequence() != 1 {
		t.Fatalf(
			"second cut-zero recovery marker = cursor %d next %d",
			secondLog.marker.Cursor(), secondLog.marker.NextSequence(),
		)
	}
	if got := second.Stats().MarkerSyncs; got != 1 {
		t.Fatalf("clean equal-base recovery marker syncs = %d, want 1", got)
	}
	if _, found := collectionDoc(t, secondCollections[0], "aborted-cut-zero"); found {
		t.Fatal("second cut-zero recovery resurrected aborted row")
	}
	secondMembers := []NamedCollection{
		{Name: "system", Collection: secondCollections[0]},
		{Name: "user", Collection: secondCollections[1]},
	}
	checkpointGroupPut(t, second, 1, secondMembers[:1], "after-cut-zero-recovery")
	if err := second.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after cut-zero recovery: %v", err)
	}
	closeCheckpointGroupTestHandles(t, secondCollections, secondLog, second)

	thirdCollections, thirdLog, third := openCheckpointGroupTestCopy(t, damaged)
	if _, found := collectionDoc(t, thirdCollections[0], "after-cut-zero-recovery"); !found {
		t.Fatal("post-cut-zero recovery update was not durable")
	}
	if _, found := collectionDoc(t, thirdCollections[0], "aborted-cut-zero"); found {
		t.Fatal("post-cut-zero recovery reopen resurrected aborted row")
	}
	closeCheckpointGroupTestHandles(t, thirdCollections, thirdLog, third)
}

func TestCheckpointGroupCutZeroRecoveryInvalidationCrashCuts(t *testing.T) {
	for _, test := range []struct {
		name string
		plan storeio.TxnMarkerFaultPlan
	}{
		{
			name: "invalidation-write",
			plan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultRecoveryInvalidateError,
			},
		},
		{
			name: "torn-invalidation",
			plan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultTornRecoveryInvalidate,
			},
		},
		{
			name: "invalidation-sync",
			plan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 0,
			},
		},
		{
			name: "torn-successor-header",
			plan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultTornRecycle,
			},
		},
		{
			name: "successor-header-sync",
			plan: storeio.TxnMarkerFaultPlan{
				Phase: storeio.TxnMarkerFaultSyncError, SyncIndex: 1,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members[:1], "aborted-cut-zero")
			damaged := copyCheckpointGroupDirectory(t, dir)
			previousHook := checkpointGroupBeforeMarkerRecoveryRecycleHook
			t.Cleanup(func() {
				checkpointGroupBeforeMarkerRecoveryRecycleHook = previousHook
			})
			var markerFault *storeio.FaultTxnMarker
			checkpointGroupBeforeMarkerRecoveryRecycleHook = func(marker *storeio.TxnMarker) {
				markerFault = storeio.NewFaultTxnMarker(marker)
				markerFault.Program(test.plan)
			}
			requests, files := checkpointGroupTestOpenRequests(t, damaged)
			collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
				damaged,
				TxnLogOptions{},
				requests,
				[]string{"system", "user"},
				CheckpointGroupOptions{CheckpointEvery: 8},
			)
			checkpointGroupBeforeMarkerRecoveryRecycleHook = previousHook
			for _, file := range files {
				_ = file.Close()
			}
			if collections != nil || log != nil || recovered != nil || err == nil ||
				markerFault == nil || !markerFault.Faulted() {
				t.Fatalf(
					"cut-zero invalidation fault = collections %v log %v group %v faulted %v err %v",
					collections, log, recovered,
					markerFault != nil && markerFault.Faulted(), err,
				)
			}

			crashImage := copyCheckpointGroupDirectory(t, damaged)
			firstCollections, firstLog, first := openCheckpointGroupTestCopy(t, crashImage)
			if first.AppliedIndex() != 0 || first.CheckpointAppliedIndex() != 0 ||
				firstLog.marker.Cursor() != 0 || firstLog.marker.NextSequence() != 1 {
				t.Fatalf(
					"cut-zero retry = applied %d checkpoint %d cursor %d next %d",
					first.AppliedIndex(), first.CheckpointAppliedIndex(),
					firstLog.marker.Cursor(), firstLog.marker.NextSequence(),
				)
			}
			if _, found := collectionDoc(t, firstCollections[0], "aborted-cut-zero"); found {
				t.Fatal("cut-zero retry retained aborted row")
			}
			closeCheckpointGroupTestHandles(t, firstCollections, firstLog, first)

			secondCollections, secondLog, second := openCheckpointGroupTestCopy(t, crashImage)
			if _, found := collectionDoc(t, secondCollections[0], "aborted-cut-zero"); found {
				t.Fatal("second cut-zero retry resurrected aborted row")
			}
			secondMembers := []NamedCollection{
				{Name: "system", Collection: secondCollections[0]},
				{Name: "user", Collection: secondCollections[1]},
			}
			checkpointGroupPut(t, second, 1, secondMembers[:1], "after-cut-zero-retry")
			if err := second.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after cut-zero retry: %v", err)
			}
			closeCheckpointGroupTestHandles(t, secondCollections, secondLog, second)

			thirdCollections, thirdLog, third := openCheckpointGroupTestCopy(t, crashImage)
			if _, found := collectionDoc(t, thirdCollections[0], "after-cut-zero-retry"); !found {
				t.Fatal("post-crash cut-zero update was not durable")
			}
			closeCheckpointGroupTestHandles(t, thirdCollections, thirdLog, third)
		})
	}
}

func TestCheckpointGroupLiveMarkerSyncFaultPoisonsSplitOwner(t *testing.T) {
	dir, members, log, group := checkpointGroupTestStoreWithMarkerCapacity(
		t, 8, uint64(storeio.TxnMarkerMinSectorSize),
	)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if log.marker.Cursor() != log.marker.Header().Capacity {
		t.Fatalf(
			"marker-sync fixture cursor = %d/%d",
			log.marker.Cursor(), log.marker.Header().Capacity,
		)
	}
	beforeCertificate, err := os.ReadFile(filepath.Join(dir, checkpointGroupFilename))
	if err != nil {
		t.Fatal(err)
	}
	beforeHeader := log.marker.Header()
	beforeStats := group.Stats()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	group.mu.Unlock()

	fault := errors.New("stop after live marker sync")
	previousHook := checkpointGroupFaultHook
	t.Cleanup(func() { checkpointGroupFaultHook = previousHook })
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterMarkerSync {
			return fault
		}
		return nil
	}
	called := false
	err = group.Update(2, members[:1], defaultTxnLimits(), func(batch *DatabaseBatch) error {
		called = true
		write, collectionErr := batch.Collection("system")
		if collectionErr != nil {
			return collectionErr
		}
		return write.Put([]byte("must-not-publish"), []byte(`{"n":2}`))
	})
	checkpointGroupFaultHook = previousHook
	if !called || !errors.Is(err, fault) || !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("live marker-sync fault = called %v, err %v", called, err)
	}
	afterHeader := log.marker.Header()
	if afterHeader.MarkerID != beforeHeader.MarkerID ||
		afterHeader.Epoch != beforeHeader.Epoch+1 ||
		afterHeader.RecycleCount != beforeHeader.RecycleCount+1 ||
		afterHeader.BaseSequence != beforeOwner.txnHighWater || log.marker.Cursor() != 0 {
		t.Fatalf("live marker-sync successor = before %+v after %+v cursor %d",
			beforeHeader, afterHeader, log.marker.Cursor())
	}
	afterCertificate, readErr := os.ReadFile(filepath.Join(dir, checkpointGroupFilename))
	if readErr != nil || !bytes.Equal(afterCertificate, beforeCertificate) {
		t.Fatalf("live marker-sync fault changed certificate: %v", readErr)
	}
	afterStats := group.Stats()
	if afterStats.MarkerSyncs != beforeStats.MarkerSyncs+1 ||
		afterStats.CertificateSyncs != beforeStats.CertificateSyncs {
		t.Fatalf("live marker-sync stats = before %+v after %+v", beforeStats, afterStats)
	}
	group.mu.Lock()
	afterOwner := group.certificateLocked()
	ownerPoison := group.poison
	logPoison := group.log.poison
	group.mu.Unlock()
	if afterOwner.sequence != beforeOwner.sequence ||
		!equalCheckpointGroupCertificateBody(afterOwner, beforeOwner) ||
		!errors.Is(ownerPoison, ErrCommitOutcomeUnknown) ||
		!errors.Is(logPoison, ErrCommitOutcomeUnknown) {
		t.Fatalf(
			"live marker-sync owner = before %+v after %+v poison %v/%v",
			beforeOwner, afterOwner, ownerPoison, logPoison,
		)
	}
	if _, found := collectionDoc(t, members[0].Collection, "must-not-publish"); found {
		t.Fatal("live marker-sync fault published the requested update")
	}
	if err := group.Update(2, members[:1], defaultTxnLimits(), func(*DatabaseBatch) error {
		return nil
	}); !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("poisoned live marker-sync owner accepted another update: %v", err)
	}

	crashImage := copyCheckpointGroupDirectory(t, dir)
	opened, reopenedLog, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 ||
		reopenedLog.marker.Header().BaseSequence != 1 {
		t.Fatalf(
			"live marker-sync recovery = applied %d checkpoint %d base %d",
			reopened.AppliedIndex(), reopened.CheckpointAppliedIndex(),
			reopenedLog.marker.Header().BaseSequence,
		)
	}
	for _, collection := range opened {
		if _, found := collectionDoc(t, collection, "certified"); !found {
			t.Fatal("live marker-sync recovery lost certified row")
		}
		if _, found := collectionDoc(t, collection, "must-not-publish"); found {
			t.Fatal("live marker-sync recovery retained refused row")
		}
	}
}

func TestCheckpointGroupCertificateDoesNotDependOnMarkerDecisionSync(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")

	fault := errors.New("stop after certificate sync")
	previousHook := checkpointGroupFaultHook
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterCertificateSync {
			return fault
		}
		return nil
	}
	err := group.Checkpoint()
	checkpointGroupFaultHook = previousHook
	if !errors.Is(err, fault) {
		t.Fatalf("Checkpoint fault = %v", err)
	}

	crashImage := copyCheckpointGroupDirectory(t, dir)
	clearCheckpointGroupTestPoison(group)
	markerPath := filepath.Join(crashImage, txnMarkerFilename)
	marker, err := os.OpenFile(markerPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := marker.Stat()
	if err == nil {
		region := make([]byte, info.Size()-2*storeio.TxnMarkerHeaderSize)
		_, err = marker.WriteAt(region, 2*storeio.TxnMarkerHeaderSize)
	}
	err = errors.Join(err, marker.Sync(), marker.Close())
	if err != nil {
		t.Fatalf("erase marker implementation log: %v", err)
	}

	collections, recoveredLog, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf("checkpoint cut = %d, want 1", reopened.CheckpointAppliedIndex())
	}
	for _, collection := range collections {
		if _, ok := collectionDoc(t, collection, "certified"); !ok {
			t.Fatal("certificate-authorized journal prepare was not replayed")
		}
	}
	if base := recoveredLog.marker.Header().BaseSequence; base != reopened.txnBase || base != 1 {
		t.Fatalf("erased-prefix recovery anchor = marker %d certificate %d", base, reopened.txnBase)
	}
	closeCheckpointGroupTestHandles(t, collections, recoveredLog, reopened)
	secondCollections, secondLog, second := openCheckpointGroupTestCopy(t, crashImage)
	if base := secondLog.marker.Header().BaseSequence; base != second.txnBase || base != 1 {
		t.Fatalf("second erased-prefix recovery anchor = marker %d certificate %d", base, second.txnBase)
	}
	for _, collection := range secondCollections {
		if _, ok := collectionDoc(t, collection, "certified"); !ok {
			t.Fatal("certificate-authorized row missing after second recovery")
		}
	}
	closeCheckpointGroupTestHandles(t, secondCollections, secondLog, second)
}

func TestCheckpointGroupRejectsNonContiguousSurvivingMarkerPrefix(t *testing.T) {
	_, named, log, _ := newCheckpointGroupTestStore(t, 8)
	members, err := checkpointGroupMembers(named)
	if err != nil {
		t.Fatal(err)
	}
	log.commitMu.Lock()
	_, err = log.marker.AppendDecision(2, []storeio.TxnParticipant{{
		StoreID: members[0].storeID, JournalID: members[0].journalID,
		PreparedGeneration: 1,
	}})
	var decisions *storeio.TxnDecisions
	if err == nil {
		decisions, err = rescanTxnLogMarker(log)
	}
	log.commitMu.Unlock()
	if err != nil {
		t.Fatalf("build marker prefix: %v", err)
	}
	if err := validateCheckpointMarkerRecords(decisions, 0, members); !errors.Is(err, ErrCheckpointGroupCorrupt) {
		t.Fatalf("non-contiguous marker prefix = %v", err)
	}
}

func TestCheckpointGroupRecoveryRepairsMissingOrTornMarker(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func(*testing.T, string)
	}{
		{
			name: "missing",
			damage: func(t *testing.T, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "torn-headers",
			damage: func(t *testing.T, path string) {
				file, err := os.OpenFile(path, os.O_RDWR, 0)
				if err == nil {
					_, err = file.WriteAt(
						make([]byte, 2*storeio.TxnMarkerHeaderSize), 0,
					)
				}
				if file != nil {
					err = errors.Join(err, file.Sync(), file.Close())
				}
				if err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "certified")
			group.mu.Lock()
			wantMarkerID := group.markerID
			wantMarkerEpoch := group.markerEpoch + 1
			wantBaseSequence := group.txn
			group.mu.Unlock()
			fault := errors.New("stop after certificate sync")
			previousHook := checkpointGroupFaultHook
			checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
				if point == checkpointGroupAfterCertificateSync {
					return fault
				}
				return nil
			}
			err := group.Checkpoint()
			checkpointGroupFaultHook = previousHook
			if !errors.Is(err, fault) {
				t.Fatalf("Checkpoint fault = %v", err)
			}

			crashImage := copyCheckpointGroupDirectory(t, dir)
			clearCheckpointGroupTestPoison(group)
			test.damage(t, filepath.Join(crashImage, txnMarkerFilename))
			collections, recoveredLog, reopened := openCheckpointGroupTestCopy(t, crashImage)
			if reopened.CheckpointAppliedIndex() != 1 {
				t.Fatalf("repaired checkpoint cut = %d", reopened.CheckpointAppliedIndex())
			}
			header := recoveredLog.marker.Header()
			if header.MarkerID != wantMarkerID || header.Epoch != wantMarkerEpoch ||
				header.BaseSequence != wantBaseSequence {
				t.Fatalf("anchored replacement marker = %+v, want id %x epoch %d base %d",
					header, wantMarkerID, wantMarkerEpoch, wantBaseSequence)
			}
			for _, collection := range collections {
				if _, ok := collectionDoc(t, collection, "certified"); !ok {
					t.Fatal("certificate-authorized prepare was not replayed")
				}
			}
			if info, statErr := os.Stat(filepath.Join(crashImage, txnMarkerFilename)); statErr != nil || !info.Mode().IsRegular() {
				t.Fatalf("replacement marker = %v, %v", info, statErr)
			}
			closeCheckpointGroupTestHandles(t, collections, recoveredLog, reopened)

			secondCollections, _, second := openCheckpointGroupTestCopy(t, crashImage)
			secondMembers := make([]NamedCollection, len(secondCollections))
			for index, collection := range secondCollections {
				secondMembers[index] = NamedCollection{
					Name: []string{"system", "user"}[index], Collection: collection,
				}
			}
			checkpointGroupPut(t, second, 2, secondMembers, "after-repair")
			if err := second.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after second reopen: %v", err)
			}
		})
	}
}

func corruptNewestCheckpointGroupCertificateSlot(t testing.TB, dir string) {
	t.Helper()
	path := filepath.Join(dir, checkpointGroupFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	newestSlot := -1
	var newestSequence uint64
	for slot := 0; slot < checkpointGroupSlots; slot++ {
		start := slot * checkpointGroupSlotBytes
		certificate, decodeErr := decodeCheckpointGroupCertificate(
			data[start : start+checkpointGroupSlotBytes],
		)
		if decodeErr == nil && (newestSlot < 0 || certificate.sequence > newestSequence) {
			newestSlot = slot
			newestSequence = certificate.sequence
		}
	}
	if newestSlot < 0 {
		t.Fatal("no valid certificate slot to tear")
	}
	data[newestSlot*checkpointGroupSlotBytes] ^= 0x80
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupMarkerRepairCrashCutsReopenTwiceAndAdvance(t *testing.T) {
	for _, test := range []struct {
		name                      string
		capturePoint              checkpointGroupFaultPoint
		captureAfterMarker        bool
		tearNewCertificate        bool
		selectedRepairCertificate bool
	}{
		{
			name: "anchored-marker-before-certificate", captureAfterMarker: true,
		},
		{
			name:                      "full-certificate-write-before-sync",
			capturePoint:              checkpointGroupAfterCertificateWrite,
			selectedRepairCertificate: true,
		},
		{
			name:               "torn-certificate-write-before-sync",
			capturePoint:       checkpointGroupAfterCertificateWrite,
			tearNewCertificate: true,
		},
		{
			name:                      "after-certificate-sync",
			capturePoint:              checkpointGroupAfterCertificateSync,
			selectedRepairCertificate: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "certified")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			group.mu.Lock()
			wantMarkerID := group.markerID
			group.mu.Unlock()

			damaged := copyCheckpointGroupDirectory(t, dir)
			if err := os.Remove(filepath.Join(damaged, txnMarkerFilename)); err != nil {
				t.Fatal(err)
			}
			var crashImage string
			fault := errors.New("stop marker repair recovery")
			previousMintHook := databaseTxnAfterMintHook
			previousCheckpointHook := checkpointGroupFaultHook
			defer func() {
				databaseTxnAfterMintHook = previousMintHook
				checkpointGroupFaultHook = previousCheckpointHook
			}()
			if test.captureAfterMarker {
				databaseTxnAfterMintHook = func(*TxnLog) {
					if crashImage == "" {
						crashImage = copyCheckpointGroupDirectory(t, damaged)
					}
				}
			}
			checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
				if test.captureAfterMarker {
					if point == checkpointGroupAfterCertificateWrite {
						return fault
					}
					return nil
				}
				if point == test.capturePoint {
					crashImage = copyCheckpointGroupDirectory(t, damaged)
					return fault
				}
				return nil
			}

			requests, files := checkpointGroupTestOpenRequests(t, damaged)
			collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
				damaged, TxnLogOptions{}, requests, []string{"system", "user"},
				CheckpointGroupOptions{CheckpointEvery: 8},
			)
			databaseTxnAfterMintHook = previousMintHook
			checkpointGroupFaultHook = previousCheckpointHook
			if recovered != nil || log != nil || collections != nil || !errors.Is(err, fault) {
				t.Fatalf("faulted marker repair = collections %v log %v group %v err %v",
					collections, log, recovered, err)
			}
			for _, file := range files {
				_ = file.Close()
			}
			if crashImage == "" {
				t.Fatal("marker repair crash image was not captured")
			}
			if test.tearNewCertificate {
				corruptNewestCheckpointGroupCertificateSlot(t, crashImage)
			}
			anchoredMarker, _, err := storeio.OpenTxnMarker(
				filepath.Join(crashImage, txnMarkerFilename), storeio.TxnMarkerOptions{},
			)
			if err != nil {
				t.Fatalf("open anchored crash marker: %v", err)
			}
			anchoredHeader := anchoredMarker.Header()
			if err := anchoredMarker.Close(); err != nil {
				t.Fatal(err)
			}
			if anchoredHeader.MarkerID != wantMarkerID ||
				anchoredHeader.BaseSequence != 1 || anchoredHeader.RecycleCount != 1 {
				t.Fatalf("anchored crash marker = %+v", anchoredHeader)
			}
			wantFirstEpoch := anchoredHeader.Epoch
			wantFirstRecycleCount := anchoredHeader.RecycleCount
			if test.selectedRepairCertificate {
				wantFirstEpoch++
				wantFirstRecycleCount++
			}

			firstCollections, firstLog, first := openCheckpointGroupTestCopy(t, crashImage)
			if first.AppliedIndex() != 1 || first.CheckpointAppliedIndex() != 1 {
				t.Fatalf("first crash reopen cut = %d/%d",
					first.AppliedIndex(), first.CheckpointAppliedIndex())
			}
			firstHeader := firstLog.marker.Header()
			if firstHeader.MarkerID != wantMarkerID ||
				firstHeader.Epoch != wantFirstEpoch || firstHeader.BaseSequence != 1 ||
				firstHeader.RecycleCount != wantFirstRecycleCount {
				t.Fatalf("first crash reopen marker = %+v, anchored %+v",
					firstHeader, anchoredHeader)
			}
			closeCheckpointGroupTestHandles(t, firstCollections, firstLog, first)

			secondCollections, _, second := openCheckpointGroupTestCopy(t, crashImage)
			secondMembers := []NamedCollection{
				{Name: "system", Collection: secondCollections[0]},
				{Name: "user", Collection: secondCollections[1]},
			}
			checkpointGroupPut(t, second, 2, secondMembers, "after-crash-repair")
			if err := second.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after second crash reopen: %v", err)
			}
			for _, collection := range secondCollections {
				if _, found := collectionDoc(t, collection, "after-crash-repair"); !found {
					t.Fatal("post-repair update is missing")
				}
			}
		})
	}
}

func TestCheckpointGroupMarkerRepairExhaustionDoesNotUnlinkMarker(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*checkpointGroupCertificate)
	}{
		{
			name: "certificate-sequence",
			mutate: func(certificate *checkpointGroupCertificate) {
				certificate.sequence = math.MaxUint64
				certificate.markerID[0] ^= 0xff
			},
		},
		{
			name: "marker-epoch",
			mutate: func(certificate *checkpointGroupCertificate) {
				certificate.markerEpoch = math.MaxUint64
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "certified")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkpointGroupPut(t, group, 2, members[:1], "uncertified-dirty")
			crashImage := copyCheckpointGroupDirectory(t, dir)

			certificatePath := filepath.Join(crashImage, checkpointGroupFilename)
			raw, err := os.ReadFile(certificatePath)
			if err != nil {
				t.Fatal(err)
			}
			var selected checkpointGroupCertificate
			for slot := 0; slot < checkpointGroupSlots; slot++ {
				start := slot * checkpointGroupSlotBytes
				candidate, decodeErr := decodeCheckpointGroupCertificate(
					raw[start : start+checkpointGroupSlotBytes],
				)
				if decodeErr == nil && candidate.sequence > selected.sequence {
					selected = candidate
				}
			}
			if selected.sequence == 0 {
				t.Fatal("test certificate has no valid slot")
			}
			test.mutate(&selected)
			encoded, err := encodeCheckpointGroupCertificate(selected)
			if err != nil {
				t.Fatal(err)
			}
			clear(raw)
			start := int(selected.sequence%checkpointGroupSlots) * checkpointGroupSlotBytes
			copy(raw[start:start+checkpointGroupSlotBytes], encoded)
			if err := os.WriteFile(certificatePath, raw, 0o600); err != nil {
				t.Fatal(err)
			}

			markerPath := filepath.Join(crashImage, txnMarkerFilename)
			markerBefore, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal(err)
			}
			beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)
			requests, files := checkpointGroupTestOpenRequests(t, crashImage)
			collections, log, reopened, err := OpenCollectionsWithCheckpointGroup(
				crashImage, TxnLogOptions{}, requests, []string{"system", "user"},
				CheckpointGroupOptions{CheckpointEvery: 8},
			)
			if collections != nil || log != nil || reopened != nil ||
				!errors.Is(err, ErrCheckpointGroupSequence) {
				t.Fatalf("exhausted repair = collections %v log %v group %v err %v",
					collections, log, reopened, err)
			}
			for _, file := range files {
				_ = file.Close()
			}
			markerAfter, err := os.ReadFile(markerPath)
			if err != nil || !bytes.Equal(markerAfter, markerBefore) {
				t.Fatalf("marker changed before exhausted repair refusal: %v", err)
			}
			requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
		})
	}
}

func TestCheckpointGroupMarkerRepairExactLastSuccessorsReopenReadOnly(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*checkpointGroupCertificate)
		check  func(*testing.T, checkpointGroupCertificate, *TxnLog)
	}{
		{
			name: "certificate-sequence",
			mutate: func(certificate *checkpointGroupCertificate) {
				certificate.sequence = math.MaxUint64 - 1
			},
			check: func(t *testing.T, certificate checkpointGroupCertificate, _ *TxnLog) {
				if certificate.sequence != math.MaxUint64 {
					t.Fatalf("repaired terminal certificate sequence = %d", certificate.sequence)
				}
			},
		},
		{
			name: "marker-epoch",
			mutate: func(certificate *checkpointGroupCertificate) {
				certificate.markerEpoch = math.MaxUint64 - 1
			},
			check: func(t *testing.T, certificate checkpointGroupCertificate, log *TxnLog) {
				if certificate.markerEpoch != math.MaxUint64 ||
					log.marker.Header().Epoch != math.MaxUint64 {
					t.Fatalf(
						"repaired terminal marker epoch = certificate %d header %d",
						certificate.markerEpoch, log.marker.Header().Epoch,
					)
				}
			},
		},
		{
			name: "transaction-high-water",
			mutate: func(certificate *checkpointGroupCertificate) {
				certificate.txnHighWater = math.MaxUint64 - 1
			},
			check: func(t *testing.T, certificate checkpointGroupCertificate, log *TxnLog) {
				header := log.marker.Header()
				if certificate.txnBase != math.MaxUint64-1 ||
					certificate.txnHighWater != math.MaxUint64-1 ||
					header.BaseSequence != math.MaxUint64-1 ||
					log.marker.NextSequence() != math.MaxUint64 {
					t.Fatalf(
						"repaired terminal transaction anchor = certificate %d/%d marker %d next %d",
						certificate.txnBase, certificate.txnHighWater,
						header.BaseSequence, log.marker.NextSequence(),
					)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, _, _, _ := newCheckpointGroupTestStore(t, 8)
			damaged := copyCheckpointGroupDirectory(t, dir)
			checkpointGroupTestRewriteSingleDiskCertificate(t, damaged, test.mutate)
			if err := os.Remove(filepath.Join(damaged, txnMarkerFilename)); err != nil {
				t.Fatal(err)
			}

			collections, log, group := openCheckpointGroupTestCopy(t, damaged)
			group.mu.Lock()
			certificate := group.certificateLocked()
			group.mu.Unlock()
			test.check(t, certificate, log)
			closeCheckpointGroupTestHandles(t, collections, log, group)
			checkpointGroupTestRequireTerminalReopenSucceeds(t, damaged)
		})
	}
}

func TestCheckpointGroupMarkerRepairTerminalTransactionAnchorFailsBeforeReplacement(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, members[:1], "uncertified-dirty")
	crashImage := copyCheckpointGroupDirectory(t, dir)
	certificatePath := filepath.Join(crashImage, checkpointGroupFilename)
	raw, err := os.ReadFile(certificatePath)
	if err != nil {
		t.Fatal(err)
	}
	var selected checkpointGroupCertificate
	for slot := 0; slot < checkpointGroupSlots; slot++ {
		start := slot * checkpointGroupSlotBytes
		candidate, decodeErr := decodeCheckpointGroupCertificate(
			raw[start : start+checkpointGroupSlotBytes],
		)
		if decodeErr == nil && candidate.sequence > selected.sequence {
			selected = candidate
		}
	}
	if selected.sequence == 0 {
		t.Fatal("test certificate has no valid slot")
	}
	selected.txnHighWater = math.MaxUint64
	encoded, err := encodeCheckpointGroupCertificate(selected)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	start := int(selected.sequence%checkpointGroupSlots) * checkpointGroupSlotBytes
	copy(raw[start:start+checkpointGroupSlotBytes], encoded)
	if err := os.WriteFile(certificatePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(crashImage, txnMarkerFilename)); err != nil {
		t.Fatal(err)
	}
	beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)
	for attempt := 0; attempt < 2; attempt++ {
		requests, files := checkpointGroupTestOpenRequests(t, crashImage)
		collections, log, reopened, err := OpenCollectionsWithCheckpointGroup(
			crashImage,
			TxnLogOptions{},
			requests,
			[]string{"system", "user"},
			CheckpointGroupOptions{CheckpointEvery: 8},
		)
		for _, file := range files {
			_ = file.Close()
		}
		if collections != nil || log != nil || reopened != nil ||
			!errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf(
				"terminal repair attempt %d = collections %v log %v group %v err %v",
				attempt,
				collections,
				log,
				reopened,
				err,
			)
		}
		requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
	}
}

func TestCheckpointGroupMarkerRepairRetriesEveryAnchoredCreateFault(t *testing.T) {
	for _, test := range []struct {
		name  string
		phase storeio.TxnMarkerFaultPhase
	}{
		{"header-write", storeio.TxnMarkerFaultCreateHeaderWrite},
		{"file-sync", storeio.TxnMarkerFaultCreateFileSync},
		{"parent-directory-sync", storeio.TxnMarkerFaultCreateParentDirSync},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "certified")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			group.mu.Lock()
			wantMarkerID := group.markerID
			group.mu.Unlock()
			damaged := copyCheckpointGroupDirectory(t, dir)
			if err := os.Remove(filepath.Join(damaged, txnMarkerFilename)); err != nil {
				t.Fatal(err)
			}

			storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{Phase: test.phase})
			defer storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
			requests, files := checkpointGroupTestOpenRequests(t, damaged)
			collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
				damaged, TxnLogOptions{}, requests, []string{"system", "user"},
				CheckpointGroupOptions{CheckpointEvery: 8},
			)
			if collections != nil || log != nil || recovered != nil || err == nil ||
				!storeio.TxnMarkerCreateFaulted() {
				t.Fatalf("anchored create fault = collections %v log %v group %v err %v fired %v",
					collections, log, recovered, err, storeio.TxnMarkerCreateFaulted())
			}
			for _, file := range files {
				_ = file.Close()
			}
			storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})

			firstCollections, firstLog, first := openCheckpointGroupTestCopy(t, damaged)
			if first.AppliedIndex() != 1 || first.CheckpointAppliedIndex() != 1 ||
				firstLog.marker.Header().MarkerID != wantMarkerID {
				t.Fatalf("retry after anchored create fault = cut %d/%d marker %+v",
					first.AppliedIndex(), first.CheckpointAppliedIndex(), firstLog.marker.Header())
			}
			closeCheckpointGroupTestHandles(t, firstCollections, firstLog, first)

			secondCollections, _, second := openCheckpointGroupTestCopy(t, damaged)
			secondMembers := []NamedCollection{
				{Name: "system", Collection: secondCollections[0]},
				{Name: "user", Collection: secondCollections[1]},
			}
			checkpointGroupPut(t, second, 2, secondMembers, "after-create-fault")
			if err := second.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after anchored create retry: %v", err)
			}
		})
	}
}

func TestCheckpointGroupMissingCertificateOnlyAdmitsCleanActivation(t *testing.T) {
	for _, test := range []struct {
		name      string
		nonempty  bool
		wantError error
	}{
		{name: "clean", wantError: ErrCheckpointGroupMissing},
		{name: "active", nonempty: true, wantError: ErrCheckpointGroupCorrupt},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			if test.nonempty {
				checkpointGroupPut(t, group, 1, members, "certified")
				if err := group.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			crashImage := copyCheckpointGroupDirectory(t, dir)
			if err := os.Remove(filepath.Join(crashImage, checkpointGroupFilename)); err != nil {
				t.Fatal(err)
			}
			if test.nonempty {
				// Remove the recyclable implementation prefix as well. The durable
				// roots alone must still prevent a generic-recovery fallback.
				marker, err := os.OpenFile(filepath.Join(crashImage, txnMarkerFilename), os.O_RDWR, 0)
				if err == nil {
					info, statErr := marker.Stat()
					if statErr != nil {
						err = statErr
					} else {
						_, err = marker.WriteAt(
							make([]byte, info.Size()-2*storeio.TxnMarkerHeaderSize),
							2*storeio.TxnMarkerHeaderSize,
						)
					}
				}
				if marker != nil {
					err = errors.Join(err, marker.Sync(), marker.Close())
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			requests, files := checkpointGroupTestOpenRequests(t, crashImage)
			_, _, _, err := OpenCollectionsWithCheckpointGroup(
				crashImage, TxnLogOptions{}, requests,
				[]string{"system", "user"}, CheckpointGroupOptions{},
			)
			for _, file := range files {
				_ = file.Close()
			}
			if !errors.Is(err, test.wantError) {
				t.Fatalf("missing-certificate open = %v, want %v", err, test.wantError)
			}
		})
	}
}

func TestNewCheckpointGroupRefusesGenericAdoptionOfExistingCertificate(t *testing.T) {
	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	// Retired live handles remain fenced until resource close. They cannot be
	// recycled as an accidental generic-adoption path around the certificate.
	if _, err := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{},
	); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("NewCheckpointGroup retired handles = %v", err)
	}
	for _, member := range members {
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	// The persistent fence rejects every fresh generic opener before it can
	// replay a journal or publish a physical root. Only the certificate-aware
	// recovery entry point may reopen these resources.
	wantBytes := checkpointGroupDirectoryBytes(t, dir)
	requests, files := checkpointGroupTestOpenRequests(t, dir)
	_, _, err := OpenCollectionsWithTransactions(
		dir, TxnLogOptions{}, requests,
	)
	for _, file := range files {
		_ = file.Close()
	}
	if !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("generic catalog reopen = %v", err)
	}
	for _, member := range members {
		file, openErr := os.OpenFile(
			filepath.Join(dir, member.Name+".vjc"), os.O_RDWR, 0,
		)
		if openErr != nil {
			t.Fatal(openErr)
		}
		_, openErr = Open(file, txnTestOptions())
		_ = file.Close()
		if !errors.Is(openErr, ErrCheckpointGroupRecoveryRequired) {
			t.Fatalf("standalone reopen %q = %v", member.Name, openErr)
		}
	}
	if genericLog, openErr := NewTxnLog(
		dir, TxnLogOptions{},
	); genericLog != nil || !errors.Is(openErr, ErrCheckpointGroupRecoveryRequired) {
		if genericLog != nil {
			_ = genericLog.Close()
		}
		t.Fatalf("NewTxnLog beside certificate = %v", openErr)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, wantBytes)

	requests, files = checkpointGroupTestOpenRequests(t, dir)
	collections, reopenedLog, reopened, err := OpenCollectionsWithCheckpointGroup(
		dir, TxnLogOptions{}, requests, []string{"system", "user"},
		CheckpointGroupOptions{},
	)
	if err != nil {
		t.Fatalf("checkpoint-group reopen: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = reopenedLog.Close()
		for _, file := range files {
			_ = file.Close()
		}
	})
	if reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf("checkpoint cut = %d", reopened.CheckpointAppliedIndex())
	}
	for i, collection := range collections {
		if _, ok := collectionDoc(t, collection, "certified"); !ok {
			t.Fatalf("member %d lost certified state", i)
		}
	}
}

func TestCheckpointGroupPersistentFencePreservesCertifiedCrashCut(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, members, "uncertified-suffix")
	crashImage := copyCheckpointGroupDirectory(t, dir)
	wantBytes := checkpointGroupDirectoryBytes(t, crashImage)

	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	_, _, err := OpenCollectionsWithTransactions(
		crashImage, TxnLogOptions{}, requests,
	)
	for _, file := range files {
		_ = file.Close()
	}
	if !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("generic crash recovery = %v", err)
	}
	direct, err := os.OpenFile(
		filepath.Join(crashImage, "system.vjc"), os.O_RDWR, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(direct, txnTestOptions())
	_ = direct.Close()
	if !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
		t.Fatalf("standalone crash recovery = %v", err)
	}
	requireCheckpointGroupDirectoryBytes(t, crashImage, wantBytes)

	collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf(
			"reopened cuts = applied %d checkpoint %d",
			reopened.AppliedIndex(), reopened.CheckpointAppliedIndex(),
		)
	}
	for i, collection := range collections {
		if _, ok := collectionDoc(t, collection, "certified"); !ok {
			t.Fatalf("member %d lost certified state", i)
		}
		if _, ok := collectionDoc(t, collection, "uncertified-suffix"); ok {
			t.Fatalf("member %d imported uncertified physical state", i)
		}
	}
}

func TestCheckpointGroupPersistentFenceRejectsConcurrentGenericOpen(t *testing.T) {
	dir, members, log, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "certified")
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	for _, member := range members {
		if err := member.Collection.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	wantBytes := checkpointGroupDirectoryBytes(t, dir)

	const attempts = 32
	start := make(chan struct{})
	errorsOut := make(chan error, attempts)
	var wait sync.WaitGroup
	for i := 0; i < attempts; i++ {
		attempt := i
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if attempt%2 == 0 {
				file, err := os.OpenFile(
					filepath.Join(dir, "system.vjc"), os.O_RDWR, 0,
				)
				if err == nil {
					_, err = Open(file, txnTestOptions())
					_ = file.Close()
				}
				errorsOut <- err
				return
			}
			requests := make([]TransactionCollectionOpen, 2)
			files := make([]*os.File, 0, 2)
			for index, name := range []string{"system", "user"} {
				file, err := os.OpenFile(
					filepath.Join(dir, name+".vjc"), os.O_RDWR, 0,
				)
				if err != nil {
					for _, opened := range files {
						_ = opened.Close()
					}
					errorsOut <- err
					return
				}
				files = append(files, file)
				requests[index] = TransactionCollectionOpen{
					File: file, Options: txnTestOptions(),
				}
			}
			_, _, err := OpenCollectionsWithTransactions(
				dir, TxnLogOptions{}, requests,
			)
			for _, file := range files {
				_ = file.Close()
			}
			errorsOut <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
			t.Fatalf("concurrent generic reopen = %v", err)
		}
	}
	requireCheckpointGroupDirectoryBytes(t, dir, wantBytes)
}

func TestCheckpointGroupPersistentFenceRejectsOpenDatabase(t *testing.T) {
	db, err := OpenDatabase(
		t.TempDir(), DatabaseOptions{Options: txnTestOptions()},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	members := make([]NamedCollection, 0, 2)
	for _, name := range []string{"system", "user"} {
		collection, err := db.CreateCollection(name, txnTestOptions())
		if err != nil {
			t.Fatal(err)
		}
		members = append(members, NamedCollection{Name: name, Collection: collection})
	}
	log, err := ensureDatabaseTxnLog(db)
	if err != nil {
		t.Fatal(err)
	}
	group, err := NewCheckpointGroup(log, members, CheckpointGroupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := OpenDatabase(
		db.Dir(), DatabaseOptions{Options: txnTestOptions()},
	); reopened != nil || !errors.Is(err, ErrCheckpointGroupRecoveryRequired) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("OpenDatabase beside checkpoint certificate = %v", err)
	}
}

func TestCheckpointGroupSnapshotPressureCoordinatesCertificate(t *testing.T) {
	_, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "pending")
	before := members[0].Collection.committer.DurableGeneration()
	if snapshot, err := members[0].Collection.Snapshot(); snapshot != nil ||
		!errors.Is(err, ErrCheckpointGroupPressure) {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		t.Fatalf("single-member Snapshot = %v, %v", snapshot, err)
	}
	if after := members[0].Collection.committer.DurableGeneration(); after != before {
		t.Fatalf("single-member snapshot advanced durable generation %d -> %d", before, after)
	}
	cut, err := SnapshotCollections(members)
	if err != nil {
		t.Fatalf("SnapshotCollections: %v", err)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if group.CheckpointAppliedIndex() != 1 {
		t.Fatalf("coordinated checkpoint cut = %d", group.CheckpointAppliedIndex())
	}
}

func TestCheckpointGroupCrashCutsAreAllOldOrAllNew(t *testing.T) {
	t.Run("before-certificate-is-all-old", func(t *testing.T) {
		dir, members, _, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "transition")
		fault := errors.New("stop during journal barrier")
		seen := 0
		previousHook := checkpointGroupFaultHook
		checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
			if point == checkpointGroupAfterJournalSync {
				seen++
				if seen == 1 {
					return fault
				}
			}
			return nil
		}
		err := group.Checkpoint()
		checkpointGroupFaultHook = previousHook
		if !errors.Is(err, fault) {
			t.Fatalf("Checkpoint fault = %v", err)
		}
		crashImage := copyCheckpointGroupDirectory(t, dir)
		clearCheckpointGroupTestPoison(group)
		collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
		if reopened.CheckpointAppliedIndex() != 0 {
			t.Fatalf("old checkpoint cut = %d", reopened.CheckpointAppliedIndex())
		}
		for _, collection := range collections {
			if _, ok := collectionDoc(t, collection, "transition"); ok {
				t.Fatal("pre-certificate transition survived")
			}
		}
	})

	t.Run("partial-physical-fold-is-all-new", func(t *testing.T) {
		dir, members, _, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "transition")
		fault := errors.New("stop after one physical fold")
		seen := 0
		previousHook := checkpointGroupFaultHook
		checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
			if point == checkpointGroupAfterPhysicalCheckpoint {
				seen++
				if seen == 1 {
					return fault
				}
			}
			return nil
		}
		err := group.Checkpoint()
		checkpointGroupFaultHook = previousHook
		if !errors.Is(err, fault) {
			t.Fatalf("Checkpoint fault = %v", err)
		}
		crashImage := copyCheckpointGroupDirectory(t, dir)
		collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
		if reopened.CheckpointAppliedIndex() != 1 {
			t.Fatalf("new checkpoint cut = %d", reopened.CheckpointAppliedIndex())
		}
		for _, collection := range collections {
			if _, ok := collectionDoc(t, collection, "transition"); !ok {
				t.Fatal("certified transition missing after partial fold")
			}
		}
	})
}
