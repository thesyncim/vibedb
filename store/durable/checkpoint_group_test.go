package durable

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
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

	t.Run("authenticated-reserved", func(t *testing.T) {
		image := bytes.Clone(raw)
		slot := newestBytes(image)
		slot[14] = 1
		resign(slot)
		if _, err := openRaw(t, image); !errors.Is(err, ErrCheckpointGroupCorrupt) {
			t.Fatalf("reserved-byte certificate = %v", err)
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

func TestCheckpointGroupActivationPostRenameFailureFailsClosed(t *testing.T) {
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
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterCertificateRename {
			return injected
		}
		return nil
	}
	group, activationErr := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{},
	)
	checkpointGroupFaultHook = previousHook
	if group != nil || !errors.Is(activationErr, injected) ||
		!errors.Is(activationErr, ErrCommitOutcomeUnknown) {
		t.Fatalf("post-rename activation = group %v, error %v", group, activationErr)
	}
	if _, err := os.Stat(filepath.Join(db.Dir(), checkpointGroupFilename)); err != nil {
		t.Fatalf("published certificate is not visible: %v", err)
	}
	for _, member := range members {
		if _, err := member.Collection.Put(
			[]byte("forbidden"), []byte(`{"n":1}`),
		); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q Put after uncertain activation = %v", member.Name, err)
		}
		if err := member.Collection.Flush(); !errors.Is(err, ErrCheckpointGroupOwned) {
			t.Fatalf("member %q Flush after uncertain activation = %v", member.Name, err)
		}
	}
	if err := UpdateCollections(
		log, members, defaultTxnLimits(), func(*DatabaseBatch) error { return nil },
	); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("UpdateCollections after uncertain activation = %v", err)
	}
	if _, err := db.CreateCollection("extra", txnTestOptions()); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("CreateCollection after uncertain activation = %v", err)
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
	if err := db.Close(); err != nil {
		t.Fatalf("resource close after terminal activation fence: %v", err)
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
	collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
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

	collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if reopened.CheckpointAppliedIndex() != 1 {
		t.Fatalf("checkpoint cut = %d, want 1", reopened.CheckpointAppliedIndex())
	}
	for _, collection := range collections {
		if _, ok := collectionDoc(t, collection, "certified"); !ok {
			t.Fatal("certificate-authorized journal prepare was not replayed")
		}
	}
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
			collections, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
			if reopened.CheckpointAppliedIndex() != 1 {
				t.Fatalf("repaired checkpoint cut = %d", reopened.CheckpointAppliedIndex())
			}
			for _, collection := range collections {
				if _, ok := collectionDoc(t, collection, "certified"); !ok {
					t.Fatal("certificate-authorized prepare was not replayed")
				}
			}
			if info, statErr := os.Stat(filepath.Join(crashImage, txnMarkerFilename)); statErr != nil || !info.Mode().IsRegular() {
				t.Fatalf("replacement marker = %v, %v", info, statErr)
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
