package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

func verifyTestOptions() durable.Options {
	return durable.Options{
		Backend:          durable.BackendPortable,
		ResidentBytes:    32 << 20,
		Durability:       durable.DurabilitySync,
		PageSize:         4096,
		MaxPageSize:      64 << 10,
		InlineValueBytes: 2048,
		MaxDocumentBytes: 2048,
		GroupLimit:       1,
	}
}

func openVerifyTestDB(t *testing.T, names ...string) (*durable.Database, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := durable.OpenDatabase(dir, durable.DatabaseOptions{
		Options: verifyTestOptions(),
	})
	if err != nil {
		t.Fatalf("OpenDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range names {
		if _, err := db.CreateCollection(name, verifyTestOptions()); err != nil {
			t.Fatalf("CreateCollection(%s): %v", name, err)
		}
	}
	return db, dir
}

func collectionPath(t *testing.T, dir, name string) string {
	t.Helper()
	filename, ok := collectionname.Encode(name)
	if !ok {
		t.Fatalf("encode %q", name)
	}
	return filepath.Join(dir, filename)
}

func directoryDigest(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(h, name)
		_, _ = h.Write([]byte{0})
		sum := sha256.Sum256(data)
		_, _ = h.Write(sum[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func mustUpdateAB(t *testing.T, db *durable.Database) {
	t.Helper()
	if err := db.Update(func(batch *durable.DatabaseBatch) error {
		a, err := batch.Collection("a")
		if err != nil {
			return err
		}
		b, err := batch.Collection("b")
		if err != nil {
			return err
		}
		if err := a.Put([]byte("k"), []byte(`{"n":1}`)); err != nil {
			return err
		}
		return b.Put([]byte("k"), []byte(`{"n":1}`))
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func readStoreJournalIDs(
	t *testing.T, primaryPath string,
) (storeID, journalID [16]byte, journalPath string) {
	t.Helper()
	id, err := readPrimaryIdentity(primaryPath)
	if err != nil {
		t.Fatalf("readPrimaryIdentity: %v", err)
	}
	return id.StoreID, id.JournalID, durable.RecoveryJournalPath(primaryPath)
}

func appendConditional(
	t *testing.T,
	journalPath string,
	markerID [16]byte,
	epoch, txnID, generation uint64,
) {
	t.Helper()
	file, err := os.OpenFile(journalPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer file.Close()
	journal, err := storeio.OpenRecoveryJournal(file)
	if err != nil {
		t.Fatalf("OpenRecoveryJournal: %v", err)
	}
	defer journal.Close()
	if generation <= journal.BaseGeneration() {
		generation = journal.BaseGeneration() + 1
	}
	entries := []storeio.RecoveryBatchEntry{{
		Kind:  storeio.RecoveryRecordKindPut,
		Key:   []byte("verify-seed"),
		Value: []byte(`{"seed":true}`),
	}}
	if _, err := journal.AppendConditionalBatch(
		generation, markerID, epoch, txnID, entries,
	); err != nil {
		t.Fatalf("AppendConditionalBatch: %v", err)
	}
	if err := journal.Sync(true); err != nil {
		t.Fatalf("journal Sync: %v", err)
	}
}

// TestVerifyDatabaseCleanPass proves a committed two-collection database
// verifies with no pairing findings and exit 0.
func TestVerifyDatabaseCleanPass(t *testing.T) {
	db, dir := openVerifyTestDB(t, "a", "b")
	mustUpdateAB(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	before := directoryDigest(t, dir)
	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", dir)
	after := directoryDigest(t, dir)
	if before != after {
		t.Fatalf("verify mutated the database directory")
	}
	if code != 0 {
		t.Fatalf("verify(clean) = %d; stderr=%q stdout=%q", code, stderr, stdout)
	}
	if strings.Contains(stdout, "finding kind=") {
		t.Fatalf("verify(clean) reported findings: %q", stdout)
	}
	if !strings.Contains(stdout, "result ok") {
		t.Fatalf("verify(clean) stdout = %q, want result ok", stdout)
	}
}

// TestVerifyDatabaseTornDecision seeds a truncatable decision-record prefix
// and expects a distinct torn_decision diagnostic.
func TestVerifyDatabaseTornDecision(t *testing.T) {
	db, dir := openVerifyTestDB(t, "a", "b")
	mustUpdateAB(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, txnMarkerFilename)
	marker, _, err := storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatalf("OpenTxnMarker: %v", err)
	}
	fm := storeio.NewFaultTxnMarker(marker)
	fm.Program(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultTornAppend, AppendIndex: 0,
	})
	aStore, aJournal, _ := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
	bStore, bJournal, _ := readStoreJournalIDs(t, collectionPath(t, dir, "b"))
	// Torn append writes a prefix and reports the short write. The checksummed
	// record is incomplete, so inspection retains that residue as the distinct
	// torn-decision diagnostic.
	if _, err := marker.AppendDecision(99, []storeio.TxnParticipant{
		{StoreID: aStore, JournalID: aJournal, PreparedGeneration: 2},
		{StoreID: bStore, JournalID: bJournal, PreparedGeneration: 2},
	}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("torn AppendDecision = %v, want io.ErrShortWrite", err)
	}
	if !fm.Faulted() {
		t.Fatal("torn append fault did not fire")
	}
	_ = marker.Close()

	before := directoryDigest(t, dir)
	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", dir)
	after := directoryDigest(t, dir)
	if before != after {
		t.Fatalf("verify mutated the database directory")
	}
	if code == 0 {
		t.Fatalf("verify(torn) = 0; want non-zero; stdout=%q stderr=%q", stdout, stderr)
	}
	if !strings.Contains(stdout, "finding kind=torn_decision") {
		t.Fatalf("stdout=%q, want torn_decision", stdout)
	}
}

// TestVerifyDatabaseMissingParticipant deletes a named participant file under
// a live decision and expects missing_participant.
func TestVerifyDatabaseMissingParticipant(t *testing.T) {
	db, dir := openVerifyTestDB(t, "a", "b")
	mustUpdateAB(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	primary := collectionPath(t, dir, "b")
	journal := durable.RecoveryJournalPath(primary)
	if err := os.Remove(primary); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(journal)

	before := directoryDigest(t, dir)
	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", dir)
	after := directoryDigest(t, dir)
	if before != after {
		t.Fatalf("verify mutated the database directory")
	}
	if code == 0 {
		t.Fatalf("verify(missing) = 0; want non-zero; stdout=%q stderr=%q", stdout, stderr)
	}
	if !strings.Contains(stdout, "finding kind=missing_participant") {
		t.Fatalf("stdout=%q, want missing_participant", stdout)
	}
}

// TestVerifyDatabaseEpochMismatch plants a kind-4 record whose MarkerEpoch
// disagrees with txn.vtm and expects epoch_mismatch.
func TestVerifyDatabaseEpochMismatch(t *testing.T) {
	db, dir := openVerifyTestDB(t, "a", "b")
	// Mint journals with a single-collection write so we have a conditional-
	// format journal without a multi-collection decision.
	a, ok := db.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	if err := a.Update(func(batch *durable.WriteBatch) error {
		return batch.Put([]byte("k"), []byte(`{"n":0}`))
	}); err != nil {
		t.Fatalf("Update a: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	markerPath := filepath.Join(dir, txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(markerPath, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatalf("CreateTxnMarker: %v", err)
	}
	header := marker.Header()
	_ = marker.Close()

	_, _, journalPath := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
	appendConditional(t, journalPath, header.MarkerID, header.Epoch+7, 1, 2)

	before := directoryDigest(t, dir)
	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", dir)
	after := directoryDigest(t, dir)
	if before != after {
		t.Fatalf("verify mutated the database directory")
	}
	if code == 0 {
		t.Fatalf("verify(epoch) = 0; want non-zero; stdout=%q stderr=%q", stdout, stderr)
	}
	if !strings.Contains(stdout, "finding kind=epoch_mismatch") {
		t.Fatalf("stdout=%q, want epoch_mismatch", stdout)
	}
}

// TestVerifyDatabaseInDoubtRecord plants a same-epoch conditional with no
// decision — the offline pairing detector for the silent prefix window.
func TestVerifyDatabaseInDoubtRecord(t *testing.T) {
	db, dir := openVerifyTestDB(t, "a", "b")
	a, ok := db.Collection("a")
	if !ok {
		t.Fatal("missing a")
	}
	if err := a.Update(func(batch *durable.WriteBatch) error {
		return batch.Put([]byte("k"), []byte(`{"n":0}`))
	}); err != nil {
		t.Fatalf("Update a: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	markerPath := filepath.Join(dir, txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(markerPath, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatalf("CreateTxnMarker: %v", err)
	}
	header := marker.Header()
	_ = marker.Close()

	_, _, journalPath := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
	appendConditional(t, journalPath, header.MarkerID, header.Epoch, 9, 2)

	before := directoryDigest(t, dir)
	code, stdout, stderr := captureRun(t, "vibedb-verify", "verify", dir)
	after := directoryDigest(t, dir)
	if before != after {
		t.Fatalf("verify mutated the database directory")
	}
	if code == 0 {
		t.Fatalf("verify(in_doubt) = 0; want non-zero; stdout=%q stderr=%q", stdout, stderr)
	}
	if !strings.Contains(stdout, "finding kind=in_doubt") {
		t.Fatalf("stdout=%q, want in_doubt", stdout)
	}
	for _, other := range []string{"torn_decision", "missing_participant", "epoch_mismatch"} {
		if strings.Contains(stdout, "finding kind="+other) {
			t.Fatalf("stdout=%q, unexpected %s beside in_doubt", stdout, other)
		}
	}
}

// TestVerifyDatabaseNeverWrites is an explicit digest pin across every seeded
// corruption shape: the tool is offline and must not mutate input bytes.
func TestVerifyDatabaseNeverWrites(t *testing.T) {
	cases := []struct {
		name string
		seed func(t *testing.T) string
	}{
		{"clean", func(t *testing.T) string {
			db, dir := openVerifyTestDB(t, "a", "b")
			mustUpdateAB(t, db)
			_ = db.Close()
			return dir
		}},
		{"in_doubt", func(t *testing.T) string {
			db, dir := openVerifyTestDB(t, "a")
			a, _ := db.Collection("a")
			_ = a.Update(func(batch *durable.WriteBatch) error {
				return batch.Put([]byte("k"), []byte(`{"n":0}`))
			})
			_ = db.Close()
			marker, err := storeio.CreateTxnMarker(
				filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			header := marker.Header()
			_ = marker.Close()
			_, _, journalPath := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
			appendConditional(t, journalPath, header.MarkerID, header.Epoch, 1, 2)
			return dir
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.seed(t)
			before := directoryDigest(t, dir)
			_, _, _ = captureRun(t, "vibedb-verify", "verify", dir)
			after := directoryDigest(t, dir)
			if before != after {
				t.Fatalf("verify wrote to the database directory (%s)", tc.name)
			}
		})
	}
}

// TestVerifyDatabaseDistinctDiagnostics asserts the four seeded corruption
// shapes emit four distinct finding kinds.
func TestVerifyDatabaseDistinctDiagnostics(t *testing.T) {
	kinds := map[string]string{}

	t.Run("collect_torn", func(t *testing.T) {
		db, dir := openVerifyTestDB(t, "a", "b")
		mustUpdateAB(t, db)
		_ = db.Close()
		path := filepath.Join(dir, txnMarkerFilename)
		marker, _, err := storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		fm := storeio.NewFaultTxnMarker(marker)
		fm.Program(storeio.TxnMarkerFaultPlan{
			Phase: storeio.TxnMarkerFaultTornAppend, AppendIndex: 0,
		})
		aStore, aJournal, _ := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
		if _, err := marker.AppendDecision(99, []storeio.TxnParticipant{{
			StoreID: aStore, JournalID: aJournal, PreparedGeneration: 2,
		}}); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("torn AppendDecision = %v, want io.ErrShortWrite", err)
		}
		if !fm.Faulted() {
			t.Fatal("torn append fault did not fire")
		}
		_ = marker.Close()
		_, stdout, _ := captureRun(t, "vibedb-verify", "verify", dir)
		if strings.Contains(stdout, "finding kind=torn_decision") {
			kinds["torn_decision"] = "torn_decision"
		}
	})
	t.Run("collect_missing", func(t *testing.T) {
		db, dir := openVerifyTestDB(t, "a", "b")
		mustUpdateAB(t, db)
		_ = db.Close()
		_ = os.Remove(collectionPath(t, dir, "b"))
		_ = os.Remove(durable.RecoveryJournalPath(collectionPath(t, dir, "b")))
		_, stdout, _ := captureRun(t, "vibedb-verify", "verify", dir)
		if strings.Contains(stdout, "finding kind=missing_participant") {
			kinds["missing_participant"] = "missing_participant"
		}
	})
	t.Run("collect_epoch", func(t *testing.T) {
		db, dir := openVerifyTestDB(t, "a")
		a, _ := db.Collection("a")
		_ = a.Update(func(batch *durable.WriteBatch) error {
			return batch.Put([]byte("k"), []byte(`{"n":0}`))
		})
		_ = db.Close()
		marker, err := storeio.CreateTxnMarker(
			filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		header := marker.Header()
		_ = marker.Close()
		_, _, journalPath := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
		appendConditional(t, journalPath, header.MarkerID, header.Epoch+3, 1, 2)
		_, stdout, _ := captureRun(t, "vibedb-verify", "verify", dir)
		if strings.Contains(stdout, "finding kind=epoch_mismatch") {
			kinds["epoch_mismatch"] = "epoch_mismatch"
		}
	})
	t.Run("collect_in_doubt", func(t *testing.T) {
		db, dir := openVerifyTestDB(t, "a")
		a, _ := db.Collection("a")
		_ = a.Update(func(batch *durable.WriteBatch) error {
			return batch.Put([]byte("k"), []byte(`{"n":0}`))
		})
		_ = db.Close()
		marker, err := storeio.CreateTxnMarker(
			filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		header := marker.Header()
		_ = marker.Close()
		_, _, journalPath := readStoreJournalIDs(t, collectionPath(t, dir, "a"))
		appendConditional(t, journalPath, header.MarkerID, header.Epoch, 1, 2)
		_, stdout, _ := captureRun(t, "vibedb-verify", "verify", dir)
		if strings.Contains(stdout, "finding kind=in_doubt") {
			kinds["in_doubt"] = "in_doubt"
		}
	})

	for _, want := range []string{
		"torn_decision", "missing_participant", "epoch_mismatch", "in_doubt",
	} {
		if kinds[want] == "" {
			t.Fatalf("missing distinct diagnostic %q; got %v", want, kinds)
		}
	}
	if len(kinds) != 4 {
		t.Fatalf("expected 4 distinct kinds, got %v", kinds)
	}
}

func TestVerifyDatabaseUsageStillRejectsArity(t *testing.T) {
	code, stdout, stderr := captureRun(
		t, "vibedb-verify", "verify", "a", "b",
	)
	if code != 2 {
		t.Fatalf("code=%d want 2", code)
	}
	if !strings.Contains(stderr, "usage:") {
		t.Fatalf("stderr=%q", stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout=%q", stdout)
	}
}
