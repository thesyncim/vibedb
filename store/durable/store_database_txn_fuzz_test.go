package durable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// FuzzDatabaseTxnRecovery mutates decision-log and journal-tail bytes of
// seeded two-collection crash images. OpenDatabase must fail closed or recover
// every decided transaction all-in / all-out across participants — never a
// torn subset — and every opened collection must satisfy the existing
// internal-consistency oracle.

const (
	txnFuzzMagic      = "DBTX"
	txnFuzzVersion    = 1
	txnFuzzMaxBlob    = 2 << 20
	txnFuzzMaxFiles   = 32
	txnFuzzMaxNameLen = 200
	txnFuzzMultiPref  = "m/"
)

var errTxnFuzzCodec = errors.New("txn fuzz codec")

type txnFuzzFile struct {
	name string
	data []byte
}

func packTxnFuzzDir(t testing.TB, dir string) []byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]txnFuzzFile, 0, len(entries))
	total := 6
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.ContainsAny(name, `/\`) || len(name) > txnFuzzMaxNameLen {
			t.Fatalf("refusing to pack %q", name)
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, txnFuzzFile{name: name, data: data})
		total += 1 + len(name) + 4 + len(data)
	}
	if len(files) == 0 || len(files) > txnFuzzMaxFiles {
		t.Fatalf("unexpected file count %d", len(files))
	}
	out := make([]byte, 0, total)
	out = append(out, txnFuzzMagic...)
	out = append(out, byte(txnFuzzVersion))
	out = append(out, byte(len(files)))
	for _, f := range files {
		out = append(out, byte(len(f.name)))
		out = append(out, f.name...)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(f.data)))
		out = append(out, lenBuf[:]...)
		out = append(out, f.data...)
	}
	return out
}

func unpackTxnFuzzBlob(dir string, blob []byte) error {
	if len(blob) < 6 || string(blob[:4]) != txnFuzzMagic || blob[4] != txnFuzzVersion {
		return errTxnFuzzCodec
	}
	n := int(blob[5])
	if n == 0 || n > txnFuzzMaxFiles {
		return errTxnFuzzCodec
	}
	at := 6
	for i := 0; i < n; i++ {
		if at >= len(blob) {
			return errTxnFuzzCodec
		}
		nameLen := int(blob[at])
		at++
		if nameLen == 0 || nameLen > txnFuzzMaxNameLen || at+nameLen+4 > len(blob) {
			return errTxnFuzzCodec
		}
		name := string(blob[at : at+nameLen])
		at += nameLen
		if strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
			return errTxnFuzzCodec
		}
		dataLen := int(binary.LittleEndian.Uint32(blob[at : at+4]))
		at += 4
		if dataLen < 0 || at+dataLen > len(blob) {
			return errTxnFuzzCodec
		}
		data := blob[at : at+dataLen]
		at += dataLen
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return err
		}
	}
	if at != len(blob) {
		return errTxnFuzzCodec
	}
	return nil
}

func openTxnFuzzDBWithAB(t testing.TB) (*Database, *Collection, *Collection) {
	t.Helper()
	db, err := OpenDatabase(t.TempDir(), DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a, err := db.CreateCollection("a", txnTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.CreateCollection("b", txnTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	return db, a, b
}

func txnFuzzMustUpdate2(t testing.TB, db *Database, aKey, aDoc, bKey, bDoc string) {
	t.Helper()
	if err := db.Update(func(batch *DatabaseBatch) error {
		a, err := batch.Collection("a")
		if err != nil {
			return err
		}
		b, err := batch.Collection("b")
		if err != nil {
			return err
		}
		if err := a.Put([]byte(aKey), []byte(aDoc)); err != nil {
			return err
		}
		return b.Put([]byte(bKey), []byte(bDoc))
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func txnFuzzPrepareUnpublished(
	t testing.TB, coll *Collection, markerID [16]byte, epoch, txnID uint64, key, doc string,
) uint64 {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	coll.journalCatalogOwned = true
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		t.Fatalf("ensure conditional: %v", err)
	}
	batch := coll.fileWriteBatch()
	defer coll.releaseFileWriteBatch(batch)
	if err := batch.Put([]byte(key), []byte(doc)); err != nil {
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchConditionalLocked(batch)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if !staged.live {
		t.Fatal("expected live staged batch")
	}
	gen := staged.generation
	if err := coll.preparePrimaryBatchConditionalLocked(
		&staged, markerID, epoch, txnID, true,
	); err != nil {
		coll.unwindStagedPrimaryBatch(&staged)
		t.Fatalf("prepare: %v", err)
	}
	coll.unwindStagedPrimaryBatch(&staged)
	return gen
}

func txnFuzzPrepareMaybePublish(
	t testing.TB, coll *Collection, markerID [16]byte, epoch, txnID uint64,
	key, doc string, publish bool,
) uint64 {
	t.Helper()
	coll.writer.Lock()
	defer coll.writer.Unlock()
	coll.journalCatalogOwned = true
	if err := coll.ensureConditionalJournalFormatLocked(); err != nil {
		t.Fatalf("ensure conditional: %v", err)
	}
	batch := coll.fileWriteBatch()
	defer coll.releaseFileWriteBatch(batch)
	if err := batch.Put([]byte(key), []byte(doc)); err != nil {
		t.Fatal(err)
	}
	staged, err := coll.stagePrimaryBatchConditionalLocked(batch)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if !staged.live {
		t.Fatal("expected live staged batch")
	}
	gen := staged.generation
	if err := coll.preparePrimaryBatchConditionalLocked(
		&staged, markerID, epoch, txnID, true,
	); err != nil {
		coll.unwindStagedPrimaryBatch(&staged)
		t.Fatalf("prepare: %v", err)
	}
	if !publish {
		coll.unwindStagedPrimaryBatch(&staged)
		return gen
	}
	coll.snapshotGate.Lock()
	coll.publishPrimaryBatchGateHeld(staged)
	coll.snapshotGate.Unlock()
	staged.live = false
	if err := coll.checkpointPastConditionalsLocked(); err != nil {
		t.Fatalf("checkpoint past conditionals: %v", err)
	}
	return coll.state.Load().root.Generation
}

func txnFuzzSeedCommitted(t testing.TB) []byte {
	t.Helper()
	db, _, _ := openTxnFuzzDBWithAB(t)
	txnFuzzMustUpdate2(t, db, "m/1", `{"n":1}`, "m/1", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	return packTxnFuzzDir(t, img)
}

func txnFuzzSeedAborted(t testing.TB) []byte {
	t.Helper()
	db, a, b := openTxnFuzzDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	_ = marker.Close()
	_ = txnFuzzPrepareUnpublished(t, a, header.MarkerID, header.Epoch, 7, "m/7", `{"n":1}`)
	_ = txnFuzzPrepareUnpublished(t, b, header.MarkerID, header.Epoch, 7, "m/7", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	return packTxnFuzzDir(t, img)
}

func txnFuzzSeedMixed(t testing.TB) []byte {
	t.Helper()
	db, a, b := openTxnFuzzDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	const txnID = uint64(3)
	genA := txnFuzzPrepareMaybePublish(t, a, header.MarkerID, header.Epoch, txnID, "m/3", `{"n":1}`, true)
	genB := txnFuzzPrepareMaybePublish(t, b, header.MarkerID, header.Epoch, txnID, "m/3", `{"n":1}`, false)
	if _, err := marker.AppendDecision(txnID, []storeio.TxnParticipant{
		{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
		{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
	}); err != nil {
		t.Fatal(err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = marker.Close()
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	return packTxnFuzzDir(t, img)
}

func txnFuzzSeedTornDecision(t testing.TB) []byte {
	t.Helper()
	db, a, b := openTxnFuzzDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	const txnID = uint64(9)
	genA := txnFuzzPrepareUnpublished(t, a, header.MarkerID, header.Epoch, txnID, "m/9", `{"n":1}`)
	genB := txnFuzzPrepareUnpublished(t, b, header.MarkerID, header.Epoch, txnID, "m/9", `{"n":1}`)
	fm := storeio.NewFaultTxnMarker(marker)
	fm.Program(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultTornAppend, AppendIndex: 0,
	})
	if _, err := marker.AppendDecision(txnID, []storeio.TxnParticipant{
		{StoreID: a.storeID, JournalID: a.journalID, PreparedGeneration: genA},
		{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
	}); err != nil {
		t.Fatal(err)
	}
	_ = marker.Close()
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	return packTxnFuzzDir(t, img)
}

func txnFuzzSeedStrayReuse(t testing.TB) []byte {
	t.Helper()
	db, a, b := openTxnFuzzDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	const txnID = uint64(11)
	// Stray undecided prepare on A for txnID; decision reuses txnID naming only B.
	// Keys are deliberately not m/-prefixed: the multi-key agreement oracle only
	// joins symmetric multi-collection writes.
	_ = txnFuzzPrepareUnpublished(t, a, header.MarkerID, header.Epoch, txnID, "stray", `{"n":1}`)
	genB := txnFuzzPrepareUnpublished(t, b, header.MarkerID, header.Epoch, txnID, "reuse", `{"n":2}`)
	if _, err := marker.AppendDecision(txnID, []storeio.TxnParticipant{
		{StoreID: b.storeID, JournalID: b.journalID, PreparedGeneration: genB},
	}); err != nil {
		t.Fatal(err)
	}
	if err := marker.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = marker.Close()
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	return packTxnFuzzDir(t, img)
}

func txnFuzzSeedDeletedLog(t testing.TB) []byte {
	t.Helper()
	db, a, _ := openTxnFuzzDBWithAB(t)
	path := filepath.Join(db.Dir(), txnMarkerFilename)
	marker, err := storeio.CreateTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	_ = marker.Close()
	_ = txnFuzzPrepareUnpublished(t, a, header.MarkerID, header.Epoch, 4, "x/4", `{"n":1}`)
	img := cloneDatabaseDir(t, db.Dir())
	_ = db.Close()
	if err := os.Remove(filepath.Join(img, txnMarkerFilename)); err != nil {
		t.Fatal(err)
	}
	return packTxnFuzzDir(t, img)
}

func preScanTxnFuzzDecisions(dir string) *storeio.TxnDecisions {
	marker, decisions, err := storeio.OpenTxnMarker(
		filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
	)
	if err != nil {
		return nil
	}
	_ = marker.Close()
	return decisions
}

func assertTxnFuzzAllInOut(t *testing.T, db *Database, prior *storeio.TxnDecisions) {
	t.Helper()
	names := db.Names(nil)
	byStore := make(map[[16]byte]*Collection, len(names))
	for _, name := range names {
		coll, ok := db.Collection(name)
		if !ok {
			t.Fatalf("missing collection %q after open", name)
		}
		byStore[coll.storeID] = coll
		assertFuzzRecoveredStoreIsSelfConsistent(t, coll)
	}

	assertTxnFuzzMultiKeyAgreement(t, db)

	if prior == nil {
		return
	}
	markerID := prior.MarkerID()
	epoch := prior.Epoch()
	for txnID := uint64(1); txnID <= prior.MaxTxnID(); txnID++ {
		participants, ok := prior.Lookup(markerID, epoch, txnID)
		if !ok {
			continue
		}
		covered := 0
		living := 0
		for _, p := range participants {
			if prior.Retired(p.StoreID) {
				continue
			}
			living++
			c, exists := byStore[p.StoreID]
			if !exists {
				t.Fatalf(
					"decided txn %d names missing participant %x after successful open",
					txnID, p.StoreID,
				)
			}
			if c.Generation() >= p.PreparedGeneration {
				covered++
			}
		}
		if living == 0 {
			continue
		}
		// Successful open must leave decided transactions all-in. All-out for a
		// durable decision would be a silent rollback of an acknowledged commit.
		if covered != living {
			t.Fatalf(
				"decided txn %d recovered torn: covered=%d living=%d",
				txnID, covered, living,
			)
		}
	}
}

func assertTxnFuzzMultiKeyAgreement(t *testing.T, db *Database) {
	t.Helper()
	a, aOK := db.Collection("a")
	b, bOK := db.Collection("b")
	if !aOK || !bOK {
		return
	}
	fromA := make(map[string]string)
	snapA, err := a.Snapshot()
	if err != nil {
		return
	}
	_ = snapA.RangeRaw(func(key, value []byte) error {
		if bytes.HasPrefix(key, []byte(txnFuzzMultiPref)) {
			fromA[string(key)] = string(value)
		}
		return nil
	})
	_ = snapA.Close()

	snapB, err := b.Snapshot()
	if err != nil {
		return
	}
	defer snapB.Close()
	_ = snapB.RangeRaw(func(key, value []byte) error {
		if !bytes.HasPrefix(key, []byte(txnFuzzMultiPref)) {
			return nil
		}
		k := string(key)
		want, ok := fromA[k]
		if !ok {
			t.Fatalf("multi key %q present in b only", k)
		}
		if want != string(value) {
			t.Fatalf("multi key %q torn: a=%q b=%q", k, want, value)
		}
		delete(fromA, k)
		return nil
	})
	for k := range fromA {
		t.Fatalf("multi key %q present in a only", k)
	}
}

func FuzzDatabaseTxnRecovery(f *testing.F) {
	f.Add(txnFuzzSeedCommitted(f))
	f.Add(txnFuzzSeedAborted(f))
	f.Add(txnFuzzSeedMixed(f))
	f.Add(txnFuzzSeedTornDecision(f))
	f.Add(txnFuzzSeedStrayReuse(f))
	f.Add(txnFuzzSeedDeletedLog(f))
	f.Add([]byte(nil))
	f.Add([]byte(txnFuzzMagic))

	directory := f.TempDir()
	f.Fuzz(func(t *testing.T, blob []byte) {
		if len(blob) > txnFuzzMaxBlob {
			t.Skip("input beyond the size this target bounds itself to")
		}
		dir := filepath.Join(directory, "img")
		_ = os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := unpackTxnFuzzBlob(dir, blob); err != nil {
			return
		}
		prior := preScanTxnFuzzDecisions(dir)
		db, err := OpenDatabase(dir, DatabaseOptions{Options: txnTestOptions()})
		if err != nil {
			// Fail-closed is always allowed, including the E2 deleted-log seed.
			return
		}
		defer func() { _ = db.Close() }()
		assertTxnFuzzAllInOut(t, db, prior)
	})
}
