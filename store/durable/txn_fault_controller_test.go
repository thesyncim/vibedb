package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// txnFaultController composes per-collection journal fault seams with the
// decision-log fault seam so crash-matrix tests can stop a multi-collection
// commit at a named ordinal and capture a whole-database image.
//
// Production code never consults this type; every field and method is
// test-only. The controller installs seams under the existing hooks
// (recoveryJournalFaultHook, databaseTxnAfterMintHook,
// recoveryJournalPostSyncHook) and leaves them restored via t.Cleanup.
type txnFaultController struct {
	t *testing.T

	mu       sync.Mutex
	journals map[string]*storeio.FaultJournal
	marker   *storeio.FaultTxnMarker
	log      *TxnLog

	prepareSyncs int
	images       []txnCrashImage
}

// txnCrashImage is one whole-database directory captured at a crash ordinal.
type txnCrashImage struct {
	Label string
	Dir   string
}

// newTxnFaultController installs the multi-collection fault controller for
// collections named in names. Journals are wrapped when each collection next
// opens or remints a recovery journal; for already-open collections the
// caller must call AttachOpenJournals after upgrading them to the conditional
// format.
func newTxnFaultController(t *testing.T, names ...string) *txnFaultController {
	t.Helper()
	c := &txnFaultController{
		t:        t,
		journals: make(map[string]*storeio.FaultJournal, len(names)),
	}
	for _, name := range names {
		c.journals[name] = nil
	}

	prevFault := recoveryJournalFaultHook
	recoveryJournalFaultHook = func(rj *storeio.RecoveryJournal) {
		fj := storeio.NewFaultJournal(rj)
		c.mu.Lock()
		// Bind to the first still-nil slot so Open-time remints still wrap.
		for _, name := range names {
			if c.journals[name] == nil {
				c.journals[name] = fj
				break
			}
		}
		c.mu.Unlock()
		if prevFault != nil {
			prevFault(rj)
		}
	}
	t.Cleanup(func() { recoveryJournalFaultHook = prevFault })

	prevMint := databaseTxnAfterMintHook
	databaseTxnAfterMintHook = func(l *TxnLog) {
		c.mu.Lock()
		c.log = l
		c.marker = storeio.NewFaultTxnMarker(l.marker)
		c.mu.Unlock()
		if prevMint != nil {
			prevMint(l)
		}
	}
	t.Cleanup(func() { databaseTxnAfterMintHook = prevMint })

	prevSync := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		c.mu.Lock()
		c.prepareSyncs++
		n := c.prepareSyncs
		c.mu.Unlock()
		if prevSync != nil {
			prevSync()
		}
		_ = n
	}
	t.Cleanup(func() { recoveryJournalPostSyncHook = prevSync })

	return c
}

// AttachOpenJournals wraps already-open collection journals. Call after
// ensureConditionalJournalFormatLocked so prepare appends hit the seam.
func (c *txnFaultController) AttachOpenJournals(named map[string]*Collection) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, coll := range named {
		if coll == nil || coll.journal == nil {
			c.t.Fatalf("AttachOpenJournals: %q missing journal", name)
		}
		c.journals[name] = storeio.NewFaultJournal(coll.journal)
	}
}

// AttachMarker wraps an already-minted decision log.
func (c *txnFaultController) AttachMarker(log *TxnLog) {
	c.t.Helper()
	if log == nil || log.marker == nil {
		c.t.Fatal("AttachMarker: nil log or marker")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = log
	c.marker = storeio.NewFaultTxnMarker(log.marker)
}

// Journal returns the fault wrapper for name, or nil if not yet attached.
func (c *txnFaultController) Journal(name string) *storeio.FaultJournal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.journals[name]
}

// Marker returns the decision-log fault wrapper, or nil if not yet attached.
func (c *txnFaultController) Marker() *storeio.FaultTxnMarker {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.marker
}

// PrepareSyncs reports how many journal sync barriers the post-sync hook saw.
func (c *txnFaultController) PrepareSyncs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepareSyncs
}

// ProgramJournal sets a journal fault plan on one participant.
func (c *txnFaultController) ProgramJournal(name string, plan storeio.JournalFaultPlan) {
	c.t.Helper()
	fj := c.Journal(name)
	if fj == nil {
		c.t.Fatalf("ProgramJournal: %q not attached", name)
	}
	fj.Program(plan)
}

// ProgramMarker sets a decision-log fault plan.
func (c *txnFaultController) ProgramMarker(plan storeio.TxnMarkerFaultPlan) {
	c.t.Helper()
	fm := c.Marker()
	if fm == nil {
		c.t.Fatal("ProgramMarker: marker not attached")
	}
	fm.Program(plan)
}

// Capture clones every regular file in src into a fresh temp directory and
// records it under label. Call while the crashed process's files are still
// on disk (before Database.Close recycles journals).
func (c *txnFaultController) Capture(label, src string) txnCrashImage {
	c.t.Helper()
	img := txnCrashImage{Label: label, Dir: cloneDatabaseDir(c.t, src)}
	c.mu.Lock()
	c.images = append(c.images, img)
	c.mu.Unlock()
	return img
}

// Images returns the crash images captured so far.
func (c *txnFaultController) Images() []txnCrashImage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]txnCrashImage, len(c.images))
	copy(out, c.images)
	return out
}

// capturePrepareSyncImages installs a post-sync hook that clones the database
// directory after each prepare sync ordinal. The previous post-sync hook from
// newTxnFaultController is replaced for the duration of the commit.
func (c *txnFaultController) capturePrepareSyncImages(dir string) (restore func()) {
	c.t.Helper()
	prev := recoveryJournalPostSyncHook
	recoveryJournalPostSyncHook = func() {
		c.mu.Lock()
		c.prepareSyncs++
		n := c.prepareSyncs
		c.mu.Unlock()
		c.Capture(fmt.Sprintf("after-prepare-sync-%d", n), dir)
	}
	return func() { recoveryJournalPostSyncHook = prev }
}

// assertReopenOutcome opens img via OpenDatabase and checks every named
// collection either holds wantDoc at key "k" (committed) or lacks "k"
// (aborted). failClosed, when non-nil, expects OpenDatabase to fail with that
// error via errors.Is instead.
func assertReopenOutcome(
	t *testing.T, img string, names []string, committed bool, wantDoc string,
) {
	t.Helper()
	db, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if err != nil {
		t.Fatalf("OpenDatabase(%s): %v", img, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, name := range names {
		coll, ok := db.Collection(name)
		if !ok {
			t.Fatalf("%s: missing collection %q", img, name)
		}
		doc, found := collectionDoc(t, coll, "k")
		if committed {
			if !found || doc != wantDoc {
				t.Fatalf("%s/%s: doc=%q found=%v want committed %q",
					img, name, doc, found, wantDoc)
			}
			continue
		}
		if found {
			t.Fatalf("%s/%s: aborted reopen applied %q", img, name, doc)
		}
	}
}

// assertReopenFailClosed expects OpenDatabase(img) to fail with want via
// errors.Is.
func assertReopenFailClosed(t *testing.T, img string, want error) {
	t.Helper()
	_, err := OpenDatabase(img, DatabaseOptions{Options: txnTestOptions()})
	if err == nil {
		t.Fatalf("OpenDatabase(%s): succeeded, want %v", img, want)
	}
	if want != nil && !errors.Is(err, want) {
		t.Fatalf("OpenDatabase(%s): err=%v want %v", img, err, want)
	}
}

// mustReadTxnMarkerBytes returns the on-disk txn.vtm bytes in dir.
func mustReadTxnMarkerBytes(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, txnMarkerFilename))
	if err != nil {
		t.Fatalf("read txn.vtm: %v", err)
	}
	return data
}

// writeTxnMarkerPrefix writes the leading size bytes of full into dir/txn.vtm,
// then extends the file back to len(full) with zeros so the preallocated
// region matches a torn append that never filled the sector padding.
func writeTxnMarkerPrefix(t *testing.T, dir string, full []byte, size int) {
	t.Helper()
	if size < 0 || size > len(full) {
		t.Fatalf("prefix size %d out of range [0,%d]", size, len(full))
	}
	path := filepath.Join(dir, txnMarkerFilename)
	if err := os.WriteFile(path, full[:size], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, int64(len(full))); err != nil {
		t.Fatal(err)
	}
}

// tearMaterializationSlot writes a non-empty but incomplete capsule prefix
// into slot 0 of the collection primary at primaryPath. Open's materialization
// recovery must treat it as in-flight / non-commit before journal replay.
func tearMaterializationSlot(t *testing.T, primaryPath string) {
	t.Helper()
	f, err := os.OpenFile(primaryPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(f)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	layout, err := storeio.MutableStoreLayout(bootstrap.PageSize)
	if err != nil {
		t.Fatalf("layout: %v", err)
	}
	// Partial magic + version bytes: non-empty so recovery inspects the slot,
	// incomplete so it is not a sealed capsule.
	prefix := []byte("SJMTRL00\x00\x00\x00\x00torn-in-flight")
	off := int64(layout.MaterializationJournalOffsets[0])
	if _, err := f.WriteAt(prefix, off); err != nil {
		t.Fatalf("tear capsule: %v", err)
	}
}
