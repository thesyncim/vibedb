package durable

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestValidateTxnLogOptionsPure(t *testing.T) {
	valid := []TxnLogOptions{
		{},
		{Capacity: storeio.TxnMarkerMinSectorSize},
		{
			Capacity:       storeio.TxnMarkerMinSectorSize,
			SealedCapacity: true,
		},
		// Validation intentionally preserves the existing unsealed rule: exact
		// geometry is deferred until marker mint/open unless capacity is sealed.
		{Capacity: storeio.TxnMarkerMaxCapacityBytes + 1},
	}
	for _, options := range valid {
		if err := ValidateTxnLogOptions(options); err != nil {
			t.Fatalf("ValidateTxnLogOptions(%+v) = %v", options, err)
		}
	}

	invalid := []TxnLogOptions{
		{SealedCapacity: true},
		{
			Capacity:       storeio.TxnMarkerMinSectorSize + 1,
			SealedCapacity: true,
		},
		{
			Capacity: storeio.TxnMarkerMaxCapacityBytes +
				storeio.TxnMarkerMinSectorSize,
			SealedCapacity: true,
		},
	}
	for _, options := range invalid {
		if err := ValidateTxnLogOptions(options); !errors.Is(
			err, ErrSealedJournalCapacity,
		) {
			t.Fatalf(
				"ValidateTxnLogOptions(%+v) = %v, want %v",
				options, err, ErrSealedJournalCapacity,
			)
		}
	}
}

func TestTxnLogReconfigureUnmintedIsReversibleAndSideEffectFree(t *testing.T) {
	dir := t.TempDir()
	initial := TxnLogOptions{Capacity: storeio.TxnMarkerMinSectorSize}
	configured := TxnLogOptions{
		Capacity:       2 * storeio.TxnMarkerMinSectorSize,
		SealedCapacity: true,
	}
	log, err := NewTxnLog(dir, initial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	if err := log.ReconfigureUnminted(configured); err != nil {
		t.Fatalf("configure sealed profile: %v", err)
	}
	if got := log.Options(); got != configured {
		t.Fatalf("configured options = %+v, want %+v", got, configured)
	}
	if err := log.ReconfigureUnminted(initial); err != nil {
		t.Fatalf("restore initial profile: %v", err)
	}
	if got := log.Options(); got != initial {
		t.Fatalf("restored options = %+v, want %+v", got, initial)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("reconfigure created directory entries: %v", entries)
	}
}

func TestTxnLogReconfigureUnmintedProvesRegisteredDirectoryMembership(t *testing.T) {
	dir := t.TempDir()
	initial := TxnLogOptions{Capacity: storeio.TxnMarkerMinSectorSize}
	log, err := NewTxnLog(dir, initial)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	foreign := openTxnNamedCollection(
		t, t.TempDir(), "foreign", txnTestOptions(),
	)
	log.registerCollection(foreign.Collection)

	if err := log.ReconfigureUnminted(TxnLogOptions{}); !errors.Is(
		err, ErrTransactionLogDirectoryMismatch,
	) {
		t.Fatalf("reconfigure foreign member = %v, want mismatch", err)
	}
	if got := log.Options(); got != initial {
		t.Fatalf("failed reconfigure changed options to %+v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
		t.Fatalf("failed membership proof minted txn.vtm: %v", err)
	}
}

func TestTxnLogOptionsReturnsSnapshot(t *testing.T) {
	want := TxnLogOptions{Capacity: 3 * storeio.TxnMarkerMinSectorSize}
	log, err := NewTxnLog(t.TempDir(), want)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	snapshot := log.Options()
	if snapshot != want {
		t.Fatalf("Options = %+v, want %+v", snapshot, want)
	}
	snapshot.Capacity++
	snapshot.SealedCapacity = !snapshot.SealedCapacity
	if got := log.Options(); got != want {
		t.Fatalf("mutating snapshot changed retained options to %+v", got)
	}
	var nilLog *TxnLog
	if got := nilLog.Options(); got != (TxnLogOptions{}) {
		t.Fatalf("nil Options = %+v, want zero", got)
	}
}

func TestTxnLogReconfigureUnmintedRejectsDurableEvidence(t *testing.T) {
	t.Run("existing marker", func(t *testing.T) {
		dir := t.TempDir()
		initial := TxnLogOptions{Capacity: storeio.TxnMarkerMinSectorSize}
		log, err := NewTxnLog(dir, initial)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })

		marker, err := storeio.CreateTxnMarker(
			filepath.Join(dir, txnMarkerFilename), storeio.TxnMarkerOptions{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := marker.Close(); err != nil {
			t.Fatal(err)
		}
		if err := log.ReconfigureUnminted(TxnLogOptions{}); !errors.Is(
			err, ErrTransactionLogRecoveryRequired,
		) {
			t.Fatalf("reconfigure over marker = %v, want recovery required", err)
		}
		if log.marker != nil {
			t.Fatal("reconfigure adopted an existing marker")
		}
		if got := log.Options(); got != initial {
			t.Fatalf("failed reconfigure changed options to %+v", got)
		}
		if err := log.EnsureMinted(); !errors.Is(
			err, ErrTransactionLogRecoveryRequired,
		) {
			t.Fatalf("EnsureMinted over marker = %v, want recovery required", err)
		}
		if log.marker != nil {
			t.Fatal("EnsureMinted adopted an existing marker")
		}
		if log.poison == nil {
			t.Fatal("exclusive-create refusal did not poison the transaction log")
		}
		if err := log.EnsureMinted(); !errors.Is(err, ErrTxnLogPoisoned) {
			t.Fatalf("EnsureMinted retry = %v, want poisoned", err)
		}
	})

	t.Run("conditional journal", func(t *testing.T) {
		dir := t.TempDir()
		initial := TxnLogOptions{Capacity: storeio.TxnMarkerMinSectorSize}
		log, err := NewTxnLog(dir, initial)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })

		sourceDB, _, _ := openTxnDBWithAB(t)
		mustTxnUpdate2(t, sourceDB, "k", `{"n":1}`, "k", `{"n":1}`)
		source := RecoveryJournalPath(filepath.Join(
			sourceDB.Dir(), collectionFilename(t, "b"),
		))
		contents, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(
			dir, strings.Repeat("0123456789abcdef", 4)+".vjc.rjournal",
		)
		if err := os.WriteFile(destination, contents, 0o600); err != nil {
			t.Fatal(err)
		}

		if err := log.ReconfigureUnminted(TxnLogOptions{}); !errors.Is(
			err, ErrTransactionLogMissing,
		) {
			t.Fatalf("reconfigure over conditional = %v, want missing marker", err)
		}
		if got := log.Options(); got != initial {
			t.Fatalf("failed reconfigure changed options to %+v", got)
		}
		if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
			t.Fatalf("failed reconfigure minted txn.vtm: %v", err)
		}
	})
}

func TestTxnLogEnsureMintedProvesRegisteredDirectoryMembership(t *testing.T) {
	t.Run("foreign member", func(t *testing.T) {
		dir := t.TempDir()
		log, err := NewTxnLog(dir, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		foreign := openTxnNamedCollection(
			t, t.TempDir(), "foreign", txnTestOptions(),
		)
		log.registerCollection(foreign.Collection)

		if err := log.EnsureMinted(); !errors.Is(
			err, ErrTransactionLogDirectoryMismatch,
		) {
			t.Fatalf("EnsureMinted foreign member = %v, want mismatch", err)
		}
		if _, err := os.Stat(filepath.Join(dir, txnMarkerFilename)); !os.IsNotExist(err) {
			t.Fatalf("failed membership proof minted txn.vtm: %v", err)
		}
	})

	t.Run("exact local member", func(t *testing.T) {
		dir := t.TempDir()
		log, err := NewTxnLog(dir, TxnLogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		local := openTxnNamedCollection(t, dir, "local", txnTestOptions())
		log.registerCollection(local.Collection)

		if err := log.EnsureMinted(); err != nil {
			t.Fatalf("EnsureMinted local member: %v", err)
		}
		if log.marker == nil {
			t.Fatal("EnsureMinted did not retain the minted marker")
		}
		if err := log.EnsureMinted(); err != nil {
			t.Fatalf("idempotent EnsureMinted: %v", err)
		}
	})
}
