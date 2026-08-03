package durable_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestFacadeOpenFileCloseCachesTerminalPersistenceError(t *testing.T) {
	getFault, restore := durable.InstallJournalFaultForFacadeTest()
	defer restore()
	file, err := os.OpenFile(
		filepath.Join(t.TempDir(), "terminal-close.vjc"),
		os.O_CREATE|os.O_RDWR, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	collection, err := vibedb.OpenFile(file, vibedb.AdvancedOptions{
		Durability: vibedb.Buffered,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("k", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put("k", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	fault := getFault()
	if fault == nil {
		t.Fatal("journal fault seam was not installed")
	}
	fault.Program(storeio.JournalFaultPlan{
		Phase:       storeio.JournalFaultENOSPCAppend,
		AppendIndex: fault.Appends(),
	})
	flushErr := collection.Flush()
	if flushErr == nil || !fault.Faulted() {
		t.Fatalf("faulted Flush = %v, fired=%v", flushErr, fault.Faulted())
	}
	first := collection.Close()
	if first == nil || !errors.Is(first, flushErr) {
		t.Fatalf("first Close = %v, want sticky Flush error %v", first, flushErr)
	}
	if !collection.CloseCompleted() {
		t.Fatal("terminal Close did not release the facade's descriptor borrow")
	}
	if second := collection.Close(); second != first {
		t.Fatalf("second Close = %v, want cached exact error %v", second, first)
	}
}
