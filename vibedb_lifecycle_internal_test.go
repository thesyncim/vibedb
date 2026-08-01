package vibedb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

type backendRaceResult struct {
	memoryPresent bool
	diskPresent   bool
	err           error
}

func TestFacadeDatabaseCloseRetriesUntilDurableOwnerCompletes(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "close-retry.vdb"))
	if err != nil {
		t.Fatal(err)
	}
	users := db.Collection("users")
	if _, err := users.Put("1", []byte(`{"name":"Ada"}`)); err != nil {
		t.Fatal(err)
	}
	engine, ok := db.disk.Collection("users")
	if !ok {
		t.Fatal("resolved durable collection is absent")
	}
	snapshot, err := engine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first := db.Close()
	if !errors.Is(first, storeio.ErrLeasesActive) {
		_ = snapshot.Close()
		t.Fatalf("first facade Close = %v, want %v", first, storeio.ErrLeasesActive)
	}
	if db.CloseCompleted() || users.CloseCompleted() || users.disk.Load() == nil {
		_ = snapshot.Close()
		t.Fatalf("incomplete facade close = db %v handle %v retained engine %v",
			db.CloseCompleted(), users.CloseCompleted(), users.disk.Load() != nil)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	second := db.Close()
	if second != nil {
		t.Fatalf("retry facade Close = %v", second)
	}
	if !db.CloseCompleted() || !users.CloseCompleted() || users.disk.Load() != nil {
		t.Fatalf("completed facade close = db %v handle %v retained engine %v",
			db.CloseCompleted(), users.CloseCompleted(), users.disk.Load() != nil)
	}
	if repeated := db.Close(); repeated != second {
		t.Fatalf("repeated facade Close = %v, want cached exact %v", repeated, second)
	}
}

func TestLazyReopenResolutionNeverTurnsConcurrentDatabaseCloseIntoAbsence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lazy-close.vdb")
	seed, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Collection("users").Put("1", []byte(`{"name":"Ada"}`)); err != nil {
		_ = seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	for iteration := range 64 {
		db, err := Open(path)
		if err != nil {
			t.Fatalf("iteration %d Open: %v", iteration, err)
		}
		lazy := db.Collection("users")
		start := make(chan struct{})
		resolved := make(chan backendRaceResult, 1)
		closed := make(chan error, 1)
		go func() {
			<-start
			memory, disk, resolveErr := lazy.backend(false)
			resolved <- backendRaceResult{
				memoryPresent: memory != nil,
				diskPresent:   disk != nil,
				err:           resolveErr,
			}
		}()
		go func() {
			<-start
			closed <- db.Close()
		}()
		close(start)
		result := <-resolved
		if closeErr := <-closed; closeErr != nil {
			t.Fatalf("iteration %d Close: %v", iteration, closeErr)
		}
		if !result.memoryPresent && !result.diskPresent &&
			!errors.Is(result.err, ErrClosed) {
			t.Fatalf("iteration %d resolved existing collection as absence during Close", iteration)
		}
	}
}

func TestStandaloneBackendNeverTurnsConcurrentCloseIntoAbsence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "standalone.vjc")
	for iteration := range 64 {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		collection, err := OpenFile(file, AdvancedOptions{})
		if err != nil {
			_ = file.Close()
			t.Fatalf("iteration %d OpenFile: %v", iteration, err)
		}
		start := make(chan struct{})
		resolved := make(chan backendRaceResult, 1)
		closed := make(chan error, 1)
		go func() {
			<-start
			memory, disk, resolveErr := collection.backend(false)
			resolved <- backendRaceResult{
				memoryPresent: memory != nil,
				diskPresent:   disk != nil,
				err:           resolveErr,
			}
		}()
		go func() {
			<-start
			closed <- collection.Close()
		}()
		close(start)
		result := <-resolved
		closeErr := <-closed
		fileErr := file.Close()
		if closeErr != nil || fileErr != nil {
			t.Fatalf("iteration %d Close = collection %v, file %v",
				iteration, closeErr, fileErr)
		}
		if !result.memoryPresent && !result.diskPresent &&
			!errors.Is(result.err, ErrClosed) {
			t.Fatalf("iteration %d resolved standalone backend as absence during Close", iteration)
		}
	}
}
