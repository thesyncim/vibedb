//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package durable

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
	"golang.org/x/sys/unix"
)

func TestFileOpenWriterLockWaitPreservesRecoveryAndClearsPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collection")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testFileStoreOptions()
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := collection.Put([]byte("key"), []byte(`{"value":7}`)); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN)
	contender, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	if _, err := Open(contender, options); !errors.Is(err, storeio.ErrWriterLocked) {
		t.Fatalf("default open: %v", err)
	}
	options.OpenWriterLockContext = context.Background()
	options.OpenWriterLockDeadline = time.Now().Add(10 * time.Millisecond)
	if _, err := Open(contender, options); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded open: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused open changed file: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options.OpenWriterLockContext, options.OpenWriterLockDeadline = ctx, time.Now().Add(time.Second)
	type result struct {
		collection *Collection
		err        error
	}
	done := make(chan result, 1)
	go func() { c, err := Open(contender, options); done <- result{c, err} }()
	select {
	case got := <-done:
		if got.collection != nil {
			_ = got.collection.Close()
		}
		t.Fatalf("open passed live owner: %v", got.err)
	case <-time.After(15 * time.Millisecond):
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	defer got.collection.Close()
	if got.collection.options.OpenWriterLockContext != nil || !got.collection.options.OpenWriterLockDeadline.IsZero() {
		t.Fatal("runtime opening policy retained by collection")
	}
	cancel()
	assertPrimaryRaw(t, got.collection, "key", []byte(`{"value":7}`), true)
	if _, err := got.collection.Put([]byte("after"), []byte(`{"value":8}`)); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointGroupWriterLockWaitDefersReplayUntilEveryMemberAdmitted(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "committed")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkpointGroupPut(t, group, 2, members, "not-certified")
	if group.CheckpointAppliedIndex() != 1 || members[0].Collection.journal.Cursor() == 0 {
		t.Fatal("fixture lacks a certified cut followed by an uncertified journal suffix")
	}
	copyDir := copyCheckpointGroupDirectory(t, dir)
	names := []string{"system", "user"}
	requests := make([]TransactionCollectionOpen, len(names))
	before := make(map[string][]byte)
	options := txnTestOptions()
	options.OpenWriterLockContext = context.Background()
	options.OpenWriterLockDeadline = time.Now().Add(15 * time.Millisecond)
	for i, name := range names {
		path := filepath.Join(copyDir, name+".vjc")
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		requests[i] = TransactionCollectionOpen{File: file, Options: options}
		for _, candidate := range []string{path, path + ".rjournal"} {
			raw, err := os.ReadFile(candidate)
			if err != nil {
				t.Fatal(err)
			}
			before[candidate] = raw
		}
	}
	owner, err := os.OpenFile(requests[1].File.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(owner.Fd()), unix.LOCK_UN)
	collections, log, reopened, err := OpenCollectionsWithCheckpointGroup(copyDir, TxnLogOptions{}, requests, names, CheckpointGroupOptions{CheckpointEvery: 8})
	if !errors.Is(err, context.DeadlineExceeded) || collections != nil || log != nil || reopened != nil {
		t.Fatalf("member lock refusal leaked recovery handles: %v", err)
	}
	for path, want := range before {
		raw, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, want) {
			t.Fatalf("lock refusal replayed %s: %v", path, err)
		}
	}
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	collections, log, reopened, err = OpenCollectionsWithCheckpointGroup(copyDir, TxnLogOptions{}, requests, names, CheckpointGroupOptions{CheckpointEvery: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer closeCheckpointGroupTestHandles(t, collections, log, reopened)
	if reopened.AppliedIndex() != 1 {
		t.Fatalf("recovered applied cut=%d", reopened.AppliedIndex())
	}
	for _, collection := range collections {
		assertPrimaryRaw(t, collection, "committed", []byte(`{"n":1}`), true)
		assertPrimaryRaw(t, collection, "not-certified", nil, false)
		if collection.options.OpenWriterLockContext != nil || !collection.options.OpenWriterLockDeadline.IsZero() {
			t.Fatal("checkpoint member retained startup context")
		}
	}
}
