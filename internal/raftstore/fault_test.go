package raftstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestPersistRecordFailureIsDefiniteAndExactRetrySucceeds(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	original := store.options.ops.writeAt
	failed := false
	writeCalls := 0
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writeCalls++
		if !failed && offset >= HeaderBytes {
			failed = true
			n, _ := file.WriteAt(data[:recordPrefixBytes], offset)
			return n, io.ErrShortWrite
		}
		return original(file, data, offset)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrPersistenceDefinite) || errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("record failure = %v", err)
	}
	writesBeforeConflict := writeCalls
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1}); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed-to-empty retry = %v", err)
	}
	if writeCalls != writesBeforeConflict {
		t.Fatalf("changed-to-empty retry wrote %d times", writeCalls-writesBeforeConflict)
	}
	store.options.ops.writeAt = original
	if err := store.Persist(batch); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
}

func TestPersistReadySyncFailureBindsAttemptedPayload(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalBarrier := testRecordBarrier(store)
	originalWrite := store.options.ops.writeAt
	originalSync := store.options.ops.sync
	failed := false
	currentWrites, finalSyncs := 0, 0
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if offset < HeaderBytes {
			currentWrites++
		}
		return originalWrite(file, data, offset)
	}
	store.options.ops.recordBarrier = func(file *os.File) error {
		if !failed {
			failed = true
			return errors.New("injected record barrier")
		}
		return originalBarrier(file)
	}
	store.options.ops.sync = func(file *os.File) error {
		finalSyncs++
		return originalSync(file)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Ready sync = %v", err)
	}
	if currentWrites != 0 || finalSyncs != 0 {
		t.Fatalf("record barrier failure reached current phase: writes=%d syncs=%d", currentWrites, finalSyncs)
	}
	changed := batch
	changed.Entries = []*pb.Entry{entry(2, 2, "changed")}
	if err := store.Persist(changed); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed retry = %v", err)
	}
	store.options.ops.recordBarrier = originalBarrier
	if err := store.Persist(batch); err != nil {
		t.Fatalf("exact retry = %v", err)
	}
}

func TestPersistPartialReadyRecordDefiniteExactRetryOverwrites(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	original := store.options.ops.writeAt
	failed := false
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if !failed && offset >= HeaderBytes {
			failed = true
			n, _ := file.WriteAt(data[:137], offset)
			return n, io.ErrShortWrite
		}
		return original(file, data, offset)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrPersistenceDefinite) || errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("record failure = %v", err)
	}
	if last, err := store.LastIndex(); err != nil || last != 1 {
		t.Fatalf("read after definite record failure = %d, %v", last, err)
	}
	if got, err := store.CapacityProfile(); err != nil || got == (CapacityProfile{}) {
		t.Fatalf("capacity profile after definite record failure = %+v, %v", got, err)
	}
	store.options.ops.writeAt = original
	if err := store.Persist(batch); err != nil {
		t.Fatalf("settle exact retry: %v", err)
	}
	last, err := store.LastIndex()
	if err != nil || last != 2 {
		t.Fatalf("LastIndex = %d, %v", last, err)
	}
}

func TestPersistReadySyncUnknownExactRetrySettlesWithoutRewrite(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalBarrier := testRecordBarrier(store)
	barrierCall := 0
	store.options.ops.recordBarrier = func(file *os.File) error {
		barrierCall++
		if barrierCall == 1 {
			if err := originalBarrier(file); err != nil {
				return err
			}
			return errors.New("injected Ready sync outcome")
		}
		return originalBarrier(file)
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Ready sync = %v", err)
	}
	if _, err := store.LastIndex(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("read with unknown Ready sync = %v", err)
	}
	writes := 0
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		writes++
		return file.WriteAt(data, offset)
	}
	retryBarriers := 0
	store.options.ops.recordBarrier = func(file *os.File) error {
		retryBarriers++
		return originalBarrier(file)
	}
	if err := store.Persist(batch); err != nil {
		t.Fatalf("settle exact bytes: %v", err)
	}
	if writes != 0 || retryBarriers != 1 {
		t.Fatalf("retry rewrites=%d syncs=%d, want 0/1", writes, retryBarriers)
	}
}

func TestPersistNonemptyReadyOperationOrder(t *testing.T) {
	for _, mustSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("must-sync-%t", mustSync), func(t *testing.T) {
			_, store, _ := createTestStore(t)
			incarnation, err := store.BeginIncarnation()
			if err != nil {
				t.Fatal(err)
			}
			originalRead := store.options.ops.readAt
			originalWrite := store.options.ops.writeAt
			originalBarrier := testRecordBarrier(store)
			originalSync := store.options.ops.sync
			trace := make([]string, 0, 7)
			store.options.ops.observeNamespaceProof = func() { trace = append(trace, "proof") }
			store.options.ops.readAt = func(file *os.File, data []byte, offset int64) (int, error) {
				trace = append(trace, "slot-read")
				return originalRead(file, data, offset)
			}
			store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
				phase := "record-write"
				if offset < HeaderBytes {
					phase = "slot-write"
				}
				trace = append(trace, phase)
				return originalWrite(file, data, offset)
			}
			store.options.ops.recordBarrier = func(file *os.File) error {
				trace = append(trace, "record-barrier")
				return originalBarrier(file)
			}
			store.options.ops.sync = func(file *os.File) error {
				trace = append(trace, "final-sync")
				return originalSync(file)
			}
			before := store.SyncCount()
			batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1,
				HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: mustSync}
			if err := store.Persist(batch); err != nil {
				t.Fatal(err)
			}
			want := "[record-write proof]"
			wantSyncs := uint64(0)
			if mustSync {
				want = "[record-write record-barrier proof]"
				wantSyncs = 1
			}
			if got := fmt.Sprint(trace); got != want {
				t.Fatalf("operation trace = %s, want %s", got, want)
			}
			if syncs := store.SyncCount() - before; syncs != wantSyncs {
				t.Fatalf("durability phase count = %d, want %d", syncs, wantSyncs)
			}
		})
	}
}

func TestPersistNamespaceChangeAfterReadySyncIsUnknown(t *testing.T) {
	path, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalBarrier := testRecordBarrier(store)
	originalWrite := store.options.ops.writeAt
	originalSync := store.options.ops.sync
	currentWrites, finalSyncs := 0, 0
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if offset < HeaderBytes {
			currentWrites++
		}
		return originalWrite(file, data, offset)
	}
	store.options.ops.sync = func(file *os.File) error {
		finalSyncs++
		return originalSync(file)
	}
	store.options.ops.recordBarrier = func(file *os.File) error {
		if err := originalBarrier(file); err != nil {
			return err
		}
		return os.Rename(path, path+".moved-after-record")
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrNamespaceChanged) ||
		!errors.Is(err, ErrPersistenceUnknown) || errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("post-sync namespace change = %v", err)
	}
	if currentWrites != 0 || finalSyncs != 0 {
		t.Fatalf("post-record namespace change reached current phase: writes=%d syncs=%d", currentWrites, finalSyncs)
	}
}

func TestPersistReadySyncFailureRetainsExactPendingAttempt(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	store.options.ops.recordBarrier = func(*os.File) error {
		return errors.New("injected Ready sync failure")
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1,
		HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "x")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Ready sync failure = %v", err)
	}
	changed := batch
	changed.Entries = []*pb.Entry{entry(2, 2, "changed")}
	if err := store.Persist(changed); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed pending retry = %v", err)
	}
}

func TestBeginIncarnationUnknownIsExactlyRetryable(t *testing.T) {
	_, store, _ := createTestStore(t)
	original := store.options.ops.writeAt
	failed := false
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if !failed {
			failed = true
			n, _ := file.WriteAt(data[:99], offset)
			return n, io.ErrShortWrite
		}
		return original(file, data, offset)
	}
	if _, err := store.BeginIncarnation(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Begin failure = %v", err)
	}
	store.options.ops.writeAt = original
	incarnation, err := store.BeginIncarnation()
	if err != nil || incarnation != 1 {
		t.Fatalf("Begin retry = %d, %v", incarnation, err)
	}
	if _, err := store.BeginIncarnation(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("second Begin = %v", err)
	}
}

func TestCreatePreallocationFailureLeavesOfficialNameAbsent(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "raft.wal")
	options := testOptions()
	injected := errors.New("injected ENOSPC")
	options.ops.preallocate = func(*os.File, int64) error { return injected }
	if _, err := Create(path, testIdentity(), testKey(), testBootstrap(), options); !errors.Is(err, injected) || !errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("Create = %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("official path after failed Create = %v", err)
	}
}

func TestOpenAllocationRepairFailureReturnsNoHandleOrLease(t *testing.T) {
	path, store, options := createTestStore(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected allocation repair ENOSPC")
	failedOptions := options
	failedOptions.ops.ensureAllocated = func(*os.File, int64) error { return injected }
	if _, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), failedOptions); !errors.Is(err, injected) || !errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("Open allocation repair = %v", err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("lease retained after failed Open: %v", err)
	}
	defer reopened.Close()
}

func TestOpenNeverReusesCreateOnlyPreallocator(t *testing.T) {
	path, store, options := createTestStore(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	options.ops.preallocate = func(*os.File, int64) error { return errors.New("Create-only preallocator called by Open") }
	verified := false
	options.ops.ensureAllocated = func(*os.File, int64) error { verified = true; return nil }
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !verified {
		t.Fatal("Open skipped physical-allocation verification")
	}
}

func TestReadySequenceIncludesEmptyBatches(t *testing.T) {
	_, store, _ := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ID 2 first = %v", err)
	}
	empty := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1}
	if err := store.Persist(empty); err != nil {
		t.Fatal(err)
	}
	changed := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "changed")}}
	if err := store.Persist(changed); !errors.Is(err, ErrRetryConflict) {
		t.Fatalf("changed empty ID reuse = %v", err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(empty); !errors.Is(err, ErrInvalid) {
		t.Fatalf("regressed retry = %v", err)
	}
}

func TestOpenRequiresFreshBeginAndRejectsNoncanonicalEmptySnapshot(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("old incarnation Persist = %v", err)
	}
	newIncarnation, err := reopened.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	malformed := &pb.Snapshot{Data: []byte("hidden")}
	if err := reopened.Persist(raftmodel.PersistBatch{NodeIncarnation: newIncarnation, ReadyID: 1, Snapshot: malformed}); !errors.Is(err, ErrUnsupportedSnapshot) {
		t.Fatalf("noncanonical empty snapshot = %v", err)
	}
}

func TestParentPathRebindPoisonsEvenEmptyReady(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("renaming an open parent is not portable on Windows")
	}
	outer := t.TempDir()
	parent := filepath.Join(outer, "live")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "raft.wal")
	options := testOptions()
	store, err := Create(path, testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(outer, "moved")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1}); !errors.Is(err, ErrNamespaceChanged) {
		t.Fatalf("empty Ready after rebind = %v", err)
	}
}

func TestLeafReplacementAndAliasesPoisonEmptyReadyFence(t *testing.T) {
	tests := []struct {
		name    string
		replace func(t *testing.T, path string, size int64)
	}{
		{name: "same-size-regular", replace: func(t *testing.T, path string, size int64) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(size); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink-alias", replace: func(t *testing.T, path string, _ int64) {
			alias := path + ".saved"
			if err := os.Link(path, alias); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Base(alias), path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "foreign-hardlink", replace: func(t *testing.T, path string, size int64) {
			foreign := path + ".foreign"
			file, err := os.OpenFile(foreign, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(size); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(foreign, path); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, store, options := createTestStore(t)
			incarnation, err := store.BeginIncarnation()
			if err != nil {
				t.Fatal(err)
			}
			test.replace(t, path, options.MaxFileBytes)
			if err := store.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1}); !errors.Is(err, ErrNamespaceChanged) {
				t.Fatalf("empty Ready after leaf replacement = %v", err)
			}
			if got, err := store.CapacityProfile(); got != (CapacityProfile{}) ||
				!errors.Is(err, ErrNamespaceChanged) {
				t.Fatalf("capacity profile after namespace poison = %+v, %v", got, err)
			}
		})
	}
}

func TestCloseOpenIgnoresPartialReadyTailAndFencesOldReadyKey(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalWrite := store.options.ops.writeAt
	failed := false
	store.options.ops.writeAt = func(file *os.File, data []byte, offset int64) (int, error) {
		if !failed && offset >= HeaderBytes {
			failed = true
			written, _ := file.WriteAt(data[:137], offset)
			return written, io.ErrShortWrite
		}
		return originalWrite(file, data, offset)
	}
	oldBatch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "orphan")}}
	if err := store.Persist(oldBatch); !errors.Is(err, ErrPersistenceDefinite) {
		t.Fatalf("partial Ready = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("Open older selected image: %v", err)
	}
	defer reopened.Close()
	last, err := reopened.LastIndex()
	if err != nil || last != 1 || reopened.RecoveredTornCurrentSlot() {
		t.Fatalf("older image last=%d torn=%v err=%v", last, reopened.RecoveredTornCurrentSlot(), err)
	}
	newIncarnation, err := reopened.BeginIncarnation()
	if err != nil || newIncarnation != incarnation+1 {
		t.Fatalf("fresh incarnation=%d err=%v", newIncarnation, err)
	}
	if err := reopened.Persist(oldBatch); !errors.Is(err, ErrInvalid) {
		t.Fatalf("old Ready key resumed after Open = %v", err)
	}
}

func TestCloseOpenRecoversReadyWhoseSyncReturnedUnknown(t *testing.T) {
	path, store, options := createTestStore(t)
	incarnation, err := store.BeginIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	originalBarrier := testRecordBarrier(store)
	barrierCall := 0
	store.options.ops.recordBarrier = func(file *os.File) error {
		barrierCall++
		if err := originalBarrier(file); err != nil {
			return err
		}
		if barrierCall == 1 {
			return errors.New("injected Ready sync outcome")
		}
		return nil
	}
	batch := raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, HardState: hard(2, 2), Entries: []*pb.Entry{entry(2, 2, "selected")}, MustSync: true}
	if err := store.Persist(batch); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("Ready sync = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, testIdentity(), testBootstrap().TopologyRecoveryEpoch, testKey(), options)
	if err != nil {
		t.Fatalf("Open newer selected image: %v", err)
	}
	defer reopened.Close()
	last, err := reopened.LastIndex()
	if err != nil || last != 2 {
		t.Fatalf("newer image last=%d err=%v", last, err)
	}
	state, _, err := reopened.InitialState()
	if err != nil || state.GetTerm() != 2 || state.GetCommit() != 2 {
		t.Fatalf("newer HardState=%v err=%v", state, err)
	}
	newIncarnation, err := reopened.BeginIncarnation()
	if err != nil || newIncarnation != incarnation+1 {
		t.Fatalf("fresh incarnation=%d err=%v", newIncarnation, err)
	}
}
