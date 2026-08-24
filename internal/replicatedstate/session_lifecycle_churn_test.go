package replicatedstate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

// This is a fixture-specific physical guardrail, not a product limit. The
// fixed page geometry stays below 1 MiB in the exercised 16/8/2-cycle matrix;
// four MiB leaves cross-filesystem slack while still catching operation-count
// proportional abandonment immediately.
const sessionLifecycleChurnFileCeiling = uint64(4 << 20)

// TestSessionLifecycleChurnRetainsOnlyTheConfiguredRing fills the retry ring,
// releases it, closes every durable handle, and reconstructs the machine from
// disk repeatedly. Operation count and reopen count never become retained-row
// dimensions: a live identity owns exactly state+header+R rows, and an exact
// release leaves only the state row and its epoch high-water fence.
func TestSessionLifecycleChurnRetainsOnlyTheConfiguredRing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window uint16
		cycles int
	}{
		{name: "window-1", window: 1, cycles: 16},
		{name: "window-8", window: 8, cycles: 8},
		{name: "window-256", window: MaxSessionRetryWindow, cycles: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newPersistentSessionLifecycleStore(t, tc.window)
			defer store.close(t)
			if _, err := store.machine.InstallSnapshot(store.bootstrap); err != nil {
				t.Fatal(err)
			}
			nextIndex := uint64(2)
			prototype := commandValue(store.binding, 1)
			var firstAllocated, firstHighWater uint64

			for cycle := 0; cycle < tc.cycles; cycle++ {
				openBytes := encodeCommand(t, sessionOpenFor(prototype))
				applyLifecycleCommand(t, store.machine, nextIndex, openBytes)
				token := nextIndex
				nextIndex++
				assertLifecycleRowsBounded(t, store, tc.window)

				// Sequence one is Open. Fill the remaining R-1 canonical slots
				// with absent deletes so only the hidden collection is dirtied.
				for sequence := uint64(2); sequence <= uint64(tc.window); sequence++ {
					command := lifecycleDeleteCommand(
						store.binding, token, sequence,
					)
					applyLifecycleCommand(
						t, store.machine, nextIndex, encodeCommand(t, command),
					)
					nextIndex++
					assertLifecycleRowsBounded(t, store, tc.window)
				}

				retirement := commandValue(store.binding, uint64(tc.window))
				retirement.ClientEpoch = token
				retirement = sessionRetirement(retirement)
				applyLifecycleCommand(
					t, store.machine, nextIndex, encodeCommand(t, retirement),
				)
				nextIndex++
				capacity, err := store.machine.SessionCapacityState()
				if err != nil {
					t.Fatal(err)
				}
				if capacity.SessionCount != 1 ||
					capacity.SessionSlotCount != uint64(tc.window) ||
					capacity.SessionEpochHighWater != token ||
					store.system.Collection.Len() != uint64(tc.window)+2 {
					t.Fatalf("saturated cycle %d = %+v rows=%d, want one R=%d ring",
						cycle, capacity, store.system.Collection.Len(), tc.window)
				}

				release := sessionRelease(retirement)
				applyLifecycleCommand(
					t, store.machine, nextIndex, encodeCommand(t, release),
				)
				releaseIndex := nextIndex
				nextIndex++
				capacity, err = store.machine.SessionCapacityState()
				if err != nil {
					t.Fatal(err)
				}
				if capacity.SessionCount != 0 || capacity.SessionSlotCount != 0 ||
					capacity.SessionEpochHighWater != token ||
					store.system.Collection.Len() != 1 {
					t.Fatalf("released cycle %d = %+v rows=%d, want fence-only image",
						cycle, capacity, store.system.Collection.Len())
				}

				reopenStarted := time.Now()
				store.reopen(t)
				reopenElapsed := time.Since(reopenStarted)
				info, statErr := store.systemFile.Stat()
				if statErr != nil {
					t.Fatal(statErr)
				}
				if info.Size() <= 0 || info.Size()%4096 != 0 ||
					uint64(info.Size()) > sessionLifecycleChurnFileCeiling {
					t.Fatalf("system main-file high-water = %d, want positive page multiple", info.Size())
				}
				allocated, measured, allocatedErr := sessionLifecycleAllocatedBytes(
					store.systemFile,
				)
				if allocatedErr != nil || measured && (allocated == 0 ||
					allocated > uint64(info.Size()) ||
					allocated > sessionLifecycleChurnFileCeiling) {
					t.Fatalf("system allocated bytes = %d measured=%t size=%d err=%v",
						allocated, measured, info.Size(), allocatedErr)
				}
				if cycle == 0 {
					firstAllocated, firstHighWater = allocated, uint64(info.Size())
				}
				t.Logf(
					"cycle=%d retry_window=%d reopen=%s main_high_water_bytes=%d allocated_bytes=%d measured=%t first_high_water_bytes=%d first_allocated_bytes=%d",
					cycle, tc.window, reopenElapsed, info.Size(), allocated, measured,
					firstHighWater, firstAllocated,
				)
				capacity, err = store.machine.SessionCapacityState()
				if err != nil {
					t.Fatal(err)
				}
				if capacity.Applied != releaseIndex || capacity.SessionCount != 0 ||
					capacity.SessionSlotCount != 0 ||
					capacity.SessionEpochHighWater != token ||
					store.system.Collection.Len() != 1 {
					t.Fatalf("reopened cycle %d = %+v rows=%d, want exact released cut",
						cycle, capacity, store.system.Collection.Len())
				}
			}
		})
	}
}

func lifecycleDeleteCommand(
	binding Binding,
	epoch uint64,
	sequence uint64,
) replication.Command {
	command := commandValue(binding, sequence-1)
	command.ClientEpoch = epoch
	command.AckThrough = 0
	command.Batches[0].Mutations = []replication.Mutation{{
		Kind: replication.MutationDelete,
		Key:  []byte("permanently-absent"),
	}}
	return command
}

func applyLifecycleCommand(
	t testing.TB,
	machine *Machine,
	index uint64,
	command []byte,
) {
	t.Helper()
	publication, err := machine.ApplyNormal(normalMeta(index), command)
	if err != nil || publication.Applied != index {
		t.Fatalf("apply lifecycle command %d = %+v, %v", index, publication, err)
	}
}

func assertLifecycleRowsBounded(
	t testing.TB,
	store *persistentSessionLifecycleStore,
	window uint16,
) {
	t.Helper()
	if rows := store.system.Collection.Len(); rows > uint64(window)+2 {
		t.Fatalf("retained system rows = %d, exceed state+header+window = %d",
			rows, uint64(window)+2)
	}
}

type persistentSessionLifecycleStore struct {
	dir            string
	systemPath     string
	userPath       string
	systemOptions  durable.Options
	userOptions    durable.Options
	machineOptions Options
	binding        Binding
	bootstrap      *pb.Snapshot
	systemFile     *os.File
	userFile       *os.File
	system         CollectionTarget
	user           CollectionTarget
	log            *durable.TxnLog
	machine        *Machine
}

func newPersistentSessionLifecycleStore(
	t testing.TB,
	window uint16,
) *persistentSessionLifecycleStore {
	t.Helper()
	systemDocuments := max(3, int(window)+2)
	store := &persistentSessionLifecycleStore{
		dir:           t.TempDir(),
		systemOptions: durable.Options{OpaqueValues: true, MaxBatchDocuments: systemDocuments},
		userOptions:   durable.Options{},
		binding:       testBinding(),
		bootstrap:     testBootstrap(),
	}
	store.systemPath = filepath.Join(store.dir, "system.vdb")
	store.userPath = filepath.Join(store.dir, "user.vdb")
	store.machineOptions = Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 2,
			MaxDocuments:   max(systemDocuments, 67),
			MaxBytes:       64 << 20,
		},
		MaxSessions: 1,
		RetryWindow: window,
	}
	store.create(t)
	return store
}

func (s *persistentSessionLifecycleStore) create(t testing.TB) {
	t.Helper()
	var err error
	s.systemFile, err = os.OpenFile(
		s.systemPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	s.userFile, err = os.OpenFile(
		s.userPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err != nil {
		t.Fatal(err)
	}
	system, err := durable.Create(s.systemFile, s.systemOptions)
	if err != nil {
		t.Fatal(err)
	}
	user, err := durable.Create(s.userFile, s.userOptions)
	if err != nil {
		t.Fatal(err)
	}
	s.system = systemTargetOf(system)
	s.user = targetOf(user)
	s.log, err = durable.NewTxnLog(s.dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.machine, err = Open(
		s.binding, s.bootstrap, s.system,
		UserCollection{Name: "docs", Target: s.user}, s.log, s.machineOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (s *persistentSessionLifecycleStore) reopen(t testing.TB) {
	t.Helper()
	s.close(t)
	var err error
	s.systemFile, err = os.OpenFile(s.systemPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	s.userFile, err = os.OpenFile(s.userPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	system, err := durable.Open(s.systemFile, s.systemOptions)
	if err != nil {
		t.Fatal(err)
	}
	user, err := durable.Open(s.userFile, s.userOptions)
	if err != nil {
		t.Fatal(err)
	}
	s.system = systemTargetOf(system)
	s.user = targetOf(user)
	s.log, err = durable.NewTxnLog(s.dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.machine, err = Open(
		s.binding, s.bootstrap, s.system,
		UserCollection{Name: "docs", Target: s.user}, s.log, s.machineOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (s *persistentSessionLifecycleStore) close(t testing.TB) {
	t.Helper()
	var errs []error
	if s.log != nil {
		errs = append(errs, s.log.Close())
		s.log = nil
	}
	if s.system.Collection != nil {
		errs = append(errs, s.system.Collection.Close())
		s.system.Collection = nil
	}
	if s.user.Collection != nil {
		errs = append(errs, s.user.Collection.Close())
		s.user.Collection = nil
	}
	if s.systemFile != nil {
		errs = append(errs, s.systemFile.Close())
		s.systemFile = nil
	}
	if s.userFile != nil {
		errs = append(errs, s.userFile.Close())
		s.userFile = nil
	}
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("close session lifecycle store: %v", err)
	}
}
