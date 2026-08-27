package durable_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

const sessionCrashRetryWindow = uint16(8)

// TestSessionReleaseSingleCollectionCrashAtomicity drives the exact release
// batch through every append/sync failure class exposed by the existing journal
// fault device. Reopen may select the complete retired pre-image or the complete
// released post-image, but never a state/header/slot mixture.
func TestSessionReleaseSingleCollectionCrashAtomicity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase storeio.JournalFaultPhase
	}{
		{name: "torn-append", phase: storeio.JournalFaultTornAppend},
		{name: "dropped-append", phase: storeio.JournalFaultDropAppend},
		{name: "enospc-append", phase: storeio.JournalFaultENOSPCAppend},
		{name: "sync-outcome-unknown", phase: storeio.JournalFaultSyncError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSessionReleaseCrashFixture(t, false)
			defer fixture.close()
			plan := storeio.JournalFaultPlan{Phase: tc.phase}
			if tc.phase == storeio.JournalFaultSyncError {
				plan.SyncIndex = 0
			} else {
				plan.AppendIndex = 0
			}
			fault := durable.InstallCollectionJournalFaultForSessionLifecycleTest(
				fixture.collections[0], plan,
			)
			if fault == nil {
				t.Fatal("system recovery journal did not install the test fault device")
			}
			_, _ = fixture.machine.ApplyNormal(
				sessionCrashMeta(4), fixture.release,
			)
			if !fault.Faulted() {
				t.Fatal("programmed release crash boundary did not fire")
			}
			image := cloneSessionCrashDirectory(t, fixture.dir)
			assertSessionReleaseCrashImage(t, image, false)
		})
	}

	t.Run("acknowledged-post-image", func(t *testing.T) {
		fixture := newSessionReleaseCrashFixture(t, false)
		defer fixture.close()
		if _, err := fixture.machine.ApplyNormal(
			sessionCrashMeta(4), fixture.release,
		); err != nil {
			t.Fatal(err)
		}
		image := cloneSessionCrashDirectory(t, fixture.dir)
		if outcome := assertSessionReleaseCrashImage(t, image, false); outcome != "post" {
			t.Fatalf("acknowledged release recovered %q, want post", outcome)
		}
	})
}

// TestSessionReleaseCaptureTransactionCrashAtomicity repeats the proof with a
// transition-capture participant. Release now dirties two collections and the
// synced txn.vtm decision is the sole commit point. Faults span a missing/torn
// decision and an ambiguous decision-sync result.
func TestSessionReleaseCaptureTransactionCrashAtomicity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase storeio.TxnMarkerFaultPhase
	}{
		{name: "torn-decision", phase: storeio.TxnMarkerFaultTornAppend},
		{name: "decision-write-error", phase: storeio.TxnMarkerFaultAppendError},
		{name: "decision-enospc", phase: storeio.TxnMarkerFaultENOSPCAppend},
		{name: "decision-sync-outcome-unknown", phase: storeio.TxnMarkerFaultSyncError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSessionReleaseCrashFixture(t, true)
			defer fixture.close()
			plan := storeio.TxnMarkerFaultPlan{Phase: tc.phase}
			if tc.phase == storeio.TxnMarkerFaultSyncError {
				plan.SyncIndex = 0
			} else {
				plan.AppendIndex = 0
			}
			fault := durable.InstallTxnMarkerFaultForSessionLifecycleTest(
				fixture.log, plan,
			)
			if fault == nil {
				t.Fatal("capture transactions did not mint txn.vtm before release")
			}
			_, _ = fixture.machine.ApplyNormal(
				sessionCrashMeta(4), fixture.release,
			)
			if !fault.Faulted() {
				t.Fatal("programmed release decision fault did not fire")
			}
			image := cloneSessionCrashDirectory(t, fixture.dir)
			assertSessionReleaseCrashImage(t, image, true)
		})
	}

	t.Run("acknowledged-post-image", func(t *testing.T) {
		fixture := newSessionReleaseCrashFixture(t, true)
		defer fixture.close()
		if _, err := fixture.machine.ApplyNormal(
			sessionCrashMeta(4), fixture.release,
		); err != nil {
			t.Fatal(err)
		}
		image := cloneSessionCrashDirectory(t, fixture.dir)
		if outcome := assertSessionReleaseCrashImage(t, image, true); outcome != "post" {
			t.Fatalf("acknowledged captured release recovered %q, want post", outcome)
		}
	})
}

type sessionReleaseCrashFixture struct {
	dir         string
	paths       []string
	files       []*os.File
	collections []*durable.Collection
	options     []durable.Options
	binding     replicatedstate.Binding
	bootstrap   *pb.Snapshot
	machine     *replicatedstate.Machine
	log         *durable.TxnLog
	release     []byte
}

func newSessionReleaseCrashFixture(
	t testing.TB,
	withCapture bool,
) *sessionReleaseCrashFixture {
	t.Helper()
	fixture := &sessionReleaseCrashFixture{
		dir:       t.TempDir(),
		binding:   sessionCrashBinding(),
		bootstrap: sessionCrashBootstrap(),
	}
	fixture.paths = []string{
		filepath.Join(fixture.dir, "system.vdb"),
		filepath.Join(fixture.dir, "user.vdb"),
	}
	fixture.options = []durable.Options{
		{OpaqueValues: true, MaxBatchDocuments: 2*int(sessionCrashRetryWindow) + 2},
		{},
	}
	if withCapture {
		fixture.paths = append(fixture.paths, filepath.Join(fixture.dir, "capture.vdb"))
		fixture.options = append(fixture.options, durable.Options{})
	}
	collections := make([]*durable.Collection, len(fixture.paths))
	for i := range fixture.paths {
		file, err := os.OpenFile(
			fixture.paths[i], os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		fixture.files = append(fixture.files, file)
		collections[i], err = durable.Create(file, fixture.options[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	fixture.collections = collections
	var err error
	fixture.log, err = durable.NewTxnLog(fixture.dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	machineOptions := sessionCrashMachineOptions(withCapture)
	system := sessionCrashTarget(collections[0], true)
	user := sessionCrashTarget(collections[1], false)
	fixture.machine, err = replicatedstate.Open(
		fixture.binding, fixture.bootstrap, system,
		replicatedstate.UserCollection{Name: "docs", Target: user},
		fixture.log, machineOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	if withCapture {
		capture := &sessionAtomicCapture{target: replicatedstate.TransitionCaptureTarget{
			Name: "capture", Collection: collections[2],
		}}
		if err := fixture.machine.BeginTransitionCapture(capture); err != nil {
			t.Fatal(err)
		}
	}
	open := sessionCrashCommand(fixture.binding)
	open.Kind = replication.CommandSessionOpen
	open.ClientEpoch = 0
	open.ClientSequence = 1
	open.AckThrough = 0
	open.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	open.Batches = nil
	open.Fingerprint = replication.Digest{1, 2, 3, 4}
	openBytes := sessionCrashEncode(t, open)
	if _, err := fixture.machine.ApplyNormal(sessionCrashMeta(2), openBytes); err != nil {
		t.Fatal(err)
	}
	retirement := sessionCrashCommand(fixture.binding)
	retirement.Kind = replication.CommandSessionRetire
	retirement.ClientEpoch = 2
	retirement.ClientSequence = 2
	retirement.AckThrough = 1
	retirement.Batches = nil
	retirement.Fingerprint = replication.Digest{5, 6, 7, 8}
	retirementBytes := sessionCrashEncode(t, retirement)
	if _, err := fixture.machine.ApplyNormal(
		sessionCrashMeta(3), retirementBytes,
	); err != nil {
		t.Fatal(err)
	}
	release := retirement
	release.Kind = replication.CommandSessionRelease
	fixture.release = sessionCrashEncode(t, release)
	capacity, err := fixture.machine.SessionCapacityState()
	if err != nil || capacity.Applied != 3 || capacity.SessionCount != 1 ||
		capacity.SessionSlotCount != 2 || capacity.SessionEpochHighWater != 2 {
		t.Fatalf("retired crash fixture = %+v, %v", capacity, err)
	}
	return fixture
}

func (f *sessionReleaseCrashFixture) close() {
	if f == nil {
		return
	}
	if f.log != nil {
		_ = f.log.Close()
		f.log = nil
	}
	// Machine targets own these handles, but the test deliberately tolerates a
	// sticky failure on teardown after capturing the crash image.
	for i := range f.collections {
		_ = f.collections[i].Close()
	}
	f.collections = nil
	for i := range f.files {
		_ = f.files[i].Close()
	}
	f.files = nil
}

func assertSessionReleaseCrashImage(
	t testing.TB,
	dir string,
	withCapture bool,
) string {
	t.Helper()
	paths := []string{
		filepath.Join(dir, "system.vdb"),
		filepath.Join(dir, "user.vdb"),
	}
	options := []durable.Options{
		{OpaqueValues: true, MaxBatchDocuments: 2*int(sessionCrashRetryWindow) + 2},
		{},
	}
	if withCapture {
		paths = append(paths, filepath.Join(dir, "capture.vdb"))
		options = append(options, durable.Options{})
	}
	files := make([]*os.File, len(paths))
	for i := range paths {
		var err error
		files[i], err = os.OpenFile(paths[i], os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()

	var (
		collections []*durable.Collection
		log         *durable.TxnLog
		err         error
	)
	if withCapture {
		requests := make([]durable.TransactionCollectionOpen, len(files))
		for i := range requests {
			requests[i] = durable.TransactionCollectionOpen{
				File: files[i], Options: options[i],
			}
		}
		collections, log, err = durable.OpenCollectionsWithTransactions(
			dir, durable.TxnLogOptions{}, requests,
		)
	} else {
		collections = make([]*durable.Collection, len(files))
		for i := range files {
			collections[i], err = durable.Open(files[i], options[i])
			if err != nil {
				break
			}
		}
		if err == nil {
			log, err = durable.NewTxnLog(dir, durable.TxnLogOptions{})
		}
	}
	if err != nil {
		t.Fatalf("reopen release crash image: %v", err)
	}
	defer func() {
		if log != nil {
			_ = log.Close()
		}
		for _, collection := range collections {
			if collection != nil {
				_ = collection.Close()
			}
		}
	}()

	machineOptions := sessionCrashMachineOptions(withCapture)
	if withCapture {
		machineOptions.TransitionCapture = &sessionAtomicCapture{
			target: replicatedstate.TransitionCaptureTarget{
				Name: "capture", Collection: collections[2],
			},
		}
	}
	machine, err := replicatedstate.Open(
		sessionCrashBinding(), sessionCrashBootstrap(),
		sessionCrashTarget(collections[0], true),
		replicatedstate.UserCollection{
			Name: "docs", Target: sessionCrashTarget(collections[1], false),
		},
		log, machineOptions,
	)
	if err != nil {
		t.Fatalf("open replicated release image: %v", err)
	}
	capacity, err := machine.SessionCapacityState()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	systemRows := 0
	if err := snapshot.RangeSystem(func(_, _ []byte) error {
		systemRows++
		return nil
	}); err != nil {
		_ = snapshot.Close()
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	captureRows := uint64(0)
	if withCapture {
		captureRows = collections[2].Len()
	}
	switch {
	case capacity.Applied == 3 && capacity.SessionCount == 1 &&
		capacity.SessionSlotCount == 2 && capacity.SessionEpochHighWater == 2 &&
		systemRows == 5 && (!withCapture || captureRows == 3):
		_, lookupErr := machine.LookupCompletion(sessionCrashReleaseBytes(t))
		if !errors.Is(lookupErr, replicatedstate.ErrCompletionNotFound) {
			t.Fatalf("pre-image release lookup = %v, want ErrCompletionNotFound", lookupErr)
		}
		return "pre"
	case capacity.Applied == 4 && capacity.SessionCount == 0 &&
		capacity.SessionSlotCount == 0 && capacity.SessionEpochHighWater == 2 &&
		systemRows == 2 && (!withCapture || captureRows == 4):
		_, lookupErr := machine.LookupCompletion(sessionCrashReleaseBytes(t))
		if !errors.Is(lookupErr, replicatedstate.ErrSessionReleased) {
			t.Fatalf("post-image release lookup = %v, want ErrSessionReleased", lookupErr)
		}
		return "post"
	default:
		t.Fatalf("mixed release crash image: capacity=%+v systemRows=%d captureRows=%d",
			capacity, systemRows, captureRows)
		return ""
	}
}

func sessionCrashMachineOptions(withCapture bool) replicatedstate.Options {
	collections := 2
	documents := replicatedstate.MaxDistinctMutations + 4
	if withCapture {
		collections = 3
		documents++
	}
	return replicatedstate.Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: collections,
			MaxDocuments:   documents,
			MaxBytes:       64 << 20,
		},
		MaxSessions: 1,
		RetryWindow: sessionCrashRetryWindow,
	}
}

func sessionCrashTarget(
	collection *durable.Collection,
	opaque bool,
) replicatedstate.CollectionTarget {
	target := replicatedstate.CollectionTarget{
		Collection: collection,
		Limits: replicatedstate.CollectionLimits{
			MaxKeyBytes:          collection.MaxKeyBytes(),
			MaxDocumentBytes:     collection.MaxDocumentBytes(),
			MaxDistinctMutations: collection.MaxBatchDocuments(),
			MaxBatchBytes:        collection.MaxBatchBytes(),
		},
	}
	if opaque {
		target.Validation = replicatedstate.ValidationOpaqueBinary
		return target
	}
	target.Validation = replicatedstate.ValidationDeterministicMutation
	target.ValidationDigest = [32]byte{1, 3, 3, 7}
	target.Validator = sessionCrashValidator{}
	return target
}

type sessionCrashValidator struct{}

func (sessionCrashValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func (sessionCrashValidator) ValidateDelete(_, _ []byte, _ bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

type sessionAtomicCapture struct {
	target  replicatedstate.TransitionCaptureTarget
	current uint64
	pending uint64
}

func (c *sessionAtomicCapture) Target() replicatedstate.TransitionCaptureTarget {
	return c.target
}

func (*sessionAtomicCapture) MaxEncodedBytes(
	replicatedstate.TransitionCaptureBounds,
) (int, error) {
	return 64, nil
}

func (c *sessionAtomicCapture) Begin(
	state replicatedstate.State,
	publish func(key, value []byte) error,
) error {
	if c == nil || c.target.Collection == nil || state.Applied == 0 || publish == nil {
		return replicatedstate.ErrTransitionCapture
	}
	if c.target.Collection.Len() == 0 {
		if err := publish([]byte("header"), []byte(`{"capture":true}`)); err != nil {
			return err
		}
	}
	c.current = state.Applied
	c.pending = 0
	return nil
}

func (c *sessionAtomicCapture) AppendTransition(
	dst []byte,
	transition replicatedstate.CapturedTransition,
) ([]byte, error) {
	if c == nil || transition.Applied != c.current+1 || c.pending != 0 {
		return dst, replicatedstate.ErrTransitionCapture
	}
	start := len(dst)
	dst = append(dst, `{"applied":`...)
	dst = strconv.AppendUint(dst, transition.Applied, 10)
	dst = append(dst, '}')
	c.pending = transition.Applied
	if len(dst)-start > 64 {
		return dst[:start], replicatedstate.ErrTransitionCapture
	}
	return dst, nil
}

func (c *sessionAtomicCapture) Published(
	transition replicatedstate.CapturedTransition,
) error {
	if c == nil || c.pending != transition.Applied || transition.Applied != c.current+1 {
		return replicatedstate.ErrTransitionCapture
	}
	c.current = transition.Applied
	c.pending = 0
	return nil
}

func sessionCrashBinding() replicatedstate.Binding {
	return replicatedstate.Binding{
		ClusterID: sessionCrashID(1), ClusterIncarnation: sessionCrashID(2),
		TopologyRecoveryEpoch: 3, Distribution: "dist", Shard: "shard",
		AllocationGeneration: 4, ShardIncarnation: sessionCrashID(5),
		GroupID: sessionCrashID(6), ActivePolicyGeneration: 7,
		ProtectionEpoch: 8, OwnershipEpoch: 9, SchemaGeneration: 10,
		RoutingVersion: 11, RouteGeneration: 12,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
	}
}

func sessionCrashID(seed byte) replication.ID128 {
	var id replication.ID128
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func sessionCrashBootstrap() *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{
		Data: []byte("static-bootstrap"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
}

func sessionCrashCommand(binding replicatedstate.Binding) replication.Command {
	return replication.Command{
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.ActivePolicyGeneration,
		ProtectionEpoch: binding.ProtectionEpoch, OwnershipEpoch: binding.OwnershipEpoch,
		SchemaGeneration: binding.SchemaGeneration, RoutingVersion: binding.RoutingVersion,
		RouteGeneration: binding.RouteGeneration, Tenant: []byte("tenant"),
		ClientID: sessionCrashID(77),
	}
}

func sessionCrashReleaseBytes(t testing.TB) []byte {
	t.Helper()
	retirement := sessionCrashCommand(sessionCrashBinding())
	retirement.Kind = replication.CommandSessionRelease
	retirement.ClientEpoch = 2
	retirement.ClientSequence = 2
	retirement.AckThrough = 1
	retirement.Fingerprint = replication.Digest{5, 6, 7, 8}
	return sessionCrashEncode(t, retirement)
}

func sessionCrashEncode(t testing.TB, command replication.Command) []byte {
	t.Helper()
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func sessionCrashMeta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func cloneSessionCrashDirectory(t testing.TB, source string) string {
	t.Helper()
	destination := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(destination, entry.Name()), data, 0o600,
		); err != nil {
			t.Fatal(err)
		}
	}
	return destination
}
