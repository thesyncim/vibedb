package raftmember

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type runtimeFixture struct {
	runtime  *Runtime
	wal      *raftstore.Store
	walPath  string
	walKey   raftstore.Key
	walID    raftstore.Identity
	database *sqldriver.Database
	apply    *sqldriver.ReplicatedApply
	base     sqldriver.ReplicatedShardStoreIdentity
	applyID  sqldriver.ReplicatedApplyIdentity
	sqlPath  string
	options  raftstore.Options
}

func newRuntimeFixture(t testing.TB, seed byte, voters []uint64) runtimeFixture {
	options := testWALOptions()
	options.MaxFileBytes = 256 << 20
	options.MaxLiveBytes = 2 * raftstore.MinimumReadyLiveBytes
	return newRuntimeFixtureWithOptions(t, seed, voters, options)
}

func newRuntimeFixtureWithOptions(
	t testing.TB,
	seed byte,
	voters []uint64,
	options raftstore.Options,
) runtimeFixture {
	t.Helper()
	identity := testWALIdentity(seed)
	if len(voters) == 0 {
		voters = []uint64{identity.MemberID}
	}
	walPath := filepath.Join(t.TempDir(), "runtime.wal")
	key := testWALKey()
	index, term := uint64(1), uint64(1)
	wal, err := raftstore.Create(walPath, identity, key, raftstore.Bootstrap{
		TopologyRecoveryEpoch: testTopologyRecoveryEpoch,
		Snapshot: &pb.Snapshot{
			Data: []byte("raftmember-runtime-bootstrap"),
			Metadata: &pb.SnapshotMetadata{
				Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: voters},
			},
		},
	}, options)
	if err != nil {
		t.Fatalf("create runtime WAL: %v", err)
	}
	sqlPath, database, _ := prepareSQLRoot(t, identity, "runtime")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind runtime SQL", err)
	if err != nil {
		t.Fatalf("bind runtime SQL: %v", err)
	}
	applyOptions := testApplyOptions()
	apply, applyID, err := OpenPreparedApply(wal, database, authority, base, applyOptions)
	skipIfStrictAllocationUnsupported(t, "open runtime apply", err)
	if err != nil {
		t.Fatalf("open runtime apply: %v", err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatalf("initialize runtime apply: %v", err)
	}
	runtime, err := AdoptRuntime(wal, database, apply)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatalf("adopt runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return runtimeFixture{
		runtime: runtime, wal: wal, walPath: walPath, walKey: key, walID: identity,
		database: database, apply: apply, base: base,
		applyID: applyID, sqlPath: sqlPath, options: options,
	}
}

func drainRuntime(t testing.TB, runtime *Runtime, send func(OutboundMessage) error) {
	t.Helper()
	if send == nil {
		send = func(OutboundMessage) error { return nil }
	}
	var workspace ReadyWorkspace
	for step := 0; step < 10000; step++ {
		result, err := runtime.DriveReady(&workspace, send, settleTestApplied)
		if err != nil {
			t.Fatalf("DriveReady step %d: %v", step, err)
		}
		if !result.Progressed() {
			return
		}
	}
	t.Fatal("Runtime Ready drain did not converge")
}

func settleTestApplied(AppliedBatch) error { return nil }

func TestRuntimeQuiesceSQLGenerationRetainsRaftAndWAL(t *testing.T) {
	fixture := newRuntimeFixture(t, 248, nil)
	runtime := fixture.runtime
	if err := runtime.ConfigureWALGeneration(WALGenerationDriverOptions{
		IntervalTicks: 10, Key: fixture.walKey,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.InstallSQLGeneration(
		fixture.database, fixture.apply, fixture.base, fixture.applyID,
	); !errors.Is(err, ErrSchemaGenerationSwap) {
		t.Fatalf("install before quiesce error=%v", err)
	}
	if err := runtime.QuiesceSQLGeneration(); err != nil {
		t.Fatal(err)
	}
	if !runtime.schemaGenerationQuiesced || runtime.apply != nil || runtime.database != nil ||
		runtime.node == nil || runtime.wal == nil || runtime.walGeneration != nil ||
		runtime.schemaWALResume == nil {
		t.Fatalf("quiesced runtime apply=%p database=%p node=%p wal=%p",
			runtime.apply, runtime.database, runtime.node, runtime.wal)
	}
	if err := runtime.Propose([]byte{1}); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("proposal after quiesce error=%v", err)
	}
}

func TestRuntimeIdentityPublishesPortableMachineManifest(t *testing.T) {
	fixture := newRuntimeFixture(t, 209, nil)
	profile, err := fixture.apply.CapacityQualificationProfile()
	if err != nil {
		t.Fatal(err)
	}
	identity := fixture.runtime.Identity()
	if identity.RelationManifestDigest == ([32]byte{}) ||
		identity.RelationManifestDigest != profile.RelationManifestDigest {
		t.Fatalf(
			"runtime manifest = %x, machine profile = %x",
			identity.RelationManifestDigest, profile.RelationManifestDigest,
		)
	}
	if identity.RelationManifestDigest == fixture.base.RelationManifestDigest {
		t.Fatal("runtime advertised the replica-local SQL catalog manifest")
	}
}

func openRuntimeTestSession(
	t testing.TB,
	runtime *Runtime,
	apply *sqldriver.ReplicatedApply,
	base sqldriver.ReplicatedShardStoreIdentity,
) uint64 {
	t.Helper()
	command := testApplySessionOpen(base)
	if err := runtime.Propose(command); err != nil {
		t.Fatalf("propose session open: %v", err)
	}
	drainRuntime(t, runtime, nil)
	lookup, err := apply.LookupCompletion(command)
	if err != nil {
		t.Fatalf("lookup session open: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultSessionOpened ||
		completion.ClientEpoch == 0 || completion.ClientSequence != 1 ||
		completion.AppliedSequence != completion.ClientEpoch {
		t.Fatalf("runtime session-open completion = %+v, %v", completion, err)
	}
	return completion.ClientEpoch
}

func TestAdoptRuntimeOwnsExactPairAndMintsOneIncarnation(t *testing.T) {
	identity := testWALIdentity(210)
	_, wal, _, _ := createWAL(t, identity)
	_, database, _ := prepareSQLRoot(t, identity, "runtime-owner")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind runtime owner SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	applyOptions := testApplyOptions()
	applyOptions.MaxSessions = 2
	apply, _, err := OpenPreparedApply(wal, database, authority, base, applyOptions)
	skipIfStrictAllocationUnsupported(t, "open runtime owner apply", err)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}

	session, err := database.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := AdoptRuntime(wal, database, apply); got != nil || !errors.Is(err, ErrRuntimeOwnership) {
		t.Fatalf("adopt with live session = %v, %v", got, err)
	}
	if wal.CurrentIncarnation() != 0 {
		t.Fatalf("read-only ownership failure minted incarnation %d", wal.CurrentIncarnation())
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	runtime, err := AdoptRuntime(wal, database, apply)
	if err != nil {
		t.Fatal(err)
	}
	got := runtime.Identity()
	if got.MemberID != identity.MemberID || got.StoreID != identity.StoreID ||
		got.Group.GroupID != identity.GroupID || got.NodeIncarnation != 1 {
		t.Fatalf("Runtime identity = %+v", got)
	}
	if wal.CurrentIncarnation() != 1 {
		t.Fatalf("durable incarnation = %d, want 1", wal.CurrentIncarnation())
	}
	if session, err := database.NewSession(t.Context()); session != nil ||
		!errors.Is(err, sqldriver.ErrDatabaseClosed) {
		t.Fatalf("runtime-owned database session = %v, %v", session, err)
	}
	if _, err := AdoptRuntime(wal, database, apply); err == nil {
		t.Fatal("second adoption unexpectedly succeeded")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("idempotent Close: %v", err)
	}
	if err := runtime.Tick(); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("Tick after Close = %v", err)
	}
}

func TestAdoptStagedRuntimeResumesExactPristineIncarnationOnly(t *testing.T) {
	identity := testWALIdentity(211)
	walPath, wal, _, options := createWAL(t, identity)
	sqlPath, database, _ := prepareSQLRoot(t, identity, "staged-runtime-retry")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind staged runtime SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	apply, applyIdentity, err := OpenPreparedApply(wal, database, authority, base, testApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	first, err := AdoptStagedRuntime(wal, database, apply, 1)
	if err != nil || first.Identity().NodeIncarnation != 1 {
		t.Fatalf("first staged adoption=%v err=%v", first, err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	reopen := func() (*raftstore.Store, *sqldriver.Database, *sqldriver.ReplicatedApply) {
		t.Helper()
		reopenedWAL, openErr := raftstore.Open(walPath, identity,
			testTopologyRecoveryEpoch, testWALKey(), options)
		if openErr != nil {
			t.Fatal(openErr)
		}
		reopenedDB, reopenedApply, openErr := OpenBoundSQLWithApply(
			sqlPath, reopenedWAL, authority, base, applyIdentity,
		)
		if openErr != nil {
			_ = reopenedWAL.Close()
			t.Fatal(openErr)
		}
		return reopenedWAL, reopenedDB, reopenedApply
	}
	reopenedWAL, reopenedDB, reopenedApply := reopen()
	retried, err := AdoptStagedRuntime(reopenedWAL, reopenedDB, reopenedApply, 1)
	if err != nil || retried.Identity().NodeIncarnation != 1 || reopenedWAL.CurrentIncarnation() != 1 {
		t.Fatalf("exact staged retry=%v incarnation=%d err=%v", retried, reopenedWAL.CurrentIncarnation(), err)
	}
	// A merely observed empty Ready is deliberately not a durable WAL event.
	// Campaign first so this retry persists real HardState/log progress; the
	// next process must then reject staged adoption at the consumed target.
	if err = retried.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, retried, nil)
	if err = retried.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedWAL, reopenedDB, reopenedApply = reopen()
	if got, retryErr := AdoptStagedRuntime(reopenedWAL, reopenedDB, reopenedApply, 1); got != nil || retryErr == nil {
		t.Fatalf("persisted-Ready staged retry=%v err=%v", got, retryErr)
	}
	// Failed adoption owns and closes all three handles. Reopen the WAL to prove
	// the rejection neither reminted nor otherwise changed the durable target.
	proofWAL, err := raftstore.Open(walPath, identity,
		testTopologyRecoveryEpoch, testWALKey(), options)
	if err != nil {
		t.Fatal(err)
	}
	if proofWAL.CurrentIncarnation() != 1 {
		t.Fatalf("rejected retry changed durable incarnation %d", proofWAL.CurrentIncarnation())
	}
	if err = proofWAL.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeCampaignProposalAndReadyOrdering(t *testing.T) {
	fixture := newRuntimeFixture(t, 220, nil)
	status, err := fixture.runtime.Status()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := fixture.runtime.WALRetentionInput()
	if err != nil {
		t.Fatal(err)
	}
	if status.CheckpointApplied != fence || fence != fixture.apply.CheckpointAppliedIndex() ||
		fence > status.Applied {
		t.Fatalf("runtime checkpoint fence = status %+v fence %d apply %d",
			status, fence, fixture.apply.CheckpointAppliedIndex())
	}
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.Tick(); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("Tick with uncaptured Ready = %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)

	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	key, _ := orderedkey.AppendJSONString(nil, []byte(`"a"`), orderedkey.Ascending)
	command := testApplyCommand(fixture.base, epoch, 2, key, []byte(`{"id":"a","value":1}`))
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	lookup, err := fixture.apply.LookupCompletion(command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("completion = %+v, %v", completion, err)
	}
}

func TestRuntimeBatchesNormalProposalsIntoOneReady(t *testing.T) {
	fixture := newRuntimeFixture(t, 221, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	command := testApplySessionOpen(fixture.base)
	lastBefore, err := fixture.wal.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	syncsBefore := fixture.wal.SyncCount()
	remainingBefore := fixture.wal.RemainingBytes()
	for entry := 0; entry < raftmodel.MaxProposalBatchEntries; entry++ {
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatalf("Propose(%d) error = %v", entry, err)
		}
	}
	if fixture.wal.SyncCount() != syncsBefore || fixture.wal.RemainingBytes() != remainingBefore {
		t.Fatalf(
			"proposal admission mutated WAL: syncs=%d->%d remaining=%d->%d",
			syncsBefore, fixture.wal.SyncCount(), remainingBefore, fixture.wal.RemainingBytes(),
		)
	}
	if fixture.runtime.proposalBatchEntries != raftmodel.MaxProposalBatchEntries {
		t.Fatalf("proposal batch entries = %d", fixture.runtime.proposalBatchEntries)
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, raftmodel.ErrReadyPending) ||
		!errors.Is(err, raftmodel.ErrAdmissionBound) {
		t.Fatalf("proposal beyond entry limit = %v", err)
	}
	if err := fixture.runtime.ReadIndex([]byte("after-proposals")); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("ReadIndex across proposal batch = %v", err)
	}
	change := &pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: runtimeUint64Ptr(2),
		Context: make([]byte, MembershipTransitionDigestBytes)}
	if err := fixture.runtime.ProposeConfChange(change); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("ProposeConfChange across proposal batch = %v", err)
	}

	captured, err := fixture.runtime.DriveReady(new(ReadyWorkspace), nil, settleTestApplied)
	if err != nil || captured.Kind != DriveCaptured {
		t.Fatalf("DriveReady(capture) = %+v, %v", captured, err)
	}
	if fixture.runtime.proposalBatchEntries != 0 || fixture.runtime.proposalBatchBytes != 0 {
		t.Fatalf(
			"proposal counters after capture = %d/%d",
			fixture.runtime.proposalBatchEntries, fixture.runtime.proposalBatchBytes,
		)
	}
	progress, ok := fixture.runtime.node.CurrentReady()
	if !ok || progress.ReadyID != captured.ReadyID {
		t.Fatalf("captured Ready progress = %+v, %t", progress, ok)
	}
	persisted, err := fixture.runtime.DriveReady(new(ReadyWorkspace), nil, settleTestApplied)
	if err != nil || persisted.Kind != DrivePersisted || persisted.ReadyID != captured.ReadyID {
		t.Fatalf("DriveReady(persist) = %+v, %v; capture=%+v", persisted, err, captured)
	}
	lastAfter, err := fixture.wal.LastIndex()
	if err != nil || lastAfter-lastBefore != uint64(raftmodel.MaxProposalBatchEntries) {
		t.Fatalf("persisted log range = %d..%d, err=%v", lastBefore, lastAfter, err)
	}
	if syncs := fixture.wal.SyncCount() - syncsBefore; syncs != 1 {
		t.Fatalf("batched Ready sync count = %d, want 1", syncs)
	}
	recordBytes := remainingBefore - fixture.wal.RemainingBytes()
	if recordBytes <= 0 || recordBytes > int64(fixture.options.MaxRecordBytes) {
		t.Fatalf("batched Ready record bytes = %d", recordBytes)
	}
	drainRuntime(t, fixture.runtime, nil)

	if err := fixture.runtime.ReadIndex([]byte("before-proposal")); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("proposal across ReadIndex barrier = %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.ProposeConfChange(change); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("proposal across configuration barrier = %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)
}

func TestRuntimeMultiEntryProposalAdmissionIsAllOrNone(t *testing.T) {
	fixture := newRuntimeFixture(t, 224, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	command := testApplySessionOpen(fixture.base)
	lastBefore, err := fixture.wal.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if err = fixture.runtime.ProposeBatch([][]byte{command, nil}); !errors.Is(err, raftmodel.ErrAdmissionBound) {
		t.Fatalf("malformed ProposeBatch() error = %v", err)
	}
	if fixture.runtime.proposalBatchEntries != 0 || fixture.runtime.proposalBatchBytes != 0 {
		t.Fatalf("refused batch counters = %d/%d", fixture.runtime.proposalBatchEntries, fixture.runtime.proposalBatchBytes)
	}
	if last, lastErr := fixture.wal.LastIndex(); lastErr != nil || last != lastBefore {
		t.Fatalf("refused batch WAL index = %d, %v; want %d", last, lastErr, lastBefore)
	}
	if err = fixture.runtime.ProposeBatch([][]byte{command, command}); err != nil {
		t.Fatalf("valid ProposeBatch() error = %v", err)
	}
	if fixture.runtime.proposalBatchEntries != 2 ||
		fixture.runtime.proposalBatchBytes != int64(2*len(command)) {
		t.Fatalf("admitted batch counters = %d/%d", fixture.runtime.proposalBatchEntries, fixture.runtime.proposalBatchBytes)
	}
	captured, err := fixture.runtime.DriveReady(new(ReadyWorkspace), nil, settleTestApplied)
	if err != nil || captured.Kind != DriveCaptured {
		t.Fatalf("DriveReady(capture) = %+v, %v", captured, err)
	}
	persisted, err := fixture.runtime.DriveReady(new(ReadyWorkspace), nil, settleTestApplied)
	if err != nil || persisted.Kind != DrivePersisted || persisted.ReadyID != captured.ReadyID {
		t.Fatalf("DriveReady(persist) = %+v, %v", persisted, err)
	}
	if last, lastErr := fixture.wal.LastIndex(); lastErr != nil || last-lastBefore != 2 {
		t.Fatalf("multi-entry persisted range = %d..%d, %v", lastBefore, last, lastErr)
	}
}

func TestRuntimeProposalBatchByteLimitIsExact(t *testing.T) {
	fixture := newRuntimeFixture(t, 222, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	command := testApplySessionOpen(fixture.base)
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatal(err)
	}
	fixture.runtime.proposalBatchBytes = raftmodel.MaxProposalBatchBytes - int64(len(command))
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatalf("Propose at exact byte limit = %v", err)
	}
	if fixture.runtime.proposalBatchBytes != raftmodel.MaxProposalBatchBytes {
		t.Fatalf("proposal batch bytes = %d", fixture.runtime.proposalBatchBytes)
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, raftmodel.ErrReadyPending) ||
		!errors.Is(err, raftmodel.ErrAdmissionBound) {
		t.Fatalf("Propose beyond byte limit = %v", err)
	}
	if fixture.runtime.proposalBatchEntries != 2 ||
		fixture.runtime.proposalBatchBytes != raftmodel.MaxProposalBatchBytes {
		t.Fatalf(
			"proposal counters changed on refusal = %d/%d",
			fixture.runtime.proposalBatchEntries, fixture.runtime.proposalBatchBytes,
		)
	}
	drainRuntime(t, fixture.runtime, nil)
}

func TestRuntimeOversizedFirstProposalOccupiesBatchAlone(t *testing.T) {
	fixture := newRuntimeFixture(t, 223, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)

	key, ok := orderedkey.AppendJSONString(nil, []byte(`"large"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode oversized proposal key")
	}
	document := make([]byte, 0, int(raftmodel.MaxProposalBatchBytes)+64)
	document = append(document, []byte(`{"id":"large","value":"`)...)
	document = append(document, bytes.Repeat([]byte{'x'}, int(raftmodel.MaxProposalBatchBytes))...)
	document = append(document, '"', '}')
	command := testApplyCommand(fixture.base, epoch, 2, key, document)
	if len(command) <= int(raftmodel.MaxProposalBatchBytes) || len(command) > raftmodel.MaxProposalBytes {
		t.Fatalf("oversized command bytes = %d", len(command))
	}
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatalf("Propose(oversized first) = %v", err)
	}
	if fixture.runtime.proposalBatchEntries != 1 ||
		fixture.runtime.proposalBatchBytes != int64(len(command)) {
		t.Fatalf(
			"oversized proposal counters = %d/%d",
			fixture.runtime.proposalBatchEntries, fixture.runtime.proposalBatchBytes,
		)
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, raftmodel.ErrReadyPending) ||
		!errors.Is(err, raftmodel.ErrAdmissionBound) {
		t.Fatalf("second proposal after oversized first = %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)
}

func TestRuntimeFailureClearsProposalBatchCounters(t *testing.T) {
	runtime := &Runtime{
		proposalBatchEntries: raftmodel.MaxProposalBatchEntries,
		proposalBatchBytes:   raftmodel.MaxProposalBatchBytes,
	}
	if err := runtime.fail(raftmodel.ErrAdmissionBound); !errors.Is(err, ErrRuntimeFailed) {
		t.Fatalf("fail() = %v", err)
	}
	if runtime.proposalBatchEntries != 0 || runtime.proposalBatchBytes != 0 {
		t.Fatalf(
			"failed proposal counters = %d/%d",
			runtime.proposalBatchEntries, runtime.proposalBatchBytes,
		)
	}
}

func TestRuntimeRestartsFromCertifiedImmutableBaseAndAppendsNormally(t *testing.T) {
	fixture := newRuntimeFixture(t, 219, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	key, _ := orderedkey.AppendJSONString(nil, []byte(`"base"`), orderedkey.Ascending)
	command := testApplyCommand(fixture.base, epoch, 2, key, []byte(`{"id":"base","value":1}`))
	if err := fixture.runtime.Propose(command); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	before, err := fixture.runtime.Publication()
	if err != nil || before.Applied <= 1 {
		t.Fatalf("source publication = %+v, %v", before, err)
	}
	cut, err := fixture.apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(
		&artifact, cut, replicatedstate.SnapshotArtifactOptions{},
	)
	closeErr := cut.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("snapshot artifact = %v, close=%v", err, closeErr)
	}
	static, err := fixture.wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	base, err := replicatedstate.BuildSnapshotBase(manifest, static)
	if err != nil || base.GetMetadata().GetIndex() != before.Applied {
		t.Fatalf("BuildSnapshotBase = %v, %v", base, err)
	}
	identity := fixture.wal.Identity()
	activation := sqldriver.ReplicatedChildActivation{
		Apply: fixture.apply, ApplyIdentity: fixture.applyID,
		SnapshotBase: base, ArtifactManifest: manifest,
	}
	wrong := activation
	wrong.ArtifactManifest.Digest[0] ^= 0xff
	rejectedPath := filepath.Join(t.TempDir(), "rejected.wal")
	if got, err := CreateStagedChildWAL(
		rejectedPath, identity, testWALKey(), testTopologyRecoveryEpoch,
		testAuthorityProfile(), fixture.base, wrong, fixture.options,
	); got != nil || !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("mismatched staged WAL = %v, %v", got, err)
	}
	if _, err := os.Stat(rejectedPath); !os.IsNotExist(err) {
		t.Fatalf("mismatched staged WAL created namespace: %v", err)
	}

	replacementPath := filepath.Join(t.TempDir(), "replacement.wal")
	newWAL, err := CreateStagedChildWAL(
		replacementPath, identity, testWALKey(),
		testTopologyRecoveryEpoch, testAuthorityProfile(), fixture.base, activation,
		fixture.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = newWAL.Close(); err != nil {
		t.Fatal(err)
	}
	newWAL, err = OpenOrCreateStagedChildWAL(
		replacementPath, identity, testWALKey(), testTopologyRecoveryEpoch,
		testAuthorityProfile(), fixture.base, activation, fixture.options,
	)
	if err != nil {
		t.Fatalf("settle existing staged WAL: %v", err)
	}
	if err := fixture.runtime.Close(); err != nil {
		_ = newWAL.Close()
		t.Fatal(err)
	}
	reopenedDB, reopenedApply, err := OpenBoundSQLWithApply(
		fixture.sqlPath, newWAL, testAuthorityProfile(), fixture.base, fixture.applyID,
	)
	if err != nil {
		_ = newWAL.Close()
		t.Fatal(err)
	}
	restarted, err := AdoptRuntime(newWAL, reopenedDB, reopenedApply)
	if err != nil {
		if restarted != nil {
			_ = restarted.Close()
		}
		t.Fatal(err)
	}
	defer restarted.Close()
	after, err := restarted.Publication()
	if err != nil || after.Applied != before.Applied ||
		after.DataChainDigest != before.DataChainDigest ||
		after.ReplicaSetVersion != before.ReplicaSetVersion ||
		after.ConfState.Equivalent(before.ConfState) != nil {
		t.Fatalf("restarted publication = %+v, %v; before %+v", after, err, before)
	}
	retained, err := restarted.SnapshotBaseCertificate()
	if err != nil || retained.Digest == ([32]byte{}) ||
		retained.Manifest.State.Applied != before.Applied {
		t.Fatalf("retained snapshot-base certificate = %+v, %v", retained, err)
	}
	retainedState, err := restarted.SnapshotState()
	if err != nil || retainedState.SnapshotBaseDigest != retained.Digest {
		t.Fatalf("retained snapshot-base/state mismatch = %x/%x, %v",
			retained.Digest, retainedState.SnapshotBaseDigest, err)
	}
	drainRuntime(t, restarted, nil)
	if err := restarted.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, restarted, nil)
	key, _ = orderedkey.AppendJSONString(nil, []byte(`"tail"`), orderedkey.Ascending)
	tail := testApplyCommand(fixture.base, epoch, 3, key, []byte(`{"id":"tail","value":2}`))
	if err := restarted.Propose(tail); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, restarted, nil)
	final, err := restarted.Publication()
	if err != nil || final.Applied <= after.Applied {
		t.Fatalf("ordinary suffix publication = %+v, %v; base %+v", final, err, after)
	}
}

func TestRuntimeConfigurationAndReadControlPorts(t *testing.T) {
	fixture := newRuntimeFixture(t, 221, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	peer := fixture.runtime.identity.MemberID + 1
	change := &pb.ConfChange{
		Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: runtimeUint64Ptr(peer),
		Context: make([]byte, MembershipTransitionDigestBytes),
	}
	if err := fixture.runtime.ProposeConfChange(change); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.ProposeConfChange(change); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("second configuration proposal before drain = %v", err)
	}
	change.NodeId = runtimeUint64Ptr(peer + 1)
	drainRuntime(t, fixture.runtime, nil)
	publication, err := fixture.runtime.Publication()
	if err != nil {
		t.Fatal(err)
	}
	if publication.ReplicaSetVersion != publication.Applied ||
		len(publication.ConfState.GetLearners()) != 1 ||
		publication.ConfState.GetLearners()[0] != peer {
		t.Fatalf("configuration publication = %+v", publication)
	}

	context := []byte("linearizable-read")
	if err := fixture.runtime.ReadIndex(context); err != nil {
		t.Fatal(err)
	}
	context[0] = 'X'
	var outcomes []raftmodel.ReadOutcome
	for step := 0; step < 1000; step++ {
		result, driveErr := fixture.runtime.DriveReady(
			new(ReadyWorkspace), func(OutboundMessage) error { return nil }, settleTestApplied,
		)
		if driveErr != nil {
			t.Fatalf("DriveReady step %d: %v", step, driveErr)
		}
		outcomes = append(outcomes, result.ReadOutcomes...)
		if !result.Progressed() {
			break
		}
	}
	if len(outcomes) != 1 || outcomes[0].Err != nil ||
		string(outcomes[0].Barrier.Context) != "linearizable-read" ||
		outcomes[0].Barrier.Index == 0 || outcomes[0].Barrier.Term == 0 ||
		outcomes[0].Barrier.Incarnation != fixture.runtime.identity.NodeIncarnation {
		t.Fatalf("read outcomes = %+v", outcomes)
	}
	publication, err = fixture.runtime.Publication()
	if err != nil || publication.Applied < outcomes[0].Barrier.Index {
		t.Fatalf("published cut %+v behind outcome %+v: %v", publication, outcomes[0], err)
	}
}

func TestRuntimeReconstructsCanonicalDurablePromotionBeforeApply(t *testing.T) {
	fixture := newRuntimeFixture(t, 222, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	target := fixture.runtime.identity.MemberID + 1
	digest := MembershipTransitionDigest(fixture.runtime.identity.Group,
		[16]byte{1}, 2, 3, fixture.runtime.identity.MemberID, target)
	if err := fixture.runtime.ProposeConfChange(&pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: runtimeUint64Ptr(target),
		Context: append([]byte(nil), digest[:]...),
	}); err != nil {
		t.Fatal(err)
	}
	workspace := new(ReadyWorkspace)
	var proof DurablePromotionProof
	var found bool
	var err error
	for step := 0; step < 32 && !found; step++ {
		result, driveErr := fixture.runtime.DriveReady(workspace, nil, settleTestApplied)
		if driveErr != nil {
			t.Fatalf("drive step %d = %+v, %v", step, result, driveErr)
		}
		if !result.Progressed() {
			t.Fatalf("runtime became idle before durable promotion at step %d", step)
		}
		if result.Kind == DriveEntry {
			t.Fatal("promotion applied before its durable commit witness was observed")
		}
		if result.Kind != DrivePersisted {
			continue
		}
		proof, found, err = fixture.runtime.DurablePromotion(target)
		if err != nil {
			t.Fatal(err)
		}
	}
	before, err := fixture.runtime.Publication()
	if err != nil || !found || proof.TargetMember != target ||
		proof.Version <= before.ReplicaSetVersion || proof.AuthorizationDigest != digest {
		t.Fatalf("proof=%+v found=%t publication=%+v err=%v", proof, found, before, err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_ = fixture.runtime.node.PublishedApplied()
	}); allocations != 0 {
		t.Fatalf("published applied coordinate allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _, _ = fixture.runtime.DurablePromotion(target)
	}); allocations != 0 {
		t.Fatalf("cached durable promotion allocations = %v, want 0", allocations)
	}
}

func TestRuntimeRejectsDurableUncommittedPromotionAfterRestart(t *testing.T) {
	identity := testWALIdentity(223)
	peer := identity.MemberID + 1
	target := peer + 1
	fixture := newRuntimeFixture(t, 223, []uint64{identity.MemberID, peer})
	drainRuntime(t, fixture.runtime, nil)
	electRuntimeWithPeer(t, fixture.runtime, identity.MemberID, peer)
	digest := MembershipTransitionDigest(fixture.runtime.identity.Group,
		[16]byte{2}, 3, 4, identity.MemberID, target)
	if err := fixture.runtime.ProposeConfChange(&pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: runtimeUint64Ptr(target),
		Context: append([]byte(nil), digest[:]...),
	}); err != nil {
		t.Fatal(err)
	}
	var promotion *pb.Message
	drainRuntime(t, fixture.runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgApp && outbound.To == peer &&
			len(outbound.Message.GetEntries()) != 0 {
			promotion = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if promotion == nil {
		t.Fatal("promotion produced no durable append")
	}
	lastPromotion := promotion.GetEntries()[len(promotion.GetEntries())-1].GetIndex()
	commit, err := fixture.wal.DurableCommit()
	if err != nil || lastPromotion <= commit {
		t.Fatalf("promotion index=%d durable commit=%d err=%v", lastPromotion, commit, err)
	}
	if proof, found, err := fixture.runtime.DurablePromotion(target); err != nil || found {
		t.Fatalf("uncommitted proof=%+v found=%t err=%v", proof, found, err)
	}
	if err = fixture.runtime.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedWAL, err := raftstore.Open(fixture.walPath, fixture.walID,
		testTopologyRecoveryEpoch, fixture.walKey, fixture.options)
	if err != nil {
		t.Fatal(err)
	}
	reopenedDB, reopenedApply, err := OpenBoundSQLWithApply(fixture.sqlPath,
		reopenedWAL, testAuthorityProfile(), fixture.base, fixture.applyID)
	if err != nil {
		_ = reopenedWAL.Close()
		t.Fatal(err)
	}
	restarted, err := AdoptRuntime(reopenedWAL, reopenedDB, reopenedApply)
	if err != nil {
		if restarted != nil {
			_ = restarted.Close()
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if proof, found, err := restarted.DurablePromotion(target); err != nil || found {
		t.Fatalf("reopened uncommitted proof=%+v found=%t err=%v", proof, found, err)
	}
}

func electRuntimeWithPeer(t testing.TB, runtime *Runtime, local, peer uint64) {
	t.Helper()
	if err := runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	var preVote *pb.Message
	drainRuntime(t, runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgPreVote && outbound.To == peer {
			preVote = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if preVote == nil {
		t.Fatal("campaign produced no pre-vote")
	}
	if err := runtime.StepMessage(&pb.Message{Type: pb.MsgPreVoteResp.Enum(),
		From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(local),
		Term: runtimeUint64Ptr(preVote.GetTerm())}); err != nil {
		t.Fatal(err)
	}
	var vote *pb.Message
	drainRuntime(t, runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgVote && outbound.To == peer {
			vote = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if vote == nil {
		t.Fatal("pre-vote produced no vote")
	}
	if err := runtime.StepMessage(&pb.Message{Type: pb.MsgVoteResp.Enum(),
		From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(local),
		Term: runtimeUint64Ptr(vote.GetTerm())}); err != nil {
		t.Fatal(err)
	}
	var appendMessage *pb.Message
	drainRuntime(t, runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgApp && outbound.To == peer &&
			len(outbound.Message.GetEntries()) != 0 {
			appendMessage = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if appendMessage == nil {
		t.Fatal("election produced no leader append")
	}
	last := appendMessage.GetEntries()[len(appendMessage.GetEntries())-1].GetIndex()
	if err := runtime.StepMessage(&pb.Message{Type: pb.MsgAppResp.Enum(),
		From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(local),
		Term: runtimeUint64Ptr(appendMessage.GetTerm()), Index: runtimeUint64Ptr(last)}); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, runtime, nil)
}

func TestRuntimePersistsBeforeOutboundAndRetriesSink(t *testing.T) {
	identity := testWALIdentity(230)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, 230, []uint64{identity.MemberID, peer})
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}

	retryErr := errors.New("outbox full")
	var first *pb.Message
	failedOnce := false
	persisted := false
	for step := 0; step < 1000; step++ {
		result, err := fixture.runtime.DriveReady(new(ReadyWorkspace), func(outbound OutboundMessage) error {
			if !persisted {
				t.Fatal("outbound callback ran before the Ready persistence phase")
			}
			if !failedOnce {
				failedOnce = true
				first = proto.Clone(outbound.Message).(*pb.Message)
				return retryErr
			}
			if !proto.Equal(first, outbound.Message) {
				t.Fatalf("retried message changed: first=%v retry=%v", first, outbound.Message)
			}
			return nil
		}, settleTestApplied)
		if errors.Is(err, retryErr) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if result.Kind == DrivePersisted {
			persisted = true
		}
		if !result.Progressed() {
			break
		}
	}
	if !failedOnce || first == nil {
		t.Fatal("campaign produced no retried outbound message")
	}
}

func TestRuntimeStepDetachesOrdinaryMessageAndRejectsSnapshot(t *testing.T) {
	identity := testWALIdentity(240)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, 240, []uint64{identity.MemberID, peer})
	drainRuntime(t, fixture.runtime, nil)
	term := uint64(1)
	message := &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(identity.MemberID), Term: &term,
		Context: []byte("original"),
	}
	if err := fixture.runtime.StepMessage(message); err != nil {
		t.Fatal(err)
	}
	message.Context[0] = 'X'
	var response *pb.Message
	drainRuntime(t, fixture.runtime, func(outbound OutboundMessage) error {
		response = proto.Clone(outbound.Message).(*pb.Message)
		return nil
	})
	if response == nil || string(response.GetContext()) != "original" {
		t.Fatalf("detached heartbeat response = %v", response)
	}
	if err := fixture.runtime.StepMessage(&pb.Message{
		Type: pb.MsgSnap.Enum(), From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(identity.MemberID), Term: &term,
		Snapshot: &pb.Snapshot{},
	}); !errors.Is(err, raftmodel.ErrUnsupported) {
		t.Fatalf("snapshot Step = %v, want unsupported", err)
	}
}

func TestRuntimeStepsCurrentLeaderTimeoutNow(t *testing.T) {
	identity := testWALIdentity(241)
	peer := identity.MemberID + 1
	newFollower := func(t *testing.T) (runtimeFixture, uint64) {
		t.Helper()
		fixture := newRuntimeFixture(t, 241, []uint64{identity.MemberID, peer})
		drainRuntime(t, fixture.runtime, nil)
		status, err := fixture.runtime.Status()
		if err != nil {
			t.Fatal(err)
		}
		term := status.Term + 1
		if err := fixture.runtime.StepMessage(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(peer),
			To: runtimeUint64Ptr(identity.MemberID), Term: &term, Commit: runtimeUint64Ptr(status.Commit),
		}); err != nil {
			t.Fatal(err)
		}
		drainRuntime(t, fixture.runtime, nil)
		return fixture, term
	}

	fixture, term := newFollower(t)
	if err := fixture.runtime.StepMessage(&pb.Message{
		Type: pb.MsgTimeoutNow.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &term,
	}); err != nil {
		t.Fatalf("current leader TimeoutNow = %v", err)
	}

	stale, currentTerm := newFollower(t)
	currentTerm--
	if err := stale.runtime.StepMessage(&pb.Message{
		Type: pb.MsgTimeoutNow.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &currentTerm,
	}); err == nil {
		t.Fatal("stale TimeoutNow was accepted")
	}
}

func TestRuntimeTransfersLeaderWithExactTimeoutNow(t *testing.T) {
	identity := testWALIdentity(242)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, 242, []uint64{identity.MemberID, peer})
	drainRuntime(t, fixture.runtime, nil)

	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	var preVote *pb.Message
	drainRuntime(t, fixture.runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgPreVote && outbound.To == peer {
			preVote = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if preVote == nil {
		t.Fatal("campaign produced no pre-vote request")
	}
	if err := fixture.runtime.StepMessage(&pb.Message{
		Type: pb.MsgPreVoteResp.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: runtimeUint64Ptr(preVote.GetTerm()),
	}); err != nil {
		t.Fatal(err)
	}
	var vote *pb.Message
	drainRuntime(t, fixture.runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgVote && outbound.To == peer {
			vote = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if vote == nil {
		t.Fatal("pre-vote response produced no vote request")
	}
	if err := fixture.runtime.StepMessage(&pb.Message{
		Type: pb.MsgVoteResp.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: runtimeUint64Ptr(vote.GetTerm()),
	}); err != nil {
		t.Fatal(err)
	}
	var appendMessage *pb.Message
	drainRuntime(t, fixture.runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgApp && outbound.To == peer {
			appendMessage = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if appendMessage == nil || len(appendMessage.GetEntries()) == 0 {
		t.Fatalf("leadership produced no append: %v", appendMessage)
	}
	lastIndex := appendMessage.GetEntries()[len(appendMessage.GetEntries())-1].GetIndex()
	if err := fixture.runtime.StepMessage(&pb.Message{
		Type: pb.MsgAppResp.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: runtimeUint64Ptr(appendMessage.GetTerm()),
		Index: runtimeUint64Ptr(lastIndex),
	}); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	if err := fixture.runtime.TransferLeader(peer); err != nil {
		t.Fatal(err)
	}
	var timeoutNow *pb.Message
	drainRuntime(t, fixture.runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == pb.MsgTimeoutNow {
			timeoutNow = proto.Clone(outbound.Message).(*pb.Message)
		}
		return nil
	})
	if timeoutNow == nil || timeoutNow.GetFrom() != identity.MemberID || timeoutNow.GetTo() != peer ||
		timeoutNow.GetTerm() == 0 {
		t.Fatalf("leader transfer message = %v", timeoutNow)
	}
	if _, err := MeasureOrdinaryMessage(timeoutNow); err != nil {
		t.Fatalf("generated TimeoutNow is not canonical: %v", err)
	}
}

func TestRuntimeTerminalWALCapacityFailureLatches(t *testing.T) {
	options := testWALOptions()
	options.MaxFileBytes = 384 << 20
	options.MaxLiveBytes = 3 * raftstore.MinimumReadyLiveBytes
	// The static bootstrap owns record one and the single-member campaign fills
	// the remaining three records. The next protocol input must terminally fail
	// at ReserveReady before it reaches the Raft core.
	options.MaxRecords = 4
	fixture := newRuntimeFixtureWithOptions(t, 245, nil, options)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.ReadIndex([]byte("full-wal-read")); err != nil {
		t.Fatalf("ReadIndex at sealed WAL limit: %v", err)
	}
	var outcomes []raftmodel.ReadOutcome
	for step := 0; step < 1000; step++ {
		result, err := fixture.runtime.DriveReady(
			new(ReadyWorkspace), func(OutboundMessage) error { return nil }, settleTestApplied,
		)
		if err != nil {
			t.Fatalf("ReadIndex DriveReady step %d: %v", step, err)
		}
		outcomes = append(outcomes, result.ReadOutcomes...)
		if !result.Progressed() {
			break
		}
	}
	if len(outcomes) != 1 || outcomes[0].Err != nil ||
		string(outcomes[0].Barrier.Context) != "full-wal-read" {
		t.Fatalf("ReadIndex at sealed WAL outcomes = %+v", outcomes)
	}
	command := testApplySessionOpen(fixture.base)
	if err := fixture.runtime.Tick(); !errors.Is(err, raftstore.ErrFull) ||
		!errors.Is(err, ErrRuntimeFailed) {
		t.Fatalf("Tick at sealed WAL limit = %v", err)
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, raftstore.ErrFull) ||
		!errors.Is(err, ErrRuntimeFailed) {
		t.Fatalf("proposal after terminal WAL limit = %v", err)
	}
}

func TestRuntimePoisonedApplyAdmissionLatches(t *testing.T) {
	fixture := newRuntimeFixture(t, 246, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Propose([]byte("not a command")); err == nil {
		t.Fatal("malformed proposal was accepted")
	}
	if failure := fixture.runtime.Failure(); failure != nil {
		t.Fatalf("ordinary proposal refusal faulted Runtime: %v", failure)
	}
	command := testApplySessionOpen(fixture.base)
	if _, err := fixture.apply.ApplyNormal(raftmodel.ApplyMeta{
		Index: 1, Term: 1, Type: pb.EntryNormal,
	}, command); err == nil {
		t.Fatal("conflicting direct apply did not poison the state machine")
	}
	if err := fixture.runtime.Propose(command); !errors.Is(err, replicatedstate.ErrApplyPoisoned) ||
		!errors.Is(err, ErrRuntimeFailed) {
		t.Fatalf("proposal against poisoned apply = %v", err)
	}
	if failure := fixture.runtime.Failure(); !errors.Is(failure, replicatedstate.ErrApplyPoisoned) ||
		!errors.Is(failure, ErrRuntimeFailed) {
		t.Fatalf("latched apply failure = %v", failure)
	}
}

func TestCloneOrdinaryMessageRejectsGraphAmplification(t *testing.T) {
	base := &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(1), To: runtimeUint64Ptr(2),
		Term: runtimeUint64Ptr(1),
	}
	cycle := proto.Clone(base).(*pb.Message)
	cycle.Responses = []*pb.Message{cycle}
	if _, _, err := CloneOrdinaryMessage(cycle); err == nil {
		t.Fatal("recursive Responses graph was accepted")
	}
	vote := uint64(1)
	withVote := proto.Clone(base).(*pb.Message)
	withVote.Vote = &vote
	if _, _, err := CloneOrdinaryMessage(withVote); err == nil {
		t.Fatal("local Vote field was accepted")
	}
	withSnapshot := proto.Clone(base).(*pb.Message)
	withSnapshot.Snapshot = &pb.Snapshot{}
	if _, _, err := CloneOrdinaryMessage(withSnapshot); err == nil {
		t.Fatal("snapshot field was accepted")
	}
	unknown := proto.Clone(base).(*pb.Message)
	unknown.ProtoReflect().SetUnknown([]byte{0x80, 0x06, 0x00})
	if _, _, err := CloneOrdinaryMessage(unknown); err == nil {
		t.Fatal("unknown message fields were accepted")
	}

	payload := make([]byte, raftmodel.MaxProposalBytes)
	entries := make([]*pb.Entry, 5)
	for index := range entries {
		entries[index] = &pb.Entry{
			Type: pb.EntryNormal.Enum(), Index: runtimeUint64Ptr(uint64(index + 2)),
			Term: runtimeUint64Ptr(1), Data: payload,
		}
	}
	amplified := &pb.Message{
		Type: pb.MsgApp.Enum(), From: runtimeUint64Ptr(1), To: runtimeUint64Ptr(2),
		Term: runtimeUint64Ptr(1), Index: runtimeUint64Ptr(1), LogTerm: runtimeUint64Ptr(1),
		Entries: entries,
	}
	if _, _, err := CloneOrdinaryMessage(amplified); !errors.Is(err, raftmodel.ErrAdmissionBound) {
		t.Fatalf("aliased aggregate payload = %v", err)
	}

	owned, size, err := CloneOrdinaryMessage(base)
	if err != nil || size == 0 {
		t.Fatalf("CloneOrdinaryMessage = %v, %d, %v", owned, size, err)
	}
	base.Term = runtimeUint64Ptr(9)
	if owned.GetTerm() != 1 {
		t.Fatalf("owned clone aliased caller term %d", owned.GetTerm())
	}
}

func TestMeasureOrdinaryMessageAdmitsOnlyExactTimeoutNow(t *testing.T) {
	base := &pb.Message{
		Type: pb.MsgTimeoutNow.Enum(), From: runtimeUint64Ptr(1), To: runtimeUint64Ptr(2),
		Term: runtimeUint64Ptr(3),
	}
	if size, err := MeasureOrdinaryMessage(base); err != nil || size == 0 {
		t.Fatalf("exact TimeoutNow = %d, %v", size, err)
	}
	tests := []struct {
		name   string
		mutate func(*pb.Message)
	}{
		{name: "zero term", mutate: func(message *pb.Message) { message.Term = runtimeUint64Ptr(0) }},
		{name: "missing term", mutate: func(message *pb.Message) { message.Term = nil }},
		{name: "entry", mutate: func(message *pb.Message) { message.Entries = []*pb.Entry{{}} }},
		{name: "snapshot", mutate: func(message *pb.Message) { message.Snapshot = &pb.Snapshot{} }},
		{name: "context", mutate: func(message *pb.Message) { message.Context = []byte("x") }},
		{name: "explicit index", mutate: func(message *pb.Message) { message.Index = runtimeUint64Ptr(0) }},
		{name: "explicit commit", mutate: func(message *pb.Message) { message.Commit = runtimeUint64Ptr(0) }},
		{name: "reject", mutate: func(message *pb.Message) { value := false; message.Reject = &value }},
		{name: "response", mutate: func(message *pb.Message) { message.Responses = []*pb.Message{{}} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := proto.Clone(base).(*pb.Message)
			test.mutate(message)
			if _, err := MeasureOrdinaryMessage(message); err == nil {
				t.Fatal("unsafe TimeoutNow was accepted")
			}
		})
	}
}

func runtimeUint64Ptr(value uint64) *uint64 { return &value }
