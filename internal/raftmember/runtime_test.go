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
	applyOptions.MaxCompletions = uint64(options.MaxEntries) + 1024
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
		runtime: runtime, wal: wal, database: database, apply: apply, base: base,
		applyID: applyID, sqlPath: sqlPath, options: options,
	}
}

func drainRuntime(t testing.TB, runtime *Runtime, send func(OutboundMessage) error) {
	t.Helper()
	if send == nil {
		send = func(OutboundMessage) error { return nil }
	}
	for step := 0; step < 10000; step++ {
		result, err := runtime.DriveReady(send)
		if err != nil {
			t.Fatalf("DriveReady step %d: %v", step, err)
		}
		if !result.Progressed() {
			return
		}
	}
	t.Fatal("Runtime Ready drain did not converge")
}

func TestAdoptRuntimeOwnsExactPairAndMintsOneIncarnation(t *testing.T) {
	identity := testWALIdentity(210)
	_, wal, _, options := createWAL(t, identity)
	_, database, _ := prepareSQLRoot(t, identity, "runtime-owner")
	authority := testAuthorityProfile()
	base, err := BindPreparedSQL(wal, database, authority, "docs")
	skipIfStrictAllocationUnsupported(t, "bind runtime owner SQL", err)
	if err != nil {
		t.Fatal(err)
	}
	applyOptions := testApplyOptions()
	applyOptions.MaxCompletions = uint64(options.MaxEntries)
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

func TestRuntimeCampaignProposalAndReadyOrdering(t *testing.T) {
	fixture := newRuntimeFixture(t, 220, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.Tick(); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("Tick with uncaptured Ready = %v", err)
	}
	drainRuntime(t, fixture.runtime, nil)

	key, _ := orderedkey.AppendJSONString(nil, []byte(`"a"`), orderedkey.Ascending)
	command := testApplyCommand(fixture.base, 1, key, []byte(`{"id":"a","value":1}`))
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

func TestRuntimeRestartsFromCertifiedImmutableBaseAndAppendsNormally(t *testing.T) {
	fixture := newRuntimeFixture(t, 219, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	key, _ := orderedkey.AppendJSONString(nil, []byte(`"base"`), orderedkey.Ascending)
	command := testApplyCommand(fixture.base, 1, key, []byte(`{"id":"base","value":1}`))
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

	newWAL, err := CreateStagedChildWAL(
		filepath.Join(t.TempDir(), "replacement.wal"), identity, testWALKey(),
		testTopologyRecoveryEpoch, testAuthorityProfile(), fixture.base, activation,
		fixture.options,
	)
	if err != nil {
		t.Fatal(err)
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
	drainRuntime(t, restarted, nil)
	if err := restarted.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, restarted, nil)
	key, _ = orderedkey.AppendJSONString(nil, []byte(`"tail"`), orderedkey.Ascending)
	tail := testApplyCommand(fixture.base, 2, key, []byte(`{"id":"tail","value":2}`))
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
		result, driveErr := fixture.runtime.DriveReady(func(OutboundMessage) error { return nil })
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
		result, err := fixture.runtime.DriveReady(func(outbound OutboundMessage) error {
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
		})
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
		result, err := fixture.runtime.DriveReady(func(OutboundMessage) error { return nil })
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
	key, _ := orderedkey.AppendJSONString(nil, []byte(`"full"`), orderedkey.Ascending)
	command := testApplyCommand(fixture.base, 1, key, []byte(`{"id":"full"}`))
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
	key, _ := orderedkey.AppendJSONString(nil, []byte(`"poison"`), orderedkey.Ascending)
	command := testApplyCommand(fixture.base, 1, key, []byte(`{"id":"poison"}`))
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

func runtimeUint64Ptr(value uint64) *uint64 { return &value }
