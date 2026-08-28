package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRouteGateStaleMembershipSettlesWithoutCorruptingSession(t *testing.T) {
	for _, wrap := range []bool{false, true} {
		name := "first-gate"
		if wrap {
			name = "overwrite-retained-outcome"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			prototype := commandValue(fixture.binding, 1)
			_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
			gate := routegate.Command{
				Operation: routegate.OperationAcquireShared, Epoch: 1,
				Identity: routegate.Identity(sha256.Sum256([]byte("stale-gate"))),
				Binding:  routegate.Binding(sha256.Sum256([]byte("stale-binding"))),
			}
			sequence, applied := uint64(1), uint64(3)
			if wrap {
				// Reuse one pin while filling the ring. The stale command then
				// replaces a slot that has an auxiliary RouteGate outcome.
				for i := uint16(0); i < fixture.machine.options.RetryWindow; i++ {
					request := commandValue(fixture.binding, sequence)
					request.ClientEpoch = epoch
					reason := routegate.ReasonAcquired
					if i > 0 {
						reason = routegate.ReasonIdempotent
					}
					applyRouteGateAndReason(t, fixture.machine, applied, request, gate, reason)
					sequence++
					applied++
				}
			}
			before, err := fixture.machine.RouteGateStatus()
			if err != nil {
				t.Fatal(err)
			}
			request := commandValue(fixture.binding, sequence)
			request.Kind, request.Batches, request.ClientEpoch = replication.CommandRouteGate, nil, epoch
			request.RouteGate, err = routegate.AppendCommand(nil, gate)
			if err != nil {
				t.Fatal(err)
			}
			encoded := encodeCommand(t, request)
			if err = fixture.machine.AdmitCommand(encoded); err != nil {
				t.Fatalf("admit before membership change: %v", err)
			}
			if _, err = fixture.machine.ApplyConfiguration(raftmodel.ApplyMeta{
				Index: applied, Term: 2, Type: pb.EntryConfChange,
			}, &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}); err != nil {
				t.Fatal(err)
			}
			applied++
			if err = fixture.machine.AdmitCommand(encoded); !errors.Is(err, ErrStaleCommand) {
				t.Fatalf("stale gate admission must refuse without poisoning the machine: %v", err)
			}
			// A command admitted before configuration may already be in the
			// Raft log. Its deterministic refusal must survive apply and replay.
			if _, err = fixture.machine.ApplyNormal(normalMeta(applied), encoded); err != nil {
				t.Fatal(err)
			}
			first, err := fixture.machine.LookupCompletion(encoded)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := replication.OpenCompletion(first.Bytes)
			if err != nil || completion.ResultCode != ResultStaleFence || completion.AppliedSequence != applied {
				t.Fatalf("stale completion=%+v err=%v", completion, err)
			}
			reopened, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
			if err != nil {
				t.Fatalf("reopen stale gate: %v", err)
			}
			if _, err = reopened.ApplyNormal(normalMeta(applied+1), encoded); err != nil {
				t.Fatal(err)
			}
			retry, err := reopened.LookupCompletion(encoded)
			if err != nil || !bytes.Equal(first.Bytes, retry.Bytes) {
				t.Fatalf("exact retry changed completion: %v", err)
			}
			after, err := reopened.RouteGateStatus()
			if err != nil || after != before {
				t.Fatalf("stale command mutated gate: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}

func TestRouteGateApplySettlementReopenAndExactRetry(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)

	identity := routegate.Identity(sha256.Sum256([]byte("request/participant/wave-1")))
	binding := routegate.Binding(sha256.Sum256([]byte("physical-route-and-command-fence")))
	gateBytes, err := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: binding,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := commandValue(fixture.binding, 1)
	request.Kind = replication.CommandRouteGate
	request.ClientEpoch = epoch
	request.Batches = nil
	request.RouteGate = gateBytes
	request.Fingerprint = sha256.Sum256([]byte("route-gate/acquire/participant-wave-1"))
	encoded := encodeCommand(t, request)
	if err := fixture.machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("AdmitCommand: %v", err)
	}
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), encoded)
	if err != nil || publication.Applied != 3 {
		t.Fatalf("ApplyNormal = %+v, %v", publication, err)
	}

	first, err := fixture.machine.LookupCompletion(encoded)
	if err != nil || first.AppliedSequence != 3 {
		t.Fatalf("LookupCompletion = %+v, %v", first, err)
	}
	completion, err := replication.OpenCompletion(first.Bytes)
	if err != nil || completion.ResultFormat != ResultFormatRouteGate ||
		completion.ResultCode != ResultRouteGate {
		t.Fatalf("completion = %+v, %v", completion, err)
	}
	outcome, err := routegate.OpenOutcome(completion.InlineResult)
	if err != nil || outcome.Reason != routegate.ReasonAcquired || !outcome.Mutated ||
		outcome.Status.ActivePins != 1 || outcome.Status.Epoch != 1 {
		t.Fatalf("outcome = %+v, %v", outcome, err)
	}
	status, err := fixture.machine.RouteGateStatus()
	if err != nil || status != outcome.Status {
		t.Fatalf("status = %+v, %v", status, err)
	}
	pin, found, err := fixture.machine.RouteGatePin(identity)
	if err != nil || !found || pin.Binding != binding || pin.State != routegate.PinHeld {
		t.Fatalf("pin = %+v, %v, %v", pin, found, err)
	}
	readSnapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var artifact bytes.Buffer
	manifest, err := WriteSnapshotArtifact(&artifact, readSnapshot, SnapshotArtifactOptions{})
	if err != nil {
		t.Fatalf("WriteSnapshotArtifact: %v", err)
	}
	base, err := BuildSnapshotBase(manifest, fixture.bootstrap)
	if err != nil {
		t.Fatalf("BuildSnapshotBase with retained route-gate rows: %v", err)
	}
	if _, err := OpenSnapshotBase(base); err != nil {
		t.Fatalf("OpenSnapshotBase with retained route-gate rows: %v", err)
	}
	if err = readSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	stageRoot := t.TempDir()
	system := systemTargetOf(createTargetAt(t, stageRoot, "system", durable.Options{}).Collection)
	user := createTargetAt(t, stageRoot, "user", durable.Options{})
	stage, err := NewSnapshotArtifactStage(manifest, system, user, nil)
	if err != nil {
		t.Fatalf("stage with retained route-gate rows: %v", err)
	}
	if _, err := stage.Receive(bytes.NewReader(artifact.Bytes()), func([]byte) error { return nil }); err != nil {
		t.Fatalf("receive route-gate artifact: %v", err)
	}
	stageLog, err := durable.NewTxnLog(stageRoot, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stageLog.Close() })
	staged, err := stage.OpenCandidate(fixture.bootstrap, stageLog, machineOptionsFor(user))
	if err != nil {
		t.Fatalf("reopen staged route-gate state: %v", err)
	}
	if status, err := staged.RouteGateStatus(); err != nil || status != outcome.Status {
		t.Fatalf("staged gate status=%+v err=%v", status, err)
	}
	if pin, found, err := staged.RouteGatePin(identity); err != nil || !found || pin.Binding != binding {
		t.Fatalf("staged gate pin=%+v found=%t err=%v", pin, found, err)
	}
	var headRows, pinRows, resultRows int
	if _, err = VerifySnapshotArtifact(bytes.NewReader(artifact.Bytes()), SnapshotArtifactCallbacks{
		Row: func(collection SnapshotArtifactCollection, key, _ []byte) error {
			if collection != SnapshotArtifactSystem || len(key) == 0 {
				return nil
			}
			switch key[0] {
			case routeGateHeadPrefix:
				headRows++
			case routeGatePinPrefix:
				pinRows++
			case routeGateResultPrefix:
				resultRows++
			}
			return nil
		},
	}); err != nil || headRows != 1 || pinRows != 1 || resultRows != 1 {
		t.Fatalf("route-gate artifact rows = %d/%d/%d, %v", headRows, pinRows, resultRows, err)
	}

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("Open after route-gate apply: %v", err)
	}
	reopenedStatus, err := reopened.RouteGateStatus()
	if err != nil || reopenedStatus != outcome.Status {
		t.Fatalf("reopened status = %+v, %v", reopenedStatus, err)
	}
	publication, err = reopened.ApplyNormal(normalMeta(4), encoded)
	if err != nil || publication.Applied != 4 {
		t.Fatalf("exact retry apply = %+v, %v", publication, err)
	}
	retry, err := reopened.LookupCompletion(encoded)
	if err != nil || retry.AppliedSequence != 3 || !bytes.Equal(retry.Bytes, first.Bytes) {
		t.Fatalf("exact retry settlement = %+v, %v", retry, err)
	}
	finalStatus, err := reopened.RouteGateStatus()
	if err != nil || finalStatus != outcome.Status {
		t.Fatalf("retry mutated reopened gate = %+v, %v", finalStatus, err)
	}
}

func TestRouteGateSharedAndExclusiveAuthorityAreSeparated(t *testing.T) {
	binding := testBinding()
	shared := commandValue(binding, 1)
	shared.Kind = replication.CommandRouteGate
	shared.Batches = nil
	shared.RouteGate, _ = routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: routegate.Identity(sha256.Sum256([]byte("shared"))),
		Binding:  routegate.Binding(sha256.Sum256([]byte("shared-binding"))),
	})
	if _, err := replication.AppendCommand(nil, shared); err != nil {
		t.Fatalf("data-authority shared acquire: %v", err)
	}
	shared.AuthorityClass = replication.CommandAuthorityTopology
	if _, err := replication.AppendCommand(nil, shared); err == nil {
		t.Fatal("topology authority acquired a shared request pin")
	}

	drain := shared
	drain.AuthorityClass = replication.CommandAuthorityTopology
	drain.RouteGate, _ = routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationBeginExclusive, Epoch: 1,
		Identity: routegate.Identity(sha256.Sum256([]byte("drain"))),
		Binding:  routegate.Binding(sha256.Sum256([]byte("topology-plan"))),
	})
	if _, err := replication.AppendCommand(nil, drain); err != nil {
		t.Fatalf("topology-authority exclusive drain: %v", err)
	}
	drain.AuthorityClass = replication.CommandAuthorityData
	if _, err := replication.AppendCommand(nil, drain); err == nil {
		t.Fatal("data authority acquired an exclusive topology drain")
	}
}

func TestRouteGateIsASingletonPhysicalApplyBoundary(t *testing.T) {
	fixture := newNormalBatchFixture(t, 8, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	prototype := commandValue(fixture.binding, 1)
	_, _, epoch := applySessionOpen(t, fixture.machine, 2, prototype)
	gateBytes, _ := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: routegate.Identity(sha256.Sum256([]byte("batch-boundary"))),
		Binding:  routegate.Binding(sha256.Sum256([]byte("batch-binding"))),
	})
	command := commandValue(fixture.binding, 1)
	command.Kind, command.Batches, command.RouteGate = replication.CommandRouteGate, nil, gateBytes
	command.ClientEpoch = epoch
	encoded := encodeCommand(t, command)
	entries := []raftmodel.NormalApply{{Meta: normalMeta(3), Data: encoded}}
	witnesses := make([][32]byte, 1)
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, witnesses)
	if err != nil || applied != 0 || publication != (raftmodel.Publication{}) {
		t.Fatalf("ApplyNormalBatch = %d, %+v, %v", applied, publication, err)
	}
	publication, err = fixture.machine.ApplyNormal(entries[0].Meta, entries[0].Data)
	if err != nil || publication.Applied != 3 {
		t.Fatalf("singleton fallback = %+v, %v", publication, err)
	}
}

func TestRouteGatePinsOnlyOnePhysicalWaveAcrossRetryAndDrain(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	dataPrototype := commandValue(fixture.binding, 1)
	_, _, dataEpoch := applySessionOpen(t, fixture.machine, 2, dataPrototype)
	identity := routegate.Identity(sha256.Sum256([]byte("request-44/participant-7/wave-3")))
	oldRoute := routegate.Binding(sha256.Sum256([]byte(
		"group=7;route-generation=12;member=7;endpoint=old;fence=41;command=abc",
	)))
	changedRouteInSameWave := routegate.Binding(sha256.Sum256([]byte(
		"group=7;route-generation=13;member=9;endpoint=new;fence=42;command=abc",
	)))
	acquire := commandValue(fixture.binding, 1)
	acquire.ClientEpoch = dataEpoch
	applyRouteGateAndReason(t, fixture.machine, 3, acquire, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: oldRoute,
	}, routegate.ReasonAcquired)

	refresh := commandValue(fixture.binding, 2)
	refresh.ClientEpoch = dataEpoch
	applyRouteGateAndReason(t, fixture.machine, 4, refresh, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: identity, Binding: changedRouteInSameWave,
	}, routegate.ReasonIdentityConflict)

	topology := commandValue(fixture.binding, 1)
	topology.AuthorityClass = replication.CommandAuthorityTopology
	topology.ClientID = id128(99)
	_, _, topologyEpoch := applySessionOpen(t, fixture.machine, 5, topology)
	topology.ClientEpoch = topologyEpoch
	drainIdentity := routegate.Identity(sha256.Sum256([]byte("move/shard-7/to-member-9")))
	drainBinding := routegate.Binding(sha256.Sum256([]byte("catalog-op=71;fence=42")))
	applyRouteGateAndReason(t, fixture.machine, 6, topology, routegate.Command{
		Operation: routegate.OperationBeginExclusive, Epoch: 1,
		Identity: drainIdentity, Binding: drainBinding,
	}, routegate.ReasonDrainPending)

	release := commandValue(fixture.binding, 3)
	release.ClientEpoch = dataEpoch
	applyRouteGateAndReason(t, fixture.machine, 7, release, routegate.Command{
		Operation: routegate.OperationReleaseShared, Epoch: 1,
		Identity: identity, Binding: oldRoute,
	}, routegate.ReasonReleased)
	status, err := fixture.machine.RouteGateStatus()
	if err != nil || status.Drain.State != routegate.DrainActive || status.ActivePins != 0 {
		t.Fatalf("physical wave release did not activate drain: %+v, %v", status, err)
	}

	finishDrain := topology
	finishDrain.ClientSequence = 3
	applyRouteGateAndReason(t, fixture.machine, 8, finishDrain, routegate.Command{
		Operation: routegate.OperationReleaseExclusive, Epoch: 1,
		Identity: drainIdentity, Binding: drainBinding,
	}, routegate.ReasonDrainReleased)

	// The durable logical request remains outside this gate. After its prior
	// Pending wave is durably advanced and released, a later wave may resolve
	// fresh physical placement and acquire under the next admission epoch.
	newWaveIdentity := routegate.Identity(sha256.Sum256(
		[]byte("request-44/participant-7/wave-4"),
	))
	newRoute := routegate.Binding(sha256.Sum256([]byte(
		"group=7;route-generation=13;member=9;endpoint=new;fence=42;command=def",
	)))
	newWave := commandValue(fixture.binding, 4)
	newWave.ClientEpoch = dataEpoch
	applyRouteGateAndReason(t, fixture.machine, 9, newWave, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 2,
		Identity: newWaveIdentity, Binding: newRoute,
	}, routegate.ReasonAcquired)
	if pin, found, pinErr := fixture.machine.RouteGatePin(newWaveIdentity); pinErr != nil ||
		!found || pin.Binding != newRoute || pin.State != routegate.PinHeld {
		t.Fatalf("refreshed physical wave pin = %+v, %v, %v", pin, found, pinErr)
	}
}

func applyRouteGateAndReason(
	t testing.TB,
	machine *Machine,
	index uint64,
	outer replication.Command,
	gate routegate.Command,
	want routegate.Reason,
) {
	t.Helper()
	gateBytes, err := routegate.AppendCommand(nil, gate)
	if err != nil {
		t.Fatal(err)
	}
	outer.Kind, outer.Batches, outer.RouteGate = replication.CommandRouteGate, nil, gateBytes
	outer.Fingerprint = sha256.Sum256(append([]byte("route-gate/"), gateBytes...))
	encoded := encodeCommand(t, outer)
	publication, err := machine.ApplyNormal(normalMeta(index), encoded)
	if err != nil || publication.Applied != index {
		t.Fatalf("ApplyNormal(%d) = %+v, %v", index, publication, err)
	}
	lookup, err := machine.LookupCompletion(encoded)
	if err != nil {
		t.Fatalf("LookupCompletion(%d): %v", index, err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultFormat != ResultFormatRouteGate {
		t.Fatalf("OpenCompletion(%d) = %+v, %v", index, completion, err)
	}
	outcome, err := routegate.OpenOutcome(completion.InlineResult)
	if err != nil || outcome.Reason != want {
		t.Fatalf("route-gate outcome(%d) = %+v, %v, want %d", index, outcome, err, want)
	}
}
