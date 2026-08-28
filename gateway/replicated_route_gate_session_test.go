package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

// This adapter bypasses Raft only: session admission, durable apply, result
// lookup, wire grammars, and restart all use their production implementations.
type routeSessionMachineClient struct {
	machine      *replicatedstate.Machine
	batched      bool
	state        shardservice.ReplicatedMemberState
	hide         bool
	first, retry []byte
}

func (client *routeSessionMachineClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	var frame bytes.Buffer
	if err := shardservice.EncodeReplicatedRequest(&frame, request); err != nil {
		return nil, err
	}
	if request.Operation == shardservice.ReplicatedProbe {
		state := client.state
		state.Fence.MemberID, state.Fence.StoreID, state.Fence.NodeIncarnation = endpoint.Member, endpoint.StoreID, endpoint.NodeIncarnation
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedHandshake, HasState: true, State: state}, nil
	}
	if request.Operation == shardservice.ReplicatedRouteGateRead {
		result, err := client.machine.RouteGateRead(request.MinimumApplied)
		if err != nil {
			return nil, err
		}
		value, err := shardservice.AppendReplicatedRouteGateReadValue(nil, result.Status)
		if err != nil {
			return nil, err
		}
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRouteGateReadResult,
			HasState: true, State: client.state, ReadApplied: result.Fence.Applied, Value: value}, nil
	}
	if err := client.machine.AdmitCommand(request.Command); err != nil {
		return nil, err
	}
	index := client.state.Applied + 1
	meta := raftmodel.ApplyMeta{Index: index, Term: client.state.Fence.Term, Type: pb.EntryNormal}
	applied := 0
	if client.batched {
		var witnesses [1][32]byte
		var err error
		applied, _, err = client.machine.ApplyNormalBatch([]raftmodel.NormalApply{{Meta: meta, Data: request.Command}}, witnesses[:])
		if err != nil {
			return nil, err
		}
	}
	if applied == 0 {
		if _, err := client.machine.ApplyNormal(meta, request.Command); err != nil {
			return nil, err
		}
	}
	client.state.Applied, client.state.Commit = index, index
	lookup, err := client.machine.LookupCompletion(request.Command)
	if errors.Is(err, replicatedstate.ErrSessionReleased) {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: client.state, RequestDigest: replicatedRequestDigest(request.Command),
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionReleased, AppliedIndex: index}}, nil
	}
	if err != nil {
		return nil, err
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	if command.Kind() == replication.CommandRouteGate && client.hide {
		client.hide = false
		client.first = bytes.Clone(lookup.Bytes)
		return nil, errors.New("lost committed route-gate response")
	}
	if command.Kind() == replication.CommandRouteGate && len(client.first) != 0 {
		client.retry = bytes.Clone(lookup.Bytes)
	}
	response := &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion, HasState: true, State: client.state,
		RequestDigest: replicatedRequestDigest(request.Command), Completion: lookup.Bytes,
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: index,
			CompletionAppliedSequence: lookup.AppliedSequence, CompletionBytes: len(lookup.Bytes)}}
	frame.Reset()
	if err := shardservice.EncodeReplicatedResponse(&frame, response); err != nil {
		return nil, err
	}
	return response, nil
}

type routeSessionValidator struct{}

func (routeSessionValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}
func (routeSessionValidator) ValidateDelete(_, _ []byte, _ bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

func newRouteSessionMachine(t *testing.T) (ReplicatedRoute, *routeSessionMachineClient, func()) {
	return newRouteSessionMachineWithCheckpoint(t, false)
}

func newRouteSessionMachineWithCheckpoint(t *testing.T, checkpoint bool) (ReplicatedRoute, *routeSessionMachineClient, func()) {
	t.Helper()
	route, _, states := testReplicatedRouteCommand(t)
	dir := t.TempDir()
	open := func(name string, opaque bool) replicatedstate.CollectionTarget {
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, durable.Options{OpaqueValues: opaque})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		target := replicatedstate.CollectionTarget{Collection: collection,
			Validation:       replicatedstate.ValidationDeterministicMutation,
			ValidationDigest: sha256.Sum256([]byte("route-session-test")), Validator: routeSessionValidator{},
			Limits: replicatedstate.CollectionLimits{MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
				MaxDistinctMutations: collection.MaxBatchDocuments(), MaxBatchBytes: collection.MaxBatchBytes()}}
		if opaque {
			target.Validation = replicatedstate.ValidationOpaqueBinary
			target.ValidationDigest = [32]byte{}
			target.Validator = nil
		}
		return target
	}
	system, user := open("system", true), open("docs", false)
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	binding := replicatedstate.Binding{ClusterID: route.Group.ClusterID, ClusterIncarnation: route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: route.Group.TopologyRecoveryEpoch, Distribution: string(route.Distribution), Shard: string(route.Shard),
		AllocationGeneration: route.AllocationGeneration, ShardIncarnation: route.Group.ShardIncarnation, GroupID: route.Group.GroupID,
		ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	index, term := uint64(1), uint64(1)
	bootstrap := &pb.Snapshot{Data: []byte("route-session-test"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
	options := replicatedstate.Options{MaxSessions: 8, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 2, MaxDocuments: user.Limits.MaxDistinctMutations + 4, MaxBytes: 64 << 20}}
	if checkpoint {
		group, err := durable.NewCheckpointGroup(log, []durable.NamedCollection{
			{Name: replicatedstate.SystemCollectionName, Collection: system.Collection},
			{Name: "docs", Collection: user.Collection},
		}, durable.CheckpointGroupOptions{CheckpointEvery: 1024})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = group.Close() })
		options.CheckpointGroup = group
	}
	client := &routeSessionMachineClient{state: states["m2"], batched: checkpoint}
	reopen := func() {
		machine, err := replicatedstate.Open(binding, bootstrap, system, replicatedstate.UserCollection{Name: "docs", Target: user}, log, options)
		if err != nil {
			t.Fatal(err)
		}
		client.machine = machine
	}
	reopen()
	if _, err := client.machine.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	route.Command.RelationManifestDigest = snapshot.Fence().RelationManifestDigest
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	client.state.Fence.Command = route.Command
	client.state.Applied, client.state.Commit, client.state.CheckpointApplied = 1, 1, 1
	return route, client, reopen
}

func TestNativeRouteGateSessionDurableExactRetry(t *testing.T) {
	route, client, reopenMachine := newRouteSessionMachine(t)
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(t.TempDir(), "route-session")
	journalBinding, err := NativeSessionJournalBinding(route, string(route.Distribution), string(route.Shard), []byte("tenant"), 1, serviceauthz.CapabilityDataWrite)
	if err != nil {
		t.Fatal(err)
	}
	open := func() *NativeSession {
		journal, err := OpenNativeSessionJournal(NativeSessionJournalOptions{Path: journalPath, ClientID: replication.ID128{7}, MaxCommandBytes: 1 << 20, Binding: journalBinding})
		if err != nil {
			t.Fatal(err)
		}
		session, err := NewNativeSession(NativeSessionOptions{Executor: executor, Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard),
			Tenant: []byte("tenant"), ClientID: replication.ID128{7}, Resolver: BaseRelationResolver{Relation: 1}, Journal: journal,
			ProposalCapability: serviceauthz.CapabilityDataWrite, MaxRelationBatches: 1, MaxMutations: 1, MaxCommandBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		return session
	}
	session := open()
	if _, err := session.Open(ctx, 1<<60); err != nil {
		t.Fatal(err)
	}
	gate := routegate.Command{Operation: routegate.OperationAcquireShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
	applied := client.state.Applied
	if err := session.prepareRouteGate(gate); err != nil {
		t.Fatal(err)
	}
	if client.state.Applied != applied {
		t.Fatal("prepare admitted a proposal before ledger intent")
	}
	exact := session.PendingCommand()
	changed := gate
	changed.Identity[0]++
	if err := session.prepareRouteGate(changed); !errors.Is(err, ErrNativeCommandPending) || !bytes.Equal(exact, session.PendingCommand()) {
		t.Fatalf("pending gate replaced: %v", err)
	}
	borrowed := session.PendingCommand()
	borrowed[0] ^= 1
	if !bytes.Equal(exact, session.PendingCommand()) {
		t.Fatal("caller mutated retained journal command")
	}
	for _, mutate := range []func(*NativeSessionJournalOptions){
		func(o *NativeSessionJournalOptions) { o.Binding[0]++ },
		func(o *NativeSessionJournalOptions) { o.ClientID[0]++ },
		func(o *NativeSessionJournalOptions) { o.RetryHome[0]++ },
	} {
		options := NativeSessionJournalOptions{Path: journalPath, ClientID: replication.ID128{7}, MaxCommandBytes: 1 << 20, Binding: journalBinding}
		mutate(&options)
		if _, err := OpenNativeSessionJournal(options); !errors.Is(err, ErrNativeSessionJournal) {
			t.Fatalf("foreign journal identity accepted: %v", err)
		}
	}
	command, err := replication.OpenCommand(exact)
	if err != nil || command.ClientEpoch != 2 || command.ClientSequence != 2 || command.AckThrough != 1 || command.Fingerprint != nativeCommandViewFingerprint(command) {
		t.Fatalf("route command did not use actual session state: %v", err)
	}
	session = open()
	if !bytes.Equal(exact, session.PendingCommand()) {
		t.Fatal("prepared journal changed bytes on reopen")
	}
	client.hide = true
	if _, err := session.RetryPending(ctx); err == nil {
		t.Fatal("lost response was acknowledged")
	}
	if !session.Status().Pending || !bytes.Equal(exact, session.PendingCommand()) {
		t.Fatal("outcome-unknown lost exact pending bytes")
	}
	beforeRetry, err := client.machine.RouteGateStatus()
	if err != nil || beforeRetry.ActivePins != 1 || beforeRetry.ReleasedPins != 0 {
		t.Fatalf("unknown result not committed: %+v %v", beforeRetry, err)
	}
	reopenMachine()
	session = open()
	if _, err := session.RetryPending(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(client.first, client.retry) || session.Status().NextSequence != 3 {
		t.Fatal("committed retry changed result or sequence")
	}
	afterRetry, err := client.machine.RouteGateStatus()
	if err != nil || afterRetry != beforeRetry {
		t.Fatalf("retry mutated gate: %+v %v", afterRetry, err)
	}
	gate.Operation = routegate.OperationReleaseShared
	if _, err := session.RouteGate(ctx, gate); err != nil {
		t.Fatal(err)
	}
	status, err := client.machine.RouteGateStatus()
	if err != nil || status.ActivePins != 0 || status.ReleasedPins != 1 {
		t.Fatalf("gate not released: %+v %v", status, err)
	}
	if err := session.RetireReleaseAndDestroy(ctx); err != nil {
		t.Fatal(err)
	}
}

func routeGateSessionCommand(t *testing.T, gate routegate.Command) replication.Command {
	t.Helper()
	route, _, _ := testReplicatedRouteCommand(t)
	capability := serviceauthz.CapabilityDataWrite
	if gate.Operation >= routegate.OperationBeginExclusive {
		capability = serviceauthz.CapabilityTopology
	}
	session := &NativeSession{route: route, distribution: string(route.Distribution), shard: string(route.Shard), tenant: []byte("tenant"), clientID: replication.ID128{7}, proposalCapability: capability}
	command := session.commandHeader(replication.CommandRouteGate, 2, 3, 2)
	var err error
	command.RouteGate, err = routegate.AppendCommand(nil, gate)
	if err != nil {
		t.Fatal(err)
	}
	command.Fingerprint = nativeCommandFingerprint(command)
	return command
}
func routeGateSessionCompletion(t *testing.T, command replication.CommandView, outcome routegate.Outcome, mutate func(*replication.CompletionBytes)) replication.CompletionView {
	t.Helper()
	raw, err := routegate.AppendOutcome(nil, outcome)
	if err != nil {
		t.Fatal(err)
	}
	completion := replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation, TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution: command.Distribution, Shard: command.Shard, AllocationGeneration: command.AllocationGeneration, ShardIncarnation: command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion: command.ReplicaSetVersion, ActivePolicyGeneration: command.ActivePolicyGeneration, ProtectionEpoch: command.ProtectionEpoch, RoutingVersion: command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: command.ClientEpoch, ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint, RetryHome: command.RetryHome, AppliedSequence: 9,
		ResultCode: replicatedstate.ResultRouteGate, ResultFormat: replicatedstate.ResultFormatRouteGate, Storage: replication.CompletionInline, ResultLength: uint64(len(raw)), InlineResult: raw,
	}
	if mutate != nil {
		mutate(&completion)
	}
	completion.ResultDigest = replication.CompletionResultDigest(completion.ResultCode, completion.ResultFormat, completion.InlineResult)
	encoded, err := replication.AppendCompletionBytes(nil, completion)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := replication.OpenCompletion(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return opened
}
func TestNativeRouteGateCompletionBindsOutcomeAndSession(t *testing.T) {
	gate := routegate.Command{Operation: routegate.OperationAcquireShared, Epoch: 7, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
	command := routeGateSessionCommand(t, gate)
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := replication.OpenCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	outcome := routegate.Outcome{Reason: routegate.ReasonAcquired, Mutated: true, Status: routegate.Status{Epoch: 7, Revision: 1, ActivePins: 1, RetainedRecords: 1}}
	good := routeGateSessionCompletion(t, view, outcome, nil)
	if !nativeCompletionMatches(view, good) {
		t.Fatal("exact gate completion rejected")
	}
	for _, mutate := range []func(*replication.CompletionBytes){
		func(c *replication.CompletionBytes) { c.ClientID[0]++ },
		func(c *replication.CompletionBytes) { c.ClientEpoch++ },
		func(c *replication.CompletionBytes) { c.ClientSequence++ },
		func(c *replication.CompletionBytes) { c.Fingerprint[0]++ },
		func(c *replication.CompletionBytes) { c.RetryHome[0]++ },
		func(c *replication.CompletionBytes) { c.Tenant = []byte("foreign") },
		func(c *replication.CompletionBytes) { c.GroupID[0]++ },
		func(c *replication.CompletionBytes) { c.AllocationGeneration++ },
		func(c *replication.CompletionBytes) { c.RouteGeneration++ },
		func(c *replication.CompletionBytes) { c.ResultCode = replicatedstate.ResultApplied },
	} {
		if nativeCompletionMatches(view, routeGateSessionCompletion(t, view, outcome, mutate)) {
			t.Fatal("foreign canonical session/result accepted")
		}
	}
	for _, foreign := range []routegate.Outcome{
		{Reason: routegate.ReasonReleased, Mutated: true, Status: routegate.Status{Epoch: 7, Revision: 1, ReleasedPins: 1, RetainedRecords: 1}},
		{Reason: routegate.ReasonAcquired, Mutated: true, Status: routegate.Status{Epoch: 8, Revision: 1, ActivePins: 1, RetainedRecords: 1}},
		{Reason: routegate.ReasonAcquired, Mutated: true, Status: routegate.Status{Epoch: 7, Revision: 1}},
	} {
		if nativeCompletionMatches(view, routeGateSessionCompletion(t, view, foreign, nil)) {
			t.Fatal("canonical impossible gate outcome accepted")
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !nativeCompletionMatches(view, good) {
			panic("completion")
		}
	}); allocations != 0 {
		t.Fatalf("completion allocs=%g", allocations)
	}
}
func TestNativeRouteGateFingerprintBindsExactTransitionZeroAlloc(t *testing.T) {
	gate := routegate.Command{Operation: routegate.OperationAcquireShared, Epoch: 7, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
	command := routeGateSessionCommand(t, gate)
	var wire [2048]byte
	raw, err := replication.AppendCommand(wire[:0], command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := replication.OpenCommand(raw)
	if err != nil || nativeCommandViewFingerprint(view) != command.Fingerprint {
		t.Fatalf("view fingerprint %v", err)
	}
	for _, mutate := range []func(*routegate.Command){
		func(c *routegate.Command) { c.Operation = routegate.OperationReleaseShared },
		func(c *routegate.Command) { c.Epoch++ },
		func(c *routegate.Command) { c.Identity[0]++ },
		func(c *routegate.Command) { c.Binding[0]++ },
	} {
		foreign := gate
		mutate(&foreign)
		other := routeGateSessionCommand(t, foreign)
		if other.Fingerprint == command.Fingerprint {
			t.Fatal("gate semantic collision")
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		command.Fingerprint = nativeCommandFingerprint(command)
		raw, err := replication.AppendCommand(wire[:0], command)
		if err != nil {
			panic(err)
		}
		view, err := replication.OpenCommand(raw)
		if err != nil || nativeCommandViewFingerprint(view) != command.Fingerprint {
			panic("view")
		}
	}); allocations != 0 {
		t.Fatalf("fingerprint allocs=%g", allocations)
	}
}

func TestNativeRouteGateDrainCompletionBindsExactOperation(t *testing.T) {
	machine, _ := routegate.NewMachine(1, 8)
	gate := routegate.Command{Operation: routegate.OperationBeginExclusive, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
	for _, operation := range []routegate.Operation{routegate.OperationBeginExclusive, routegate.OperationReleaseExclusive} {
		gate.Operation = operation
		outcome := machine.Apply(gate)
		command := routeGateSessionCommand(t, gate)
		raw, err := replication.AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		view, err := replication.OpenCommand(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !nativeCompletionMatches(view, routeGateSessionCompletion(t, view, outcome, nil)) {
			t.Fatal("exact drain completion refused")
		}
		foreign := outcome
		foreign.Status.Drain.Identity[0]++
		if nativeCompletionMatches(view, routeGateSessionCompletion(t, view, foreign, nil)) {
			t.Fatal("foreign drain identity accepted")
		}
		foreign = outcome
		foreign.Status.Drain.Binding[0]++
		if nativeCompletionMatches(view, routeGateSessionCompletion(t, view, foreign, nil)) {
			t.Fatal("foreign drain binding accepted")
		}
	}
}
func TestNativeRouteGateOutcomeMatchesActualKernelTraces(t *testing.T) {
	machine, ok := routegate.NewMachine(1, 8)
	if !ok {
		t.Fatal("gate")
	}
	var random uint64 = 9173
	for i := 0; i < 10000; i++ {
		random = random*6364136223846793005 + 1442695040888963407
		status := machine.Status()
		command := routegate.Command{Operation: routegate.Operation(1 + random%5), Epoch: status.Epoch, Identity: routegate.Identity{byte(1 + (random>>8)%12)}, Binding: routegate.Binding{byte(1 + (random>>16)%2)}}
		if command.Operation == routegate.OperationCompactReleased {
			command.Identity = routegate.Identity{}
			command.Binding = routegate.Binding{}
		}
		if i%7 == 0 && status.Epoch > 1 {
			command.Epoch--
		}
		if i%11 == 0 {
			command.Epoch++
		}
		outcome := machine.Apply(command)
		if _, err := routegate.AppendOutcome(nil, outcome); err != nil || !nativeRouteGateOutcomeMatches(command, outcome) {
			t.Fatalf("trace%d command%+v outcome%+v %v", i, command, outcome, err)
		}
	}
	// A held pin survives admission-epoch compaction; releasing the old
	// acquisition epoch must remain valid and is not a stale new acquire.
	machine, _ = routegate.NewMachine(1, 8)
	acquire := routegate.Command{Operation: routegate.OperationAcquireShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
	machine.Apply(acquire)
	machine.Apply(routegate.Command{Operation: routegate.OperationCompactReleased, Epoch: 1})
	acquire.Operation = routegate.OperationReleaseShared
	outcome := machine.Apply(acquire)
	if outcome.Reason != routegate.ReasonReleased || outcome.Status.Epoch != 2 || !nativeRouteGateOutcomeMatches(acquire, outcome) {
		t.Fatal("old acquisition epoch release refused")
	}
}

func TestNativeRouteGatePreparationAuthorityAndBounds(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	for _, operation := range []routegate.Operation{routegate.OperationAcquireShared, routegate.OperationReleaseShared, routegate.OperationBeginExclusive, routegate.OperationReleaseExclusive, routegate.OperationCompactReleased} {
		transition := routegate.Command{Operation: operation, Epoch: 7, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
		if operation == routegate.OperationCompactReleased {
			transition.Identity = routegate.Identity{}
			transition.Binding = routegate.Binding{}
		}
		for _, capability := range []serviceauthz.Capability{serviceauthz.CapabilityDataWrite, serviceauthz.CapabilityTopology, serviceauthz.CapabilityExecutionPin} {
			session := &NativeSession{route: route, distribution: string(route.Distribution), shard: string(route.Shard), tenant: []byte("tenant"), clientID: replication.ID128{7}, proposalCapability: capability, phase: nativeSessionActive, epoch: 2, nextSequence: 3, ackThrough: 2, maxCommand: 2048}
			err := session.prepareRouteGate(transition)
			allowed := capability == serviceauthz.CapabilityDataWrite && operation <= routegate.OperationReleaseShared || capability == serviceauthz.CapabilityTopology && operation >= routegate.OperationBeginExclusive
			if !allowed {
				if !errors.Is(err, ErrNativeSession) || session.pending {
					t.Fatalf("authority %d op%d err%v", capability, operation, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			view, err := replication.OpenCommand(session.PendingCommand())
			if err != nil || view.Fingerprint != nativeCommandViewFingerprint(view) {
				t.Fatalf("prepared %v", err)
			}
			if operation >= routegate.OperationBeginExclusive && view.AuthorityClass != replication.CommandAuthorityTopology {
				t.Fatal("lost topology authority")
			}
		}
	}
	session := &NativeSession{route: route, distribution: string(route.Distribution), shard: string(route.Shard), tenant: []byte("tenant"), clientID: replication.ID128{7}, proposalCapability: serviceauthz.CapabilityDataWrite, phase: nativeSessionActive, epoch: 2, nextSequence: 3, ackThrough: 2, maxCommand: 1}
	transition := routegate.Command{Operation: routegate.OperationAcquireShared, Epoch: 1, Identity: routegate.Identity{1}, Binding: routegate.Binding{2}}
	if err := session.prepareRouteGate(transition); !errors.Is(err, ErrNativeBundleBound) || session.pending {
		t.Fatalf("oversized preparation %v", err)
	}
	if _, err := session.RouteGate(nil, transition); !errors.Is(err, ErrNativeSession) || session.pending {
		t.Fatalf("nil context prepared command %v", err)
	}
	session.maxCommand = 2048
	if err := session.prepareRouteGate(transition); err != nil {
		t.Fatal(err)
	}
	session.clearPending()
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := session.prepareRouteGate(transition); err != nil {
			panic(err)
		}
		session.clearPending()
	}); allocations != 0 {
		t.Fatalf("warm preparation allocations=%g", allocations)
	}
}
