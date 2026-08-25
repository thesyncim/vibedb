package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

type nativeSessionClient struct {
	state               shardservice.ReplicatedMemberState
	unknownMutationOnce bool
	mutationUnknownSeen bool
	preAdmissionRetry   bool
	unknownCommand      []byte
	retriedCommand      []byte
	applied             uint64
	probes              int
}

func (client *nativeSessionClient) DoReplicated(
	_ context.Context,
	_ ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	if command.Kind() == replication.CommandMutationBatch && client.unknownMutationOnce &&
		!client.mutationUnknownSeen {
		client.mutationUnknownSeen = true
		client.unknownCommand = append([]byte(nil), request.Command...)
		return nil, errors.New("connection disappeared after exact frame")
	}
	if command.Kind() == replication.CommandMutationBatch && client.mutationUnknownSeen &&
		len(client.retriedCommand) == 0 {
		client.retriedCommand = append([]byte(nil), request.Command...)
	}
	if command.Kind() == replication.CommandMutationBatch && client.mutationUnknownSeen &&
		client.preAdmissionRetry {
		return &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalAdmissionBound,
			HasState: true, State: client.state,
		}, nil
	}
	if command.Kind() == replication.CommandSessionRelease {
		client.applied++
		appliedIndex := uint64(99) + client.applied
		client.state.Commit = appliedIndex
		client.state.Applied = appliedIndex
		return &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalDeterministic,
			HasState: true, State: client.state,
			RequestDigest: replicatedRequestDigest(request.Command),
			Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeSessionReleased, AppliedIndex: appliedIndex,
			},
		}, nil
	}
	client.applied++
	resultCode := uint32(replicatedstate.ResultApplied)
	clientEpoch := command.ClientEpoch
	appliedSequence := uint64(99) + client.applied
	switch command.Kind() {
	case replication.CommandSessionOpen:
		resultCode = replicatedstate.ResultSessionOpened
		clientEpoch = appliedSequence
	case replication.CommandSessionRetire:
		resultCode = replicatedstate.ResultSessionRetired
	case replication.CommandSessionRenew:
		resultCode = replicatedstate.ResultSessionRenewed
	case replication.CommandSessionRevoke:
		resultCode = replicatedstate.ResultSessionRevoked
	}
	completion, err := appendNativeSessionCompletion(
		nil, command, clientEpoch, appliedSequence, resultCode,
	)
	if err != nil {
		return nil, err
	}
	appliedIndex := appliedSequence
	if client.state.Commit < appliedIndex {
		client.state.Commit = appliedIndex
	}
	client.state.Applied = appliedIndex
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: client.state,
		RequestDigest: replicatedRequestDigest(request.Command),
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: appliedIndex,
			CompletionAppliedSequence: appliedSequence, CompletionBytes: len(completion),
		},
		Completion: completion,
	}, nil
}

func TestNativeSessionPutDeleteExactUnknownRetryAndLifecycle(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	client := &nativeSessionClient{
		state: states["m2"], unknownMutationOnce: true,
	}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("tenant"), ClientID: replication.ID128{9},
		RetryHome:          replication.RetryHome{7},
		Resolver:           BaseRelationResolver{Relation: 1},
		MaxRelationBatches: 4, MaxMutations: 8,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	initialProbes := client.probes
	if initialProbes == 0 {
		t.Fatal("session Open did not perform the initial live serving handshake")
	}
	status := session.Status()
	if !status.Active || status.Epoch != 100 || status.NextSequence != 2 ||
		status.AckThrough != 1 || status.LeaseDeadline != 1000 {
		t.Fatalf("opened status = %+v", status)
	}
	if _, err := session.Put(context.Background(), []byte{1}, []byte(`{"bad":}`)); !errors.Is(err, ErrNativeDocument) {
		t.Fatalf("invalid Put = %v", err)
	}
	_, err = session.Put(
		context.Background(), []byte{1}, []byte(`{"id":1,"value":"exact"}`),
	)
	var unknown *raftservice.UnknownOutcomeError
	if !errors.As(err, &unknown) || !session.Status().Pending ||
		!bytes.Equal(session.PendingCommand(), client.unknownCommand) {
		t.Fatalf("unknown Put = %T %v status=%+v", err, err, session.Status())
	}
	exposed := session.PendingCommand()
	exposed[0] ^= 0xff
	if bytes.Equal(exposed, session.PendingCommand()) ||
		!bytes.Equal(session.PendingCommand(), client.unknownCommand) {
		t.Fatal("PendingCommand exposed mutable session-owned retry bytes")
	}
	if _, err := session.Delete(context.Background(), []byte{1}); !errors.Is(err, ErrNativeCommandPending) {
		t.Fatalf("operation while pending = %v", err)
	}
	putResult, err := session.RetryPending(context.Background())
	if err != nil || putResult.Completion.ResultCode != replicatedstate.ResultApplied ||
		!bytes.Equal(client.unknownCommand, client.retriedCommand) {
		t.Fatalf("retry = result %+v error %v", putResult, err)
	}
	if _, err := session.Delete(context.Background(), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !session.Status().Retired {
		t.Fatalf("retired status = %+v", session.Status())
	}
	released, err := session.Release(context.Background())
	if err != nil || !released.Released || !session.Status().Released {
		t.Fatalf("release = %+v, %v; status %+v", released, err, session.Status())
	}
	if client.probes != initialProbes {
		t.Fatalf("steady native session added probes: initial=%d final=%d", initialProbes, client.probes)
	}
}

func TestNativeSessionRetryPreservesUnknownAcrossCalls(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	client := &nativeSessionClient{
		state: states["m2"], unknownMutationOnce: true, preAdmissionRetry: true,
	}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("tenant"), ClientID: replication.ID128{0x31},
		Resolver:           BaseRelationResolver{Relation: 1},
		MaxRelationBatches: 4, MaxMutations: 8,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	_, err = session.Put(context.Background(), []byte{1}, []byte(`{"id":1}`))
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) || !session.Status().Pending {
		t.Fatalf("initial unknown = %T %v status=%+v", err, err, session.Status())
	}
	want := session.PendingCommand()
	_, err = session.RetryPending(context.Background())
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) || !session.Status().Pending ||
		!bytes.Equal(want, session.PendingCommand()) ||
		!bytes.Equal(want, client.retriedCommand) {
		t.Fatalf("retry refusal = %T %v status=%+v", err, err, session.Status())
	}
}

type countingNativeResolver struct {
	calls int
}

func (resolver *countingNativeResolver) ResolveNative(
	builder *RelationBundleBuilder,
	mutation NativeMutation,
) error {
	resolver.calls++
	return builder.Add(1, replication.Mutation{
		Kind: mutation.Kind, Key: mutation.Key, Value: mutation.Value,
	})
}

func TestNativeSessionRejectsIdentityAndMutationBoundsBeforeOwnedWork(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	client := &nativeSessionClient{state: states["m2"]}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base := NativeSessionOptions{
		Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("tenant"), ClientID: replication.ID128{9},
		Resolver: BaseRelationResolver{Relation: 1}, MaxRelationBatches: 4,
		MaxMutations: 8, InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	}
	tests := []struct {
		name   string
		mutate func(*NativeSessionOptions)
	}{
		{"distribution bound", func(options *NativeSessionOptions) {
			options.Distribution = string(bytes.Repeat([]byte{'d'}, replication.MaxIdentityBytes+1))
		}},
		{"distribution utf8", func(options *NativeSessionOptions) { options.Distribution = "\xff" }},
		{"distribution nul", func(options *NativeSessionOptions) { options.Distribution = "bad\x00name" }},
		{"shard bound", func(options *NativeSessionOptions) {
			options.Shard = string(bytes.Repeat([]byte{'s'}, replication.MaxIdentityBytes+1))
		}},
		{"tenant bound", func(options *NativeSessionOptions) {
			options.Tenant = bytes.Repeat([]byte{'t'}, replication.MaxIdentityBytes+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := base
			test.mutate(&options)
			if _, err := NewNativeSession(options); !errors.Is(err, ErrNativeSession) {
				t.Fatalf("NewNativeSession = %v", err)
			}
		})
	}

	resolver := &countingNativeResolver{}
	base.Resolver = resolver
	session, err := NewNativeSession(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(
		context.Background(), bytes.Repeat([]byte{'k'}, replication.MaxMutationKeyBytes+1),
	); !errors.Is(err, ErrNativeBundleBound) {
		t.Fatalf("oversized Delete key = %v", err)
	}
	if _, err := session.Put(
		context.Background(), []byte("key"),
		bytes.Repeat([]byte{'{'}, base.MaxCommandBytes+1),
	); !errors.Is(err, ErrNativeBundleBound) {
		t.Fatalf("oversized Put document = %v", err)
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls after rejected bounds = %d", resolver.calls)
	}
}

func TestNativeGatewayPinsCatalogRF3RouteAndNeverFallsBackToSQL(t *testing.T) {
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	snapshot, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 7, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	state := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: descriptor.Group, AllocationGeneration: uint64(descriptor.AllocationGeneration),
			Command: descriptor.Command, MemberID: descriptor.Replicas[0].Member,
			StoreID:         descriptor.Replicas[0].StoreID,
			NodeIncarnation: descriptor.Replicas[0].NodeIncarnation, Term: 2,
		},
		LeaderID: descriptor.Replicas[0].Member, Commit: 1, Applied: 1,
		CheckpointApplied: 1,
	}
	client := &nativeSessionClient{state: state}
	native, err := NewNativeGateway(NativeGatewayOptions{
		Catalog: NewCatalogHolder(snapshot), Client: client, MaxAttempts: 2,
		AttemptTimeout:     time.Second,
		Resolver:           BaseRelationResolver{Relation: 1},
		MaxRelationBatches: 4, MaxMutations: 8,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := native.NewSession(NativeSessionRequest{
		Distribution: descriptor.Distribution, Shard: descriptor.Shard,
		Tenant: []byte("tenant"), ClientID: replication.ID128{0x51},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Put(context.Background(), []byte{1}, []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := native.NewSession(NativeSessionRequest{
		Distribution: descriptor.Distribution, Shard: "not-replicated",
		Tenant: []byte("tenant"), ClientID: replication.ID128{0x52},
	}); !errors.Is(err, ErrReplicatedRoute) {
		t.Fatalf("missing RF3 route = %v", err)
	}
}

func TestRelationBundleBuilderDenseOrderBoundAndWarmAllocations(t *testing.T) {
	builder := newRelationBundleBuilder(3, 3)
	mutation := replication.Mutation{Kind: replication.MutationDelete, Key: []byte{1}}
	if err := builder.Add(1, mutation); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(2, mutation); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(1, mutation); !errors.Is(err, ErrNativeBundleBound) {
		t.Fatalf("reordered relation = %v", err)
	}
	builder.reset()
	allocations := testing.AllocsPerRun(1000, func() {
		builder.reset()
		if err := builder.Add(1, mutation); err != nil {
			panic(err)
		}
		if err := builder.Add(2, mutation); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("warm bundle allocations = %f", allocations)
	}
}

func TestNativeCommandFingerprintAndCanonicalAssemblyWarmZeroAlloc(t *testing.T) {
	command := replication.Command{
		Kind:      replication.CommandMutationBatch,
		ClusterID: replication.ID128{1}, ClusterIncarnation: replication.ID128{2},
		TopologyRecoveryEpoch: 3, Distribution: "orders", Shard: "0000-ffff",
		AllocationGeneration: 4, ShardIncarnation: replication.ID128{5},
		GroupID: replication.ID128{6}, ReplicaSetVersion: 7,
		ActivePolicyGeneration: 8, ProtectionEpoch: 9, OwnershipEpoch: 10,
		SchemaGeneration: 11, RoutingVersion: 12, RouteGeneration: 13,
		Tenant: []byte("tenant"), ClientID: replication.ID128{14},
		ClientEpoch: 15, ClientSequence: 16, AckThrough: 15,
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: []byte{1, 2},
				Value: []byte(`{"id":1}`),
			}},
		}},
	}
	command.Fingerprint = nativeCommandFingerprint(command)
	size, err := replication.CommandSize(command)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 0, size)
	allocations := testing.AllocsPerRun(1000, func() {
		command.Fingerprint = nativeCommandFingerprint(command)
		var appendErr error
		buffer, appendErr = replication.AppendCommand(buffer[:0], command)
		if appendErr != nil || len(buffer) != size {
			panic("canonical command assembly failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("warm command assembly allocations = %f", allocations)
	}
	want := command.Fingerprint
	mutations := []struct {
		name   string
		mutate func(*replication.Command)
	}{
		{"topology", func(value *replication.Command) { value.TopologyRecoveryEpoch++ }},
		{"distribution", func(value *replication.Command) { value.Distribution += "-other" }},
		{"shard", func(value *replication.Command) { value.Shard += "-other" }},
		{"allocation", func(value *replication.Command) { value.AllocationGeneration++ }},
		{"shard-incarnation", func(value *replication.Command) { value.ShardIncarnation[0]++ }},
		{"group", func(value *replication.Command) { value.GroupID[0]++ }},
		{"replica-set", func(value *replication.Command) { value.ReplicaSetVersion++ }},
		{"policy", func(value *replication.Command) { value.ActivePolicyGeneration++ }},
		{"protection", func(value *replication.Command) { value.ProtectionEpoch++ }},
		{"ownership", func(value *replication.Command) { value.OwnershipEpoch++ }},
		{"schema", func(value *replication.Command) { value.SchemaGeneration++ }},
		{"routing", func(value *replication.Command) { value.RoutingVersion++ }},
		{"route", func(value *replication.Command) { value.RouteGeneration++ }},
		{"ack", func(value *replication.Command) { value.AckThrough-- }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			changed := command
			test.mutate(&changed)
			if nativeCommandFingerprint(changed) == want {
				t.Fatal("semantic command field did not change fingerprint")
			}
		})
	}
}

func TestNativeCompletionResultCodeIsBoundToCommandKind(t *testing.T) {
	allResults := []uint32{
		replicatedstate.ResultApplied,
		replicatedstate.ResultStaleFence,
		replicatedstate.ResultUnknownRelation,
		replicatedstate.ResultInvalidDocument,
		replicatedstate.ResultTargetBound,
		replicatedstate.ResultWrongShard,
		replicatedstate.ResultIndexConflict,
		replicatedstate.ResultSessionRetired,
		replicatedstate.ResultSessionOpened,
		replicatedstate.ResultSessionRenewed,
		replicatedstate.ResultSessionRevoked,
	}
	allowed := map[replication.CommandKind]map[uint32]bool{
		replication.CommandMutationBatch: {
			replicatedstate.ResultApplied:         true,
			replicatedstate.ResultStaleFence:      true,
			replicatedstate.ResultUnknownRelation: true,
			replicatedstate.ResultInvalidDocument: true,
			replicatedstate.ResultTargetBound:     true,
			replicatedstate.ResultWrongShard:      true,
			replicatedstate.ResultIndexConflict:   true,
		},
		replication.CommandSessionRetire: {
			replicatedstate.ResultStaleFence:     true,
			replicatedstate.ResultSessionRetired: true,
		},
		replication.CommandSessionOpen: {
			replicatedstate.ResultSessionOpened: true,
		},
		replication.CommandSessionRenew: {
			replicatedstate.ResultStaleFence:     true,
			replicatedstate.ResultSessionRenewed: true,
		},
		replication.CommandSessionRevoke: {
			replicatedstate.ResultStaleFence:     true,
			replicatedstate.ResultSessionRevoked: true,
		},
		replication.CommandSessionRelease: {},
	}
	for kind, results := range allowed {
		for _, result := range allResults {
			if got, want := nativeCompletionResultMatches(kind, result), results[result]; got != want {
				t.Fatalf("kind=%d result=%d match=%t, want %t", kind, result, got, want)
			}
		}
	}
}

func appendNativeSessionCompletion(
	dst []byte,
	command replication.CommandView,
	clientEpoch, appliedSequence uint64,
	resultCode uint32,
) ([]byte, error) {
	digest := replication.CompletionResultDigest(
		resultCode, replicatedstate.ResultFormatMutation, nil,
	)
	return replication.AppendCompletionBytes(dst, replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch,
		Distribution:          command.Distribution, Shard: command.Shard,
		AllocationGeneration: command.AllocationGeneration,
		ShardIncarnation:     command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: clientEpoch,
		ClientSequence: command.ClientSequence, Fingerprint: command.Fingerprint,
		RetryHome: command.RetryHome, AppliedSequence: appliedSequence,
		ResultCode: resultCode, ResultFormat: replicatedstate.ResultFormatMutation,
		Storage: replication.CompletionInline, ResultDigest: digest,
	})
}
