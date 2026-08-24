package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	ErrNativeSession        = errors.New("gateway: invalid native replicated session")
	ErrNativeSessionState   = errors.New("gateway: native replicated session state rejects operation")
	ErrNativeCommandPending = errors.New("gateway: an exact native command remains outcome-unknown")
	ErrNativeBundleBound    = errors.New("gateway: native relation bundle exceeds its bound")
	ErrNativeDocument       = errors.New("gateway: native Put document is not valid vibejson")
)

// NativeMutation is one byte-native logical mutation before its base and
// index relations are resolved. Key is already the engine's canonical ordered
// key; Value is exact vibejson for Put and empty for Delete.
type NativeMutation struct {
	Kind  replication.MutationKind
	Key   []byte
	Value []byte
}

// BundleResolver emits dense, strictly increasing relation IDs for one native
// mutation. It must not retain builder or input bytes. The resulting command is
// consumed directly by replicatedstate.OpenBundle; relation names never enter
// this boundary or the RF3 serving wire.
type BundleResolver interface {
	ResolveNative(*RelationBundleBuilder, NativeMutation) error
}

// BaseRelationResolver maps a native mutation to one dense relation. It is the
// base/local-index path; a global-index resolver can emit additional
// authenticated dense relations without changing the session or wire grammar.
type BaseRelationResolver struct {
	Relation replication.RelationID
}

func (resolver BaseRelationResolver) ResolveNative(
	builder *RelationBundleBuilder,
	mutation NativeMutation,
) error {
	return builder.Add(resolver.Relation, replication.Mutation{
		Kind: mutation.Kind, Key: mutation.Key, Value: mutation.Value,
	})
}

// RelationBundleBuilder is bounded caller-session workspace. A resolver emits
// relations in canonical increasing order; repeated Add calls for the current
// relation preserve semantic mutation order.
type RelationBundleBuilder struct {
	batches      []replication.RelationMutationBatch
	mutations    []replication.Mutation
	maxRelations int
	maxMutations int
	batchStart   int
}

func newRelationBundleBuilder(maxRelations, maxMutations int) RelationBundleBuilder {
	return RelationBundleBuilder{
		batches:      make([]replication.RelationMutationBatch, 0, maxRelations),
		mutations:    make([]replication.Mutation, 0, maxMutations),
		maxRelations: maxRelations, maxMutations: maxMutations,
	}
}

// Add appends one borrowed mutation under relation. It performs no allocation
// after builder construction and fails before exceeding either item bound.
func (builder *RelationBundleBuilder) Add(
	relation replication.RelationID,
	mutation replication.Mutation,
) error {
	if builder == nil || relation == 0 || relation > replication.MaxRelationID ||
		len(builder.mutations) == builder.maxMutations {
		return ErrNativeBundleBound
	}
	if len(builder.batches) == 0 || builder.batches[len(builder.batches)-1].Relation != relation {
		if len(builder.batches) == builder.maxRelations ||
			len(builder.batches) != 0 && builder.batches[len(builder.batches)-1].Relation >= relation {
			return ErrNativeBundleBound
		}
		builder.batchStart = len(builder.mutations)
		builder.batches = append(builder.batches, replication.RelationMutationBatch{Relation: relation})
	}
	builder.mutations = append(builder.mutations, mutation)
	last := &builder.batches[len(builder.batches)-1]
	last.Mutations = builder.mutations[builder.batchStart:len(builder.mutations)]
	return nil
}

func (builder *RelationBundleBuilder) reset() {
	clear(builder.mutations)
	clear(builder.batches)
	builder.mutations = builder.mutations[:0]
	builder.batches = builder.batches[:0]
	builder.batchStart = 0
}

// NativeSessionOptions fixes one byte-native client identity and bounded
// command workspace. The session is intentionally serialized; one caller owns
// sequence advancement and exact pending-command retry.
type NativeSessionOptions struct {
	Executor     *ReplicatedExecutor
	Route        ReplicatedRoute
	Distribution string
	Shard        string
	Tenant       []byte
	ClientID     replication.ID128
	RetryHome    replication.RetryHome
	Resolver     BundleResolver

	MaxRelationBatches  int
	MaxMutations        int
	InitialCommandBytes int
	MaxCommandBytes     int
}

type nativeSessionPhase uint8

const (
	nativeSessionNew nativeSessionPhase = iota
	nativeSessionActive
	nativeSessionRetired
	nativeSessionReleased
)

// NativeSession owns one bounded command buffer. It is not safe for concurrent
// use: serialization is part of its request-identity and acknowledgement
// contract, not an incidental implementation restriction.
type NativeSession struct {
	executor            *ReplicatedExecutor
	route               ReplicatedRoute
	distribution        string
	shard               string
	tenant              []byte
	clientID            replication.ID128
	retryHome           replication.RetryHome
	resolver            BundleResolver
	bundle              RelationBundleBuilder
	command             []byte
	maxCommand          int
	pending             bool
	phase               nativeSessionPhase
	epoch               uint64
	nextSequence        uint64
	ackThrough          uint64
	leaseDeadline       int64
	terminalSequence    uint64
	terminalFingerprint replication.Digest
	leader              shardservice.ReplicatedMemberState
}

// NativeSessionStatus is a detached fixed-width view. Pending means callers
// must call RetryPending with the retained exact command before any new work.
type NativeSessionStatus struct {
	Epoch         uint64
	NextSequence  uint64
	AckThrough    uint64
	LeaseDeadline int64
	Pending       bool
	Active        bool
	Retired       bool
	Released      bool
}

// NativeResult carries a canonical completion. Release is the one lifecycle
// operation whose durable postcondition is reported as a typed no-completion
// result; Released distinguishes that successful path.
type NativeResult struct {
	Outcome    raftserve.Outcome
	Completion replication.CompletionView
	Released   bool
}

func NewNativeSession(options NativeSessionOptions) (*NativeSession, error) {
	if options.MaxRelationBatches == 0 {
		options.MaxRelationBatches = 16
	}
	if options.MaxMutations == 0 {
		options.MaxMutations = 64
	}
	if options.MaxCommandBytes == 0 {
		options.MaxCommandBytes = replication.MaxCommandBytes
	}
	if options.InitialCommandBytes == 0 {
		options.InitialCommandBytes = 4 << 10
	}
	if options.Executor == nil || !validReplicatedRoute(options.Route) ||
		!validNativeSessionIdentity(options.Distribution, options.Shard, options.Tenant) ||
		options.ClientID == (replication.ID128{}) || options.Resolver == nil ||
		options.MaxRelationBatches <= 0 || options.MaxRelationBatches > replication.MaxRelationBatches ||
		options.MaxMutations <= 0 || options.MaxMutations > replication.MaxMutations ||
		options.MaxCommandBytes <= 0 || options.MaxCommandBytes > replication.MaxCommandBytes ||
		options.InitialCommandBytes <= 0 || options.InitialCommandBytes > options.MaxCommandBytes {
		return nil, ErrNativeSession
	}
	route := options.Route
	route.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
	tenant := append([]byte(nil), options.Tenant...)
	tenant = tenant[:len(tenant):len(tenant)]
	return &NativeSession{
		executor: options.Executor, route: route,
		distribution: strings.Clone(options.Distribution), shard: strings.Clone(options.Shard),
		tenant: tenant, clientID: options.ClientID, retryHome: options.RetryHome,
		resolver: options.Resolver,
		bundle:   newRelationBundleBuilder(options.MaxRelationBatches, options.MaxMutations),
		command:  make([]byte, 0, options.InitialCommandBytes), maxCommand: options.MaxCommandBytes,
		nextSequence: 1,
	}, nil
}

func (session *NativeSession) Status() NativeSessionStatus {
	if session == nil {
		return NativeSessionStatus{}
	}
	return NativeSessionStatus{
		Epoch: session.epoch, NextSequence: session.nextSequence,
		AckThrough: session.ackThrough, LeaseDeadline: session.leaseDeadline,
		Pending: session.pending, Active: session.phase == nativeSessionActive,
		Retired:  session.phase == nativeSessionRetired,
		Released: session.phase == nativeSessionReleased,
	}
}

// PendingCommand returns a detached copy of the exact outcome-unknown command.
// Callers may mutate or retain it without changing the session's owned retry
// bytes. The session keeps its private command until deterministic settlement.
func (session *NativeSession) PendingCommand() []byte {
	if session == nil || !session.pending {
		return nil
	}
	return append([]byte(nil), session.command...)
}

func (session *NativeSession) Open(
	ctx context.Context,
	nextDeadlineUnixNano int64,
) (NativeResult, error) {
	if session == nil || session.phase != nativeSessionNew || session.pending ||
		nextDeadlineUnixNano <= 0 {
		return NativeResult{}, sessionStateError(session)
	}
	command := session.commandHeader(replication.CommandSessionOpen, 0, 1, 0)
	command.NextDeadlineUnixNano = nextDeadlineUnixNano
	return session.prepareAndExecute(ctx, command, false)
}

func (session *NativeSession) Put(
	ctx context.Context,
	key, document []byte,
) (NativeResult, error) {
	return session.mutate(ctx, NativeMutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
}

func (session *NativeSession) Delete(
	ctx context.Context,
	key []byte,
) (NativeResult, error) {
	return session.mutate(ctx, NativeMutation{Kind: replication.MutationDelete, Key: key})
}

func (session *NativeSession) mutate(
	ctx context.Context,
	mutation NativeMutation,
) (NativeResult, error) {
	if session == nil || ctx == nil || session.phase != nativeSessionActive || session.pending ||
		session.nextSequence == 0 || session.nextSequence == math.MaxUint64 {
		return NativeResult{}, sessionStateError(session)
	}
	// Reject byte bounds before vibejson validation, resolver fanout, command
	// hashing, or buffer growth. An oversized value cannot fit this session's
	// command even if the global mutation bound is larger.
	if len(mutation.Key) == 0 || len(mutation.Key) > replication.MaxMutationKeyBytes {
		return NativeResult{}, ErrNativeBundleBound
	}
	switch mutation.Kind {
	case replication.MutationPut:
		if len(mutation.Value) == 0 ||
			len(mutation.Value) > replication.MaxMutationValueBytes ||
			len(mutation.Value) > session.maxCommand {
			return NativeResult{}, ErrNativeBundleBound
		}
		if !vibejson.Valid(mutation.Value) {
			return NativeResult{}, ErrNativeDocument
		}
	case replication.MutationDelete:
		if len(mutation.Value) != 0 {
			return NativeResult{}, ErrNativeBundleBound
		}
	default:
		return NativeResult{}, ErrNativeBundleBound
	}
	session.bundle.reset()
	if err := session.resolver.ResolveNative(&session.bundle, mutation); err != nil {
		return NativeResult{}, err
	}
	if len(session.bundle.batches) == 0 || len(session.bundle.mutations) == 0 {
		session.bundle.reset()
		return NativeResult{}, ErrNativeBundleBound
	}
	command := session.commandHeader(
		replication.CommandMutationBatch, session.epoch, session.nextSequence, session.ackThrough,
	)
	command.Batches = session.bundle.batches
	result, err := session.prepareAndExecute(ctx, command, false)
	session.bundle.reset()
	return result, err
}

func (session *NativeSession) Renew(
	ctx context.Context,
	expectedDeadlineUnixNano, nextDeadlineUnixNano int64,
) (NativeResult, error) {
	if session == nil || session.phase != nativeSessionActive || session.pending ||
		expectedDeadlineUnixNano != session.leaseDeadline || expectedDeadlineUnixNano <= 0 ||
		nextDeadlineUnixNano <= expectedDeadlineUnixNano || session.nextSequence == math.MaxUint64 {
		return NativeResult{}, sessionStateError(session)
	}
	command := session.commandHeader(
		replication.CommandSessionRenew, session.epoch, session.nextSequence, session.ackThrough,
	)
	command.ExpectedDeadlineUnixNano = expectedDeadlineUnixNano
	command.NextDeadlineUnixNano = nextDeadlineUnixNano
	return session.prepareAndExecute(ctx, command, false)
}

func (session *NativeSession) Revoke(
	ctx context.Context,
	expectedDeadlineUnixNano int64,
) (NativeResult, error) {
	if session == nil || session.phase != nativeSessionActive || session.pending ||
		expectedDeadlineUnixNano != session.leaseDeadline || expectedDeadlineUnixNano <= 0 ||
		session.nextSequence == 0 {
		return NativeResult{}, sessionStateError(session)
	}
	command := session.commandHeader(
		replication.CommandSessionRevoke, session.epoch, session.nextSequence, session.ackThrough,
	)
	command.ExpectedDeadlineUnixNano = expectedDeadlineUnixNano
	return session.prepareAndExecute(ctx, command, false)
}

func (session *NativeSession) Retire(ctx context.Context) (NativeResult, error) {
	if session == nil || session.phase != nativeSessionActive || session.pending ||
		session.nextSequence == 0 {
		return NativeResult{}, sessionStateError(session)
	}
	command := session.commandHeader(
		replication.CommandSessionRetire, session.epoch, session.nextSequence,
		session.nextSequence-1,
	)
	return session.prepareAndExecute(ctx, command, false)
}

func (session *NativeSession) Release(ctx context.Context) (NativeResult, error) {
	if session == nil || session.phase != nativeSessionRetired || session.pending ||
		session.terminalSequence == 0 || session.terminalFingerprint == (replication.Digest{}) {
		return NativeResult{}, sessionStateError(session)
	}
	command := session.commandHeader(
		replication.CommandSessionRelease, session.epoch, session.terminalSequence,
		session.terminalSequence-1,
	)
	command.Fingerprint = session.terminalFingerprint
	return session.prepareAndExecute(ctx, command, true)
}

// RetryPending resubmits only the retained byte-identical command. It never
// reconstructs from mutable session or catalog state.
func (session *NativeSession) RetryPending(ctx context.Context) (NativeResult, error) {
	if session == nil || !session.pending {
		return NativeResult{}, ErrNativeSessionState
	}
	return session.executePending(ctx, true)
}

func (session *NativeSession) commandHeader(
	kind replication.CommandKind,
	epoch, sequence, ackThrough uint64,
) replication.Command {
	commandFence := session.route.Command
	return replication.Command{
		Kind:                  kind,
		ClusterID:             session.route.Group.ClusterID,
		ClusterIncarnation:    session.route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: session.route.Group.TopologyRecoveryEpoch,
		Distribution:          session.distribution, Shard: session.shard,
		AllocationGeneration:   session.route.AllocationGeneration,
		ShardIncarnation:       session.route.Group.ShardIncarnation,
		GroupID:                session.route.Group.GroupID,
		ReplicaSetVersion:      commandFence.ReplicaSetVersion,
		ActivePolicyGeneration: commandFence.ActivePolicyGeneration,
		ProtectionEpoch:        commandFence.ProtectionEpoch,
		OwnershipEpoch:         commandFence.OwnershipEpoch,
		SchemaGeneration:       commandFence.SchemaGeneration,
		RoutingVersion:         commandFence.RoutingVersion,
		RouteGeneration:        commandFence.RouteGeneration,
		Tenant:                 session.tenant, ClientID: session.clientID,
		ClientEpoch: epoch, ClientSequence: sequence, AckThrough: ackThrough,
		RetryHome: session.retryHome,
	}
}

func (session *NativeSession) prepareAndExecute(
	ctx context.Context,
	command replication.Command,
	preserveFingerprint bool,
) (NativeResult, error) {
	if ctx == nil || session.pending {
		return NativeResult{}, ErrNativeSessionState
	}
	if !preserveFingerprint {
		command.Fingerprint = nativeCommandFingerprint(command)
	}
	size, err := replication.CommandSize(command)
	if err != nil {
		return NativeResult{}, err
	}
	if size > session.maxCommand {
		return NativeResult{}, ErrNativeBundleBound
	}
	if cap(session.command) < size {
		session.command = make([]byte, 0, size)
	}
	session.command, err = replication.AppendCommand(session.command[:0], command)
	if err != nil {
		return NativeResult{}, err
	}
	session.pending = true
	return session.executePending(ctx, false)
}

func (session *NativeSession) executePending(
	ctx context.Context,
	priorUnknown bool,
) (NativeResult, error) {
	var hint *shardservice.ReplicatedMemberState
	if session.leader != (shardservice.ReplicatedMemberState{}) {
		hint = &session.leader
	}
	result, err := session.executor.propose(
		ctx, session.route, session.command, hint, priorUnknown,
	)
	if err != nil {
		if errors.Is(err, raftservice.ErrOutcomeUnknown) {
			return NativeResult{}, err
		}
		command, openErr := replication.OpenCommand(session.command)
		if openErr == nil && command.Kind() == replication.CommandSessionRelease &&
			errors.Is(err, replicatedstate.ErrSessionReleased) {
			session.phase = nativeSessionReleased
			session.clearPending()
			return NativeResult{Released: true}, nil
		}
		session.clearPending()
		return NativeResult{}, err
	}
	session.leader = result.State
	command, err := replication.OpenCommand(session.command)
	if err != nil {
		return NativeResult{}, err
	}
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil || !nativeCompletionMatches(command, completion) {
		return NativeResult{}, &raftservice.UnknownOutcomeError{
			Command: append([]byte(nil), session.command...), Cause: ErrReplicatedRoute,
		}
	}
	session.finishCompletion(command, completion)
	session.clearPending()
	return NativeResult{Outcome: result.Outcome, Completion: completion}, nil
}

func (session *NativeSession) finishCompletion(
	command replication.CommandView,
	completion replication.CompletionView,
) {
	if command.Kind() == replication.CommandSessionOpen {
		session.epoch = completion.ClientEpoch
		session.nextSequence = 2
		session.ackThrough = 1
		session.leaseDeadline = command.NextDeadlineUnixNano
		session.phase = nativeSessionActive
		return
	}
	session.ackThrough = command.ClientSequence
	if command.ClientSequence == math.MaxUint64 {
		session.nextSequence = 0
	} else {
		session.nextSequence = command.ClientSequence + 1
	}
	switch completion.ResultCode {
	case replicatedstate.ResultSessionRenewed:
		session.leaseDeadline = command.NextDeadlineUnixNano
	case replicatedstate.ResultSessionRetired, replicatedstate.ResultSessionRevoked:
		session.phase = nativeSessionRetired
		session.terminalSequence = command.ClientSequence
		session.terminalFingerprint = command.Fingerprint
		if completion.ResultCode == replicatedstate.ResultSessionRevoked {
			session.leaseDeadline = 0
		}
	}
}

func (session *NativeSession) clearPending() {
	session.pending = false
	session.command = session.command[:0]
}

func sessionStateError(session *NativeSession) error {
	if session != nil && session.pending {
		return ErrNativeCommandPending
	}
	return ErrNativeSessionState
}

func nativeCompletionMatches(
	command replication.CommandView,
	completion replication.CompletionView,
) bool {
	if completion.ResultFormat != replicatedstate.ResultFormatMutation ||
		completion.Storage != replication.CompletionInline ||
		completion.ResultLength != 0 || len(completion.InlineResult) != 0 ||
		!nativeCompletionResultMatches(command.Kind(), completion.ResultCode) {
		return false
	}
	clientEpoch := command.ClientEpoch
	if command.Kind() == replication.CommandSessionOpen {
		clientEpoch = completion.ClientEpoch
		if completion.AppliedSequence != completion.ClientEpoch {
			return false
		}
	} else if completion.AppliedSequence <= completion.ClientEpoch {
		return false
	}
	return completion.ClusterID == command.ClusterID &&
		completion.ClusterIncarnation == command.ClusterIncarnation &&
		completion.TopologyRecoveryEpoch == command.TopologyRecoveryEpoch &&
		bytes.Equal(completion.Distribution, command.Distribution) &&
		bytes.Equal(completion.Shard, command.Shard) &&
		completion.AllocationGeneration == command.AllocationGeneration &&
		completion.ShardIncarnation == command.ShardIncarnation &&
		completion.GroupID == command.GroupID &&
		completion.ReplicaSetVersion == command.ReplicaSetVersion &&
		completion.ActivePolicyGeneration == command.ActivePolicyGeneration &&
		completion.ProtectionEpoch == command.ProtectionEpoch &&
		completion.RoutingVersion == command.RoutingVersion &&
		completion.RouteGeneration == command.RouteGeneration &&
		bytes.Equal(completion.Tenant, command.Tenant) &&
		completion.ClientID == command.ClientID && completion.ClientEpoch == clientEpoch &&
		completion.ClientSequence == command.ClientSequence &&
		completion.Fingerprint == command.Fingerprint && completion.RetryHome == command.RetryHome
}

func nativeCompletionResultMatches(kind replication.CommandKind, result uint32) bool {
	switch kind {
	case replication.CommandMutationBatch:
		switch result {
		case replicatedstate.ResultApplied,
			replicatedstate.ResultStaleFence,
			replicatedstate.ResultUnknownCollection,
			replicatedstate.ResultInvalidDocument,
			replicatedstate.ResultTargetBound,
			replicatedstate.ResultWrongShard:
			return true
		}
	case replication.CommandSessionRetire:
		return result == replicatedstate.ResultSessionRetired ||
			result == replicatedstate.ResultStaleFence
	case replication.CommandSessionOpen:
		return result == replicatedstate.ResultSessionOpened
	case replication.CommandSessionRenew:
		return result == replicatedstate.ResultSessionRenewed ||
			result == replicatedstate.ResultStaleFence
	case replication.CommandSessionRevoke:
		return result == replicatedstate.ResultSessionRevoked ||
			result == replicatedstate.ResultStaleFence
	case replication.CommandSessionRelease:
		return false
	}
	return false
}

func validNativeSessionIdentity(distribution, shard string, tenant []byte) bool {
	return validNativeTextIdentity(distribution) && validNativeTextIdentity(shard) &&
		len(tenant) > 0 && len(tenant) <= replication.MaxIdentityBytes
}

func validNativeTextIdentity(value string) bool {
	return len(value) > 0 && len(value) <= replication.MaxIdentityBytes &&
		utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

var nativeFingerprintDomain = []byte("vibedb/gateway/native-command\x00")

func nativeCommandFingerprint(command replication.Command) replication.Digest {
	hasher := sha256.New()
	_, _ = hasher.Write(nativeFingerprintDomain)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.Kind))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.ClusterID[:])
	_, _ = hasher.Write(command.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(scalar[:], command.TopologyRecoveryEpoch)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Distribution)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write([]byte(command.Distribution))
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Shard)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write([]byte(command.Shard))
	binary.LittleEndian.PutUint64(scalar[:], command.AllocationGeneration)
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.ShardIncarnation[:])
	_, _ = hasher.Write(command.GroupID[:])
	binary.LittleEndian.PutUint64(scalar[:], command.ReplicaSetVersion)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.ActivePolicyGeneration)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.ProtectionEpoch)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.OwnershipEpoch)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.SchemaGeneration)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.RoutingVersion)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.RouteGeneration)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Tenant)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.Tenant)
	_, _ = hasher.Write(command.ClientID[:])
	binary.LittleEndian.PutUint64(scalar[:], command.ClientEpoch)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.ClientSequence)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.AckThrough)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.ExpectedDeadlineUnixNano))
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.NextDeadlineUnixNano))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.RetryHome[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Batches)))
	_, _ = hasher.Write(scalar[:])
	for _, batch := range command.Batches {
		binary.LittleEndian.PutUint64(scalar[:], uint64(batch.Relation))
		_, _ = hasher.Write(scalar[:])
		binary.LittleEndian.PutUint64(scalar[:], uint64(len(batch.Mutations)))
		_, _ = hasher.Write(scalar[:])
		for _, mutation := range batch.Mutations {
			binary.LittleEndian.PutUint64(scalar[:], uint64(mutation.Kind))
			_, _ = hasher.Write(scalar[:])
			binary.LittleEndian.PutUint64(scalar[:], uint64(len(mutation.Key)))
			_, _ = hasher.Write(scalar[:])
			_, _ = hasher.Write(mutation.Key)
			binary.LittleEndian.PutUint64(scalar[:], uint64(len(mutation.Value)))
			_, _ = hasher.Write(scalar[:])
			_, _ = hasher.Write(mutation.Value)
		}
	}
	var result replication.Digest
	_ = hasher.Sum(result[:0])
	return result
}
