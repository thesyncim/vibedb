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

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
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
	// Relation optionally names an exact dense relation. Zero selects the base.
	Relation            replication.RelationID
	Kind                replication.MutationKind
	Key                 []byte
	Value               []byte
	ExpectedValueLength uint64
	ExpectedValueDigest replication.Digest
}

// ExactRelationResolver admits an immutable schema-generation-bound set of
// dense relation IDs. Relation names never enter the command or Raft log.
type ExactRelationResolver struct {
	Base      replication.RelationID
	Relations []replication.RelationID
}

func (resolver ExactRelationResolver) ResolveNative(
	builder *RelationBundleBuilder,
	mutation NativeMutation,
) error {
	relation := mutation.Relation
	if relation == 0 {
		relation = resolver.Base
	}
	allowed := relation == resolver.Base
	for index := range resolver.Relations {
		if resolver.Relations[index] == relation {
			allowed = true
			break
		}
	}
	if !allowed {
		return ErrNativeBundleBound
	}
	return builder.Add(relation, replication.Mutation{
		Kind: mutation.Kind, Key: mutation.Key, Value: mutation.Value,
		ExpectedValueLength: mutation.ExpectedValueLength,
		ExpectedValueDigest: mutation.ExpectedValueDigest,
	})
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
		ExpectedValueLength: mutation.ExpectedValueLength,
		ExpectedValueDigest: mutation.ExpectedValueDigest,
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
	Journal      *NativeSessionJournal
	// CatalogBootstrap enables placement discovery for the reserved catalog
	// control session before its first Open. It does not alter the durable
	// journal binding or permit a data session to follow stale routes.
	CatalogBootstrap *Snapshot
	// ProposalCapability is the exact authorization class placed on every
	// probe and proposal. DataWrite, Topology, and ExecutionPin are admitted.
	ProposalCapability serviceauthz.Capability
	// ScopedCoordination admits only route-pin or execution-pin operations and
	// their session lifecycle, never data mutations or topology changes.
	ScopedCoordination bool

	MaxRelationBatches  int
	MaxMutations        int
	InitialCommandBytes int
	MaxCommandBytes     int
}

// NativeSessionJournalBinding returns the portable identity of one durable
// base-relation session. Replica addresses are deliberately excluded: routing
// can change while an exact pending command remains retryable. Every field
// interpreted by replicated apply is included, including the manifest digest.
func NativeSessionJournalBinding(
	route ReplicatedRoute,
	distribution, shard string,
	tenant []byte,
	relation replication.RelationID,
	capability serviceauthz.Capability,
) (replication.Digest, error) {
	if !validReplicatedRoute(route) ||
		distribution != string(route.Distribution) || shard != string(route.Shard) ||
		!validNativeSessionIdentity(distribution, shard, tenant) ||
		relation == 0 || relation > replication.MaxRelationID ||
		(capability != serviceauthz.CapabilityDataWrite &&
			capability != serviceauthz.CapabilityTopology &&
			capability != serviceauthz.CapabilityExecutionPin) {
		return replication.Digest{}, ErrNativeSession
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/native-session-journal/1"))
	_, _ = hash.Write(route.Group.ClusterID[:])
	_, _ = hash.Write(route.Group.ClusterIncarnation[:])
	_, _ = hash.Write(route.Group.ShardIncarnation[:])
	_, _ = hash.Write(route.Group.GroupID[:])
	var scalar [8]byte
	writeScalar := func(value uint64) {
		binary.LittleEndian.PutUint64(scalar[:], value)
		_, _ = hash.Write(scalar[:])
	}
	writeBytes := func(value []byte) {
		writeScalar(uint64(len(value)))
		_, _ = hash.Write(value)
	}
	writeScalar(route.Group.TopologyRecoveryEpoch)
	writeScalar(route.AllocationGeneration)
	writeScalar(route.Command.ReplicaSetVersion)
	writeScalar(route.Command.ActivePolicyGeneration)
	writeScalar(route.Command.ProtectionEpoch)
	writeScalar(route.Command.OwnershipEpoch)
	writeScalar(route.Command.SchemaGeneration)
	_, _ = hash.Write(route.Command.RelationManifestDigest[:])
	writeScalar(route.Command.RoutingVersion)
	writeScalar(route.Command.RouteGeneration)
	writeBytes([]byte(distribution))
	writeBytes([]byte(shard))
	writeBytes(tenant)
	writeScalar(uint64(relation))
	writeScalar(uint64(capability))
	var digest replication.Digest
	copy(digest[:], hash.Sum(nil))
	return digest, nil
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
	gateCommand         [routegate.CommandBytes]byte
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
	journal             *NativeSessionJournal
	proposalCapability  serviceauthz.Capability
	scopedCoordination  bool
	catalogControl      bool
	catalogBootstrap    *Snapshot
	catalogHolder       *CatalogHolder
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

// NativeSessionMatchesControlBinding reports whether session is the dedicated
// durable controller session for one exact live serving fence. It deliberately
// checks the member/store incarnation against the route's authenticated replica
// set as well as every apply-visible command generation. A controller can use
// this before handing destructive work to NativeSession without exposing or
// copying the session's private route and journal state.
func NativeSessionMatchesControlBinding(
	session *NativeSession,
	fence raftservice.ServingFence,
	tenant []byte,
	clientID replication.ID128,
	relation replication.RelationID,
	capability serviceauthz.Capability,
) bool {
	if session == nil || session.journal == nil || clientID == (replication.ID128{}) ||
		relation == 0 || !fence.Command.Valid() || fence.MemberID == 0 ||
		fence.StoreID == ([16]byte{}) || fence.NodeIncarnation == 0 || fence.Term == 0 ||
		session.clientID != clientID || !bytes.Equal(session.tenant, tenant) ||
		session.proposalCapability != capability ||
		nativeSessionBaseRelation(session) != relation ||
		session.route.Group != fence.Group ||
		session.route.AllocationGeneration != fence.AllocationGeneration ||
		session.route.Command != fence.Command {
		return false
	}
	for index := range session.route.Replicas {
		replica := session.route.Replicas[index]
		if replica.Member == fence.MemberID && replica.StoreID == fence.StoreID &&
			replica.NodeIncarnation == fence.NodeIncarnation {
			return true
		}
	}
	return false
}

// NativeSessionSupportsMutationBound proves that a worst-case conditional
// delete batch inside the supplied independent key-count/key-byte ceilings fits
// the session's configured mutation, relation, and canonical command bounds.
// It is a cold controller-construction check; the serving hot path remains
// allocation-free apart from its preallocated command workspace.
func NativeSessionSupportsMutationBound(
	session *NativeSession,
	relation replication.RelationID,
	maxKeys, maxKeyBytes int,
) bool {
	if session == nil || relation == 0 || relation > replication.MaxRelationID ||
		maxKeys <= 0 || maxKeys > session.bundle.maxMutations ||
		maxKeyBytes <= 0 || maxKeyBytes > replication.MaxCommandBytes ||
		nativeSessionBaseRelation(session) != relation {
		return false
	}
	count := maxKeys
	if count > maxKeyBytes {
		count = maxKeyBytes
	}
	// These are independent upper bounds, not a requirement to fill both.
	// A loose byte ceiling cannot make a bounded key batch unrepresentable:
	// each individual key is already capped by the command grammar.
	maxKeyBytes = min(maxKeyBytes, count*replication.MaxMutationKeyBytes)
	largest := (maxKeyBytes + count - 1) / count
	dummy := make([]byte, largest)
	mutations := make([]replication.Mutation, count)
	digest := replication.Digest{1}
	remaining := maxKeyBytes
	for index := range mutations {
		left := count - index
		size := (remaining + left - 1) / left
		mutations[index] = replication.Mutation{
			Kind: replication.MutationDeleteDigestEqual, Key: dummy[:size],
			ExpectedValueLength: 1, ExpectedValueDigest: digest,
		}
		remaining -= size
	}
	command := session.commandHeader(
		replication.CommandMutationBatch, session.epoch, session.nextSequence, session.ackThrough,
	)
	command.Fingerprint = replication.Digest{1}
	relationCount := min(count, session.bundle.maxRelations)
	command.Batches = make([]replication.RelationMutationBatch, relationCount)
	for index := 0; index < relationCount; index++ {
		start := index * count / relationCount
		end := (index + 1) * count / relationCount
		command.Batches[index] = replication.RelationMutationBatch{
			Relation: replication.RelationID(index + 1), Mutations: mutations[start:end],
		}
	}
	size, err := replication.CommandSize(command)
	return err == nil && size <= session.maxCommand-replication.RetainedPruneProofBytes
}

// NativeSessionSupportsExactRelations proves that the session resolver is
// pinned to precisely the expected schema-generation relation set.
func NativeSessionSupportsExactRelations(
	session *NativeSession,
	base replication.RelationID,
	relations []replication.RelationID,
) bool {
	if session == nil || nativeSessionBaseRelation(session) != base {
		return false
	}
	if len(relations)+1 > session.bundle.maxRelations {
		return false
	}
	if len(relations) == 0 {
		switch resolver := session.resolver.(type) {
		case BaseRelationResolver:
			return resolver.Relation == base
		case *BaseRelationResolver:
			return resolver != nil && resolver.Relation == base
		case ExactRelationResolver:
			return resolver.Base == base && len(resolver.Relations) == 0
		case *ExactRelationResolver:
			return resolver != nil && resolver.Base == base && len(resolver.Relations) == 0
		default:
			return false
		}
	}
	var exact ExactRelationResolver
	switch resolver := session.resolver.(type) {
	case ExactRelationResolver:
		exact = resolver
	case *ExactRelationResolver:
		if resolver == nil {
			return false
		}
		exact = *resolver
	default:
		return false
	}
	if exact.Base != base || len(exact.Relations) != len(relations) {
		return false
	}
	for index := range relations {
		if exact.Relations[index] != relations[index] {
			return false
		}
	}
	return true
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
	if options.ScopedCoordination && options.ProposalCapability != serviceauthz.CapabilityDataWrite && options.ProposalCapability != serviceauthz.CapabilityExecutionPin {
		return nil, ErrNativeSession
	}
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
		options.Distribution != string(options.Route.Distribution) ||
		options.Shard != string(options.Route.Shard) ||
		!validNativeSessionIdentity(options.Distribution, options.Shard, options.Tenant) ||
		options.ClientID == (replication.ID128{}) || options.Resolver == nil ||
		options.MaxRelationBatches <= 0 || options.MaxRelationBatches > replication.MaxRelationBatches ||
		options.MaxMutations <= 0 || options.MaxMutations > replication.MaxMutations ||
		options.MaxCommandBytes <= 0 || options.MaxCommandBytes > replication.MaxCommandBytes ||
		options.InitialCommandBytes <= 0 || options.InitialCommandBytes > options.MaxCommandBytes {
		return nil, ErrNativeSession
	}
	if options.ProposalCapability != serviceauthz.CapabilityDataWrite &&
		options.ProposalCapability != serviceauthz.CapabilityTopology &&
		options.ProposalCapability != serviceauthz.CapabilityExecutionPin {
		return nil, ErrNativeSession
	}
	if options.CatalogBootstrap != nil && (!catalogBootstrapRoute(options.Route) ||
		options.ProposalCapability != serviceauthz.CapabilityTopology) {
		return nil, ErrNativeSession
	}
	route := options.Route
	route.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
	tenant := append([]byte(nil), options.Tenant...)
	tenant = tenant[:len(tenant):len(tenant)]
	session := &NativeSession{
		executor: options.Executor, route: route,
		distribution: strings.Clone(options.Distribution), shard: strings.Clone(options.Shard),
		tenant: tenant, clientID: options.ClientID, retryHome: options.RetryHome,
		resolver: options.Resolver,
		bundle:   newRelationBundleBuilder(options.MaxRelationBatches, options.MaxMutations),
		command:  make([]byte, 0, options.InitialCommandBytes), maxCommand: options.MaxCommandBytes,
		nextSequence:       1,
		journal:            options.Journal,
		proposalCapability: options.ProposalCapability,
		scopedCoordination: options.ScopedCoordination,
		catalogControl:     options.CatalogBootstrap != nil, catalogBootstrap: options.CatalogBootstrap,
	}
	if options.Journal != nil {
		relation := nativeResolverBaseRelation(options.Resolver)
		expectedBinding, bindingErr := NativeSessionJournalBinding(
			options.Route, options.Distribution, options.Shard, options.Tenant,
			relation, options.ProposalCapability,
		)
		state, loadErr := options.Journal.load()
		if options.ScopedCoordination {
			expectedBinding = scopedNativeSessionJournalBinding(expectedBinding)
		}
		if bindingErr != nil || loadErr != nil ||
			options.Journal.maxCommand != options.MaxCommandBytes ||
			options.Journal.binding != expectedBinding ||
			state.clientID != options.ClientID || state.retryHome != options.RetryHome {
			return nil, errors.Join(bindingErr, loadErr, ErrNativeSession)
		}
		session.restoreDurableState(state)
	}
	return session, nil
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

// PutIfAbsentOrEqual creates one document or confirms an exact canonical
// retry. A different existing value returns a deterministic conflict.
func (session *NativeSession) PutIfAbsentOrEqual(
	ctx context.Context,
	key, document []byte,
) (NativeResult, error) {
	return session.mutate(ctx, NativeMutation{
		Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: document,
	})
}

func (session *NativeSession) Delete(
	ctx context.Context,
	key []byte,
) (NativeResult, error) {
	return session.mutate(ctx, NativeMutation{Kind: replication.MutationDelete, Key: key})
}

// CompareDelete removes one document only when its current raw identity
// matches. An already absent value is an idempotent success.
func (session *NativeSession) CompareDelete(
	ctx context.Context,
	key []byte,
	expectedLength uint64,
	expectedDigest replication.Digest,
) (NativeResult, error) {
	return session.mutate(ctx, NativeMutation{
		Kind: replication.MutationDeleteDigestEqual, Key: key,
		ExpectedValueLength: expectedLength, ExpectedValueDigest: expectedDigest,
	})
}

// ComparePut atomically replaces one document only when the current raw value
// has the exact expected length and SHA-256 digest.
func (session *NativeSession) ComparePut(
	ctx context.Context,
	key, document []byte,
	expectedLength uint64,
	expectedDigest replication.Digest,
) (NativeResult, error) {
	return session.mutate(ctx, NativeMutation{
		Kind: replication.MutationPutDigestEqual, Key: key, Value: document,
		ExpectedValueLength: expectedLength, ExpectedValueDigest: expectedDigest,
	})
}

func (session *NativeSession) mutate(
	ctx context.Context,
	mutation NativeMutation,
) (NativeResult, error) {
	mutations := [...]NativeMutation{mutation}
	return session.MutateBatch(ctx, mutations[:])
}

// MutateBatch proposes one atomic ordered logical batch. It is primarily the
// control-plane primitive for publishing an operation record with its bounded
// discovery directory in the same RF3 entry; partial visibility is impossible.
// Resolver expansion remains covered by the session's fixed relation/mutation
// bounds, and the exact encoded command is retained on outcome unknown.
func (session *NativeSession) MutateBatch(
	ctx context.Context,
	mutations []NativeMutation,
) (NativeResult, error) {
	return session.mutateBatch(ctx, mutations, replication.Digest{}, false)
}

// RetainedPruneBatch proposes the dedicated topology-only physical cleanup
// command. Ordinary data and topology mutations cannot opt into its sealed-away
// ownership semantics.
func (session *NativeSession) RetainedPruneBatch(
	ctx context.Context,
	mutations []NativeMutation,
	proof replication.RetainedPruneProof,
) (NativeResult, error) {
	if session == nil || session.proposalCapability != serviceauthz.CapabilityTopology ||
		!proof.Valid() {
		return NativeResult{}, sessionStateError(session)
	}
	return session.mutateBatch(ctx, mutations, proof.BatchDigest, true, &proof)
}

func (session *NativeSession) mutateBatch(
	ctx context.Context,
	mutations []NativeMutation,
	fingerprint replication.Digest,
	preserveFingerprint bool,
	prune ...*replication.RetainedPruneProof,
) (NativeResult, error) {
	if session == nil || ctx == nil || session.phase != nativeSessionActive || session.pending ||
		session.proposalCapability == serviceauthz.CapabilityExecutionPin ||
		session.nextSequence == 0 || session.nextSequence == math.MaxUint64 {
		return NativeResult{}, sessionStateError(session)
	}
	if len(mutations) == 0 || len(mutations) > session.bundle.maxMutations {
		return NativeResult{}, ErrNativeBundleBound
	}
	session.bundle.reset()
	for index := range mutations {
		if err := session.validateNativeMutation(mutations[index]); err != nil {
			session.bundle.reset()
			return NativeResult{}, err
		}
		if err := session.resolver.ResolveNative(&session.bundle, mutations[index]); err != nil {
			session.bundle.reset()
			return NativeResult{}, err
		}
	}
	if len(session.bundle.batches) == 0 || len(session.bundle.mutations) == 0 {
		session.bundle.reset()
		return NativeResult{}, ErrNativeBundleBound
	}
	command := session.commandHeader(
		replication.CommandMutationBatch, session.epoch, session.nextSequence, session.ackThrough,
	)
	if len(prune) == 1 && prune[0] != nil {
		command.Kind = replication.CommandRetainedPrune
		command.RetainedPrune = *prune[0]
	} else if len(prune) != 0 {
		session.bundle.reset()
		return NativeResult{}, ErrNativeSession
	}
	command.Fingerprint = fingerprint
	command.Batches = session.bundle.batches
	result, err := session.prepareAndExecute(ctx, command, preserveFingerprint)
	session.bundle.reset()
	return result, err
}

// ExecutionPin proposes one canonical logical catalog/schema pin transition.
// The typed fixed-width kernel is encoded inside the session's exact durable
// retry command; callers cannot inject raw replicated command bytes.
func (session *NativeSession) ExecutionPin(
	ctx context.Context,
	transition executionpin.Command,
) (NativeResult, error) {
	if session == nil || ctx == nil || session.phase != nativeSessionActive || session.pending ||
		session.proposalCapability != serviceauthz.CapabilityExecutionPin ||
		session.nextSequence == 0 || session.nextSequence == math.MaxUint64 {
		return NativeResult{}, sessionStateError(session)
	}
	var storage [executionpin.CommandBytes]byte
	nested, err := executionpin.AppendCommand(storage[:0], transition)
	if err != nil {
		return NativeResult{}, ErrNativeSession
	}
	command := session.commandHeader(
		replication.CommandExecutionPin, session.epoch, session.nextSequence, session.ackThrough,
	)
	command.ExecutionPin = nested
	return session.prepareAndExecute(ctx, command, false)
}

// RouteGate orders one typed shared pin or topology drain through the opened
// session's exact-retry journal. Gate epochs are participant gate epochs, not
// session epochs or execution-controller epochs.
func (session *NativeSession) RouteGate(ctx context.Context, transition routegate.Command) (NativeResult, error) {
	if ctx == nil {
		return NativeResult{}, ErrNativeSession
	}
	if err := session.prepareRouteGate(transition); err != nil {
		return NativeResult{}, err
	}
	return session.executePending(ctx, false)
}

// prepareRouteGate persists intent without admitting a proposal. The durable
// request coordinator must install these exact bytes in its ledger before it
// calls RetryPending; a local journal cannot substitute for that witness.
func (session *NativeSession) prepareRouteGate(transition routegate.Command) error {
	if session == nil || session.phase != nativeSessionActive || session.pending ||
		session.nextSequence == 0 || session.nextSequence == math.MaxUint64 {
		return sessionStateError(session)
	}
	switch transition.Operation {
	case routegate.OperationAcquireShared, routegate.OperationReleaseShared:
		if session.proposalCapability != serviceauthz.CapabilityDataWrite {
			return ErrNativeSession
		}
	case routegate.OperationBeginExclusive, routegate.OperationReleaseExclusive, routegate.OperationCompactReleased:
		if session.proposalCapability != serviceauthz.CapabilityTopology {
			return ErrNativeSession
		}
	default:
		return ErrNativeSession
	}
	nested, err := routegate.AppendCommand(session.gateCommand[:0], transition)
	if err != nil {
		return err
	}
	command := session.commandHeader(replication.CommandRouteGate, session.epoch, session.nextSequence, session.ackThrough)
	command.RouteGate = nested
	return session.prepareCommand(command, false)
}

// SplitCaptureActivate proposes one canonical source-capture activation through
// a durable topology session. The journal owns the exact nested command before
// admission, so an outcome-unknown retry cannot move the capture cut.
func (session *NativeSession) SplitCaptureActivate(
	ctx context.Context,
	activation []byte,
) (NativeResult, error) {
	if session == nil || ctx == nil || session.phase != nativeSessionActive || session.pending ||
		session.proposalCapability != serviceauthz.CapabilityTopology ||
		session.nextSequence == 0 || session.nextSequence == math.MaxUint64 ||
		len(activation) == 0 || len(activation) > replication.MaxCommandBytes {
		return NativeResult{}, sessionStateError(session)
	}
	command := session.commandHeader(
		replication.CommandSplitCaptureActivate,
		session.epoch, session.nextSequence, session.ackThrough,
	)
	command.SplitCaptureActivation = activation
	return session.prepareAndExecute(ctx, command, false)
}

func (session *NativeSession) validateNativeMutation(mutation NativeMutation) error {
	// Reject byte bounds before vibejson validation, resolver fanout, command
	// hashing, or buffer growth. An oversized value cannot fit this session's
	// command even if the global mutation bound is larger.
	if len(mutation.Key) == 0 || len(mutation.Key) > replication.MaxMutationKeyBytes {
		return ErrNativeBundleBound
	}
	switch mutation.Kind {
	case replication.MutationPut, replication.MutationPutAbsentOrEqual,
		replication.MutationPutDigestEqual:
		if len(mutation.Value) == 0 ||
			len(mutation.Value) > replication.MaxMutationValueBytes ||
			len(mutation.Value) > session.maxCommand {
			return ErrNativeBundleBound
		}
		if !vibejson.Valid(mutation.Value) {
			return ErrNativeDocument
		}
		if mutation.Kind == replication.MutationPutDigestEqual &&
			(mutation.ExpectedValueLength == 0 ||
				mutation.ExpectedValueLength > replication.MaxMutationValueBytes ||
				mutation.ExpectedValueDigest == (replication.Digest{})) {
			return ErrNativeBundleBound
		}
	case replication.MutationDelete:
		if len(mutation.Value) != 0 {
			return ErrNativeBundleBound
		}
	case replication.MutationDeleteDigestEqual:
		if len(mutation.Value) != 0 || mutation.ExpectedValueLength == 0 ||
			mutation.ExpectedValueLength > replication.MaxMutationValueBytes ||
			mutation.ExpectedValueDigest == (replication.Digest{}) {
			return ErrNativeBundleBound
		}
	default:
		return ErrNativeBundleBound
	}
	return nil
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

// RetireReleaseAndDestroy settles the exact durable session lifecycle before a
// controller abandons its current routing binding. Every outcome-unknown phase
// remains in the journal for byte-identical retry. The journal is removed only
// after replicated Release has settled, which makes a fresh session with the
// same client identity safe on the replacement binding.
func (session *NativeSession) RetireReleaseAndDestroy(ctx context.Context) error {
	if session == nil || ctx == nil || session.journal == nil {
		return ErrNativeSession
	}
	if session.pending {
		if _, err := session.RetryPending(ctx); err != nil {
			return err
		}
	}
	if session.phase == nativeSessionActive {
		if _, err := session.Retire(ctx); err != nil {
			return err
		}
	}
	if session.phase == nativeSessionRetired {
		if _, err := session.Release(ctx); err != nil {
			return err
		}
	}
	if session.phase != nativeSessionReleased || session.pending {
		return ErrNativeSessionState
	}
	return session.journal.destroyReleased()
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
	authorityClass := replication.CommandAuthorityData
	if session.proposalCapability == serviceauthz.CapabilityTopology {
		authorityClass = replication.CommandAuthorityTopology
	} else if session.proposalCapability == serviceauthz.CapabilityExecutionPin {
		authorityClass = replication.CommandAuthorityExecutionPin
	}
	if session.scopedCoordination {
		if session.proposalCapability == serviceauthz.CapabilityExecutionPin {
			authorityClass = replication.CommandAuthorityExecutionSession
		} else {
			authorityClass = replication.CommandAuthorityRouteSession
		}
	}
	return replication.Command{
		Kind:                  kind,
		AuthorityClass:        authorityClass,
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
	if ctx == nil {
		return NativeResult{}, ErrNativeSessionState
	}
	if session.catalogControl {
		route, err := session.catalogOperationalRoute(ctx)
		if err != nil {
			return NativeResult{}, err
		}
		command.ReplicaSetVersion = route.Command.ReplicaSetVersion
		command.OwnershipEpoch = route.Command.OwnershipEpoch
		command.RoutingVersion = route.Command.RoutingVersion
		command.RouteGeneration = route.Command.RouteGeneration
	}
	if err := session.prepareCommand(command, preserveFingerprint); err != nil {
		return NativeResult{}, err
	}
	return session.executePending(ctx, false)
}

func (session *NativeSession) prepareCommand(command replication.Command, preserveFingerprint bool) error {
	if session == nil || session.pending {
		return ErrNativeSessionState
	}
	if !preserveFingerprint {
		command.Fingerprint = nativeCommandFingerprint(command)
	}
	size, err := replication.CommandSize(command)
	if err != nil {
		return err
	}
	if size > session.maxCommand {
		return ErrNativeBundleBound
	}
	if cap(session.command) < size {
		session.command = make([]byte, 0, size)
	}
	session.command, err = replication.AppendCommand(session.command[:0], command)
	if err != nil {
		return err
	}
	session.pending = true
	if session.journal != nil {
		if err = session.journal.store(session.durableState()); err != nil {
			session.pending = false
			session.command = session.command[:0]
			return err
		}
	}
	return nil
}

func (session *NativeSession) executePending(
	ctx context.Context,
	priorUnknown bool,
) (NativeResult, error) {
	route := session.route
	if session.catalogControl {
		var err error
		route, err = session.catalogOperationalRoute(ctx)
		if err != nil {
			return NativeResult{}, &raftservice.UnknownOutcomeError{
				Command: append([]byte(nil), session.command...), Cause: err,
			}
		}
	}
	var hint *shardservice.ReplicatedMemberState
	// A cached leader is a latency hint only while no admitted outcome is in
	// doubt. RetryPending may follow that leader's failure; discover the current
	// leader before spending a bounded proposal attempt on the retained bytes.
	// Catalog discovery above just published a fresh authenticated leader to
	// the executor. A session-local hint from before failover must not replace
	// that observation and spend another full timeout on the stopped voter.
	if !priorUnknown && !session.catalogControl && session.leader != (shardservice.ReplicatedMemberState{}) {
		hint = &session.leader
	}
	result, err := session.executor.propose(
		ctx, route, session.command, hint, priorUnknown, session.proposalCapability,
		replicatedUnknownCommandClone,
	)
	if err != nil {
		if errors.Is(err, raftservice.ErrOutcomeUnknown) {
			return NativeResult{}, err
		}
		command, openErr := replication.OpenCommand(session.command)
		if openErr == nil && command.Kind() == replication.CommandSessionRelease &&
			errors.Is(err, replicatedstate.ErrSessionReleased) {
			prior := session.durableState()
			session.phase = nativeSessionReleased
			if persistErr := session.persistCompletedPending(prior); persistErr != nil {
				return NativeResult{}, persistErr
			}
			return NativeResult{Released: true}, nil
		}
		if persistErr := session.persistAbandonedPending(); persistErr != nil {
			return NativeResult{}, persistErr
		}
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
	prior := session.durableState()
	session.finishCompletion(command, completion)
	if err = session.persistCompletedPending(prior); err != nil {
		return NativeResult{}, err
	}
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

func (session *NativeSession) durableState() durableNativeSessionState {
	state := durableNativeSessionState{
		clientID: session.clientID, retryHome: session.retryHome, phase: session.phase,
		epoch: session.epoch, nextSequence: session.nextSequence, ackThrough: session.ackThrough,
		leaseDeadline: session.leaseDeadline, terminalSequence: session.terminalSequence,
		terminalFingerprint: session.terminalFingerprint, pending: session.pending,
	}
	if session.pending {
		state.command = session.command
	}
	return state
}

func (session *NativeSession) restoreDurableState(state durableNativeSessionState) {
	session.phase, session.epoch = state.phase, state.epoch
	session.nextSequence, session.ackThrough = state.nextSequence, state.ackThrough
	session.leaseDeadline, session.terminalSequence = state.leaseDeadline, state.terminalSequence
	session.terminalFingerprint, session.pending = state.terminalFingerprint, state.pending
	if state.pending {
		session.command = append(session.command[:0], state.command...)
	}
}

func (session *NativeSession) persistCompletedPending(prior durableNativeSessionState) error {
	completed := session.durableState()
	completed.pending, completed.command = false, nil
	if session.journal != nil {
		if err := session.journal.store(completed); err != nil {
			session.restoreDurableState(prior)
			return &raftservice.UnknownOutcomeError{
				Command: append([]byte(nil), prior.command...), Cause: err,
			}
		}
	}
	session.clearPending()
	return nil
}

func (session *NativeSession) persistAbandonedPending() error {
	prior := session.durableState()
	cleared := prior
	cleared.pending, cleared.command = false, nil
	if session.journal != nil {
		if err := session.journal.store(cleared); err != nil {
			return &raftservice.UnknownOutcomeError{
				Command: append([]byte(nil), prior.command...), Cause: err,
			}
		}
	}
	session.clearPending()
	return nil
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
	if completion.Storage != replication.CompletionInline {
		return false
	}
	if command.Kind() == replication.CommandTransaction {
		role, operation, ok := command.TransactionIdentity()
		result, err := replicatedstate.OpenTransactionCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		if !ok || err != nil ||
			completion.ResultFormat != replicatedstate.ResultFormatTransaction ||
			completion.ResultLength != uint64(len(completion.InlineResult)) ||
			result.Role != role || result.Operation != operation {
			return false
		}
	} else if command.Kind() == replication.CommandRequestLedger {
		identity, ok := command.RequestLedgerIdentity()
		result, err := replicatedstate.OpenRequestLedgerCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		if !ok || err != nil ||
			completion.ResultFormat != replicatedstate.ResultFormatRequestLedger ||
			completion.ResultLength != replicatedstate.RequestLedgerCompletionResultBytes ||
			result.Operation != identity.Operation || result.KeyDigest != identity.KeyDigest ||
			result.RequestDigest != identity.RequestDigest || result.PlanRoot != identity.PlanRoot ||
			result.RangeIdentity != identity.RangeIdentity {
			return false
		}
	} else if command.Kind() == replication.CommandRouteGate && completion.ResultFormat == replicatedstate.ResultFormatRouteGate {
		if completion.ResultCode != replicatedstate.ResultRouteGate || completion.ResultLength != routegate.OutcomeBytes ||
			len(completion.InlineResult) != routegate.OutcomeBytes {
			return false
		}
		outcome, err := routegate.OpenOutcome(completion.InlineResult)
		gate, gateErr := routegate.OpenCommand(command.RouteGateBytes())
		if err != nil || gateErr != nil || !nativeRouteGateOutcomeMatches(gate, outcome) {
			return false
		}
	} else if command.Kind() == replication.CommandExecutionPin {
		if completion.ResultFormat != replicatedstate.ResultFormatExecutionPin ||
			completion.ResultLength != executionpin.CompletionBytes ||
			len(completion.InlineResult) != executionpin.CompletionBytes {
			return false
		}
		proof, err := executionpin.OpenCompletion(completion.InlineResult)
		nested, nestedErr := command.OpenExecutionPin()
		if err != nil || nestedErr != nil || proof.Operation != nested.Operation ||
			!nativeExecutionPinResultCode(completion.ResultCode) {
			return false
		}
	} else {
		if completion.ResultFormat != replicatedstate.ResultFormatMutation ||
			completion.ResultLength != uint64(len(completion.InlineResult)) ||
			!nativeCompletionResultMatches(command.Kind(), completion.ResultCode) {
			return false
		}
		if _, err := replicatedstate.OpenMutationCompletionResult(
			completion.ResultCode, completion.InlineResult,
		); err != nil {
			return false
		}
	}
	clientEpoch := command.ClientEpoch
	if command.Kind() == replication.CommandSessionOpen {
		clientEpoch = completion.ClientEpoch
		if completion.AppliedSequence != completion.ClientEpoch {
			return false
		}
	} else if command.Kind() == replication.CommandTransaction || command.Kind() == replication.CommandRequestLedger {
		if completion.AppliedSequence == 0 {
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

// The envelope binds the whole command/session fingerprint; these checks also
// reject canonical but semantically impossible outcomes. A shared release can
// legitimately name an older acquisition epoch after compaction, so it must
// not be compared to the current gate epoch as if it were a fresh acquire.
func nativeRouteGateOutcomeMatches(command routegate.Command, outcome routegate.Outcome) bool {
	status := outcome.Status
	if outcome.Mutated && status.Revision == 0 {
		return false
	}
	if outcome.Reason == routegate.ReasonStaleEpoch {
		return status.Epoch != command.Epoch
	}
	if outcome.Reason == routegate.ReasonExhausted {
		return status.Revision == math.MaxUint64 || status.Epoch == math.MaxUint64 &&
			(command.Operation == routegate.OperationReleaseExclusive || command.Operation == routegate.OperationCompactReleased)
	}
	sameDrain := status.Drain.Identity == command.Identity &&
		status.Drain.Binding == command.Binding && status.Drain.Epoch == command.Epoch
	switch command.Operation {
	case routegate.OperationAcquireShared:
		if status.Epoch != command.Epoch {
			return false
		}
		switch outcome.Reason {
		case routegate.ReasonAcquired, routegate.ReasonIdempotent:
			return status.ActivePins != 0
		case routegate.ReasonAlreadyReleased:
			return status.ReleasedPins != 0
		case routegate.ReasonBlockedByDrain:
			return status.Drain.State == routegate.DrainPending || status.Drain.State == routegate.DrainActive
		case routegate.ReasonIdentityConflict, routegate.ReasonCapacity:
			return true
		}
	case routegate.OperationReleaseShared:
		switch outcome.Reason {
		case routegate.ReasonReleased, routegate.ReasonAlreadyReleased:
			return status.Epoch >= command.Epoch && status.ReleasedPins != 0
		case routegate.ReasonIdentityConflict:
			return true
		case routegate.ReasonCapacity:
			return status.Epoch == command.Epoch
		}
	case routegate.OperationBeginExclusive:
		if status.Epoch != command.Epoch {
			return false
		}
		switch outcome.Reason {
		case routegate.ReasonDrainPending:
			return sameDrain && status.Drain.State == routegate.DrainPending
		case routegate.ReasonDrainAcquired:
			return sameDrain && status.Drain.State == routegate.DrainActive
		case routegate.ReasonIdempotent:
			return sameDrain && (status.Drain.State == routegate.DrainPending || status.Drain.State == routegate.DrainActive)
		case routegate.ReasonIdentityConflict:
			return true
		}
	case routegate.OperationReleaseExclusive:
		switch outcome.Reason {
		case routegate.ReasonDrainReleased, routegate.ReasonIdempotent:
			return command.Epoch != math.MaxUint64 && status.Epoch == command.Epoch+1 &&
				sameDrain && status.Drain.State == routegate.DrainReleased
		case routegate.ReasonIdentityConflict, routegate.ReasonNotFound, routegate.ReasonDrainBusy:
			return status.Epoch == command.Epoch
		}
	case routegate.OperationCompactReleased:
		switch outcome.Reason {
		case routegate.ReasonCompacted:
			return command.Epoch != math.MaxUint64 && status.Epoch == command.Epoch+1 && status.Drain.State == routegate.DrainNone
		case routegate.ReasonDrainBusy:
			return status.Epoch == command.Epoch
		}
	}
	return false
}

func nativeCompletionResultMatches(kind replication.CommandKind, result uint32) bool {
	switch kind {
	case replication.CommandMutationBatch, replication.CommandRetainedPrune:
		switch result {
		case replicatedstate.ResultApplied,
			replicatedstate.ResultStaleFence,
			replicatedstate.ResultUnknownRelation,
			replicatedstate.ResultInvalidDocument,
			replicatedstate.ResultTargetBound,
			replicatedstate.ResultWrongShard,
			replicatedstate.ResultIndexConflict,
			replicatedstate.ResultIntentBusy:
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
	case replication.CommandExecutionPin:
		return nativeExecutionPinResultCode(result)
	case replication.CommandRouteGate:
		return result == replicatedstate.ResultStaleFence
	case replication.CommandSplitCaptureActivate:
		return result == replicatedstate.ResultApplied ||
			result == replicatedstate.ResultStaleFence ||
			result == replicatedstate.ResultIndexConflict
	}
	return false
}

func nativeExecutionPinResultCode(result uint32) bool {
	switch result {
	case replicatedstate.ResultApplied, replicatedstate.ResultStaleFence,
		replicatedstate.ResultIndexConflict, replicatedstate.ResultIntentBusy,
		replicatedstate.ResultTargetBound:
		return true
	default:
		return false
	}
}

func validNativeSessionIdentity(distribution, shard string, tenant []byte) bool {
	return validNativeTextIdentity(distribution) && validNativeTextIdentity(shard) &&
		len(tenant) > 0 && len(tenant) <= replication.MaxIdentityBytes
}

func validNativeTextIdentity(value string) bool {
	return len(value) > 0 && len(value) <= replication.MaxIdentityBytes &&
		utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

var (
	nativeFingerprintDomain           = []byte("vibedb/gateway/native-command\x00")
	nativeTopologyAuthorityMarker     = []byte{byte(replication.CommandAuthorityTopology)}
	nativeExecutionPinAuthorityMarker = []byte{byte(replication.CommandAuthorityExecutionPin)}
)

func nativeCommandFingerprint(command replication.Command) replication.Digest {
	hasher := sha256.New()
	_, _ = hasher.Write(nativeFingerprintDomain)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.Kind))
	_, _ = hasher.Write(scalar[:])
	if command.AuthorityClass == replication.CommandAuthorityTopology {
		_, _ = hasher.Write(nativeTopologyAuthorityMarker)
	} else if command.AuthorityClass == replication.CommandAuthorityExecutionPin {
		_, _ = hasher.Write(nativeExecutionPinAuthorityMarker)
	}
	if replication.IsScopedSessionAuthority(command.AuthorityClass) {
		scalar[0] = byte(command.AuthorityClass)
		_, _ = hasher.Write(scalar[:1])
	}
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
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Transaction)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.Transaction)
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.ExecutionPin)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.ExecutionPin)
	if command.Kind == replication.CommandRouteGate {
		binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.RouteGate)))
		_, _ = hasher.Write(scalar[:])
		_, _ = hasher.Write(command.RouteGate)
	}
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.SplitCaptureActivation)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.SplitCaptureActivation)
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
			binary.LittleEndian.PutUint64(scalar[:], mutation.ExpectedValueLength)
			_, _ = hasher.Write(scalar[:])
			_, _ = hasher.Write(mutation.ExpectedValueDigest[:])
		}
	}
	var result replication.Digest
	_ = hasher.Sum(result[:0])
	return result
}

// nativeCommandViewFingerprint recomputes the semantic fingerprint directly
// from a validated borrowed envelope. Detached recovery uses it to reject an
// opaque fingerprint alteration before selecting a replicated retry slot.
func nativeCommandViewFingerprint(command replication.CommandView) replication.Digest {
	hasher := sha256.New()
	_, _ = hasher.Write(nativeFingerprintDomain)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.Kind()))
	_, _ = hasher.Write(scalar[:])
	if command.AuthorityClass == replication.CommandAuthorityTopology {
		_, _ = hasher.Write(nativeTopologyAuthorityMarker)
	} else if command.AuthorityClass == replication.CommandAuthorityExecutionPin {
		_, _ = hasher.Write(nativeExecutionPinAuthorityMarker)
	}
	if replication.IsScopedSessionAuthority(command.AuthorityClass) {
		scalar[0] = byte(command.AuthorityClass)
		_, _ = hasher.Write(scalar[:1])
	}
	_, _ = hasher.Write(command.ClusterID[:])
	_, _ = hasher.Write(command.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(scalar[:], command.TopologyRecoveryEpoch)
	_, _ = hasher.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Distribution)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.Distribution)
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(command.Shard)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(command.Shard)
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
	transaction := command.TransactionBytes()
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(transaction)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(transaction)
	executionPin := command.ExecutionPinBytes()
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(executionPin)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(executionPin)
	if command.Kind() == replication.CommandRouteGate {
		gate := command.RouteGateBytes()
		binary.LittleEndian.PutUint64(scalar[:], uint64(len(gate)))
		_, _ = hasher.Write(scalar[:])
		_, _ = hasher.Write(gate)
	}
	splitCapture := command.SplitCaptureActivationBytes()
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(splitCapture)))
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(splitCapture)
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.RelationCount()))
	_, _ = hasher.Write(scalar[:])
	relations := command.RelationBatches()
	for relations.Next() {
		batch := relations.Batch()
		binary.LittleEndian.PutUint64(scalar[:], uint64(batch.Relation))
		_, _ = hasher.Write(scalar[:])
		binary.LittleEndian.PutUint64(scalar[:], uint64(batch.MutationCount()))
		_, _ = hasher.Write(scalar[:])
		mutations := batch.Mutations()
		for mutations.Next() {
			mutation := mutations.Mutation()
			binary.LittleEndian.PutUint64(scalar[:], uint64(mutation.Kind))
			_, _ = hasher.Write(scalar[:])
			binary.LittleEndian.PutUint64(scalar[:], uint64(len(mutation.Key)))
			_, _ = hasher.Write(scalar[:])
			_, _ = hasher.Write(mutation.Key)
			binary.LittleEndian.PutUint64(scalar[:], uint64(len(mutation.Value)))
			_, _ = hasher.Write(scalar[:])
			_, _ = hasher.Write(mutation.Value)
			binary.LittleEndian.PutUint64(scalar[:], mutation.ExpectedValueLength)
			_, _ = hasher.Write(scalar[:])
			_, _ = hasher.Write(mutation.ExpectedValueDigest[:])
		}
	}
	var result replication.Digest
	_ = hasher.Sum(result[:0])
	return result
}
