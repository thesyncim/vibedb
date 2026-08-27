package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var retainedPruneClientDomain = []byte("vibedb/split-controller/retained-prune-client\x00")

// RF3RetainedPruneProposerOptions fixes the destructive proposal identity and
// its bounded reusable workspace. Session must be a dedicated, journal-backed,
// active topology session on Relation. No other command may share it.
type RF3RetainedPruneProposerOptions struct {
	Operation OperationID
	Session   *gateway.NativeSession
	Relation  replication.RelationID
	// Relations is the exact dense set after base relation 1. Each prune
	// batch carries its own relation and exact stored values; index deletes
	// are never reconstructed from base-table documents.
	Relations     []replication.RelationID
	MaxKeys       int
	MaxKeyBytes   int
	Certificate   rangesplit.CutoverCertificate
	RetainedRange distribution.KeyRange
}

// NewRF3RetainedPruneProposerForPlan binds the destructive proposal to the
// immutable split geometry. Plans with global indexes remain closed until the
// complete relation-aware capture and image-certificate path is installed.
func NewRF3RetainedPruneProposerForPlan(
	plan *Plan,
	observed Observation,
	session *gateway.NativeSession,
	maxKeys int,
	maxKeyBytes int,
) (*RF3RetainedPruneProposer, error) {
	if plan == nil || observed.Certificate == nil || len(plan.indexRelations) != 0 ||
		plan.retained >= plan.childCount {
		return nil, ErrInvalidPlan
	}
	return NewRF3RetainedPruneProposer(RF3RetainedPruneProposerOptions{
		Operation: plan.operation, Session: session, Relation: 1,
		MaxKeys: maxKeys, MaxKeyBytes: maxKeyBytes,
		Certificate: *observed.Certificate, RetainedRange: plan.children[plan.retained].Range,
	})
}

// RF3RetainedPruneProposer turns one already-authorized retained-range batch
// into one normal RF3 mutation. Its NativeSession owns exact-byte durable
// settlement across response loss and restart; this wrapper adds operation,
// source-member, relation, digest, and memory-bound checks before admission.
type RF3RetainedPruneProposer struct {
	mu sync.Mutex

	operation     OperationID
	session       *gateway.NativeSession
	relation      replication.RelationID
	clientID      replication.ID128
	tenant        []byte
	maxKeys       int
	maxBytes      int
	mutations     []gateway.NativeMutation
	arena         []byte
	relationCount uint16
	proof         replication.RetainedPruneProof
}

func NewRF3RetainedPruneProposer(
	options RF3RetainedPruneProposerOptions,
) (*RF3RetainedPruneProposer, error) {
	if options.MaxKeys == 0 {
		options.MaxKeys = rangesplit.DefaultRetainedPruneKeys
	}
	if options.MaxKeyBytes == 0 {
		options.MaxKeyBytes = rangesplit.DefaultRetainedPruneKeyBytes
	}
	clientID := retainedPruneClientID(options.Operation)
	status := options.Session.Status()
	cut := options.Certificate.SourceCut()
	coordinates := options.Certificate.SourceCoordinates()
	proof := replication.RetainedPruneProof{
		OperationDigest:   replication.Digest(options.Operation),
		CertificateDigest: replication.Digest(options.Certificate.Digest()),
		BatchDigest:       replication.Digest{1},
		DataChainDigest:   replication.Digest(cut.DataChainDigest),
		EntryDigest:       replication.Digest(cut.EntryDigest), BaseDigest: replication.Digest(cut.BaseDigest),
		CutApplied: cut.Applied, CutTerm: cut.Term,
		OwnershipEpoch: coordinates.OwnershipEpoch, RoutingVersion: coordinates.RoutingVersion,
		RouteGeneration: coordinates.RouteGeneration, RetainedRange: options.RetainedRange,
	}
	if options.Operation == (OperationID{}) || options.Session == nil ||
		options.Relation != 1 ||
		options.MaxKeys <= 0 || options.MaxKeys > replication.MaxMutations ||
		options.MaxKeyBytes <= 0 || options.MaxKeyBytes > replication.MaxCommandBytes ||
		clientID == (replication.ID128{}) || !status.Active || !proof.Valid() ||
		!validRetainedPruneRelations(options.Relations) ||
		!gateway.NativeSessionSupportsExactRelations(options.Session, options.Relation, options.Relations) ||
		!gateway.NativeSessionSupportsMutationBound(
			options.Session, options.Relation, options.MaxKeys, options.MaxKeyBytes,
		) {
		return nil, ErrInvalidPlan
	}
	return &RF3RetainedPruneProposer{
		operation: options.Operation, session: options.Session, relation: options.Relation,
		clientID: clientID, tenant: RetainedPruneTenant(options.Operation),
		maxKeys: options.MaxKeys, maxBytes: options.MaxKeyBytes,
		mutations:     make([]gateway.NativeMutation, 0, options.MaxKeys),
		arena:         make([]byte, 0, options.MaxKeyBytes),
		relationCount: uint16(1 + len(options.Relations)),
		proof:         proof,
	}, nil
}

func validRetainedPruneRelations(relations []replication.RelationID) bool {
	if len(relations) >= replication.MaxRelationsPerBundle {
		return false
	}
	for index, relation := range relations {
		if relation != replication.RelationID(index+2) {
			return false
		}
	}
	return true
}

// RetainedPruneClientID returns the deterministic session identity reserved
// for one split operation. The domain separation prevents another controller
// operation from accidentally sharing the retry sequence or pending command.
func RetainedPruneClientID(operation OperationID) replication.ID128 {
	return retainedPruneClientID(operation)
}

func retainedPruneClientID(operation OperationID) replication.ID128 {
	var input [len("vibedb/split-controller/retained-prune-client\x00") + sha256.Size]byte
	copy(input[:], retainedPruneClientDomain)
	copy(input[len(retainedPruneClientDomain):], operation[:])
	sum := sha256.Sum256(input[:])
	var id replication.ID128
	copy(id[:], sum[:len(id)])
	return id
}

func (p *RF3RetainedPruneProposer) ProposeRetainedPrune(
	ctx context.Context,
	operation OperationID,
	fence raftservice.ServingFence,
	batch rangesplit.RetainedPruneBatch,
) error {
	if p == nil || ctx == nil || operation != p.operation ||
		batch.Relation() == 0 || uint16(batch.Relation()) > p.relationCount ||
		batch.Digest == ([sha256.Size]byte{}) || batch.Count == 0 ||
		batch.Count > uint64(p.maxKeys) || batch.KeyBytes == 0 ||
		batch.KeyBytes > uint64(p.maxBytes) || batch.DocumentBytes == 0 ||
		batch.DocumentBytes > replication.MaxCommandBytes ||
		!gateway.NativeSessionMatchesControlBinding(
			p.session, fence, p.tenant, p.clientID, p.relation,
			serviceauthz.CapabilityTopology,
		) {
		return ErrInvalidPlan
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mutations = p.mutations[:0]
	p.arena = p.arena[:0]
	iterator := batch.Iterator()
	var prior []byte
	var documentBytes uint64
	for iterator.Next() {
		key := iterator.Key()
		document := iterator.Document()
		if len(key) == 0 || len(key) > replication.MaxMutationKeyBytes ||
			len(p.mutations) >= p.maxKeys || uint64(len(p.mutations)) >= batch.Count ||
			len(document) == 0 || len(document) > replication.MaxMutationValueBytes ||
			documentBytes > batch.DocumentBytes || uint64(len(document)) > batch.DocumentBytes-documentBytes ||
			iterator.Relation() != batch.Relation() || len(key) > p.maxBytes-len(p.arena) ||
			(prior != nil && bytes.Compare(prior, key) >= 0) {
			p.reset()
			return rangesplit.ErrRetainedPrune
		}
		start := len(p.arena)
		p.arena = append(p.arena, key...)
		owned := p.arena[start:len(p.arena):len(p.arena)]
		p.mutations = append(p.mutations, gateway.NativeMutation{
			Relation: batch.Relation(), Kind: replication.MutationDeleteDigestEqual, Key: owned,
			ExpectedValueLength: uint64(len(document)), ExpectedValueDigest: replication.Digest(sha256.Sum256(document)),
		})
		documentBytes += uint64(len(document))
		prior = owned
	}
	if uint64(len(p.mutations)) != batch.Count || uint64(len(p.arena)) != batch.KeyBytes ||
		documentBytes != batch.DocumentBytes {
		p.reset()
		return rangesplit.ErrRetainedPrune
	}

	var result gateway.NativeResult
	var err error
	proof := p.proof
	proof.BatchDigest = replication.Digest(batch.Digest)
	if p.session.Status().Pending {
		if !retainedPrunePendingMatches(
			p.session.PendingCommand(), fence, p.clientID, batch.Relation(),
			proof, p.mutations,
		) {
			p.reset()
			return errors.Join(rangesplit.ErrRetainedPrune, gateway.ErrNativeCommandPending)
		}
		result, err = p.session.RetryPending(ctx)
	} else {
		result, err = p.session.RetainedPruneBatch(ctx, p.mutations, proof)
	}
	p.reset()
	if err != nil {
		return err
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied ||
		result.Outcome.AppliedIndex == 0 {
		return rangesplit.ErrRetainedPrune
	}
	return nil
}

func (p *RF3RetainedPruneProposer) reset() {
	clear(p.mutations)
	p.mutations = p.mutations[:0]
	p.arena = p.arena[:0]
}

func retainedPrunePendingMatches(
	raw []byte,
	fence raftservice.ServingFence,
	clientID replication.ID128,
	relation replication.RelationID,
	proof replication.RetainedPruneProof,
	mutations []gateway.NativeMutation,
) bool {
	command, err := replication.OpenCommand(raw)
	if err != nil || command.Kind() != replication.CommandRetainedPrune ||
		command.AuthorityClass != replication.CommandAuthorityTopology ||
		command.ClientID != clientID || command.Fingerprint != proof.BatchDigest ||
		command.MutationCount() != len(mutations) ||
		command.ClusterID != replication.ID128(fence.Group.ClusterID) ||
		command.ClusterIncarnation != replication.ID128(fence.Group.ClusterIncarnation) ||
		command.TopologyRecoveryEpoch != fence.Group.TopologyRecoveryEpoch ||
		command.ShardIncarnation != replication.ID128(fence.Group.ShardIncarnation) ||
		command.GroupID != replication.ID128(fence.Group.GroupID) ||
		command.AllocationGeneration != fence.AllocationGeneration ||
		command.ReplicaSetVersion != fence.Command.ReplicaSetVersion ||
		command.ActivePolicyGeneration != fence.Command.ActivePolicyGeneration ||
		command.ProtectionEpoch != fence.Command.ProtectionEpoch ||
		command.OwnershipEpoch != fence.Command.OwnershipEpoch ||
		command.SchemaGeneration != fence.Command.SchemaGeneration ||
		command.RoutingVersion != fence.Command.RoutingVersion ||
		command.RouteGeneration != fence.Command.RouteGeneration {
		return false
	}
	gotProof, proofOK := command.RetainedPruneProof()
	if !proofOK || gotProof != proof {
		return false
	}
	batches := command.RelationBatches()
	mutationIndex := 0
	firstBatch := true
	for batches.Next() {
		batch := batches.Batch()
		if mutationIndex >= len(mutations) || batch.Relation != mutations[mutationIndex].Relation ||
			(firstBatch && batch.Relation != relation) {
			return false
		}
		firstBatch = false
		items := batch.Mutations()
		for items.Next() {
			if mutationIndex >= len(mutations) || mutations[mutationIndex].Relation != batch.Relation {
				return false
			}
			got, want := items.Mutation(), mutations[mutationIndex]
			if got.Kind != replication.MutationDeleteDigestEqual || len(got.Value) != 0 ||
				got.ExpectedValueLength != want.ExpectedValueLength ||
				got.ExpectedValueDigest != want.ExpectedValueDigest ||
				!bytes.Equal(got.Key, want.Key) {
				return false
			}
			mutationIndex++
		}
	}
	return mutationIndex == len(mutations)
}

// RetainedPruneTenant returns the canonical printable tenant identity reserved
// for operation's dedicated topology session. The returned bytes are owned.
func RetainedPruneTenant(operation OperationID) []byte {
	const prefix = "split-prune:"
	const hex = "0123456789abcdef"
	var tenant [len(prefix) + 2*sha256.Size]byte
	copy(tenant[:], prefix)
	for index, value := range operation {
		tenant[len(prefix)+2*index] = hex[value>>4]
		tenant[len(prefix)+2*index+1] = hex[value&15]
	}
	return tenant[:]
}
