package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
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
	// GlobalIndexes is the schema-generation-bound set of global-index
	// relations physically colocated in this source RF3 group. Independently
	// placed indexes are intentionally retained: their base locator remains
	// valid after a split.
	GlobalIndexes []RF3RetainedPruneGlobalIndex
	MaxKeys       int
	MaxKeyBytes   int
	Certificate   rangesplit.CutoverCertificate
	RetainedRange distribution.KeyRange
}

type RF3RetainedPruneGlobalIndex struct {
	Relation    replication.RelationID
	Program     gateway.GlobalIndexProgram
	BaseTarget  distribution.Target
	IndexTarget distribution.Target
}

// RF3RetainedPruneProposer turns one already-authorized retained-range batch
// into one normal RF3 mutation. Its NativeSession owns exact-byte durable
// settlement across response loss and restart; this wrapper adds operation,
// source-member, relation, digest, and memory-bound checks before admission.
type RF3RetainedPruneProposer struct {
	mu sync.Mutex

	operation  OperationID
	session    *gateway.NativeSession
	relation   replication.RelationID
	clientID   replication.ID128
	tenant     []byte
	maxKeys    int
	maxBytes   int
	mutations  []gateway.NativeMutation
	arena      []byte
	indexes    []RF3RetainedPruneGlobalIndex
	workspaces []gateway.GlobalIndexWorkspace
	proof      replication.RetainedPruneProof
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
	relations := make([]replication.RelationID, len(options.GlobalIndexes))
	for index := range options.GlobalIndexes {
		relations[index] = options.GlobalIndexes[index].Relation
	}
	if options.Operation == (OperationID{}) || options.Session == nil ||
		options.Relation == 0 || options.Relation > replication.MaxRelationID ||
		options.MaxKeys <= 0 || options.MaxKeys > replication.MaxMutations ||
		options.MaxKeyBytes <= 0 || options.MaxKeyBytes > replication.MaxCommandBytes ||
		clientID == (replication.ID128{}) || !status.Active || !proof.Valid() ||
		len(options.GlobalIndexes) >= replication.MaxRelationBatches ||
		options.MaxKeys > replication.MaxMutations/(1+len(options.GlobalIndexes)) ||
		!validRetainedPruneIndexes(options.Relation, options.GlobalIndexes) ||
		!gateway.NativeSessionSupportsExactRelations(options.Session, options.Relation, relations) ||
		!gateway.NativeSessionSupportsMutationBound(
			options.Session, options.Relation, options.MaxKeys*(1+len(options.GlobalIndexes)), options.MaxKeyBytes,
		) {
		return nil, ErrInvalidPlan
	}
	return &RF3RetainedPruneProposer{
		operation: options.Operation, session: options.Session, relation: options.Relation,
		clientID: clientID, tenant: RetainedPruneTenant(options.Operation),
		maxKeys: options.MaxKeys, maxBytes: options.MaxKeyBytes,
		mutations:  make([]gateway.NativeMutation, 0, options.MaxKeys*(1+len(options.GlobalIndexes))),
		arena:      make([]byte, 0, options.MaxKeyBytes),
		indexes:    append([]RF3RetainedPruneGlobalIndex(nil), options.GlobalIndexes...),
		workspaces: make([]gateway.GlobalIndexWorkspace, len(options.GlobalIndexes)),
		proof:      proof,
	}, nil
}

func validRetainedPruneIndexes(base replication.RelationID, indexes []RF3RetainedPruneGlobalIndex) bool {
	prior := base
	for index := range indexes {
		binding := &indexes[index]
		if binding.Relation == 0 || binding.Relation > replication.MaxRelationID ||
			binding.Relation <= prior || binding.BaseTarget.Shard == "" ||
			binding.IndexTarget.Shard == "" {
			return false
		}
		prior = binding.Relation
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
	for iterator.Next() {
		key := iterator.Key()
		document := iterator.Document()
		if len(key) == 0 || len(key) > replication.MaxMutationKeyBytes ||
			len(document) == 0 || len(key) > p.maxBytes-len(p.arena) ||
			(prior != nil && bytes.Compare(prior, key) >= 0) {
			p.reset()
			return rangesplit.ErrRetainedPrune
		}
		start := len(p.arena)
		p.arena = append(p.arena, key...)
		owned := p.arena[start:len(p.arena):len(p.arena)]
		p.mutations = append(p.mutations, gateway.NativeMutation{
			Relation: p.relation, Kind: replication.MutationDeleteDigestEqual, Key: owned,
			ExpectedValueLength: uint64(len(document)), ExpectedValueDigest: replication.Digest(sha256.Sum256(document)),
		})
		for index := range p.indexes {
			binding := &p.indexes[index]
			route, routeErr := binding.Program.RouteDocument(document, &p.workspaces[index])
			if routeErr != nil || !bytes.Equal(route.BasePrimaryKey, key) ||
				!sameRetainedPruneTarget(route.BaseTarget, binding.BaseTarget) ||
				!sameRetainedPruneTarget(route.IndexTarget, binding.IndexTarget) ||
				len(route.EntryKey) == 0 || len(route.LocatorValue) == 0 ||
				len(route.EntryKey) > p.maxBytes-len(p.arena) {
				p.reset()
				return rangesplit.ErrRetainedPrune
			}
			indexStart := len(p.arena)
			p.arena = append(p.arena, route.EntryKey...)
			indexKey := p.arena[indexStart:len(p.arena):len(p.arena)]
			p.mutations = append(p.mutations, gateway.NativeMutation{
				Relation: binding.Relation, Kind: replication.MutationDeleteDigestEqual, Key: indexKey,
				ExpectedValueLength: uint64(len(route.LocatorValue)),
				ExpectedValueDigest: replication.Digest(sha256.Sum256(route.LocatorValue)),
			})
		}
		prior = owned
	}
	if uint64(len(p.mutations)) != batch.Count*uint64(1+len(p.indexes)) {
		p.reset()
		return rangesplit.ErrRetainedPrune
	}
	slices.SortFunc(p.mutations, func(left, right gateway.NativeMutation) int {
		if left.Relation < right.Relation {
			return -1
		}
		if left.Relation > right.Relation {
			return 1
		}
		return bytes.Compare(left.Key, right.Key)
	})
	for index := 1; index < len(p.mutations); index++ {
		if p.mutations[index-1].Relation == p.mutations[index].Relation &&
			bytes.Equal(p.mutations[index-1].Key, p.mutations[index].Key) {
			p.reset()
			return rangesplit.ErrRetainedPrune
		}
	}

	var result gateway.NativeResult
	var err error
	proof := p.proof
	proof.BatchDigest = replication.Digest(batch.Digest)
	if p.session.Status().Pending {
		if !retainedPrunePendingMatches(
			p.session.PendingCommand(), fence, p.clientID, p.relation,
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

func sameRetainedPruneTarget(left, right distribution.Target) bool {
	return left.Shard == right.Shard &&
		left.AllocationGeneration == right.AllocationGeneration &&
		left.OwnershipEpoch == right.OwnershipEpoch
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
			if mutationIndex >= len(mutations) {
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
