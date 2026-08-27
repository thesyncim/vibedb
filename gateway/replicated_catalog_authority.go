package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

var (
	ErrReplicatedCatalog          = errors.New("gateway: invalid replicated catalog authority")
	ErrReplicatedCatalogMissing   = errors.New("gateway: replicated catalog head is missing")
	ErrReplicatedCatalogConflict  = errors.New("gateway: replicated catalog compare-and-publish conflict")
	ErrReplicatedCatalogPending   = errors.New("gateway: replicated catalog publication remains outcome-unknown")
	ErrReplicatedOperationMissing = errors.New("gateway: replicated operation record is missing")
)

const (
	MaxReplicatedOperationBytes          = 64 << 10
	maxReplicatedOperationIntentBytes    = 40 << 10
	maxReplicatedOperations              = 64
	maxReplicatedOperationDirectoryBytes = 16 << 10
	maxReplicatedCatalogHeadWitnessBytes = 512
	maxReplicatedCatalogGenesisBytes     = 512
	// One catalog head remains one atomic relation value. The final /id
	// envelope—not merely its nested catalog payload—must fit the replicated
	// mutation grammar.
	maxReplicatedCatalogBytes = replication.MaxMutationValueBytes
)

type persistedCatalogHeadWitness struct {
	Generation uint64   `json:"generation"`
	HeadBytes  uint64   `json:"head_bytes"`
	HeadDigest [32]byte `json:"head_digest"`
}

type persistedCatalogGenesis struct {
	Generation uint64   `json:"generation"`
	HeadBytes  uint64   `json:"head_bytes"`
	HeadDigest [32]byte `json:"head_digest"`
}

type replicatedCatalogCut struct {
	head     []byte
	witness  []byte
	snapshot *Snapshot
}

// ReplicatedCatalogAuthority stores the catalog head and resumable controller
// records in the dedicated control-plane JSON relation served by its RF3 owner.
// The route is a bootstrap coordinate only; every head read is ReadIndex-fenced and
// every replacement is a raw length+SHA-256 compare inside replicated apply.
type ReplicatedCatalogAuthority struct {
	executor                           *ReplicatedExecutor
	route                              ReplicatedRoute
	relation                           replication.RelationID
	holder                             *CatalogHolder
	session                            *NativeSession
	authority                          serviceauthz.Authority
	mu                                 sync.Mutex
	scratch                            []byte
	pendingCatalog                     *Snapshot
	pendingExpected                    uint64
	pendingGrant                       membershipgrant.Grant
	pendingPostRemoveReplicaSetVersion uint64
	issuerGrants                       *replicatedIssuerGrantCache
}

type ReplicatedCatalogAuthorityOptions struct {
	Executor *ReplicatedExecutor
	Route    ReplicatedRoute
	Relation replication.RelationID
	Holder   *CatalogHolder
	// Session is the placement and relation proof for both reads and writes. It
	// must be active, bound to the reserved catalog/controlplane RF3 group, and
	// resolve logical mutations to Relation.
	Session *NativeSession
	// Authority is the exact topology principal forwarded on every probe, read,
	// proposal, and byte-identical retry. Callers cannot accidentally fall back
	// to an unclassified DataWrite request.
	Authority serviceauthz.Authority
}

func NewReplicatedCatalogAuthority(options ReplicatedCatalogAuthorityOptions) (*ReplicatedCatalogAuthority, error) {
	if options.Executor == nil || !validReplicatedRoute(options.Route) ||
		options.Route.Distribution != ReplicatedCatalogDistribution ||
		options.Route.Shard != ReplicatedCatalogShard ||
		options.Relation == 0 || options.Relation > replication.MaxRelationID ||
		options.Holder == nil || !options.Authority.Valid() || options.Session == nil ||
		options.Session.executor != options.Executor ||
		options.Session.distribution != string(ReplicatedCatalogDistribution) ||
		options.Session.shard != string(ReplicatedCatalogShard) ||
		options.Session.phase != nativeSessionActive || options.Session.pending ||
		options.Session.bundle.maxMutations < 3 ||
		options.Session.proposalCapability != serviceauthz.CapabilityTopology ||
		!sameReplicatedCatalogRoute(options.Session.route, options.Route) ||
		nativeSessionBaseRelation(options.Session) != options.Relation {
		return nil, ErrReplicatedCatalog
	}
	route := options.Route
	route.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
	return &ReplicatedCatalogAuthority{
		executor: options.Executor, route: route, relation: options.Relation,
		holder: options.Holder, session: options.Session,
		authority:    options.Authority,
		scratch:      make([]byte, 0, 4<<10),
		issuerGrants: newReplicatedIssuerGrantCache(MaxCachedReplicatedIssuerGrants),
	}, nil
}

func (authority *ReplicatedCatalogAuthority) authorizedContext(
	ctx context.Context,
) (context.Context, error) {
	if authority == nil || ctx == nil || !authority.authority.Valid() {
		return nil, ErrReplicatedCatalog
	}
	bound, err := serviceauthz.WithAuthority(ctx, authority.authority)
	return bound, err
}

func nativeSessionBaseRelation(session *NativeSession) replication.RelationID {
	if session == nil {
		return 0
	}
	return nativeResolverBaseRelation(session.resolver)
}

func nativeResolverBaseRelation(resolver BundleResolver) replication.RelationID {
	switch resolver := resolver.(type) {
	case BaseRelationResolver:
		return resolver.Relation
	case *BaseRelationResolver:
		if resolver != nil {
			return resolver.Relation
		}
	case ExactRelationResolver:
		return resolver.Base
	case *ExactRelationResolver:
		if resolver != nil {
			return resolver.Base
		}
	}
	return 0
}

func sameReplicatedCatalogRoute(left, right ReplicatedRoute) bool {
	if left.Distribution != right.Distribution || left.Shard != right.Shard ||
		left.Group != right.Group ||
		left.AllocationGeneration != right.AllocationGeneration ||
		left.Command != right.Command || left.RangeIdentity != right.RangeIdentity ||
		left.LineageDigest != right.LineageDigest ||
		left.ForwardingRuleDigest != right.ForwardingRuleDigest ||
		len(left.Replicas) != len(right.Replicas) {
		return false
	}
	for index := range left.Replicas {
		if left.Replicas[index] != right.Replicas[index] {
			return false
		}
	}
	return true
}

func (authority *ReplicatedCatalogAuthority) readRaw(
	ctx context.Context, key []byte, maximum uint32,
) (ReplicatedPointResult, error) {
	if authority == nil || ctx == nil || len(key) == 0 || maximum == 0 ||
		maximum > uint32(maxReplicatedCatalogBytes) {
		return ReplicatedPointResult{}, ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return ReplicatedPointResult{}, err
	}
	result, err := authority.executor.ReadTopologyPoint(ctx, authority.route, ReplicatedPointRead{
		Relation: authority.relation, Key: key, MinimumApplied: 1,
		// Point-read admission is certified against the relation's frozen maximum,
		// not the expected size of one logical row kind. The catalog head and its
		// smaller operation rows share this relation, so reserve the complete
		// relation bound and enforce the row-kind bound after the read.
		MaxValueBytes: uint32(maxReplicatedCatalogBytes), Linearizable: true,
	})
	if err != nil || !result.Found {
		return result, err
	}
	if len(result.Value) > int(maximum) {
		return ReplicatedPointResult{}, ErrReplicatedCatalog
	}
	return result, nil
}

// Read fetches the authoritative RF3 catalog head and validates the complete
// routing/index/lineage image before publishing it to the lock-free holder.
func (authority *ReplicatedCatalogAuthority) Read(ctx context.Context) (*Snapshot, error) {
	cut, err := authority.readCatalogCut(ctx)
	if err != nil {
		return nil, err
	}
	return authority.publishReadCatalogCut(ctx, cut.snapshot, cut.head)
}

func (authority *ReplicatedCatalogAuthority) readCatalogCut(ctx context.Context) (replicatedCatalogCut, error) {
	result, err := authority.readRaw(ctx, replicatedCatalogHeadKey, uint32(maxReplicatedCatalogBytes))
	if err != nil {
		return replicatedCatalogCut{}, err
	}
	if !result.Found {
		genesis, genesisErr := authority.readRaw(
			ctx, replicatedCatalogGenesisKey, uint32(maxReplicatedCatalogGenesisBytes),
		)
		if genesisErr != nil {
			return replicatedCatalogCut{}, genesisErr
		}
		if !genesis.Found {
			witness, witnessErr := authority.readRaw(
				ctx, replicatedCatalogHeadWitnessKey, uint32(maxReplicatedCatalogBytes),
			)
			if witnessErr != nil {
				return replicatedCatalogCut{}, witnessErr
			}
			if !witness.Found {
				return replicatedCatalogCut{}, ErrReplicatedCatalogMissing
			}
		}
		// A concurrent atomic genesis publication can linearize between the
		// first head read and either following proof read. Re-read the head once;
		// a still missing head beside an immutable genesis proof or witness is
		// durable corruption, never authorization to recreate generation one.
		result, err = authority.readRaw(
			ctx, replicatedCatalogHeadKey, uint32(maxReplicatedCatalogBytes),
		)
		if err != nil {
			return replicatedCatalogCut{}, err
		}
		if !result.Found {
			return replicatedCatalogCut{}, ErrReplicatedCatalogConflict
		}
	}
	payload, err := openTypedControlPlaneDocument(result.Value,
		replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes)
	if err != nil {
		return replicatedCatalogCut{}, err
	}
	snapshot, err := OpenSnapshotDocument(payload)
	if err != nil {
		return replicatedCatalogCut{}, err
	}
	genesisResult, err := authority.readRaw(
		ctx, replicatedCatalogGenesisKey, uint32(maxReplicatedCatalogGenesisBytes),
	)
	if err != nil || !genesisResult.Found {
		return replicatedCatalogCut{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	var genesisHead []byte
	if snapshot.Generation() == 1 {
		genesisHead = result.Value
	}
	if validateReplicatedCatalogGenesis(genesisResult.Value, genesisHead) != nil {
		return replicatedCatalogCut{}, ErrReplicatedCatalogConflict
	}
	witnessResult, err := authority.readRaw(ctx, replicatedCatalogHeadWitnessKey,
		uint32(maxReplicatedCatalogBytes))
	if err != nil {
		return replicatedCatalogCut{}, err
	}
	if !witnessResult.Found || len(witnessResult.Value) > maxReplicatedCatalogHeadWitnessBytes ||
		validateReplicatedCatalogHeadWitness(witnessResult.Value, snapshot.Generation(), result.Value) != nil {
		return replicatedCatalogCut{}, ErrReplicatedCatalogConflict
	}
	return replicatedCatalogCut{head: result.Value, witness: witnessResult.Value, snapshot: snapshot}, nil
}

func (authority *ReplicatedCatalogAuthority) publishReadCatalogCut(
	ctx context.Context, snapshot *Snapshot, raw []byte,
) (*Snapshot, error) {
	current := authority.holder.Current()
	if current == nil {
		if !authority.holder.PublishNewer(snapshot) {
			return nil, ErrReplicatedCatalogConflict
		}
	} else if snapshot.Generation() > current.Generation() {
		if err := authority.holder.publishNewerChecked(snapshot); err != nil {
			if replacementErr := authority.publishCertifiedReplicaReplacementRead(
				ctx, current, snapshot, raw,
			); replacementErr != nil {
				return nil, errors.Join(err, replacementErr)
			}
		}
	} else if snapshot.Generation() < current.Generation() {
		return nil, ErrStaleGeneration
	} else {
		currentBytes, encodeErr := appendReplicatedCatalogDocument(nil, current, maxReplicatedCatalogBytes)
		if encodeErr != nil || !bytes.Equal(currentBytes, raw) {
			return nil, errors.Join(encodeErr, ErrReplicatedCatalogConflict)
		}
	}
	return authority.holder.Current(), nil
}

// publishCertifiedReplicaReplacementRead is the only exceptional refresh
// path. Generic catalog transition validation remains unchanged: an adjacent
// generation with one apparent roster change must also carry the exact
// replicated receipt, validate both head bytes, reproduce the certified next
// snapshot, and enter CatalogHolder through its grant-aware transition.
func (authority *ReplicatedCatalogAuthority) publishCertifiedReplicaReplacementRead(
	ctx context.Context, current, next *Snapshot, nextRaw []byte,
) error {
	if authority == nil || ctx == nil || current == nil || next == nil ||
		current.Generation() == ^uint64(0) ||
		next.Generation() != current.Generation()+1 {
		return ErrReplicatedCatalogConflict
	}
	group, ok := replicaReplacementCandidateGroup(current, next)
	if !ok {
		return ErrReplicatedCatalogConflict
	}
	key, _ := replicatedReplicaReplacementReceiptKeys(group)
	result, err := authority.readRaw(
		ctx, key[:], uint32(maxReplicatedReplicaReplacementReceiptBytes),
	)
	if err != nil || !result.Found {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	currentRaw, err := appendReplicatedCatalogDocument(
		nil, current, maxReplicatedCatalogBytes,
	)
	if err != nil {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	receipt, err := openReplicaReplacementReceipt(result.Value)
	if err != nil || receipt.Grant.Group != group {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	var certified *Snapshot
	var publish func() error
	switch {
	case current.Generation() == receipt.OldGeneration &&
		next.Generation() == receipt.NewGeneration:
		grant, validateErr := validateReplicaReplacementReceipt(
			result.Value, currentRaw, nextRaw, current.Generation(), next.Generation(),
		)
		if validateErr != nil || grant.Group != group {
			return errors.Join(validateErr, ErrReplicatedCatalogConflict)
		}
		certified, err = advanceCatalogStateReplicaReplacement(current, next, grant)
		if err == nil {
			version, found := replicaSetVersionForGroup(certified, group)
			if !found || version != receipt.PublishedReplicaSetVersion {
				err = ErrReplicatedCatalogConflict
			}
		}
		publish = func() error {
			return authority.holder.publishReplicaReplacementAfter(
				current.Generation(), certified, grant,
			)
		}
	case current.Generation() == receipt.NewGeneration &&
		next.Generation() == receipt.PostRemoveGeneration:
		if receipt.NewHeadBytes != uint64(len(currentRaw)) ||
			receipt.NewHeadDigest != sha256.Sum256(currentRaw) ||
			receipt.PostRemoveHeadBytes != uint64(len(nextRaw)) ||
			receipt.PostRemoveHeadDigest != sha256.Sum256(nextRaw) {
			return ErrReplicatedCatalogConflict
		}
		err = validateReplicaReplacementPostRemoveTransition(
			current, next, receipt.Grant, receipt.PostRemoveReplicaSetVersion,
		)
		if err == nil {
			currentVersion, currentFound := replicaSetVersionForGroup(current, group)
			nextVersion, nextFound := replicaSetVersionForGroup(next, group)
			if !currentFound || !nextFound ||
				currentVersion != receipt.PublishedReplicaSetVersion ||
				nextVersion != receipt.PostRemoveReplicaSetVersion {
				err = ErrReplicatedCatalogConflict
			}
		}
		certified = next
		publish = func() error {
			return authority.holder.publishReplicaReplacementPostRemoveAfter(
				current.Generation(), certified, receipt.Grant,
				receipt.PostRemoveReplicaSetVersion,
			)
		}
	default:
		return ErrReplicatedCatalogConflict
	}
	if err != nil {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	certifiedRaw, err := appendReplicatedCatalogDocument(
		nil, certified, maxReplicatedCatalogBytes,
	)
	if err != nil || !bytes.Equal(certifiedRaw, nextRaw) {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	if err = publish(); err == nil {
		return nil
	}
	// Concurrent refreshes may both validate the same immutable receipt. The
	// loser accepts only the byte-identical generation already installed.
	installed := authority.holder.Current()
	if installed == nil || installed.Generation() != next.Generation() {
		return err
	}
	installedRaw, encodeErr := appendReplicatedCatalogDocument(
		nil, installed, maxReplicatedCatalogBytes,
	)
	if encodeErr != nil || !bytes.Equal(installedRaw, nextRaw) {
		return errors.Join(err, encodeErr, ErrReplicatedCatalogConflict)
	}
	return nil
}

func replicaReplacementCandidateGroup(current, next *Snapshot) (raftmember.GroupKey, bool) {
	if current == nil || next == nil || len(current.replicatedShards) != len(next.replicatedShards) {
		return raftmember.GroupKey{}, false
	}
	var group raftmember.GroupKey
	found := false
	for _, old := range current.replicatedShards {
		manifest := current.config.Manifests[old.manifest]
		metadata, ok := manifest.ShardMetadataAt(int(old.shard))
		if !ok {
			return raftmember.GroupKey{}, false
		}
		candidate, ok := next.replicatedShardAt(manifest.Distribution(), metadata.ID)
		if !ok || candidate.group != old.group || candidate.allocation != old.allocation {
			return raftmember.GroupKey{}, false
		}
		if candidate.command.ReplicaSetVersion == old.command.ReplicaSetVersion &&
			sameReplicatedCatalogRoster(current, old, next, candidate) {
			continue
		}
		if found {
			return raftmember.GroupKey{}, false
		}
		group, found = old.group, true
	}
	return group, found
}

func appendReplicatedCatalogHeadWitness(dst []byte, generation uint64, head []byte) ([]byte, error) {
	if generation == 0 || len(head) == 0 || len(head) > maxReplicatedCatalogBytes {
		return dst, ErrReplicatedCatalog
	}
	persisted := persistedCatalogHeadWitness{Generation: generation,
		HeadBytes: uint64(len(head)), HeadDigest: sha256.Sum256(head)}
	payload, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(dst, replicatedCatalogHeadWitnessDocumentID[:], payload,
		maxReplicatedCatalogHeadWitnessBytes)
}

func validateReplicatedCatalogHeadWitness(raw []byte, generation uint64, head []byte) error {
	payload, err := openTypedControlPlaneDocument(raw,
		replicatedCatalogHeadWitnessDocumentID[:], maxReplicatedCatalogHeadWitnessBytes)
	if err != nil {
		return err
	}
	var persisted persistedCatalogHeadWitness
	if err = vibejson.Unmarshal(payload, &persisted); err != nil || persisted.Generation != generation ||
		persisted.HeadBytes != uint64(len(head)) || persisted.HeadDigest != sha256.Sum256(head) {
		return errors.Join(err, ErrReplicatedCatalog)
	}
	canonical, err := appendReplicatedCatalogHeadWitness(nil, generation, head)
	if err != nil || !bytes.Equal(raw, canonical) {
		return errors.Join(err, ErrReplicatedCatalog)
	}
	return nil
}

func appendReplicatedCatalogGenesis(dst, head []byte) ([]byte, error) {
	if len(head) == 0 || len(head) > maxReplicatedCatalogBytes {
		return dst, ErrReplicatedCatalog
	}
	persisted := persistedCatalogGenesis{
		Generation: 1, HeadBytes: uint64(len(head)), HeadDigest: sha256.Sum256(head),
	}
	payload, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(
		dst, replicatedCatalogGenesisDocumentID[:], payload,
		maxReplicatedCatalogGenesisBytes,
	)
}

func validateReplicatedCatalogGenesis(raw, initialHead []byte) error {
	payload, err := openTypedControlPlaneDocument(
		raw, replicatedCatalogGenesisDocumentID[:], maxReplicatedCatalogGenesisBytes,
	)
	if err != nil {
		return err
	}
	var persisted persistedCatalogGenesis
	if err = vibejson.Unmarshal(payload, &persisted); err != nil ||
		persisted.Generation != 1 || persisted.HeadBytes == 0 ||
		persisted.HeadBytes > maxReplicatedCatalogBytes ||
		persisted.HeadDigest == ([sha256.Size]byte{}) {
		return errors.Join(err, ErrReplicatedCatalog)
	}
	canonicalPayload, err := vibejson.Marshal(&persisted)
	if err != nil {
		return err
	}
	canonical, err := appendControlPlaneDocument(
		nil, replicatedCatalogGenesisDocumentID[:], canonicalPayload,
		maxReplicatedCatalogGenesisBytes,
	)
	if err != nil || !bytes.Equal(raw, canonical) {
		return errors.Join(err, ErrReplicatedCatalog)
	}
	if initialHead != nil && (persisted.HeadBytes != uint64(len(initialHead)) ||
		persisted.HeadDigest != sha256.Sum256(initialHead)) {
		return ErrReplicatedCatalogConflict
	}
	return nil
}

// AttestGenesis proves that candidate is the immutable generation-one catalog
// committed atomically with the replicated head and witness. It remains valid
// after the mutable head advances and introduces no node-local bootstrap bit.
func (authority *ReplicatedCatalogAuthority) AttestGenesis(
	ctx context.Context, candidate *Snapshot,
) error {
	if authority == nil || ctx == nil || candidate == nil || candidate.Generation() != 1 {
		return ErrReplicatedCatalog
	}
	head, err := appendReplicatedCatalogDocument(nil, candidate, maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	result, err := authority.readRaw(
		ctx, replicatedCatalogGenesisKey, uint32(maxReplicatedCatalogGenesisBytes),
	)
	if err != nil || !result.Found {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	if err = validateReplicatedCatalogGenesis(result.Value, head); err != nil {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return nil
}

// Refresh implements the shipped gateway RefreshFunc using a linearizable RF3
// point read instead of the static file authority.
func (authority *ReplicatedCatalogAuthority) Refresh(ctx context.Context, staleGeneration uint64) (*Snapshot, error) {
	snapshot, err := authority.Read(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Generation() <= staleGeneration {
		return nil, ErrStaleGeneration
	}
	return snapshot, nil
}

// AuthorizeRetainedPrune converts a linearizable catalog-Raft read into the
// existing sealed post-drain cleanup capability. The RF3 read is the durable
// publication receipt: no local catalog file or second topology authority is
// introduced. CatalogHolder performs the final current-generation and local
// lease-drain checks atomically.
func (authority *ReplicatedCatalogAuthority) AuthorizeRetainedPrune(
	ctx context.Context,
	distributionName distribution.DistributionName,
	operation [sha256.Size]byte,
	certificate [sha256.Size]byte,
) (RetainedPruneAuthority, error) {
	if authority == nil || authority.holder == nil || ctx == nil {
		return nil, ErrReplicatedCatalog
	}
	snapshot, err := authority.Read(ctx)
	if err != nil {
		return nil, err
	}
	return authority.holder.AuthorizeRetainedPrune(
		DurableCatalogPublication{snapshot: snapshot, generation: snapshot.Generation()},
		distributionName, operation, certificate,
	)
}

// Publish conditionally replaces exactly expectedGeneration. Unknown outcomes
// retain byte-identical command bytes in Session; RetryPending must settle that
// command before another publication is constructed.
func (authority *ReplicatedCatalogAuthority) Publish(
	ctx context.Context, expectedGeneration uint64, next *Snapshot,
) error {
	if authority == nil || authority.session == nil || ctx == nil || next == nil {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	currentResult, err := authority.readRaw(
		ctx, replicatedCatalogHeadKey, uint32(maxReplicatedCatalogBytes),
	)
	if err != nil {
		return err
	}
	currentWitness, err := authority.readRaw(
		ctx, replicatedCatalogHeadWitnessKey, uint32(maxReplicatedCatalogBytes),
	)
	if err != nil {
		return err
	}
	genesisResult, err := authority.readRaw(
		ctx, replicatedCatalogGenesisKey, uint32(maxReplicatedCatalogGenesisBytes),
	)
	if err != nil {
		return err
	}
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedCatalogDocument(
		authority.scratch, next, maxReplicatedCatalogBytes,
	)
	if err != nil {
		return ErrCatalogTooLarge
	}
	nextWitness, err := appendReplicatedCatalogHeadWitness(nil, next.Generation(), authority.scratch)
	if err != nil {
		return err
	}
	mutations := make([]NativeMutation, 0, 3)
	var native NativeResult
	if !currentResult.Found {
		if expectedGeneration != 0 {
			return ErrCatalogGenerationMismatch
		}
		if currentWitness.Found || genesisResult.Found {
			return ErrReplicatedCatalogConflict
		}
		if next.Generation() != 1 {
			return ErrCatalogGenerationMismatch
		}
		genesis, genesisErr := appendReplicatedCatalogGenesis(nil, authority.scratch)
		if genesisErr != nil {
			return genesisErr
		}
		mutations = append(mutations,
			NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
				Key: replicatedCatalogGenesisKey, Value: genesis},
			NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
				Key: replicatedCatalogHeadKey, Value: authority.scratch},
			NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
				Key: replicatedCatalogHeadWitnessKey, Value: nextWitness},
		)
	} else {
		currentPayload, openErr := openTypedControlPlaneDocument(
			currentResult.Value, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes,
		)
		if openErr != nil {
			return openErr
		}
		current, openErr := OpenSnapshotDocument(currentPayload)
		if openErr != nil {
			return openErr
		}
		var genesisHead []byte
		if current.Generation() == 1 {
			genesisHead = currentResult.Value
		}
		if !genesisResult.Found ||
			validateReplicatedCatalogGenesis(genesisResult.Value, genesisHead) != nil {
			return ErrReplicatedCatalogConflict
		}
		if current.Generation() != expectedGeneration || next.Generation() <= expectedGeneration {
			return ErrCatalogGenerationMismatch
		}
		state, stateErr := initialCatalogState(current)
		if stateErr != nil {
			return stateErr
		}
		if _, stateErr = advanceCatalogState(state, next); stateErr != nil {
			return stateErr
		}
		if !currentWitness.Found || validateReplicatedCatalogHeadWitness(
			currentWitness.Value, current.Generation(), currentResult.Value,
		) != nil {
			return ErrReplicatedCatalogConflict
		}
		headDigest, witnessDigest := sha256.Sum256(currentResult.Value), sha256.Sum256(currentWitness.Value)
		mutations = append(mutations,
			NativeMutation{Kind: replication.MutationPutDigestEqual,
				Key: replicatedCatalogHeadKey, Value: authority.scratch,
				ExpectedValueLength: uint64(len(currentResult.Value)),
				ExpectedValueDigest: replication.Digest(headDigest)},
			NativeMutation{Kind: replication.MutationPutDigestEqual,
				Key: replicatedCatalogHeadWitnessKey, Value: nextWitness,
				ExpectedValueLength: uint64(len(currentWitness.Value)),
				ExpectedValueDigest: replication.Digest(witnessDigest)},
		)
	}
	native, err = authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if errors.Is(err, ErrNativeCommandPending) || authority.session.Status().Pending {
			authority.pendingCatalog, authority.pendingExpected = next, expectedGeneration
			authority.pendingGrant = membershipgrant.Grant{}
			authority.pendingPostRemoveReplicaSetVersion = 0
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if native.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if native.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	return authority.holder.PublishAfter(expectedGeneration, next)
}

// RetryPending resends only the session-owned byte-identical command after an
// outcome-unknown publication or operation update.
func (authority *ReplicatedCatalogAuthority) RetryPending(ctx context.Context) error {
	if authority == nil || authority.session == nil || ctx == nil {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if !authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	result, err := authority.session.RetryPending(ctx)
	if err != nil {
		return errors.Join(ErrReplicatedCatalogPending, err)
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		authority.pendingCatalog = nil
		authority.pendingExpected = 0
		authority.pendingGrant = membershipgrant.Grant{}
		authority.pendingPostRemoveReplicaSetVersion = 0
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		authority.pendingCatalog = nil
		authority.pendingExpected = 0
		authority.pendingGrant = membershipgrant.Grant{}
		authority.pendingPostRemoveReplicaSetVersion = 0
		return ErrReplicatedCatalog
	}
	if authority.pendingCatalog != nil {
		if authority.pendingPostRemoveReplicaSetVersion != 0 && authority.pendingGrant.Valid() {
			err = authority.holder.publishReplicaReplacementPostRemoveAfter(
				authority.pendingExpected, authority.pendingCatalog, authority.pendingGrant,
				authority.pendingPostRemoveReplicaSetVersion,
			)
		} else if authority.pendingGrant.Valid() {
			err = authority.holder.publishReplicaReplacementAfter(
				authority.pendingExpected, authority.pendingCatalog, authority.pendingGrant,
			)
		} else {
			err = authority.holder.PublishAfter(authority.pendingExpected, authority.pendingCatalog)
		}
		authority.pendingCatalog = nil
		authority.pendingExpected = 0
		authority.pendingGrant = membershipgrant.Grant{}
		authority.pendingPostRemoveReplicaSetVersion = 0
		return err
	}
	return nil
}

type ReplicatedOperationKind uint8
type ReplicatedOperationState uint8

const (
	ReplicatedOperationSplit ReplicatedOperationKind = iota + 1
	ReplicatedOperationMove
	// ReplicatedOperationSchema is the durable prepare/activate/abort witness
	// for one exact relation-manifest rollout. It extends the one current
	// operation grammar; there is no parallel protocol generation.
	ReplicatedOperationSchema
)

const (
	ReplicatedOperationPlanned ReplicatedOperationState = iota + 1
	ReplicatedOperationRunning
	ReplicatedOperationComplete
	ReplicatedOperationCancelled
)

// ReplicatedOperationRecord is a compact, string-free resumable controller
// witness. Cursor scalars are operation-kind-defined deterministic stage data.
type ReplicatedOperationRecord struct {
	ID                [32]byte                 `json:"id"`
	Kind              ReplicatedOperationKind  `json:"kind"`
	State             ReplicatedOperationState `json:"state"`
	Revision          uint64                   `json:"revision"`
	CatalogGeneration uint64                   `json:"catalog_generation"`
	Cursor            [8]uint64                `json:"cursor"`
	Proof             [32]byte                 `json:"proof"`
	IntentDigest      [32]byte                 `json:"intent_digest"`
	Intent            []byte                   `json:"intent"`
}

func validReplicatedOperation(record ReplicatedOperationRecord) bool {
	return record.ID != ([32]byte{}) &&
		record.Kind >= ReplicatedOperationSplit && record.Kind <= ReplicatedOperationSchema &&
		record.State >= ReplicatedOperationPlanned && record.State <= ReplicatedOperationCancelled &&
		record.Revision != 0 && record.CatalogGeneration != 0 && record.Proof != ([32]byte{}) &&
		record.IntentDigest != ([32]byte{}) && len(record.Intent) != 0 &&
		len(record.Intent) <= maxReplicatedOperationIntentBytes &&
		sha256.Sum256(record.Intent) == record.IntentDigest
}

// Equal reports exact logical and byte identity without making the record
// comparable merely for tests or settlement checks.
func (record ReplicatedOperationRecord) Equal(other ReplicatedOperationRecord) bool {
	return record.ID == other.ID && record.Kind == other.Kind && record.State == other.State &&
		record.Revision == other.Revision &&
		record.CatalogGeneration == other.CatalogGeneration && record.Cursor == other.Cursor &&
		record.Proof == other.Proof && record.IntentDigest == other.IntentDigest &&
		bytes.Equal(record.Intent, other.Intent)
}

type replicatedOperationPayload struct {
	Kind              ReplicatedOperationKind  `json:"kind"`
	State             ReplicatedOperationState `json:"state"`
	Revision          uint64                   `json:"revision"`
	CatalogGeneration uint64                   `json:"catalog_generation"`
	Cursor            [8]uint64                `json:"cursor"`
	Proof             []byte                   `json:"proof"`
	IntentDigest      []byte                   `json:"intent_digest"`
	Intent            []byte                   `json:"intent"`
}

func appendReplicatedOperation(dst []byte, record ReplicatedOperationRecord) ([]byte, error) {
	if !validReplicatedOperation(record) {
		return dst, ErrReplicatedCatalog
	}
	canonicalIntent, err := vibejson.AppendCanonicalize(nil, record.Intent)
	if err != nil || !bytes.Equal(canonicalIntent, record.Intent) {
		return dst, errors.Join(err, ErrReplicatedCatalog)
	}
	payload := replicatedOperationPayload{
		Kind: record.Kind, State: record.State, Revision: record.Revision,
		CatalogGeneration: record.CatalogGeneration, Cursor: record.Cursor,
		Proof: record.Proof[:], IntentDigest: record.IntentDigest[:], Intent: record.Intent,
	}
	raw, err := vibejson.Marshal(&payload)
	if err != nil {
		return dst, err
	}
	var identifierStorage [controlPlaneOperationIDBytes]byte
	identifier := appendReplicatedOperationDocumentID(identifierStorage[:0], record.ID)
	return appendControlPlaneDocument(dst, identifier, raw, MaxReplicatedOperationBytes)
}

func openReplicatedOperation(raw []byte) (ReplicatedOperationRecord, error) {
	if len(raw) == 0 || len(raw) > MaxReplicatedOperationBytes {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalog
	}
	id, payloadBytes, err := openReplicatedOperationDocumentID(raw)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	var payload replicatedOperationPayload
	if err = vibejson.Unmarshal(payloadBytes, &payload); err != nil ||
		len(payload.Proof) != len(id) || len(payload.IntentDigest) != len(id) {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	record := ReplicatedOperationRecord{
		ID: id, Kind: payload.Kind, State: payload.State, Revision: payload.Revision,
		CatalogGeneration: payload.CatalogGeneration, Cursor: payload.Cursor,
		Intent: payload.Intent,
	}
	copy(record.Proof[:], payload.Proof)
	copy(record.IntentDigest[:], payload.IntentDigest)
	if !validReplicatedOperation(record) {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalog
	}
	canonical, err := appendReplicatedOperation(nil, record)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return record, nil
}

type replicatedOperationDirectory struct {
	IDs [][]byte `json:"ids"`
}

func appendReplicatedOperationDirectory(dst []byte, ids [][32]byte) ([]byte, error) {
	if len(ids) > maxReplicatedOperations {
		return dst, ErrReplicatedCatalog
	}
	directory := replicatedOperationDirectory{IDs: make([][]byte, len(ids))}
	for index := range ids {
		if ids[index] == ([32]byte{}) || index != 0 && bytes.Compare(ids[index-1][:], ids[index][:]) >= 0 {
			return dst, ErrReplicatedCatalog
		}
		directory.IDs[index] = ids[index][:]
	}
	raw, err := vibejson.Marshal(&directory)
	if err != nil {
		return dst, err
	}
	return appendControlPlaneDocument(
		dst, replicatedOperationDirectoryDocumentID[:], raw,
		maxReplicatedOperationDirectoryBytes,
	)
}

func openReplicatedOperationDirectory(raw []byte) ([][32]byte, error) {
	if len(raw) == 0 || len(raw) > maxReplicatedOperationDirectoryBytes {
		return nil, ErrReplicatedCatalog
	}
	payload, err := openTypedControlPlaneDocument(
		raw, replicatedOperationDirectoryDocumentID[:], maxReplicatedOperationDirectoryBytes,
	)
	if err != nil {
		return nil, err
	}
	var directory replicatedOperationDirectory
	if err = vibejson.Unmarshal(payload, &directory); err != nil || len(directory.IDs) > maxReplicatedOperations {
		return nil, errors.Join(err, ErrReplicatedCatalog)
	}
	ids := make([][32]byte, len(directory.IDs))
	for index := range directory.IDs {
		if len(directory.IDs[index]) != len(ids[index]) {
			return nil, ErrReplicatedCatalog
		}
		copy(ids[index][:], directory.IDs[index])
		if ids[index] == ([32]byte{}) || index != 0 && bytes.Compare(ids[index-1][:], ids[index][:]) >= 0 {
			return nil, ErrReplicatedCatalog
		}
	}
	canonical, err := appendReplicatedOperationDirectory(nil, ids)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.Join(err, ErrReplicatedCatalog)
	}
	return ids, nil
}

// ReadOperationIDs returns the bounded, sorted replicated work directory.
// Absence is the canonical empty directory during bootstrap.
func (authority *ReplicatedCatalogAuthority) ReadOperationIDs(
	ctx context.Context,
) ([][32]byte, error) {
	result, err := authority.readRaw(
		ctx, replicatedOperationDirectoryKey[:], maxReplicatedOperationDirectoryBytes,
	)
	if err != nil {
		return nil, err
	}
	if !result.Found {
		return nil, nil
	}
	return openReplicatedOperationDirectory(result.Value)
}

func (authority *ReplicatedCatalogAuthority) ReadOperation(
	ctx context.Context, id [32]byte,
) (ReplicatedOperationRecord, error) {
	key := replicatedOperationKey(id)
	result, err := authority.readRaw(ctx, key[:], MaxReplicatedOperationBytes)
	if err != nil {
		return ReplicatedOperationRecord{}, err
	}
	if !result.Found {
		return ReplicatedOperationRecord{}, ErrReplicatedOperationMissing
	}
	record, err := openReplicatedOperation(result.Value)
	if err != nil || record.ID != id {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return record, nil
}

// SubmitOperation atomically creates the first immutable operation revision
// and inserts its identity into the replicated sorted work directory. A crash
// cannot strand an undiscoverable record or expose a directory entry without
// its plan. Exact retries are accepted by the same conditional batch.
func (authority *ReplicatedCatalogAuthority) SubmitOperation(
	ctx context.Context, record ReplicatedOperationRecord,
) error {
	return authority.SubmitOperations(ctx, []ReplicatedOperationRecord{record})
}

// SubmitOperations atomically admits a bounded move set into the replicated
// work directory. Every record and the complete sorted directory are one
// conditional replicated batch: controllers can observe all children or none,
// and an outcome-unknown retry reuses byte-identical command material. The
// children remain independent Raft-group sagas after admission, allowing
// snapshot/catch-up work to overlap while each catalog topology publication is
// serialized by its existing generation CAS.
func (authority *ReplicatedCatalogAuthority) SubmitOperations(
	ctx context.Context, records []ReplicatedOperationRecord,
) error {
	if authority == nil || authority.session == nil || ctx == nil || len(records) == 0 ||
		len(records) > maxReplicatedOperations {
		return ErrReplicatedCatalog
	}
	ordered := slices.Clone(records)
	for index := range ordered {
		record := ordered[index]
		if !validReplicatedOperation(record) || record.Revision != 1 ||
			record.State != ReplicatedOperationPlanned {
			return ErrReplicatedCatalog
		}
	}
	slices.SortFunc(ordered, func(left, right ReplicatedOperationRecord) int {
		return bytes.Compare(left.ID[:], right.ID[:])
	})
	for index := 1; index < len(ordered); index++ {
		if ordered[index-1].ID == ordered[index].ID {
			return ErrReplicatedCatalog
		}
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	directoryResult, err := authority.readRaw(
		ctx, replicatedOperationDirectoryKey[:], maxReplicatedOperationDirectoryBytes,
	)
	if err != nil {
		return err
	}
	var ids [][32]byte
	if directoryResult.Found {
		ids, err = openReplicatedOperationDirectory(directoryResult.Value)
		if err != nil {
			return err
		}
	}
	for _, record := range ordered {
		position := 0
		for position < len(ids) && bytes.Compare(ids[position][:], record.ID[:]) < 0 {
			position++
		}
		if position == len(ids) || ids[position] != record.ID {
			if len(ids) == maxReplicatedOperations {
				return ErrReplicatedCatalog
			}
			ids = append(ids, [32]byte{})
			copy(ids[position+1:], ids[position:])
			ids[position] = record.ID
		}
	}
	authority.scratch = authority.scratch[:0]
	ends := make([]int, len(ordered))
	for index, record := range ordered {
		authority.scratch, err = appendReplicatedOperation(authority.scratch, record)
		if err != nil {
			return err
		}
		ends[index] = len(authority.scratch)
	}
	recordBytes := len(authority.scratch)
	authority.scratch, err = appendReplicatedOperationDirectory(authority.scratch, ids)
	if err != nil {
		return err
	}
	directoryBytes := authority.scratch[recordBytes:]
	directoryMutation := NativeMutation{
		Kind: replication.MutationPutAbsentOrEqual,
		Key:  replicatedOperationDirectoryKey[:], Value: directoryBytes,
	}
	if directoryResult.Found {
		digest := sha256.Sum256(directoryResult.Value)
		directoryMutation.Kind = replication.MutationPutDigestEqual
		directoryMutation.ExpectedValueLength = uint64(len(directoryResult.Value))
		directoryMutation.ExpectedValueDigest = replication.Digest(digest)
	}
	mutations := make([]NativeMutation, 0, len(ordered)+1)
	start := 0
	for index, record := range ordered {
		key := replicatedOperationKey(record.ID)
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutAbsentOrEqual,
			Key: key[:], Value: authority.scratch[start:ends[index]]})
		start = ends[index]
	}
	mutations = append(mutations, directoryMutation)
	result, err := authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if authority.session.Status().Pending {
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	return nil
}

// PublishOperation creates revision one idempotently or CAS-replaces exactly
// the prior revision. Complete/cancelled records may only be retried unchanged.
func (authority *ReplicatedCatalogAuthority) PublishOperation(
	ctx context.Context, expectedRevision uint64, record ReplicatedOperationRecord,
) error {
	if authority == nil || authority.session == nil || ctx == nil ||
		!validReplicatedOperation(record) || record.Revision != expectedRevision+1 {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	key := replicatedOperationKey(record.ID)
	current, err := authority.readRaw(ctx, key[:], MaxReplicatedOperationBytes)
	if err != nil {
		return err
	}
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedOperation(authority.scratch, record)
	if err != nil {
		return err
	}
	var result NativeResult
	if !current.Found {
		if expectedRevision != 0 {
			return ErrReplicatedCatalogConflict
		}
		result, err = authority.session.PutIfAbsentOrEqual(ctx, key[:], authority.scratch)
	} else {
		prior, openErr := openReplicatedOperation(current.Value)
		if openErr != nil || prior.ID != record.ID || prior.Revision != expectedRevision ||
			prior.Kind != record.Kind || prior.State >= ReplicatedOperationComplete ||
			prior.IntentDigest != record.IntentDigest || !bytes.Equal(prior.Intent, record.Intent) {
			return errors.Join(openErr, ErrReplicatedCatalogConflict)
		}
		digest := sha256.Sum256(current.Value)
		result, err = authority.session.ComparePut(
			ctx, key[:], authority.scratch, uint64(len(current.Value)), replication.Digest(digest),
		)
	}
	if err != nil {
		if authority.session.Status().Pending {
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	return nil
}

// PublishReplicaMoveAbandonment is the sole exception to immutable operation
// intent: it atomically replaces a live move intent with the exact canonical
// abandonment witness while transitioning to Cancelled. The terminal record
// cannot be rewritten, and ordinary PublishOperation continues to reject
// intent replacement.
func (authority *ReplicatedCatalogAuthority) PublishReplicaMoveAbandonment(
	ctx context.Context, expectedRevision uint64, record ReplicatedOperationRecord,
) error {
	if authority == nil || authority.session == nil || ctx == nil ||
		!validReplicatedOperation(record) || record.Kind != ReplicatedOperationMove ||
		record.State != ReplicatedOperationCancelled || record.Revision != expectedRevision+1 {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	key := replicatedOperationKey(record.ID)
	current, err := authority.readRaw(ctx, key[:], MaxReplicatedOperationBytes)
	if err != nil {
		return err
	}
	if !current.Found {
		return ErrReplicatedCatalogConflict
	}
	prior, openErr := openReplicatedOperation(current.Value)
	if openErr != nil || prior.ID != record.ID || prior.Revision != expectedRevision ||
		prior.Kind != ReplicatedOperationMove || prior.State >= ReplicatedOperationComplete {
		return errors.Join(openErr, ErrReplicatedCatalogConflict)
	}
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedOperation(authority.scratch, record)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(current.Value)
	result, err := authority.session.ComparePut(ctx, key[:], authority.scratch,
		uint64(len(current.Value)), replication.Digest(digest))
	if err != nil {
		if authority.session.Status().Pending {
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	return nil
}

// DeleteOperation garbage-collects only a terminal record with the exact
// revision observed by the controller. Concurrent resume/advance cannot be
// erased; an already absent record is idempotent success.
func (authority *ReplicatedCatalogAuthority) DeleteOperation(
	ctx context.Context, id [32]byte, expectedRevision uint64,
) error {
	if authority == nil || authority.session == nil || ctx == nil ||
		id == ([32]byte{}) || expectedRevision == 0 {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	key := replicatedOperationKey(id)
	current, err := authority.readRaw(ctx, key[:], MaxReplicatedOperationBytes)
	if err != nil {
		return err
	}
	if !current.Found {
		directory, directoryErr := authority.ReadOperationIDs(ctx)
		if directoryErr != nil {
			return directoryErr
		}
		for index := range directory {
			if directory[index] == id {
				return ErrReplicatedCatalogConflict
			}
		}
		return nil
	}
	record, err := openReplicatedOperation(current.Value)
	if err != nil || record.ID != id || record.Revision != expectedRevision ||
		record.State < ReplicatedOperationComplete {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	directoryResult, err := authority.readRaw(
		ctx, replicatedOperationDirectoryKey[:], maxReplicatedOperationDirectoryBytes,
	)
	if err != nil || !directoryResult.Found {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	ids, err := openReplicatedOperationDirectory(directoryResult.Value)
	if err != nil {
		return err
	}
	position := 0
	for position < len(ids) && ids[position] != id {
		position++
	}
	if position == len(ids) {
		return ErrReplicatedCatalogConflict
	}
	copy(ids[position:], ids[position+1:])
	ids = ids[:len(ids)-1]
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedOperationDirectory(authority.scratch, ids)
	if err != nil {
		return err
	}
	recordDigest := sha256.Sum256(current.Value)
	directoryDigest := sha256.Sum256(directoryResult.Value)
	result, err := authority.session.MutateBatch(ctx, []NativeMutation{
		{Kind: replication.MutationDeleteDigestEqual, Key: key[:],
			ExpectedValueLength: uint64(len(current.Value)),
			ExpectedValueDigest: replication.Digest(recordDigest)},
		{Kind: replication.MutationPutDigestEqual, Key: replicatedOperationDirectoryKey[:],
			Value: authority.scratch, ExpectedValueLength: uint64(len(directoryResult.Value)),
			ExpectedValueDigest: replication.Digest(directoryDigest)},
	})
	if err != nil {
		if authority.session.Status().Pending {
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedCatalog
	}
	return nil
}
