package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"

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
	executor        *ReplicatedExecutor
	route           ReplicatedRoute
	relation        replication.RelationID
	holder          *CatalogHolder
	session         *NativeSession
	authority       serviceauthz.Authority
	mu              sync.Mutex
	scratch         []byte
	pendingCatalog  *Snapshot
	pendingExpected uint64
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
		authority: options.Authority,
		scratch:   make([]byte, 0, 4<<10),
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
	}
	return 0
}

func sameReplicatedCatalogRoute(left, right ReplicatedRoute) bool {
	if left.Distribution != right.Distribution || left.Shard != right.Shard ||
		left.Group != right.Group ||
		left.AllocationGeneration != right.AllocationGeneration ||
		left.Command != right.Command || len(left.Replicas) != len(right.Replicas) {
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
	return authority.publishReadCatalogCut(cut.snapshot, cut.head)
}

func (authority *ReplicatedCatalogAuthority) readCatalogCut(ctx context.Context) (replicatedCatalogCut, error) {
	result, err := authority.readRaw(ctx, replicatedCatalogHeadKey, uint32(maxReplicatedCatalogBytes))
	if err != nil {
		return replicatedCatalogCut{}, err
	}
	if !result.Found {
		return replicatedCatalogCut{}, ErrReplicatedCatalogMissing
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

func (authority *ReplicatedCatalogAuthority) publishReadCatalogCut(snapshot *Snapshot, raw []byte) (*Snapshot, error) {
	current := authority.holder.Current()
	if current == nil {
		if !authority.holder.PublishNewer(snapshot) {
			return nil, ErrReplicatedCatalogConflict
		}
	} else if snapshot.Generation() > current.Generation() {
		if err := authority.holder.publishNewerChecked(snapshot); err != nil {
			return nil, err
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
	mutations := make([]NativeMutation, 0, 2)
	var native NativeResult
	if !currentResult.Found {
		if expectedGeneration != 0 || currentWitness.Found {
			return ErrCatalogGenerationMismatch
		}
		mutations = append(mutations,
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
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		authority.pendingCatalog = nil
		return ErrReplicatedCatalog
	}
	if authority.pendingCatalog != nil {
		err = authority.holder.PublishAfter(authority.pendingExpected, authority.pendingCatalog)
		authority.pendingCatalog = nil
		authority.pendingExpected = 0
		return err
	}
	return nil
}

type ReplicatedOperationKind uint8
type ReplicatedOperationState uint8

const (
	ReplicatedOperationSplit ReplicatedOperationKind = iota + 1
	ReplicatedOperationMove
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
		record.Kind >= ReplicatedOperationSplit && record.Kind <= ReplicatedOperationMove &&
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
	if authority == nil || authority.session == nil || ctx == nil ||
		!validReplicatedOperation(record) || record.Revision != 1 ||
		record.State != ReplicatedOperationPlanned {
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
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedOperation(authority.scratch, record)
	if err != nil {
		return err
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
	key := replicatedOperationKey(record.ID)
	result, err := authority.session.MutateBatch(ctx, []NativeMutation{
		{Kind: replication.MutationPutAbsentOrEqual, Key: key[:], Value: authority.scratch[:recordBytes]},
		directoryMutation,
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
