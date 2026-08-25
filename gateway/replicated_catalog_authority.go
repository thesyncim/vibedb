package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
	replicatedCatalogHeadKeyByte = byte(0)
	replicatedOperationKeyByte   = byte(1)
	MaxReplicatedOperationBytes  = 64 << 10
	// One catalog head is one atomic relation value. This intentionally follows
	// the existing replicated mutation bound; larger catalogs fail closed until
	// a separately authenticated chunk-manifest protocol exists.
	maxReplicatedCatalogBytes = replication.MaxMutationValueBytes
)

var replicatedCatalogHeadKey = [...]byte{replicatedCatalogHeadKeyByte}

// ReplicatedCatalogAuthority stores the catalog head and resumable controller
// records in one ordinary JSON relation served by the existing RF3 owner. The
// route is a bootstrap coordinate only; every head read is ReadIndex-fenced and
// every replacement is a raw length+SHA-256 compare inside replicated apply.
type ReplicatedCatalogAuthority struct {
	executor        *ReplicatedExecutor
	route           ReplicatedRoute
	relation        replication.RelationID
	holder          *CatalogHolder
	session         *NativeSession
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
	// Session is required only for publication and operation writes. It must be
	// active, route the same RF3 group, and resolve logical mutations to Relation.
	Session *NativeSession
}

func NewReplicatedCatalogAuthority(options ReplicatedCatalogAuthorityOptions) (*ReplicatedCatalogAuthority, error) {
	if options.Executor == nil || !validReplicatedRoute(options.Route) ||
		options.Relation == 0 || options.Relation > replication.MaxRelationID ||
		options.Holder == nil || options.Session != nil &&
		(options.Session.executor != options.Executor ||
			options.Session.phase != nativeSessionActive || options.Session.pending ||
			!sameReplicatedCatalogRoute(options.Session.route, options.Route) ||
			nativeSessionBaseRelation(options.Session) != options.Relation) {
		return nil, ErrReplicatedCatalog
	}
	route := options.Route
	route.Replicas = append([]ReplicatedEndpoint(nil), route.Replicas...)
	return &ReplicatedCatalogAuthority{
		executor: options.Executor, route: route, relation: options.Relation,
		holder: options.Holder, session: options.Session,
		scratch: make([]byte, 0, 4<<10),
	}, nil
}

func nativeSessionBaseRelation(session *NativeSession) replication.RelationID {
	if session == nil {
		return 0
	}
	switch resolver := session.resolver.(type) {
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
	if left.Group != right.Group ||
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
	if authority == nil || ctx == nil || len(key) == 0 {
		return ReplicatedPointResult{}, ErrReplicatedCatalog
	}
	return authority.executor.ReadPoint(ctx, authority.route, ReplicatedPointRead{
		Relation: authority.relation, Key: key, MinimumApplied: 1,
		MaxValueBytes: maximum, Linearizable: true,
	})
}

// Read fetches the authoritative RF3 catalog head and validates the complete
// routing/index/lineage image before publishing it to the lock-free holder.
func (authority *ReplicatedCatalogAuthority) Read(ctx context.Context) (*Snapshot, error) {
	result, err := authority.readRaw(ctx, replicatedCatalogHeadKey[:], maxReplicatedCatalogBytes)
	if err != nil {
		return nil, err
	}
	if !result.Found {
		return nil, ErrReplicatedCatalogMissing
	}
	snapshot, err := OpenSnapshotDocument(result.Value)
	if err != nil {
		return nil, err
	}
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
		currentBytes, encodeErr := AppendSnapshotDocument(nil, current)
		if encodeErr != nil || !bytes.Equal(currentBytes, result.Value) {
			return nil, errors.Join(encodeErr, ErrReplicatedCatalogConflict)
		}
	}
	return authority.holder.Current(), nil
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
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	currentResult, err := authority.readRaw(ctx, replicatedCatalogHeadKey[:], maxReplicatedCatalogBytes)
	if err != nil {
		return err
	}
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = AppendSnapshotDocument(authority.scratch, next)
	if err != nil {
		return err
	}
	if len(authority.scratch) > maxReplicatedCatalogBytes {
		return ErrCatalogTooLarge
	}
	var native NativeResult
	if !currentResult.Found {
		if expectedGeneration != 0 {
			return ErrCatalogGenerationMismatch
		}
		native, err = authority.session.PutIfAbsentOrEqual(
			ctx, replicatedCatalogHeadKey[:], authority.scratch,
		)
	} else {
		current, openErr := OpenSnapshotDocument(currentResult.Value)
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
		digest := sha256.Sum256(currentResult.Value)
		native, err = authority.session.ComparePut(
			ctx, replicatedCatalogHeadKey[:], authority.scratch,
			uint64(len(currentResult.Value)), replication.Digest(digest),
		)
	}
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
}

func validReplicatedOperation(record ReplicatedOperationRecord) bool {
	return record.ID != ([32]byte{}) &&
		record.Kind >= ReplicatedOperationSplit && record.Kind <= ReplicatedOperationMove &&
		record.State >= ReplicatedOperationPlanned && record.State <= ReplicatedOperationCancelled &&
		record.Revision != 0 && record.CatalogGeneration != 0 && record.Proof != ([32]byte{})
}

func replicatedOperationKey(id [32]byte) [33]byte {
	var key [33]byte
	key[0] = replicatedOperationKeyByte
	copy(key[1:], id[:])
	return key
}

func appendReplicatedOperation(dst []byte, record ReplicatedOperationRecord) ([]byte, error) {
	if !validReplicatedOperation(record) {
		return dst, ErrReplicatedCatalog
	}
	raw, err := vibejson.Marshal(&record)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, raw)
	if err != nil || len(dst)-start > MaxReplicatedOperationBytes {
		return dst[:start], errors.Join(err, ErrReplicatedCatalog)
	}
	return dst, nil
}

func openReplicatedOperation(raw []byte) (ReplicatedOperationRecord, error) {
	if len(raw) == 0 || len(raw) > MaxReplicatedOperationBytes {
		return ReplicatedOperationRecord{}, ErrReplicatedCatalog
	}
	var record ReplicatedOperationRecord
	if err := vibejson.Unmarshal(raw, &record); err != nil || !validReplicatedOperation(record) {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	canonical, err := appendReplicatedOperation(nil, record)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ReplicatedOperationRecord{}, errors.Join(err, ErrReplicatedCatalog)
	}
	return record, nil
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

// PublishOperation creates revision one idempotently or CAS-replaces exactly
// the prior revision. Complete/cancelled records may only be retried unchanged.
func (authority *ReplicatedCatalogAuthority) PublishOperation(
	ctx context.Context, expectedRevision uint64, record ReplicatedOperationRecord,
) error {
	if authority == nil || authority.session == nil || ctx == nil ||
		!validReplicatedOperation(record) || record.Revision != expectedRevision+1 {
		return ErrReplicatedCatalog
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
			prior.Kind != record.Kind || prior.State >= ReplicatedOperationComplete {
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
		return nil
	}
	record, err := openReplicatedOperation(current.Value)
	if err != nil || record.ID != id || record.Revision != expectedRevision ||
		record.State < ReplicatedOperationComplete {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	digest := sha256.Sum256(current.Value)
	result, err := authority.session.CompareDelete(
		ctx, key[:], uint64(len(current.Value)), replication.Digest(digest),
	)
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
