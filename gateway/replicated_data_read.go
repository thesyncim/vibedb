package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	// ErrReplicatedDataRead reports a malformed public native read. The request
	// is rejected before catalog resolution or network I/O.
	ErrReplicatedDataRead = errors.New("gateway: invalid replicated data read")
	// ErrReplicatedTableRoute reports that the pinned catalog generation cannot
	// map the exact table/key pair to a serving RF3 base relation. There is no
	// legacy SQL or single-copy fallback at this boundary.
	ErrReplicatedTableRoute = errors.New("gateway: replicated table route is unavailable")
	// ErrReplicatedReadPositionMismatch reports that an applied-index floor was
	// issued by a different exact route lineage. Applied indexes are local to a
	// Raft group incarnation and must never be compared across RouteIDs.
	ErrReplicatedReadPositionMismatch = errors.New("gateway: replicated read position does not match route")
	// ErrReplicatedReadAdmission reports that the configured native-read byte
	// budget cannot reserve the relation's complete response bound.
	ErrReplicatedReadAdmission = errors.New("gateway: replicated read admission bound exceeded")
)

const (
	DefaultReplicatedReadConcurrency     = 256
	DefaultReplicatedReadInFlight        = 256 << 20
	AbsoluteMaxReplicatedReadConcurrency = 1 << 16
	AbsoluteMaxReplicatedReadInFlight    = 64 << 30
)

// ReplicatedDataReadConsistency is an explicit public point-read contract.
// The zero value is invalid so a caller cannot accidentally select stale data.
type ReplicatedDataReadConsistency uint8

const (
	ReplicatedDataReadLinearizable ReplicatedDataReadConsistency = iota + 1
	ReplicatedDataReadAtLeastApplied
)

// ReplicatedReadPosition is a portable monotonic-read witness. Applied is only
// meaningful together with the exact RouteID returned by a previous read or
// write. A split, move, schema rollout, or group replacement changes RouteID
// and causes a definite position-mismatch refusal before shard I/O.
type ReplicatedReadPosition struct {
	RouteID replication.Digest
	Applied uint64
}

// ReplicatedTableReadRequest contains byte-native public routing inputs. Table
// and Key are borrowed for the duration of Read; no string or JSON conversion
// occurs on this path.
type ReplicatedTableReadRequest struct {
	Table       []byte
	Key         []byte
	Consistency ReplicatedDataReadConsistency
	Position    ReplicatedReadPosition
}

// ReplicatedTableReadResult returns the exact route lineage and applied index
// that fenced Value. Value is the bounded native response payload.
type ReplicatedTableReadResult struct {
	Position ReplicatedReadPosition
	Found    bool
	Value    []byte
	Retries  int

	reservationOwner *ReplicatedDataReader
	reservationBytes uint64
	reservationSlot  uint32
	reservationEpoch uint64
}

// Release returns the schema-bounded response reservation after the caller has
// finished emitting or copying Value. A successful result must be released
// exactly once. Release clears Value and is idempotent for the same result
// variable; callers must not copy a live result.
func (result *ReplicatedTableReadResult) Release() {
	if result == nil || result.reservationOwner == nil || result.reservationBytes == 0 ||
		result.reservationEpoch == 0 {
		return
	}
	owner, bytes := result.reservationOwner, result.reservationBytes
	slot, epoch := result.reservationSlot, result.reservationEpoch
	result.reservationOwner, result.reservationBytes, result.Value = nil, 0, nil
	result.reservationSlot, result.reservationEpoch = 0, 0
	owner.releaseRead(slot, epoch, bytes)
}

// ReplicatedDataReader binds public table/key reads to one atomically pinned
// catalog generation and the SQL-free RF3 executor.
type ReplicatedDataReader struct {
	catalog      *CatalogHolder
	executor     *ReplicatedExecutor
	refresh      RefreshFunc
	readSlots    chan uint32
	reservations []replicatedReadReservation
	maxReadBytes uint64
	readBytes    atomic.Uint64
	nextEpoch    atomic.Uint64

	refreshMu sync.Mutex
	active    *replicatedDataCatalogRefresh
}

type replicatedDataCatalogRefresh struct {
	done          chan struct{}
	floor         uint64
	err           error
	ownerCanceled bool
}

type replicatedReadReservation struct {
	epoch atomic.Uint64
}

type ReplicatedDataReaderOptions struct {
	Catalog  *CatalogHolder
	Executor *ReplicatedExecutor
	// Refresh obtains and authenticates a catalog generation strictly newer
	// than the generation rejected by a serving fence. Definite stale-fence
	// refusals are the only failures that invoke it.
	Refresh RefreshFunc
	// MaxConcurrentReads bounds requests admitted to native RF3 point I/O.
	// Zero selects DefaultReplicatedReadConcurrency.
	MaxConcurrentReads int
	// MaxInFlightReadBytes reserves each read at its schema-authenticated
	// MaxDocumentBytes before network I/O. Zero selects
	// DefaultReplicatedReadInFlight.
	MaxInFlightReadBytes uint64
}

func NewReplicatedDataReader(
	catalog *CatalogHolder,
	executor *ReplicatedExecutor,
) (*ReplicatedDataReader, error) {
	return NewReplicatedDataReaderWithOptions(ReplicatedDataReaderOptions{
		Catalog: catalog, Executor: executor,
	})
}

func NewReplicatedDataReaderWithOptions(
	options ReplicatedDataReaderOptions,
) (*ReplicatedDataReader, error) {
	concurrency := options.MaxConcurrentReads
	if concurrency == 0 {
		concurrency = DefaultReplicatedReadConcurrency
	}
	maximumBytes := options.MaxInFlightReadBytes
	if maximumBytes == 0 {
		maximumBytes = DefaultReplicatedReadInFlight
	}
	if options.Catalog == nil || options.Catalog.Current() == nil ||
		options.Executor == nil || options.Executor.client == nil ||
		concurrency <= 0 || concurrency > AbsoluteMaxReplicatedReadConcurrency ||
		maximumBytes == 0 || maximumBytes > AbsoluteMaxReplicatedReadInFlight {
		return nil, ErrReplicatedDataRead
	}
	reader := &ReplicatedDataReader{
		catalog: options.Catalog, executor: options.Executor, refresh: options.Refresh,
		readSlots: make(chan uint32, concurrency), reservations: make([]replicatedReadReservation, concurrency),
		maxReadBytes: maximumBytes,
	}
	for slot := range reader.reservations {
		reader.readSlots <- uint32(slot)
	}
	return reader, nil
}

// Read executes one RF3 base-relation point read. Linearizable uses the
// leader's ReadIndex contract and accepts no caller position. AtLeastApplied
// prefers a follower but first proves that the supplied RouteID belongs to the
// exact currently pinned route and then enforces its applied-index floor.
func (reader *ReplicatedDataReader) Read(
	ctx context.Context,
	request ReplicatedTableReadRequest,
) (ReplicatedTableReadResult, error) {
	if reader == nil || reader.catalog == nil || reader.executor == nil || ctx == nil ||
		len(request.Table) == 0 || len(request.Table) > replication.MaxIdentityBytes ||
		len(request.Key) == 0 || len(request.Key) > replication.MaxMutationKeyBytes {
		return ReplicatedTableReadResult{}, ErrReplicatedDataRead
	}
	minimumApplied := uint64(1)
	linearizable := false
	switch request.Consistency {
	case ReplicatedDataReadLinearizable:
		if request.Position != (ReplicatedReadPosition{}) {
			return ReplicatedTableReadResult{}, ErrReplicatedDataRead
		}
		linearizable = true
	case ReplicatedDataReadAtLeastApplied:
		if request.Position.RouteID == (replication.Digest{}) || request.Position.Applied == 0 {
			return ReplicatedTableReadResult{}, ErrReplicatedDataRead
		}
		minimumApplied = request.Position.Applied
	default:
		return ReplicatedTableReadResult{}, ErrReplicatedDataRead
	}

	result, generation, err := reader.readPinned(ctx, request, minimumApplied, linearizable)
	if err == nil || !errors.Is(err, raftservice.ErrServingFence) {
		return result, err
	}
	// A serving-fence refusal is definite: the read did not execute against the
	// rejected route. Coalesce one authenticated catalog refresh, re-resolve the
	// original byte-native request, and retry exactly once. Transport ambiguity
	// and every other failure return without refresh or replay.
	if refreshErr := reader.refreshAfterFence(ctx, generation); refreshErr != nil {
		return ReplicatedTableReadResult{}, errors.Join(err, refreshErr)
	}
	result, _, err = reader.readPinned(ctx, request, minimumApplied, linearizable)
	return result, err
}

func (reader *ReplicatedDataReader) readPinned(
	ctx context.Context,
	request ReplicatedTableReadRequest,
	minimumApplied uint64,
	linearizable bool,
) (ReplicatedTableReadResult, uint64, error) {
	lease := reader.catalog.pinCurrent()
	if lease.snapshot == nil {
		return ReplicatedTableReadResult{}, 0, ErrNoCatalog
	}
	defer lease.release()

	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := lease.snapshot.ResolveReplicatedTableKey(
		request.Table, request.Key, scalarScratch[:0], replicas[:0],
	)
	if !ok {
		return ReplicatedTableReadResult{}, lease.generation, ErrReplicatedTableRoute
	}
	if !linearizable && request.Position.RouteID != resolved.RouteID {
		return ReplicatedTableReadResult{}, lease.generation, ErrReplicatedReadPositionMismatch
	}
	readBytes := uint64(resolved.Profile.MaxDocumentBytes)
	reservationSlot, reservationEpoch, err := reader.admitRead(ctx, readBytes)
	if err != nil {
		return ReplicatedTableReadResult{}, lease.generation, err
	}

	result, err := reader.executor.ReadPoint(ctx, resolved.Route, ReplicatedPointRead{
		Relation: resolved.Profile.Relation, Key: request.Key,
		MinimumApplied: minimumApplied, MaxValueBytes: resolved.Profile.MaxDocumentBytes,
		Linearizable: linearizable,
	})
	if err != nil {
		reader.releaseRead(reservationSlot, reservationEpoch, readBytes)
		return ReplicatedTableReadResult{}, lease.generation, err
	}
	return ReplicatedTableReadResult{
		Position: ReplicatedReadPosition{RouteID: resolved.RouteID, Applied: result.Applied},
		Found:    result.Found, Value: result.Value, Retries: result.Retries,
		reservationOwner: reader, reservationBytes: readBytes,
		reservationSlot: reservationSlot, reservationEpoch: reservationEpoch,
	}, lease.generation, nil
}

func (reader *ReplicatedDataReader) admitRead(
	ctx context.Context,
	bytes uint64,
) (uint32, uint64, error) {
	if reader == nil || ctx == nil || bytes == 0 || bytes > reader.maxReadBytes ||
		reader.readSlots == nil || len(reader.reservations) == 0 {
		return 0, 0, ErrReplicatedReadAdmission
	}
	var slot uint32
	select {
	case slot = <-reader.readSlots:
	case <-ctx.Done():
		return 0, 0, context.Cause(ctx)
	}
	for {
		used := reader.readBytes.Load()
		if used > reader.maxReadBytes || bytes > reader.maxReadBytes-used {
			reader.readSlots <- slot
			return 0, 0, ErrReplicatedReadAdmission
		}
		if reader.readBytes.CompareAndSwap(used, used+bytes) {
			break
		}
	}
	epoch := reader.nextEpoch.Add(1)
	if epoch == 0 {
		epoch = reader.nextEpoch.Add(1)
	}
	reader.reservations[slot].epoch.Store(epoch)
	return slot, epoch, nil
}

func (reader *ReplicatedDataReader) releaseRead(slot uint32, epoch, bytes uint64) {
	if reader == nil || bytes == 0 || epoch == 0 || reader.readSlots == nil ||
		uint64(slot) >= uint64(len(reader.reservations)) ||
		!reader.reservations[slot].epoch.CompareAndSwap(epoch, 0) {
		return
	}
	reader.readBytes.Add(^(bytes - 1))
	reader.readSlots <- slot
}

func (reader *ReplicatedDataReader) refreshAfterFence(
	ctx context.Context,
	staleGeneration uint64,
) error {
	if reader == nil || reader.catalog == nil || ctx == nil || staleGeneration == 0 {
		return ErrReplicatedDataRead
	}
	for {
		if current := reader.catalog.Current(); current != nil &&
			current.Generation() > staleGeneration {
			return nil
		}
		reader.refreshMu.Lock()
		if active := reader.active; active != nil {
			reader.refreshMu.Unlock()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-active.done:
			}
			if current := reader.catalog.Current(); current != nil &&
				current.Generation() > staleGeneration {
				return nil
			}
			if active.floor >= staleGeneration {
				if active.err != nil {
					// A refresh owner lends only its authenticated result, not
					// its request lifetime. A canceled owner must not poison a
					// still-live waiter; let that waiter become the next owner.
					if active.ownerCanceled {
						continue
					}
					return active.err
				}
				return ErrStaleGeneration
			}
			continue
		}
		active := &replicatedDataCatalogRefresh{
			done: make(chan struct{}), floor: staleGeneration,
		}
		reader.active = active
		reader.refreshMu.Unlock()

		var snapshot *Snapshot
		if reader.refresh == nil {
			active.err = ErrStaleGeneration
		} else {
			snapshot, active.err = reader.refresh(ctx, staleGeneration)
			if active.err == nil && (snapshot == nil || snapshot.Generation() <= staleGeneration) {
				active.err = ErrStaleGeneration
			}
			if active.err == nil {
				current := reader.catalog.Current()
				if current == nil || current.Generation() < snapshot.Generation() {
					if publishErr := reader.catalog.publishNewerChecked(snapshot); publishErr != nil &&
						!errors.Is(publishErr, ErrCatalogGenerationNotNewer) {
						active.err = publishErr
					}
				}
			}
		}
		if current := reader.catalog.Current(); current != nil &&
			current.Generation() > staleGeneration {
			active.err = nil
		}
		active.ownerCanceled = context.Cause(ctx) != nil
		reader.refreshMu.Lock()
		reader.active = nil
		close(active.done)
		reader.refreshMu.Unlock()
		return active.err
	}
}
