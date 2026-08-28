package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
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
	DefaultReplicatedReadConcurrency        = 256
	DefaultReplicatedReadInFlight           = 256 << 20
	DefaultReplicatedScatterConcurrency     = 16
	AbsoluteMaxReplicatedReadConcurrency    = 1 << 16
	AbsoluteMaxReplicatedScatterConcurrency = 1 << 10
	AbsoluteMaxReplicatedReadInFlight       = 64 << 30
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

// ReplicatedTableBatchPoint is one positional table/key lookup. A batch may
// cross tables only when every point resolves to the same live Raft group.
type ReplicatedTableBatchPoint struct {
	Table []byte
	Key   []byte
}

type ReplicatedTableBatchReadRequest struct {
	Points         []ReplicatedTableBatchPoint
	MaxResultBytes uint32
}

// ReplicatedTableBatchReadResult owns one packed bitmap/length/value response.
// All positions share Position because one leader ReadIndex and one coherent
// all-relation snapshot served the complete batch.
type ReplicatedTableBatchReadResult struct {
	Position ReplicatedReadPosition
	Packed   []byte
	Retries  int
	view     replicatedstate.PointReadBatchValue

	reservationOwner *ReplicatedDataReader
	reservationBytes uint64
	reservationSlot  uint32
	reservationEpoch uint64
}

func (result ReplicatedTableBatchReadResult) Count() int { return result.view.Count() }

func (result ReplicatedTableBatchReadResult) Lookup(index int) ([]byte, bool, bool) {
	return result.view.Lookup(index)
}

func (result ReplicatedTableBatchReadResult) Cursor() ReplicatedBatchPointCursor {
	return ReplicatedBatchPointCursor{cursor: result.view.Cursor()}
}

func (result *ReplicatedTableBatchReadResult) Release() {
	if result == nil || result.reservationOwner == nil || result.reservationBytes == 0 ||
		result.reservationEpoch == 0 {
		return
	}
	owner, bytes := result.reservationOwner, result.reservationBytes
	slot, epoch := result.reservationSlot, result.reservationEpoch
	result.reservationOwner, result.reservationBytes, result.Packed = nil, 0, nil
	result.reservationSlot, result.reservationEpoch = 0, 0
	result.view = replicatedstate.PointReadBatchValue{}
	owner.releaseRead(slot, epoch, bytes)
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
	catalog            *CatalogHolder
	executor           *ReplicatedExecutor
	refresh            RefreshFunc
	readSlots          chan uint32
	reservations       []replicatedReadReservation
	maxReadBytes       uint64
	readBytes          atomic.Uint64
	nextEpoch          atomic.Uint64
	scatterSlots       chan struct{}
	scatterConcurrency int
	pressure           PressureObserver

	refreshMu sync.Mutex
	active    *replicatedDataCatalogRefresh
}

// InstallPressureObserver binds the startup-only bounded pressure intake used
// by native RF3 reads. The reader is not yet reachable by request handlers when
// the gateway composes this seam, so request execution never synchronizes on
// observer replacement.
func (reader *ReplicatedDataReader) InstallPressureObserver(observer PressureObserver) bool {
	if reader == nil || observer == nil || reader.pressure != nil {
		return false
	}
	reader.pressure = observer
	return true
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
	// MaxInFlightReadBytes bounds all admitted native-read response and scatter
	// working sets. Point reads reserve the schema-authenticated document bound;
	// scatter reads additionally reserve their fixed worker windows. Zero
	// selects DefaultReplicatedReadInFlight.
	MaxInFlightReadBytes uint64
	// MaxScatterConcurrency bounds active shard-group ReadIndex calls across
	// every admitted scatter read. It never caps the number of groups in one
	// request; excess groups drain through the fixed worker lanes.
	MaxScatterConcurrency int
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
	scatterConcurrency := options.MaxScatterConcurrency
	if scatterConcurrency == 0 {
		scatterConcurrency = DefaultReplicatedScatterConcurrency
	}
	if options.Catalog == nil || options.Catalog.Current() == nil ||
		options.Executor == nil || options.Executor.client == nil ||
		concurrency <= 0 || concurrency > AbsoluteMaxReplicatedReadConcurrency ||
		scatterConcurrency <= 0 || scatterConcurrency > AbsoluteMaxReplicatedScatterConcurrency ||
		maximumBytes == 0 || maximumBytes > AbsoluteMaxReplicatedReadInFlight {
		return nil, ErrReplicatedDataRead
	}
	reader := &ReplicatedDataReader{
		catalog: options.Catalog, executor: options.Executor, refresh: options.Refresh,
		readSlots: make(chan uint32, concurrency), reservations: make([]replicatedReadReservation, concurrency),
		maxReadBytes: maximumBytes, scatterSlots: make(chan struct{}, scatterConcurrency),
		scatterConcurrency: scatterConcurrency,
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

// ReadBatch executes a same-group, multi-table linearizable read through one
// leader ReadIndex. Cross-group inputs fail before network I/O because they do
// not share a globally meaningful applied index or snapshot timestamp.
func (reader *ReplicatedDataReader) ReadBatch(
	ctx context.Context,
	request ReplicatedTableBatchReadRequest,
) (ReplicatedTableBatchReadResult, error) {
	if err := validateReplicatedTableBatchReadRequest(reader, ctx, request); err != nil {
		return ReplicatedTableBatchReadResult{}, err
	}
	result, generation, err := reader.readBatchPinned(ctx, request)
	if err == nil || !errors.Is(err, raftservice.ErrServingFence) {
		return result, err
	}
	if refreshErr := reader.refreshAfterFence(ctx, generation); refreshErr != nil {
		return ReplicatedTableBatchReadResult{}, errors.Join(err, refreshErr)
	}
	result, _, err = reader.readBatchPinned(ctx, request)
	return result, err
}

func validateReplicatedTableBatchReadRequest(
	reader *ReplicatedDataReader,
	ctx context.Context,
	request ReplicatedTableBatchReadRequest,
) error {
	if reader == nil || reader.catalog == nil || reader.executor == nil || ctx == nil ||
		len(request.Points) == 0 || request.MaxResultBytes == 0 ||
		request.MaxResultBytes > replicatedstate.MaxPointReadBatchBytes {
		return ErrReplicatedDataRead
	}
	count := uint64(len(request.Points))
	if count > uint64(^uint32(0)) || 4+(count+7)/8+count*4 > uint64(request.MaxResultBytes) {
		return ErrReplicatedReadAdmission
	}
	requestBytes := uint64(4)
	for index := range request.Points {
		if len(request.Points[index].Table) == 0 ||
			len(request.Points[index].Table) > replication.MaxIdentityBytes ||
			len(request.Points[index].Key) == 0 ||
			len(request.Points[index].Key) > replication.MaxMutationKeyBytes {
			return ErrReplicatedDataRead
		}
		requestBytes += 5 + uint64(len(request.Points[index].Key))
		if requestBytes > replicatedstate.MaxPointReadBatchBytes {
			return ErrReplicatedReadAdmission
		}
	}
	return nil
}

func (reader *ReplicatedDataReader) readBatchPinned(
	ctx context.Context,
	request ReplicatedTableBatchReadRequest,
) (ReplicatedTableBatchReadResult, uint64, error) {
	lease := reader.catalog.pinCurrent()
	if lease.snapshot == nil {
		return ReplicatedTableBatchReadResult{}, 0, ErrNoCatalog
	}
	defer lease.release()

	var route ReplicatedRoute
	var routeID replication.Digest
	maximum := uint64(request.MaxResultBytes)
	if maximum > reader.maxReadBytes {
		return ReplicatedTableBatchReadResult{}, lease.generation, ErrReplicatedReadAdmission
	}
	working := maximum
	var pressurePoints []distribution.KeyspacePoint
	if reader.pressure != nil {
		pressureBytes := uint64(len(request.Points)) * distribution.KeyspaceWidth
		if pressureBytes > reader.maxReadBytes-maximum {
			return ReplicatedTableBatchReadResult{}, lease.generation, ErrReplicatedReadAdmission
		}
		working += pressureBytes
		pressurePoints = make([]distribution.KeyspacePoint, len(request.Points))
	}
	points := make([]ReplicatedBatchPointRead, len(request.Points))
	for index := range request.Points {
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
		resolved, ok := lease.snapshot.ResolveReplicatedTableKey(
			request.Points[index].Table, request.Points[index].Key,
			scalarScratch[:0], replicas[:0],
		)
		if !ok {
			return ReplicatedTableBatchReadResult{}, lease.generation, ErrReplicatedTableRoute
		}
		if index == 0 {
			route, routeID = resolved.Route, resolved.RouteID
		} else if resolved.RouteID != routeID {
			return ReplicatedTableBatchReadResult{}, lease.generation, ErrReplicatedTableRoute
		}
		points[index] = ReplicatedBatchPointRead{
			Relation: resolved.Profile.Relation, Key: request.Points[index].Key,
		}
		if pressurePoints != nil {
			pressurePoints[index] = resolved.Point
		}
	}
	reservationSlot, reservationEpoch, err := reader.admitRead(ctx, working)
	if err != nil {
		return ReplicatedTableBatchReadResult{}, lease.generation, err
	}
	reader.observeReplicatedPressure(
		replicatedDataPressureSource(lease.snapshot, route), pressurePoints,
	)
	result, err := reader.executor.ReadPointBatch(ctx, route, ReplicatedBatchRead{
		Points: points, MinimumApplied: 1, MaxResultBytes: uint32(maximum),
	})
	if err != nil {
		reader.releaseRead(reservationSlot, reservationEpoch, working)
		return ReplicatedTableBatchReadResult{}, lease.generation, err
	}
	view, err := replicatedstate.OpenPointReadBatchValue(result.Packed)
	if err != nil || view.Count() != len(points) {
		reader.releaseRead(reservationSlot, reservationEpoch, working)
		return ReplicatedTableBatchReadResult{}, lease.generation, ErrReplicatedDataRead
	}
	if working != maximum && !reader.shrinkRead(reservationSlot, reservationEpoch, working, maximum) {
		reader.releaseRead(reservationSlot, reservationEpoch, working)
		return ReplicatedTableBatchReadResult{}, lease.generation, ErrReplicatedReadAdmission
	}
	return ReplicatedTableBatchReadResult{
		Position: ReplicatedReadPosition{RouteID: routeID, Applied: result.Applied},
		Packed:   result.Packed, Retries: result.Retries, view: view,
		reservationOwner: reader, reservationBytes: maximum,
		reservationSlot: reservationSlot, reservationEpoch: reservationEpoch,
	}, lease.generation, nil
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
	reader.observeReplicatedPressure(
		replicatedDataPressureSource(lease.snapshot, resolved.Route), []distribution.KeyspacePoint{resolved.Point},
	)

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

func (reader *ReplicatedDataReader) observeReplicatedPressure(
	source autosplit.SourceIdentity, points []distribution.KeyspacePoint,
) {
	if reader == nil || reader.pressure == nil || source == (autosplit.SourceIdentity{}) {
		return
	}
	for _, point := range points {
		reader.pressure.ObservePressure(PressureObservation{Source: source, Point: point, HasPoint: true})
	}
}

func replicatedDataPressureSource(snapshot *Snapshot, route ReplicatedRoute) autosplit.SourceIdentity {
	if snapshot == nil || route.Distribution == "" || route.Shard == "" ||
		route.AllocationGeneration == 0 {
		return autosplit.SourceIdentity{}
	}
	manifest, ok := snapshot.Manifest(route.Distribution)
	if !ok || uint64(manifest.Version()) != route.Command.RoutingVersion {
		return autosplit.SourceIdentity{}
	}
	var bucketBits uint8
	for ordinal := 0; ordinal < snapshot.DistributionCount(); ordinal++ {
		spec, found := snapshot.DistributionAt(ordinal)
		if found && spec.Name == route.Distribution {
			bucketBits = spec.EffectiveBucketBits()
			break
		}
	}
	if bucketBits == 0 {
		return autosplit.SourceIdentity{}
	}
	for ordinal := 0; ordinal < manifest.ShardCount(); ordinal++ {
		metadata, found := manifest.ShardMetadataAt(ordinal)
		if found && metadata.ID == route.Shard &&
			uint64(metadata.AllocationGeneration) == route.AllocationGeneration &&
			uint64(metadata.Epoch) == route.Command.OwnershipEpoch {
			return autosplit.SourceIdentity{Distribution: route.Distribution, Shard: route.Shard,
				AllocationGeneration: distribution.ShardAllocationGeneration(route.AllocationGeneration), Range: metadata.Range,
				BucketBits: bucketBits, RoutingVersion: manifest.Version(),
				OwnershipEpoch: metadata.Epoch}
		}
	}
	return autosplit.SourceIdentity{}
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

func (reader *ReplicatedDataReader) shrinkRead(slot uint32, epoch, from, to uint64) bool {
	if reader == nil || epoch == 0 || from < to || to == 0 ||
		uint64(slot) >= uint64(len(reader.reservations)) ||
		reader.reservations[slot].epoch.Load() != epoch {
		return false
	}
	if from != to {
		reader.readBytes.Add(^(from - to - 1))
	}
	return true
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
