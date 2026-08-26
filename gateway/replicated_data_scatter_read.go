package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

// replicatedGroupObservationReservation is a conservative retained-memory
// charge for one caller-owned observation, including its slice share. The
// actual fixed-width value is smaller; the headroom keeps admission portable
// across architecture padding changes without unsafe.Sizeof in production.
const replicatedGroupObservationReservation = uint64(192)

// ReplicatedGroupReadObservation is one honest linearizable group cut. Applied
// is meaningful only with this exact RouteID and Group. A scatter result is a
// vector of these observations, never a synthetic global timestamp.
type ReplicatedGroupReadObservation struct {
	Group   raftmember.GroupKey
	RouteID replication.Digest
	Applied uint64
	Retries int
}

// ReplicatedTableScatterReadResult owns one deterministic original-position
// merge. Observations are sorted by RouteID and describe every independent
// group cut that contributed. No result is returned unless every group wins.
type ReplicatedTableScatterReadResult struct {
	Observations []ReplicatedGroupReadObservation
	Packed       []byte
	view         replicatedstate.PointReadBatchValue

	reservationOwner *ReplicatedDataReader
	reservationBytes uint64
	reservationSlot  uint32
	reservationEpoch uint64
}

func (result ReplicatedTableScatterReadResult) Count() int { return result.view.Count() }

func (result ReplicatedTableScatterReadResult) Lookup(index int) ([]byte, bool, bool) {
	return result.view.Lookup(index)
}

func (result ReplicatedTableScatterReadResult) Cursor() ReplicatedBatchPointCursor {
	return ReplicatedBatchPointCursor{cursor: result.view.Cursor()}
}

func (result *ReplicatedTableScatterReadResult) Release() {
	if result == nil || result.reservationOwner == nil || result.reservationBytes == 0 ||
		result.reservationEpoch == 0 {
		return
	}
	owner, reserved := result.reservationOwner, result.reservationBytes
	slot, epoch := result.reservationSlot, result.reservationEpoch
	result.reservationOwner, result.reservationBytes = nil, 0
	result.reservationSlot, result.reservationEpoch = 0, 0
	result.Packed, result.Observations = nil, nil
	result.view = replicatedstate.PointReadBatchValue{}
	owner.releaseRead(slot, epoch, reserved)
}

type replicatedScatterGroup struct {
	route    ReplicatedRoute
	routeID  replication.Digest
	source   autosplit.SourceIdentity
	points   []ReplicatedBatchPointRead
	ordinals []uint32
	result   ReplicatedBatchPointResult
	err      error
	fixed    uint64
}

type replicatedScatterValue struct {
	raw   []byte
	found bool
}

// ReadScatterBatch partitions byte-native table points by exact live RouteID,
// runs one leader ReadIndex per group with fixed worker and memory bounds, and
// merges only after every group succeeds. One definite stale fence triggers at
// most one authenticated catalog refresh and one complete replay.
func (reader *ReplicatedDataReader) ReadScatterBatch(
	ctx context.Context,
	request ReplicatedTableBatchReadRequest,
) (ReplicatedTableScatterReadResult, error) {
	if err := validateReplicatedTableBatchReadRequest(reader, ctx, request); err != nil {
		return ReplicatedTableScatterReadResult{}, err
	}
	result, generation, err := reader.readScatterBatchPinned(ctx, request)
	if err == nil || !errors.Is(err, raftservice.ErrServingFence) {
		return result, err
	}
	if refreshErr := reader.refreshAfterFence(ctx, generation); refreshErr != nil {
		return ReplicatedTableScatterReadResult{}, errors.Join(err, refreshErr)
	}
	result, _, err = reader.readScatterBatchPinned(ctx, request)
	return result, err
}

func (reader *ReplicatedDataReader) readScatterBatchPinned(
	ctx context.Context,
	request ReplicatedTableBatchReadRequest,
) (ReplicatedTableScatterReadResult, uint64, error) {
	lease := reader.catalog.pinCurrent()
	if lease.snapshot == nil {
		return ReplicatedTableScatterReadResult{}, 0, ErrNoCatalog
	}
	defer lease.release()
	return reader.readScatterBatchSnapshot(ctx, request, lease.snapshot, lease.generation)
}

// readScatterBatchSnapshot keeps SQL lowering and native route resolution on
// the exact same immutable catalog generation. The caller owns the snapshot's
// lease for the complete call; stale-fence replay must reacquire and lower the
// original request against a newer generation rather than reusing old keys.
func (reader *ReplicatedDataReader) readScatterBatchSnapshot(
	ctx context.Context,
	request ReplicatedTableBatchReadRequest,
	snapshot *Snapshot,
	generation uint64,
) (ReplicatedTableScatterReadResult, uint64, error) {
	if snapshot == nil || generation == 0 {
		return ReplicatedTableScatterReadResult{}, generation, ErrNoCatalog
	}
	groups := make([]replicatedScatterGroup, 0, min(len(request.Points), 64))
	groupByRoute := make(map[replication.Digest]int, min(len(request.Points), 64))
	requestBytes := uint64(4)
	for ordinal := range request.Points {
		point := request.Points[ordinal]
		var replicas [ServingReplicaCount]ReplicatedEndpoint
		var scalarScratch [replication.MaxMutationKeyBytes + 16]byte
		resolved, ok := snapshot.ResolveReplicatedTableKey(
			point.Table, point.Key, scalarScratch[:0], replicas[:0],
		)
		if !ok {
			return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedTableRoute
		}
		groupIndex, exists := groupByRoute[resolved.RouteID]
		if !exists {
			groupIndex = len(groups)
			groupByRoute[resolved.RouteID] = groupIndex
			groups = append(groups, replicatedScatterGroup{
				route: resolved.Route, routeID: resolved.RouteID,
				source: replicatedDataPressureSource(snapshot, resolved.Route),
			})
		}
		group := &groups[groupIndex]
		group.points = append(group.points, ReplicatedBatchPointRead{
			Relation: resolved.Profile.Relation, Key: point.Key,
		})
		group.ordinals = append(group.ordinals, uint32(ordinal))
		requestBytes += 5 + uint64(len(point.Key))
	}
	slices.SortFunc(groups, func(left, right replicatedScatterGroup) int {
		return bytes.Compare(left.routeID[:], right.routeID[:])
	})

	count := uint64(len(request.Points))
	finalFixed := uint64(4) + (count+7)/8 + count*4
	resultBound := uint64(request.MaxResultBytes)
	observationBound := uint64(len(groups)) * replicatedGroupObservationReservation
	if observationBound > reader.maxReadBytes || resultBound > reader.maxReadBytes-observationBound {
		return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedReadAdmission
	}
	finalReservation := resultBound + observationBound
	payloadBound := resultBound - finalFixed
	groupFixedBytes := uint64(0)
	for index := range groups {
		groupCount := uint64(len(groups[index].points))
		groups[index].fixed = 4 + (groupCount+7)/8 + groupCount*4
		groupFixedBytes += groups[index].fixed
	}
	// Point/ordinal partitions are the only cardinality-shaped working state.
	// Charge their conservative headers plus the exact packed request bytes.
	metadataBytes := count*40 + uint64(len(groups))*512 + requestBytes + groupFixedBytes
	if metadataBytes > reader.maxReadBytes || resultBound > reader.maxReadBytes-metadataBytes {
		return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedReadAdmission
	}
	availableResponses := (reader.maxReadBytes - metadataBytes) / resultBound
	// One response-bound is retained by completed groups/final merge; the rest
	// are active workers. This derives concurrency from bytes without limiting
	// the number of groups that the workers can drain.
	if availableResponses < 2 {
		return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedReadAdmission
	}
	workerCount := min(len(groups), reader.scatterConcurrency)
	if uint64(workerCount) > availableResponses-1 {
		workerCount = int(availableResponses - 1)
	}
	if workerCount <= 0 {
		return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedReadAdmission
	}
	workingBytes := metadataBytes + uint64(workerCount+1)*resultBound
	reservationSlot, reservationEpoch, err := reader.admitRead(ctx, workingBytes)
	if err != nil {
		return ReplicatedTableScatterReadResult{}, generation, err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			reader.releaseRead(reservationSlot, reservationEpoch, workingBytes)
		}
	}()

	scatterCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var next atomic.Uint64
	var retainedMu sync.Mutex
	retainedPayload := uint64(0)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for {
				groupIndex := int(next.Add(1) - 1)
				if groupIndex >= len(groups) {
					return
				}
				group := &groups[groupIndex]
				select {
				case reader.scatterSlots <- struct{}{}:
				case <-scatterCtx.Done():
					group.err = context.Cause(scatterCtx)
					continue
				}
				reader.observeReplicatedPressure(group.source, len(group.points), false)
				result, readErr := reader.executor.ReadPointBatch(
					scatterCtx, group.route, ReplicatedBatchRead{
						Points: group.points, MinimumApplied: 1,
						MaxResultBytes: request.MaxResultBytes,
					},
				)
				<-reader.scatterSlots
				if readErr == nil {
					payloadBytes := uint64(len(result.Packed)) - group.fixed
					retainedMu.Lock()
					if payloadBytes > payloadBound-retainedPayload {
						readErr = ErrReplicatedReadAdmission
					} else {
						retainedPayload += payloadBytes
						group.result = result
					}
					retainedMu.Unlock()
				}
				if readErr != nil {
					group.err = readErr
					cancel(readErr)
				}
			}
		}()
	}
	workers.Wait()
	if err := firstReplicatedScatterError(groups, ctx); err != nil {
		return ReplicatedTableScatterReadResult{}, generation, err
	}

	values := make([]replicatedScatterValue, len(request.Points))
	observations := make([]ReplicatedGroupReadObservation, len(groups))
	for groupIndex := range groups {
		group := &groups[groupIndex]
		cursor := group.result.Cursor()
		for local, ordinal := range group.ordinals {
			raw, found, ok := cursor.Next()
			if !ok || local >= len(group.points) {
				return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedDataRead
			}
			values[ordinal] = replicatedScatterValue{raw: raw, found: found}
		}
		if _, _, ok := cursor.Next(); ok {
			return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedDataRead
		}
		observations[groupIndex] = ReplicatedGroupReadObservation{
			Group: group.route.Group, RouteID: group.routeID,
			Applied: group.result.Applied, Retries: group.result.Retries,
		}
	}
	packedBytes := finalFixed + retainedPayload
	packed := make([]byte, int(finalFixed), int(packedBytes))
	binary.LittleEndian.PutUint32(packed, uint32(len(values)))
	bitmapStart := 4
	lengthStart := bitmapStart + (len(values)+7)/8
	for ordinal := range values {
		value := values[ordinal]
		if value.found {
			packed[bitmapStart+ordinal/8] |= 1 << uint(ordinal&7)
		}
		binary.LittleEndian.PutUint32(packed[lengthStart+ordinal*4:], uint32(len(value.raw)))
		packed = append(packed, value.raw...)
	}
	view, err := replicatedstate.OpenPointReadBatchValue(packed)
	if err != nil || view.Count() != len(request.Points) || len(packed) > int(resultBound) {
		return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedDataRead
	}
	// Only the final caller-owned result remains live after merge.
	if !reader.shrinkRead(reservationSlot, reservationEpoch, workingBytes, finalReservation) {
		return ReplicatedTableScatterReadResult{}, generation, ErrReplicatedReadAdmission
	}
	releaseReservation = false
	return ReplicatedTableScatterReadResult{
		Observations: observations, Packed: packed, view: view,
		reservationOwner: reader, reservationBytes: finalReservation,
		reservationSlot: reservationSlot, reservationEpoch: reservationEpoch,
	}, generation, nil
}

func firstReplicatedScatterError(groups []replicatedScatterGroup, parent context.Context) error {
	for index := range groups {
		if groups[index].err != nil &&
			!errors.Is(groups[index].err, context.Canceled) &&
			!errors.Is(groups[index].err, context.DeadlineExceeded) {
			return groups[index].err
		}
	}
	if cause := context.Cause(parent); cause != nil {
		return cause
	}
	for index := range groups {
		if groups[index].err != nil {
			return groups[index].err
		}
	}
	return nil
}
