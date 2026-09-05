package raftservice

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// DataReadSource is separate from the privileged backup snapshot source.
// Implementations check live intents and capture an immutable publication at
// the supplied quorum floor into reusable caller-owned storage.
type DataReadSource interface {
	DataReadCutInto([]replication.RelationID, uint64, *replicatedstate.DataReadCut) error
}

type LinearizableDataReadRequest struct {
	Fence      ServingFence
	Capability serviceauthz.Capability
	Relations  []replication.RelationID
	// Authorize runs on the serialized owner cut immediately before the read
	// is admitted, matching point reads and proposal admission.
	Authorize ProposalAuthorization
}

// LinearizableDataReadCut pins both the storage cut and the serving generation.
// It is single-consumer, must not be copied, and can be reused after Close.
type LinearizableDataReadCut struct {
	source     DataReadSource
	data       replicatedstate.DataReadCut
	generation *ownerGeneration
	owner      *Owner
}

// LinearizablePointReadRequest selects the narrow live point-read lane. It
// carries the same data-read capability and serving authorization as a data
// cut, but deliberately does not materialize any durable snapshots. The
// caller must still supply the exact relation/key to PointReadInto after the
// owner has installed the cut.
type LinearizablePointReadRequest struct {
	Fence      ServingFence
	Capability serviceauthz.Capability
	// Authorize runs on the serialized owner cut immediately before the read
	// is admitted, exactly as it does for LinearizableDataReadRequest.
	Authorize ProposalAuthorization
}

// LinearizablePointReadCut pins the authorized owner generation and live read
// source after either one leader ReadIndex or one currently valid read
// authority. It owns no durable snapshot. The cut is single-consumer and must
// be closed after the point result has been copied into the SQL source.
type LinearizablePointReadCut struct {
	source         ReadSource
	fence          ServingFence
	minimumApplied uint64
	generation     *ownerGeneration
	owner          *Owner
	request        LinearizablePointReadRequest
	authorityToken raftauthority.AuthorityToken
	authorityFast  bool
	released       atomic.Bool
}

// Source exposes the authenticated point source only while the cut is open.
// SQL uses the source identity to construct its catalog-bound detached point
// session; callers still obtain row bytes only through PointReadInto.
func (cut *LinearizablePointReadCut) Source() ReadSource {
	if cut == nil || cut.owner == nil || cut.released.Load() {
		return nil
	}
	return cut.source
}

// PointReadInto reads one exact point against the cut's quorum-applied floor.
// The source performs its live intent, ownership, and publication checks; the
// cut verifies the returned fence against the serving identity that authorized
// the request. Authority-backed cuts perform a serialized final authority
// validation before returning bytes. A failed validation discards the cut and
// retries once through the original ReadIndex path.
func (cut *LinearizablePointReadCut) PointReadInto(
	ctx context.Context,
	relation replication.RelationID,
	key []byte,
	maxValueBytes int,
	dst []byte,
) (replicatedstate.PointReadResult, error) {
	if cut == nil || cut.owner == nil || cut.source == nil || cut.released.Load() || ctx == nil ||
		relation == 0 || len(key) == 0 || len(key) > replication.MaxMutationKeyBytes ||
		maxValueBytes <= 0 || maxValueBytes > replication.MaxMutationValueBytes ||
		cut.minimumApplied == 0 {
		return replicatedstate.PointReadResult{}, ErrServingFence
	}
	if err := context.Cause(ctx); err != nil {
		return replicatedstate.PointReadResult{}, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		readDst := dst
		if cut.authorityFast {
			// Do not let a source write caller-owned bytes before the final
			// serialized authority check. A failed check discards this local
			// result and retries via ReadIndex without mutating dst.
			readDst = nil
		}
		value, err := cut.source.PointReadInto(
			relation, key, cut.minimumApplied, maxValueBytes, readDst,
		)
		if err != nil {
			if attempt == 0 && cut.authorityFast && readAuthoritySourceFallback(err, false) {
				if retryErr := cut.retryReadIndex(ctx); retryErr != nil {
					return replicatedstate.PointReadResult{}, retryErr
				}
				continue
			}
			return replicatedstate.PointReadResult{}, err
		}
		if err := context.Cause(ctx); err != nil {
			return replicatedstate.PointReadResult{}, err
		}
		fenceOK := pointReadFenceMatches(value.Fence, cut.fence) &&
			value.Fence.Applied >= cut.minimumApplied && len(value.Value) <= maxValueBytes
		if !fenceOK {
			if attempt == 0 && cut.authorityFast && readAuthoritySourceFallback(nil, false) {
				if retryErr := cut.retryReadIndex(ctx); retryErr != nil {
					return replicatedstate.PointReadResult{}, retryErr
				}
				continue
			}
			return replicatedstate.PointReadResult{}, ErrServingFence
		}
		if cut.authorityFast {
			if validationErr := cut.owner.validateReadAuthority(
				ctx, cut.request.Fence, cut.generation, cut.authorityToken,
			); validationErr != nil {
				retry := attempt == 0 && readAuthorityFallback(validationErr)
				cut.owner.recordAuthorityValidation(cut.request.Fence.Group, validationErr, retry)
				if retry {
					if retryErr := cut.retryReadIndex(ctx); retryErr != nil {
						return replicatedstate.PointReadResult{}, retryErr
					}
					continue
				}
				return replicatedstate.PointReadResult{}, validationErr
			}
			cut.owner.recordAuthorityHit(cut.request.Fence.Group)
			if dst != nil {
				value.Value = append(dst[:0], value.Value...)
			}
		}
		return value, nil
	}
	return replicatedstate.PointReadResult{}, ErrServingFence
}

func (cut *LinearizablePointReadCut) retryReadIndex(ctx context.Context) error {
	if cut == nil || cut.owner == nil {
		return ErrInvalidOwner
	}
	owner, request := cut.owner, cut.request
	if err := cut.Close(); err != nil {
		return err
	}
	return owner.readLinearizablePointInto(ctx, request, cut, true)
}

// Close releases the serving-generation and pending-read leases. It is safe
// to call more than once after the first close.
func (cut *LinearizablePointReadCut) Close() error {
	if cut == nil || !cut.released.CompareAndSwap(false, true) {
		return nil
	}
	generation, owner := cut.generation, cut.owner
	cut.generation, cut.owner = nil, nil
	cut.source = nil
	cut.fence = ServingFence{}
	cut.minimumApplied = 0
	cut.request = LinearizablePointReadRequest{}
	cut.authorityToken = raftauthority.AuthorityToken{}
	cut.authorityFast = false
	if generation != nil {
		generation.release()
	}
	if owner != nil {
		owner.releasePendingRead(1)
	}
	return nil
}

// Source borrows the exact generation's data source for shard-local SQL
// execution. It is valid only while this cut retains its generation lease.
func (cut *LinearizableDataReadCut) Source() DataReadSource {
	if cut == nil || cut.owner == nil {
		return nil
	}
	return cut.source
}

// Data borrows the admitted cut. Its physical rows still require the ownership
// filtering documented by DataReadCut; this is not a backup/export capability.
func (cut *LinearizableDataReadCut) Data() *replicatedstate.DataReadCut {
	if cut == nil || cut.owner == nil {
		return nil
	}
	return &cut.data
}

func (cut *LinearizableDataReadCut) Close() error {
	if cut == nil || cut.owner == nil {
		return nil
	}
	err := cut.data.Close()
	cut.generation.release()
	cut.owner.releasePendingRead(1)
	cut.generation, cut.owner = nil, nil
	cut.source = nil
	return err
}

// ReadLinearizableDataInto acquires a leader ReadIndex, waits for apply, and
// pins the exact serving generation until the caller closes dst. Network
// boundaries must authenticate and authorize the request before calling this
// method; only the dedicated data-read capability is accepted here.
func (owner *Owner) ReadLinearizableDataInto(
	ctx context.Context, request LinearizableDataReadRequest, dst *LinearizableDataReadCut,
) error {
	if owner == nil || ctx == nil || dst == nil || request.Capability != serviceauthz.CapabilityDataRead ||
		request.Relations != nil && len(request.Relations) == 0 || len(request.Relations) > replication.MaxRelationsPerBundle {
		return ErrInvalidOwner
	}
	if dst.owner != nil {
		return replicatedstate.ErrDataReadOpen
	}
	if err := owner.reservePendingRead(1); err != nil {
		return err
	}
	admitted := false
	defer func() {
		if !admitted {
			owner.releasePendingRead(1)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadLinear, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{fence: request.Fence, delivery: delivery, authorize: request.Authorize},
	}, delivery)
	if err != nil {
		return err
	}
	defer func() {
		if !admitted {
			reply.read.generation.release()
		}
	}()
	source, ok := reply.read.source.(DataReadSource)
	if !ok {
		return ErrServingFence
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := source.DataReadCutInto(request.Relations, reply.read.minimumApplied, &dst.data); err != nil {
		return err
	}
	if err := context.Cause(ctx); err != nil {
		return errors.Join(err, dst.data.Close())
	}
	fence := dst.data.Fence()
	if !pointReadFenceMatches(fence, request.Fence) || fence.Applied < reply.read.minimumApplied {
		return errors.Join(ErrServingFence, dst.data.Close())
	}
	dst.generation, dst.owner = reply.read.generation, owner
	dst.source = source
	admitted = true
	return nil
}

// ReadLinearizablePointInto acquires the same serving authorization, applied
// floor, and generation lease as ReadLinearizableDataInto while leaving the
// point result materialization to the caller. When a current read authority is
// available, PointReadInto performs the serialized final validation and can
// retry this cut through ReadIndex if the authority expires.
func (owner *Owner) ReadLinearizablePointInto(
	ctx context.Context,
	request LinearizablePointReadRequest,
	dst *LinearizablePointReadCut,
) error {
	return owner.readLinearizablePointInto(ctx, request, dst, false)
}

func (owner *Owner) readLinearizablePointInto(
	ctx context.Context,
	request LinearizablePointReadRequest,
	dst *LinearizablePointReadCut,
	forceReadIndex bool,
) error {
	if owner == nil || ctx == nil || dst == nil ||
		request.Capability != serviceauthz.CapabilityDataRead {
		return ErrInvalidOwner
	}
	if dst.owner != nil {
		return replicatedstate.ErrDataReadOpen
	}
	if err := owner.reservePendingRead(1); err != nil {
		return err
	}
	admitted := false
	defer func() {
		if !admitted {
			owner.releasePendingRead(1)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadLinear, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{
			fence: request.Fence, delivery: delivery, authorize: request.Authorize,
			authorityEligible: !forceReadIndex, forceReadIndex: forceReadIndex,
			authorityFallback: forceReadIndex,
		},
	}, delivery)
	if err != nil {
		return err
	}
	defer func() {
		if !admitted {
			reply.read.generation.release()
		}
	}()
	source, ok := reply.read.source.(ReadSource)
	if !ok {
		return ErrServingFence
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if reply.read.minimumApplied == 0 {
		return ErrServingFence
	}
	dst.source = source
	dst.fence = request.Fence
	dst.minimumApplied = reply.read.minimumApplied
	dst.generation, dst.owner = reply.read.generation, owner
	dst.request = request
	dst.authorityToken = reply.read.authorityToken
	dst.authorityFast = reply.read.authorityFast
	dst.released.Store(false)
	admitted = true
	return nil
}

func (owners *ExecutionOwners) ReadLinearizableDataInto(
	ctx context.Context, request LinearizableDataReadRequest, dst *LinearizableDataReadCut,
) error {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return err
	}
	return owner.ReadLinearizableDataInto(ctx, request, dst)
}

func (owners *ExecutionOwners) ReadLinearizablePointInto(
	ctx context.Context, request LinearizablePointReadRequest, dst *LinearizablePointReadCut,
) error {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return err
	}
	return owner.ReadLinearizablePointInto(ctx, request, dst)
}
