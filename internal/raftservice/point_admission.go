package raftservice

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// pointReadView is one immutable serialized-owner publication. The pointer is
// replaced only when the serving permit or source generation changes; readers
// never inspect Owner.members or construct a serving map/status cut.
type pointReadView struct {
	owner      *Owner
	group      raftmember.GroupKey
	identity   raftmember.RuntimeIdentity
	command    CommandFence
	source     ReadSource
	generation *ownerGeneration
	permit     *servingFencePermit
}

// pointReadViewSlot is stable for the lifetime of one execution route. Its
// atomic pointer is the only Owner metadata read by the concurrent admission
// attempt.
type pointReadViewSlot struct {
	value atomic.Pointer[pointReadView]
}

type pointReadAdmissionHost interface {
	TryReadPointAdmission(
		raftmember.GroupKey, raftmember.RuntimeIdentity, uint64,
		func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool,
	) (attempted, admitted, authorized bool, result multiraft.PointReadAdmission, err error)
}

// normalizePointReadRequest gives the serialized fallback the same decision
// as a concurrent admission callback. The two callback forms are mutually
// exclusive: accepting both would make a fallback trust an unverified claim
// that two independently supplied predicates are equivalent.
func normalizeDataReadRequest(
	request LinearizableDataReadRequest,
) (LinearizableDataReadRequest, error) {
	if request.Authorize != nil && request.ConcurrentAuthorize != nil {
		return LinearizableDataReadRequest{}, ErrInvalidOwner
	}
	if request.ConcurrentAuthorize != nil {
		concurrent := request.ConcurrentAuthorize
		request.Authorize = func(state ServingState) bool {
			return concurrent(state)
		}
		request.ConcurrentAuthorize = nil
	}
	return request, nil
}

// tryReadLinearizableDataInto attempts the warm direct read-authority path for
// one ExecutionOwners data cut (range scans, aggregations, general SQL reads).
// It captures the immutable DataReadCutInto snapshot and validates the read
// authority token before making the cut visible. Any authority unavailability,
// expiry, or source fallback falls back cleanly to a single ReadIndex round.
func (owner *Owner) tryReadLinearizableDataInto(
	ctx context.Context,
	request LinearizableDataReadRequest,
	dst *LinearizableDataReadCut,
	slot *pointReadViewSlot,
) (bool, error) {
	if owner == nil || ctx == nil || dst == nil ||
		request.Capability != serviceauthz.CapabilityDataRead ||
		request.Relations != nil && len(request.Relations) == 0 ||
		len(request.Relations) > replication.MaxRelationsPerBundle {
		return true, ErrInvalidOwner
	}
	if request.Authorize != nil && request.ConcurrentAuthorize != nil {
		return true, ErrInvalidOwner
	}
	if dst.owner != nil {
		return true, replicatedstate.ErrDataReadOpen
	}
	if request.Authorize != nil && request.ConcurrentAuthorize == nil {
		return false, nil
	}
	if slot == nil {
		return false, nil
	}
	view := slot.value.Load()
	if view == nil || view.owner != owner || view.group != request.Fence.Group ||
		view.source == nil || view.generation == nil || view.permit == nil ||
		!view.permit.valid(request.Fence, view.generation) {
		return false, nil
	}
	source, ok := view.source.(DataReadSource)
	if !ok {
		return false, nil
	}
	if err := context.Cause(ctx); err != nil {
		return true, err
	}
	if err := owner.reservePendingRead(1); err != nil {
		return true, err
	}
	retained := false
	defer func() {
		if !retained {
			owner.releasePendingRead(1)
		}
	}()
	if !view.generation.acquire() {
		return false, nil
	}
	if !view.permit.valid(request.Fence, view.generation) {
		view.generation.release()
		return false, nil
	}
	pinned := true
	defer func() {
		if pinned {
			view.generation.release()
		}
	}()
	if err := context.Cause(ctx); err != nil {
		return true, err
	}
	direct, ok := owner.host.(pointReadAdmissionHost)
	if !ok {
		return false, nil
	}
	attempted, admitted, authorized, admission, err := direct.TryReadPointAdmission(
		request.Fence.Group, view.identity, request.Fence.Term,
		func(identity raftmember.RuntimeIdentity, status raftmember.RuntimeStatus, token raftauthority.AuthorityToken) bool {
			if request.ConcurrentAuthorize == nil {
				return true
			}
			return request.ConcurrentAuthorize(ServingState{
				Identity: identity, Command: view.command, Status: status,
			})
		},
	)
	if err != nil {
		return true, err
	}
	if !attempted {
		return false, nil
	}
	if admission.AuthorityRoundAttempt {
		owner.metrics.observeAuthorityRoundAttempt(request.Fence.Group)
	}
	if err := context.Cause(ctx); err != nil {
		return true, err
	}
	if !admitted {
		return false, nil
	}
	if !authorized {
		return true, ErrServingAuthorization
	}
	if !view.permit.valid(request.Fence, view.generation) {
		return true, ErrServingFence
	}
	minimumApplied := admission.Status.Commit
	if minimumApplied == 0 {
		return true, ErrServingFence
	}
	if err := source.DataReadCutInto(request.Relations, minimumApplied, &dst.data); err != nil {
		if readAuthoritySourceFallback(err, false) {
			view.generation.release()
			pinned = false
			owner.releasePendingRead(1)
			retained = true
			return true, owner.readLinearizableDataInto(ctx, request, dst, true)
		}
		return true, err
	}
	if err := context.Cause(ctx); err != nil {
		return true, errors.Join(err, dst.data.Close())
	}
	fence := dst.data.Fence()
	if !pointReadFenceMatches(fence, request.Fence) || fence.Applied < minimumApplied {
		closeErr := dst.data.Close()
		if closeErr == nil && readAuthoritySourceFallback(nil, false) {
			view.generation.release()
			pinned = false
			owner.releasePendingRead(1)
			retained = true
			return true, owner.readLinearizableDataInto(ctx, request, dst, true)
		}
		return true, errors.Join(ErrServingFence, closeErr)
	}
	if validationErr := owner.validateReadAuthority(
		ctx, request.Fence, view.generation, view.permit, admission.Token,
	); validationErr != nil {
		retry := readAuthorityFallback(validationErr)
		owner.recordAuthorityValidation(request.Fence.Group, validationErr, retry)
		closeErr := dst.data.Close()
		if retry && closeErr == nil {
			view.generation.release()
			pinned = false
			owner.releasePendingRead(1)
			retained = true
			return true, owner.readLinearizableDataInto(ctx, request, dst, true)
		}
		return true, errors.Join(validationErr, closeErr)
	}
	owner.recordAuthorityHit(request.Fence.Group)
	dst.generation, dst.owner = view.generation, owner
	dst.source = source
	dst.state = ServingState{Identity: admission.Identity, Command: view.command, Status: admission.Status}
	pinned = false
	retained = true
	return true, nil
}

func normalizePointReadRequest(
	request LinearizablePointReadRequest,
) (LinearizablePointReadRequest, error) {
	if request.Authorize != nil && request.ConcurrentAuthorize != nil {
		return LinearizablePointReadRequest{}, ErrInvalidOwner
	}
	if request.ConcurrentAuthorize != nil {
		concurrent := request.ConcurrentAuthorize
		request.Authorize = func(state ServingState) bool {
			return concurrent(state)
		}
		request.ConcurrentAuthorize = nil
	}
	return request, nil
}

// tryReadLinearizablePointInto attempts the bounded direct admission for one
// ExecutionOwners route. It returns admitted=false when the immutable view is
// cold, revoked, unsupported, or the lane is busy; those cases deliberately
// use the existing Owner queue. Once fresh lane state has been obtained,
// authorization and lifecycle failures are terminal and are never silently
// bypassed by another direct attempt.
func (owner *Owner) tryReadLinearizablePointInto(
	ctx context.Context,
	request LinearizablePointReadRequest,
	dst *LinearizablePointReadCut,
	slot *pointReadViewSlot,
) (bool, error) {
	if owner == nil || ctx == nil || dst == nil ||
		request.Capability != serviceauthz.CapabilityDataRead {
		return true, ErrInvalidOwner
	}
	if request.Authorize != nil && request.ConcurrentAuthorize != nil {
		return true, ErrInvalidOwner
	}
	if dst.owner != nil {
		return true, replicatedstate.ErrDataReadOpen
	}
	if request.Authorize != nil && request.ConcurrentAuthorize == nil {
		// A legacy callback is explicitly serialized. Do not accidentally run it
		// concurrently merely because a warm permit exists.
		return false, nil
	}
	if slot == nil {
		return false, nil
	}
	view := slot.value.Load()
	if view == nil || view.owner != owner || view.group != request.Fence.Group ||
		view.source == nil || view.generation == nil || view.permit == nil ||
		!view.permit.valid(request.Fence, view.generation) {
		return false, nil
	}
	if err := context.Cause(ctx); err != nil {
		return true, err
	}
	if err := owner.reservePendingRead(1); err != nil {
		return true, err
	}
	retained := false
	defer func() {
		if !retained {
			owner.releasePendingRead(1)
		}
	}()
	if !view.generation.acquire() {
		return false, nil
	}
	if !view.permit.valid(request.Fence, view.generation) {
		view.generation.release()
		return false, nil
	}
	pinned := true
	defer func() {
		if pinned {
			view.generation.release()
		}
	}()
	if err := context.Cause(ctx); err != nil {
		return true, err
	}
	if !view.permit.valid(request.Fence, view.generation) {
		return false, nil
	}

	var admission multiraft.PointReadAdmission
	var authorized bool
	var attempted bool
	var admitted bool
	var err error
	if direct, ok := owner.host.(pointReadAdmissionHost); ok {
		attempted, admitted, authorized, admission, err = direct.TryReadPointAdmission(
			request.Fence.Group, view.identity, request.Fence.Term,
			func(identity raftmember.RuntimeIdentity, status raftmember.RuntimeStatus, token raftauthority.AuthorityToken) bool {
				if request.ConcurrentAuthorize == nil {
					return true
				}
				return request.ConcurrentAuthorize(ServingState{
					Identity: identity, Command: view.command, Status: status,
				})
			},
		)
	} else {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if !attempted {
		return false, nil
	}
	if admission.AuthorityRoundAttempt {
		owner.metrics.observeAuthorityRoundAttempt(request.Fence.Group)
	}
	if err := context.Cause(ctx); err != nil {
		return true, err
	}
	if !admitted {
		return false, nil
	}
	if !authorized {
		return true, ErrServingAuthorization
	}
	if !view.permit.valid(request.Fence, view.generation) {
		return true, ErrServingFence
	}
	// The request starts at the caller's floor. The fresh Runtime commit floor
	// is the authority-backed floor used by the serialized Owner path.
	minimumApplied := admission.Status.Commit
	if minimumApplied == 0 {
		return true, ErrServingFence
	}
	dst.source = view.source
	dst.fence = request.Fence
	dst.minimumApplied = minimumApplied
	dst.state = ServingState{Identity: admission.Identity, Command: view.command, Status: admission.Status}
	dst.generation = view.generation
	dst.owner = owner
	dst.request = request
	dst.authorityToken = admission.Token
	dst.authorityFast = true
	dst.authorityPermit = view.permit
	dst.released.Store(false)
	pinned = false
	retained = true
	return true, nil
}
