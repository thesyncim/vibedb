package raftservice

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// readAuthorityOwnerHost is deliberately optional. An owner backed by a Host
// that has no configured authority policy keeps the established ReadIndex
// behavior, and existing narrow Host test doubles do not need protocol state.
type readAuthorityOwnerHost interface {
	StartReadAuthorityRound(raftmember.GroupKey) error
	ReadAuthorityToken(raftmember.GroupKey) (raftauthority.AuthorityToken, error)
	ValidateReadAuthorityToken(raftmember.GroupKey, raftauthority.AuthorityToken) error
}

// readAuthorityEnsureHost is the newer bounded renewal hook. Hosts that
// implement it decide from the checked elapsed clock when one current token is
// close enough to expiry to begin its single overlapping renewal. The owner
// merely offers the hook once per admitted read; no owner-side wall clock is
// sampled and ErrRoundActive remains benign.
type readAuthorityEnsureHost interface {
	EnsureReadAuthorityRound(raftmember.GroupKey) error
}

// tryReadAuthority captures the token and commit floor in the same serialized
// owner turn. Failure to obtain a usable token is an expected availability
// result: ensure one bounded round and let the caller use ReadIndex.
func (owner *Owner) tryReadAuthority(
	group raftmember.GroupKey,
	minimumApplied uint64,
	status raftmember.RuntimeStatus,
	member ownerMember,
	serving ServingState,
) (readAuthorization, bool, error, bool) {
	host, ok := owner.host.(readAuthorityOwnerHost)
	if !ok {
		return readAuthorization{}, false, nil, false
	}
	token, err := host.ReadAuthorityToken(group)
	if err != nil {
		// StartReadAuthorityRound only changes serialized Runtime state and
		// wakes the owner. It never performs network or disk work here. An
		// active/disabled/unavailable round is handled by the ReadIndex path.
		owner.metrics.observeAuthorityRoundAttempt(group)
		if ensureHost, ensureOK := owner.host.(readAuthorityEnsureHost); ensureOK {
			_ = ensureHost.EnsureReadAuthorityRound(group)
		} else {
			_ = host.StartReadAuthorityRound(group)
		}
		return readAuthorization{}, false, nil, true
	}
	if ensureHost, ensureOK := owner.host.(readAuthorityEnsureHost); ensureOK {
		// The hook is intentionally advisory. Its implementation owns the
		// checked clock and only starts a bounded renewal when due.
		owner.metrics.observeAuthorityRoundAttempt(group)
		_ = ensureHost.EnsureReadAuthorityRound(group)
	}
	if !member.generation.acquire() {
		return readAuthorization{}, true, ErrServingFence, true
	}
	if status.Commit > minimumApplied {
		minimumApplied = status.Commit
	}
	return readAuthorization{
		source: member.read, minimumApplied: minimumApplied,
		generation: member.generation, authorityToken: token,
		authorityFast: true, state: serving,
	}, true, nil, true
}

// validateReadAuthority re-enters the serialized owner after a source has
// captured its immutable result. The original serving fence and generation
// are carried through the request so command-fence or lifecycle changes in
// the same generation cannot pair old bytes with a new serving cut. This
// final check is the only point at which a fast-path result may become
// caller-visible.
func (owner *Owner) validateReadAuthority(
	ctx context.Context, fence ServingFence, generation *ownerGeneration,
	token raftauthority.AuthorityToken,
) error {
	if owner == nil || ctx == nil || generation == nil {
		return ErrInvalidOwner
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	request := ownerRequest{
		kind: requestReadAuthorityValidate, group: fence.Group, fence: fence,
		reply: make(chan ownerReply, 1), authorityToken: token,
		authorityGeneration: generation,
	}
	_, err := owner.enqueue(ctx, request)
	return err
}

// readAuthorityFallback reports failures that invalidate the capability or
// make the authority unavailable. Lifecycle and bounded-ingress failures must
// be returned directly instead of issuing another request that cannot be
// admitted.
func readAuthorityFallback(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrOwnerClosed) ||
		errors.Is(err, ErrIngressFull) || errors.Is(err, ErrInvalidOwner) {
		return false
	}
	return true
}

func readAuthoritySourceFallback(err error, fenceOK bool) bool {
	if fenceOK {
		return false
	}
	return err == nil || errors.Is(err, ErrServingFence) ||
		errors.Is(err, replicatedstate.ErrReadBehind)
}

func (owner *Owner) recordAuthorityValidation(
	group raftmember.GroupKey, err error, retry bool,
) {
	if owner == nil || err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrOwnerClosed) ||
		errors.Is(err, ErrIngressFull) || errors.Is(err, ErrInvalidOwner) {
		return
	}
	owner.metrics.observeAuthorityReadValidationFailure(group)
	if retry {
		owner.metrics.observeAuthorityReadValidationRetry(group)
	}
}

func (owner *Owner) recordAuthorityHit(group raftmember.GroupKey) {
	if owner != nil {
		owner.metrics.observeAuthorityReadHit(group)
	}
}

func (owner *Owner) enqueuePointRead(
	ctx context.Context, request PointReadRequest, key []byte, forceReadIndex bool,
) (ownerReply, error) {
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	return owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadLinear, group: request.Fence.Group, reply: delivery.reply,
		bytes: int64(cap(key)), read: readRequest{
			fence: request.Fence, minimumApplied: request.MinimumApplied,
			delivery: delivery, authorize: request.Authorize,
			authorityEligible: request.Capability == serviceauthz.CapabilityDataRead && !forceReadIndex,
			forceReadIndex:    forceReadIndex,
			authorityFallback: forceReadIndex,
		},
	}, delivery)
}

func (owner *Owner) enqueuePointBatchRead(
	ctx context.Context, request PointReadBatchRequest, packed []byte, forceReadIndex bool,
) (ownerReply, error) {
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	return owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadLinear, group: request.Fence.Group, reply: delivery.reply,
		bytes: int64(cap(packed)), read: readRequest{
			fence: request.Fence, minimumApplied: request.MinimumApplied,
			delivery: delivery, authorize: request.Authorize,
			authorityEligible: request.Capability == serviceauthz.CapabilityDataRead && !forceReadIndex,
			forceReadIndex:    forceReadIndex,
			authorityFallback: forceReadIndex,
		},
	}, delivery)
}
