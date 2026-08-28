package membershipgrant

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

var (
	ErrRefreshConflict = errors.New("membershipgrant: replicated and runtime grants conflict")
	ErrGrantPresent    = errors.New("membershipgrant: grant remains present during revocation")
)

// Refresh installs or confirms the exact catalog grant. It never treats an
// absent catalog value as revocation authority.
func Refresh(
	ctx context.Context, source Source, sink Sink, group raftmember.GroupKey,
) (Grant, error) {
	if ctx == nil || source == nil || sink == nil || group == (raftmember.GroupKey{}) {
		return Grant{}, ErrRefreshConflict
	}
	grant, found, err := source.ReadMembershipGrant(ctx, group)
	if err != nil {
		return Grant{}, err
	}
	if !found || !grant.Valid() || grant.Group != group {
		return Grant{}, ErrRefreshConflict
	}
	current, currentFound, err := sink.CurrentTransitionGrant(group)
	if err != nil {
		return Grant{}, err
	}
	if currentFound && current != grant {
		return Grant{}, ErrRefreshConflict
	}
	if err := sink.InstallTransitionGrant(grant); err != nil {
		return Grant{}, err
	}
	confirmed, confirmedFound, err := source.ReadMembershipGrant(ctx, group)
	if err != nil {
		return Grant{}, err
	}
	if !confirmedFound || confirmed != grant {
		// Best-effort exact rollback is safe only when the runtime has reached a
		// revocable cut. The caller must fail the runtime refresh regardless.
		_ = sink.RevokeTransitionGrant(grant)
		return Grant{}, ErrRefreshConflict
	}
	return grant, nil
}

// RefreshRevocation revokes only the exact expected runtime grant after a
// linearizable catalog lookup proves its replicated record is absent.
func RefreshRevocation(
	ctx context.Context, source Source, sink Sink, expected Grant,
) error {
	if ctx == nil || source == nil || sink == nil || !expected.Valid() {
		return ErrRefreshConflict
	}
	grant, found, err := source.ReadMembershipGrant(ctx, expected.Group)
	if err != nil {
		return err
	}
	if found {
		if grant == expected {
			return ErrGrantPresent
		}
		return ErrRefreshConflict
	}
	current, currentFound, err := sink.CurrentTransitionGrant(expected.Group)
	if err != nil {
		return err
	}
	if !currentFound {
		// Restart after an already-settled revoke reconstructs no live runtime
		// grant. Catalog absence and runtime absence are exact idempotent success.
		return nil
	}
	if current != expected {
		return ErrRefreshConflict
	}
	return sink.RevokeTransitionGrant(expected)
}
