package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
)

var ErrSplitOperationRetirement = errors.New("splitcontroller: split operation retirement rejected")

// SplitOperationTerminalAuthority certifies that the exact admitted plan is
// terminal in the replicated catalog. A local ActionComplete observation is
// deliberately insufficient authority for capability and lease retirement.
type SplitOperationTerminalAuthority interface {
	CertifySplitOperationTerminal(
		context.Context, OperationID, [sha256.Size]byte,
	) (proof [sha256.Size]byte, terminal bool, err error)
}

// TerminalSplitOperationRetirer is the single bounded-memory retirement path
// for an admitted split operation. It first obtains replicated terminal
// authority, then revokes shard capabilities and gateway routes before
// releasing retained runtime-store leases. Retrying the same request is safe.
//
// Gateway and shard processes may construct the retirer with only the indexes
// they host. At least one live index must be supplied.
type TerminalSplitOperationRetirer struct {
	mu        sync.Mutex
	authority SplitOperationTerminalAuthority
	binder    *BoundPlanAdmissionBinder
	grants    *DynamicShardActionGrants
	routes    *DynamicShardActionRoutes
}

func NewTerminalSplitOperationRetirer(
	authority SplitOperationTerminalAuthority,
	binder *BoundPlanAdmissionBinder,
	grants *DynamicShardActionGrants,
	routes *DynamicShardActionRoutes,
) (*TerminalSplitOperationRetirer, error) {
	if binder != nil {
		if grants == nil {
			grants = binder.grants
		} else if binder.grants != grants {
			return nil, ErrSplitOperationRetirement
		}
	}
	if authority == nil || binder == nil && grants == nil && routes == nil {
		return nil, ErrSplitOperationRetirement
	}
	return &TerminalSplitOperationRetirer{
		authority: authority, binder: binder, grants: grants, routes: routes,
	}, nil
}

// RetireTerminalOperation removes one exact operation admission only after an
// authenticated, nonzero terminal proof. Serialization prevents two cleanup
// attempts from interleaving lease settlement; the underlying removals and
// lease releases remain independently idempotent across outcome-unknown calls.
func (retirer *TerminalSplitOperationRetirer) RetireTerminalOperation(
	ctx context.Context, operation OperationID, digest [sha256.Size]byte,
) error {
	if retirer == nil || ctx == nil || operation == (OperationID{}) ||
		digest == ([sha256.Size]byte{}) {
		return ErrSplitOperationRetirement
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	retirer.mu.Lock()
	defer retirer.mu.Unlock()
	proof, terminal, err := retirer.authority.CertifySplitOperationTerminal(
		ctx, operation, digest,
	)
	if err != nil {
		return err
	}
	if !terminal || proof == ([sha256.Size]byte{}) {
		return ErrSplitOperationRetirement
	}
	if retirer.grants != nil {
		retirer.grants.retire(operation, digest)
	}
	if retirer.routes != nil {
		retirer.routes.retire(operation, digest)
	}
	if retirer.binder != nil {
		if err = retirer.binder.retire(operation, digest); err != nil {
			return errors.Join(ErrSplitOperationRetirement, err)
		}
	}
	return nil
}
