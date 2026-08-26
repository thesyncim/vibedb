package splitcontroller

import (
	"context"
	"errors"
	"sync"
)

// DynamicShardActionGrants is the bounded serving index populated only after
// PlanAdmissionInstaller has durably settled an operation. It contains no
// historical entries loaded by directory scan; gateway replay repopulates it
// after restart before actions become reachable.
type DynamicShardActionGrants struct {
	mu     sync.RWMutex
	grants map[shardActionGrantKey]ShardActionGrant
	limit  int
}

func NewDynamicShardActionGrants(limit int) (*DynamicShardActionGrants, error) {
	if limit <= 0 || limit > AbsoluteMaxShardActionGrants {
		return nil, ErrRemoteExecution
	}
	return &DynamicShardActionGrants{
		grants: make(map[shardActionGrantKey]ShardActionGrant, min(limit, 64)), limit: limit,
	}, nil
}

// Install publishes one operation's complete local capability set atomically.
// Existing exact keys are replaced so a gateway replay can rebind freshly
// reopened local runtimes without temporarily removing authorization.
func (grants *DynamicShardActionGrants) Install(input []ShardActionGrant) error {
	if grants == nil || len(input) == 0 || len(input) > grants.limit {
		return ErrRemoteExecution
	}
	keys := make([]shardActionGrantKey, len(input))
	for index := range input {
		grant := input[index]
		if !validShardActionGrant(grant) {
			return ErrRemoteExecution
		}
		keys[index] = shardActionGrantKey{grant.Operation, grant.PlanDigest, grant.Target}
		for prior := 0; prior < index; prior++ {
			if compareShardActionGrantKey(keys[prior], keys[index]) == 0 {
				return ErrRemoteExecution
			}
		}
	}
	grants.mu.Lock()
	defer grants.mu.Unlock()
	fresh := 0
	for _, key := range keys {
		if _, found := grants.grants[key]; !found {
			fresh++
		}
	}
	if fresh > grants.limit-len(grants.grants) {
		return ErrRemoteExecution
	}
	for index, key := range keys {
		grants.grants[key] = input[index]
	}
	return nil
}

func (grants *DynamicShardActionGrants) resolve(
	operation OperationID, digest [32]byte, target ShardActionTarget,
) (ShardActionGrant, bool) {
	if grants == nil {
		return ShardActionGrant{}, false
	}
	grants.mu.RLock()
	grant, found := grants.grants[shardActionGrantKey{operation, digest, target}]
	grants.mu.RUnlock()
	return grant, found
}

// retire removes every capability for one exact admitted operation. Matching
// the plan digest prevents a stale terminal notification from revoking a
// replacement admission for the same operation identity.
func (grants *DynamicShardActionGrants) retire(operation OperationID, digest [32]byte) {
	if grants == nil || operation == (OperationID{}) || digest == ([32]byte{}) {
		return
	}
	grants.mu.Lock()
	for key := range grants.grants {
		if key.operation == operation && key.digest == digest {
			delete(grants.grants, key)
		}
	}
	grants.mu.Unlock()
}

type PlanAdmissionGrantFactory interface {
	BuildAdmittedShardActionGrants(
		context.Context, *Plan, PlanAdmission, []*RuntimeStoreLease,
	) ([]ShardActionGrant, error)
}

// BoundPlanAdmissionBinder is the production bridge from durable admission to
// the live action dispatcher. The factory receives the already-authenticated
// Plan and may bind only local manifest-owned observers/executors.
type BoundPlanAdmissionBinder struct {
	mu      sync.Mutex
	factory PlanAdmissionGrantFactory
	grants  *DynamicShardActionGrants
	active  map[OperationID]boundPlanAdmission
	limit   int
}

type boundPlanAdmission struct {
	digest [32]byte
	leases []*RuntimeStoreLease
}

func NewBoundPlanAdmissionBinder(
	factory PlanAdmissionGrantFactory,
	grants *DynamicShardActionGrants,
) (*BoundPlanAdmissionBinder, error) {
	if factory == nil || grants == nil {
		return nil, ErrRemoteExecution
	}
	return &BoundPlanAdmissionBinder{factory: factory, grants: grants}, nil
}

func (binder *BoundPlanAdmissionBinder) BindPlanAdmission(
	ctx context.Context, plan *Plan, admission PlanAdmission, leases []*RuntimeStoreLease,
) error {
	if binder == nil || ctx == nil || plan == nil || plan.OperationID() != admission.Operation ||
		len(leases) == 0 {
		return ErrRemoteExecution
	}
	binder.mu.Lock()
	defer binder.mu.Unlock()
	if binder.active == nil {
		binder.limit = binder.grants.limit
		binder.active = make(map[OperationID]boundPlanAdmission, min(binder.limit, 64))
	}
	if current, found := binder.active[admission.Operation]; found {
		if current.digest != admission.PlanDigest {
			return ErrRemoteExecution
		}
		for _, lease := range leases {
			if err := lease.Release(); err != nil {
				return err
			}
		}
		return nil
	}
	if len(binder.active) == binder.limit {
		return ErrRemoteExecution
	}
	created, err := binder.factory.BuildAdmittedShardActionGrants(ctx, plan, admission, leases)
	if err != nil || len(created) == 0 {
		return ErrRemoteExecution
	}
	for index := range created {
		grant := created[index]
		if grant.Operation != admission.Operation || grant.PlanDigest != admission.PlanDigest ||
			grant.Plan != plan {
			return ErrRemoteExecution
		}
	}
	if err = binder.grants.Install(created); err != nil {
		return err
	}
	binder.active[admission.Operation] = boundPlanAdmission{
		digest: admission.PlanDigest, leases: append([]*RuntimeStoreLease(nil), leases...),
	}
	return nil
}

// retire releases the store pins retained by one exact admission. Release is
// attempted for every lease and is retry-safe because RuntimeStoreLease.Release
// is idempotent. The admission stays indexed when any release reports an error,
// allowing the terminal cleanup call to settle an outcome-unknown close.
func (binder *BoundPlanAdmissionBinder) retire(
	operation OperationID, digest [32]byte,
) error {
	if binder == nil || operation == (OperationID{}) || digest == ([32]byte{}) {
		return ErrRemoteExecution
	}
	binder.mu.Lock()
	defer binder.mu.Unlock()
	current, found := binder.active[operation]
	if !found {
		return nil
	}
	if current.digest != digest {
		return ErrRemoteExecution
	}
	var releaseErr error
	for _, lease := range current.leases {
		if lease == nil {
			releaseErr = errors.Join(releaseErr, ErrRemoteExecution)
			continue
		}
		releaseErr = errors.Join(releaseErr, lease.Release())
	}
	if releaseErr == nil {
		delete(binder.active, operation)
	}
	return releaseErr
}

func validShardActionGrant(grant ShardActionGrant) bool {
	return grant.Operation != (OperationID{}) && grant.PlanDigest != ([32]byte{}) &&
		grant.Target.valid() && grant.Plan != nil && grant.Observer != nil && grant.Executor != nil &&
		grant.Actions != 0 && grant.Actions&^uint16((1<<uint(ActionComplete))-1) == 0 &&
		grant.Plan.OperationID() == grant.Operation
}
