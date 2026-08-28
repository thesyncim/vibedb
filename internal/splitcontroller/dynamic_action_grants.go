package splitcontroller

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
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

func (grants *DynamicShardActionGrants) replace(
	operation OperationID, digest [32]byte, input []ShardActionGrant,
) error {
	if grants == nil || operation == (OperationID{}) || digest == ([32]byte{}) ||
		len(input) == 0 || len(input) > grants.limit {
		return ErrRemoteExecution
	}
	keys := make([]shardActionGrantKey, len(input))
	for index, grant := range input {
		if !validShardActionGrant(grant) || grant.Operation != operation || grant.PlanDigest != digest {
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
	remaining := len(grants.grants)
	for key := range grants.grants {
		if key.operation == operation && key.digest == digest {
			remaining--
		}
	}
	if len(keys) > grants.limit-remaining {
		return ErrRemoteExecution
	}
	for key := range grants.grants {
		if key.operation == operation && key.digest == digest {
			delete(grants.grants, key)
		}
	}
	for index, key := range keys {
		grants.grants[key] = input[index]
	}
	return nil
}

// rebindCatalog advances only the authenticated catalog witness for an exact
// live admission. Data-plane executors and their pinned stores remain the same
// objects: rebuilding them after publication could attempt to reclaim an SQL
// stage that has already transferred ownership to a serving child runtime.
func (grants *DynamicShardActionGrants) rebindCatalog(
	operation OperationID, digest [32]byte, catalog *gateway.Snapshot, admission PlanAdmission,
) error {
	if grants == nil || operation == (OperationID{}) || digest == ([32]byte{}) || catalog == nil ||
		admission.Operation != operation || admission.PlanDigest != digest ||
		catalog.Generation() != admission.CatalogGeneration {
		return ErrRemoteExecution
	}
	grants.mu.Lock()
	defer grants.mu.Unlock()
	found := false
	for key, grant := range grants.grants {
		if key.operation != operation || key.digest != digest {
			continue
		}
		grant.Admission, grant.Catalog = admission, catalog
		if !validShardActionGrant(grant) {
			return ErrRemoteExecution
		}
		grants.grants[key] = grant
		found = true
	}
	if !found {
		return ErrRemoteExecution
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
	if !found {
		// A source's exact seal is already part of its admitted plan. Reuse
		// that one executor/lease only for post-seal actions, without creating
		// a duplicate lifecycle owner or accepting arbitrary later epochs.
		for key, candidate := range grants.grants {
			if key.operation != operation || key.digest != digest || candidate.Plan == nil ||
				candidate.Actions&sourceSplitActionMask() == 0 {
				continue
			}
			sealed := candidate.Target
			sealed.Authority.OwnershipEpoch = uint64(candidate.Plan.children[candidate.Plan.retained].OwnershipEpoch)
			sealed.Authority.RoutingVersion = uint64(candidate.Plan.targetManifest.Version())
			sealed.Authority.RouteGeneration = candidate.Plan.next
			if sealed == target {
				grant, found = candidate, true
				grant.Target, grant.Actions = target, candidate.Actions&sourceSealedActionMask()
				break
			}
		}
	}
	grants.mu.RUnlock()
	return grant, found
}

func sourceSealedActionMask() uint16 {
	return actionBit(ActionCatchUpTail) | actionBit(ActionCertifyCutover) | actionBit(ActionPruneRetained)
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
		context.Context, *gateway.Snapshot, *Plan, PlanAdmission, []*RuntimeStoreLease,
	) ([]ShardActionGrant, error)
}

// AdmittedShardExecutorActivation separates construction from publication.
// Factories may prepare bounded handles, but observation/data capabilities do
// not become reachable until the binder has atomically installed every grant.
type AdmittedShardExecutorActivation interface {
	ActivateAdmittedShardExecutor() error
	AbortAdmittedShardExecutor() error
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
	closed  bool
}

type boundPlanAdmission struct {
	digest            [32]byte
	catalogGeneration uint64
	catalogDigest     [32]byte
	leases            []*RuntimeStoreLease
	registries        []*RuntimeStoreRegistry
	activations       []AdmittedShardExecutorActivation
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
	ctx context.Context, catalog *gateway.Snapshot, plan *Plan,
	admission PlanAdmission, leases []*RuntimeStoreLease,
) error {
	if binder == nil || ctx == nil || catalog == nil || plan == nil ||
		plan.OperationID() != admission.Operation ||
		len(leases) == 0 {
		return ErrRemoteExecution
	}
	binder.mu.Lock()
	defer binder.mu.Unlock()
	if binder.closed {
		return ErrRemoteExecution
	}
	if binder.active == nil {
		binder.limit = binder.grants.limit
		binder.active = make(map[OperationID]boundPlanAdmission, min(binder.limit, 64))
	}
	if current, found := binder.active[admission.Operation]; found {
		if current.digest != admission.PlanDigest ||
			admission.CatalogGeneration < current.catalogGeneration ||
			admission.CatalogGeneration > current.catalogGeneration+1 ||
			admission.CatalogGeneration == current.catalogGeneration &&
				admission.CatalogDigest != current.catalogDigest {
			return ErrRemoteExecution
		}
		if admission.CatalogGeneration == current.catalogGeneration {
			for _, lease := range leases {
				if err := lease.Release(); err != nil {
					return err
				}
			}
			return nil
		}
		if err := binder.grants.rebindCatalog(
			admission.Operation, admission.PlanDigest, catalog, admission,
		); err != nil {
			return err
		}
		binder.active[admission.Operation] = boundPlanAdmission{
			digest: admission.PlanDigest, catalogGeneration: admission.CatalogGeneration,
			catalogDigest: admission.CatalogDigest,
			leases:        append(append([]*RuntimeStoreLease(nil), current.leases...), leases...),
			registries:    mergeAdmissionRegistries(current.registries, leases),
			activations:   current.activations,
		}
		return nil
	}
	if len(binder.active) == binder.limit {
		return ErrRemoteExecution
	}
	created, err := binder.factory.BuildAdmittedShardActionGrants(ctx, catalog, plan, admission, leases)
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
		for index := range created {
			if activation, ok := created[index].Executor.(AdmittedShardExecutorActivation); ok {
				err = errors.Join(err, activation.AbortAdmittedShardExecutor())
			}
		}
		return err
	}
	for index := range created {
		activation, ok := created[index].Executor.(AdmittedShardExecutorActivation)
		if !ok {
			continue
		}
		if err = activation.ActivateAdmittedShardExecutor(); err != nil {
			binder.grants.retire(admission.Operation, admission.PlanDigest)
			for rollback := range created {
				if item, found := created[rollback].Executor.(AdmittedShardExecutorActivation); found {
					err = errors.Join(err, item.AbortAdmittedShardExecutor())
				}
			}
			return errors.Join(ErrRemoteExecution, err)
		}
	}
	binder.active[admission.Operation] = boundPlanAdmission{
		digest: admission.PlanDigest, catalogGeneration: admission.CatalogGeneration,
		catalogDigest: admission.CatalogDigest,
		leases:        append([]*RuntimeStoreLease(nil), leases...),
		registries:    mergeAdmissionRegistries(nil, leases),
		activations:   admittedExecutorActivations(created),
	}
	return nil
}

// Close revokes every memory-only action capability and releases its durable
// store pins. It does not collect operation directories: process shutdown is
// not replicated terminal authority, so replay can safely rebind them.
func (binder *BoundPlanAdmissionBinder) Close() error {
	if binder == nil {
		return nil
	}
	binder.mu.Lock()
	defer binder.mu.Unlock()
	if binder.closed {
		return nil
	}
	binder.closed = true
	var result error
	for operation, current := range binder.active {
		binder.grants.retire(operation, current.digest)
		for _, activation := range current.activations {
			result = errors.Join(result, activation.AbortAdmittedShardExecutor())
		}
		for _, lease := range current.leases {
			result = errors.Join(result, lease.Release())
		}
	}
	clear(binder.active)
	return result
}

func admittedExecutorActivations(grants []ShardActionGrant) []AdmittedShardExecutorActivation {
	result := make([]AdmittedShardExecutorActivation, 0, len(grants))
	for index := range grants {
		if activation, ok := grants[index].Executor.(AdmittedShardExecutorActivation); ok {
			result = append(result, activation)
		}
	}
	return result
}

// retire releases the store pins retained by one exact admission. Release is
// attempted for every lease and is retry-safe because RuntimeStoreLease.Release
// is idempotent. The admission stays indexed when any release reports an error,
// allowing the terminal cleanup call to settle an outcome-unknown close.
func (binder *BoundPlanAdmissionBinder) retire(
	ctx context.Context, operation OperationID, digest [32]byte,
) error {
	if binder == nil || ctx == nil || operation == (OperationID{}) || digest == ([32]byte{}) {
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
	for _, activation := range current.activations {
		releaseErr = errors.Join(releaseErr, activation.AbortAdmittedShardExecutor())
	}
	for _, lease := range current.leases {
		if lease == nil {
			releaseErr = errors.Join(releaseErr, ErrRemoteExecution)
			continue
		}
		releaseErr = errors.Join(releaseErr, lease.Release())
	}
	if releaseErr == nil {
		for _, registry := range current.registries {
			if registry == nil || registry.authority == nil {
				continue
			}
			_, releaseErr = registry.CollectTerminal(ctx, operation)
			if releaseErr != nil {
				break
			}
		}
		if releaseErr == nil {
			delete(binder.active, operation)
		}
	}
	return releaseErr
}

func (binder *BoundPlanAdmissionBinder) retireCertified(
	operation OperationID, digest [32]byte, catalogGeneration uint64,
	catalogDigest [32]byte, proof [32]byte,
) error {
	if binder == nil || operation == (OperationID{}) || digest == ([32]byte{}) ||
		catalogGeneration == 0 || catalogDigest == ([32]byte{}) || proof == ([32]byte{}) {
		return ErrRemoteExecution
	}
	binder.mu.Lock()
	defer binder.mu.Unlock()
	current, found := binder.active[operation]
	if !found {
		return nil
	}
	if current.digest != digest || current.catalogGeneration != catalogGeneration ||
		current.catalogDigest != catalogDigest {
		return ErrRemoteExecution
	}
	binder.grants.retire(operation, digest)
	var result error
	for _, activation := range current.activations {
		result = errors.Join(result, activation.AbortAdmittedShardExecutor())
	}
	for _, lease := range current.leases {
		result = errors.Join(result, lease.Release())
	}
	if result != nil {
		return result
	}
	for _, registry := range current.registries {
		if registry == nil {
			return ErrRemoteExecution
		}
		_, result = registry.CollectCertifiedTerminal(operation, proof)
		if result != nil {
			return result
		}
	}
	delete(binder.active, operation)
	return nil
}

// validateCertifiedRetirement checks the exact live admission before any
// sibling runtime capability is revoked. An absent admission is an idempotent
// replay after terminal collection; a mismatched live admission fails closed.
func (binder *BoundPlanAdmissionBinder) validateCertifiedRetirement(
	operation OperationID, digest [32]byte, catalogGeneration uint64,
	catalogDigest [32]byte, proof [32]byte,
) error {
	if binder == nil || operation == (OperationID{}) || digest == ([32]byte{}) ||
		catalogGeneration == 0 || catalogDigest == ([32]byte{}) || proof == ([32]byte{}) {
		return ErrRemoteExecution
	}
	binder.mu.Lock()
	defer binder.mu.Unlock()
	current, found := binder.active[operation]
	if !found {
		return nil
	}
	if current.digest != digest || current.catalogGeneration != catalogGeneration ||
		current.catalogDigest != catalogDigest {
		return ErrRemoteExecution
	}
	return nil
}

func mergeAdmissionRegistries(
	current []*RuntimeStoreRegistry, leases []*RuntimeStoreLease,
) []*RuntimeStoreRegistry {
	result := append([]*RuntimeStoreRegistry(nil), current...)
	for _, lease := range leases {
		if lease == nil || lease.registry == nil || slices.Contains(result, lease.registry) {
			continue
		}
		result = append(result, lease.registry)
	}
	return result
}

func validShardActionGrant(grant ShardActionGrant) bool {
	witnessed := validPlanAdmission(grant.Admission) && grant.Catalog != nil && len(grant.Leases) != 0 &&
		grant.Admission.Operation == grant.Operation && grant.Admission.PlanDigest == grant.PlanDigest &&
		grant.Catalog.Generation() == grant.Admission.CatalogGeneration
	return grant.Operation != (OperationID{}) && grant.PlanDigest != ([32]byte{}) &&
		grant.Target.valid() && grant.Plan != nil && (grant.Observer != nil || witnessed) && grant.Executor != nil &&
		grant.Actions != 0 && grant.Actions&^uint16((1<<uint(ActionComplete))-1) == 0 &&
		grant.Plan.OperationID() == grant.Operation
}
