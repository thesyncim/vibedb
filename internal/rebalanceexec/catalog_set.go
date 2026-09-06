package rebalanceexec

import (
	"context"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/rebalance"
)

var ErrAwaitMoveSet = errors.New("rebalanceexec: awaiting sibling catalog publication proofs")

type catalogSetAuthority interface {
	PublishReplicaReplacementSet(context.Context, uint64, *gateway.Snapshot, []membershipgrant.Grant, bool) error
}

// executeCatalogSet discovers siblings from the replicated directory, never
// a process-local batch. All targets keep their ordinary per-group Raft
// and action proofs; only the shared catalog publication is combined.
func (executor *Executor) executeCatalogSet(ctx context.Context, operation rebalance.OperationID, plan *rebalance.Plan, postRemove bool) (bool, error) {
	if executor.options.Directory == nil || executor.options.Observer == nil {
		return false, nil
	}
	authority, ok := executor.options.Catalog.(catalogSetAuthority)
	if !ok {
		return true, ErrExecutorConfig
	}
	ids, err := executor.options.Directory.ReadOperationIDs(ctx)
	if err != nil {
		return true, err
	}
	var records []gateway.ReplicatedOperationRecord
	contains := false
	for _, id := range ids {
		record, err := executor.options.Directory.ReadOperation(ctx, id)
		if errors.Is(err, gateway.ErrReplicatedOperationMissing) {
			continue
		}
		if err != nil {
			return true, err
		}
		if record.Kind != gateway.ReplicatedOperationMove {
			continue
		}
		identity, err := rebalance.InspectReplicaMoveIntent(record.Intent)
		if err != nil {
			return true, err
		}
		if identity.SourceGeneration != plan.CatalogGeneration() {
			continue
		}
		records = append(records, record)
		contains = contains || record.ID == [32]byte(operation)
	}
	if !contains {
		return true, ErrExecutionFence
	}
	if len(records) == 1 {
		return false, nil
	}
	want := rebalance.ActionPublishCatalog
	if postRemove {
		want = rebalance.ActionRefreshCatalogFence
	}
	changes := make([]gateway.ReplicaReplacementChange, 0, len(records))
	grants := make([]membershipgrant.Grant, 0, len(records))
	var current *gateway.Snapshot
	for _, record := range records {
		id := rebalance.OperationID(record.ID)
		cut, err := executor.options.Observer.ObserveReplicaMove(ctx, id, record, nil)
		if err != nil {
			return true, err
		}
		sibling, err := rebalance.OpenReplicaMoveIntent(record.Intent, cut.Catalog, cut.Publication, cut.SnapshotBase)
		if err != nil {
			return true, err
		}
		action, err := rebalance.Reconcile(sibling, cut.Observation)
		if err != nil {
			return true, err
		}
		if action.Kind != want {
			return true, fmt.Errorf("%w: group=%x action=%s expected=%s", ErrAwaitMoveSet, sibling.Group().GroupID, action.Kind, want)
		}
		execution, ok := rebalance.OpenReplicatedMoveExecution(record, sibling)
		if !ok || execution.Action != action || cut.Publication.ReplicaSetVersion != execution.PublicationReplicaSet || cut.Publication.Applied < execution.PublicationApplied {
			return true, fmt.Errorf("%w: group=%x execution_valid=%t action=%v observed_action=%v replica_set=%d/%d applied=%d/%d", ErrAwaitMoveSet, sibling.Group().GroupID, ok, execution.Action, action, execution.PublicationReplicaSet, cut.Publication.ReplicaSetVersion, execution.PublicationApplied, cut.Publication.Applied)
		}
		grant, found, err := executor.options.Grants.ReadMembershipGrant(ctx, sibling.Group())
		if err != nil || !found {
			return true, errors.Join(err, ErrGrantUnavailable)
		}
		if err = validateGrant(sibling, grant); err != nil {
			return true, err
		}
		route, err := executor.resolve(ctx, id, sibling, execution)
		if err != nil {
			return true, err
		}
		if route.Catalog == nil || route.Command.ReplicaSetVersion != execution.PublicationReplicaSet {
			return true, ErrExecutionFence
		}
		if current != nil {
			before, err := gateway.CatalogSnapshotDigest(current)
			after, otherErr := gateway.CatalogSnapshotDigest(route.Catalog)
			if err != nil || otherErr != nil || before != after {
				return true, errors.Join(err, otherErr, ErrExecutionFence)
			}
		}
		current = route.Catalog
		changes = append(changes, gateway.ReplicaReplacementChange{Grant: grant, Manifest: sibling.TargetManifest(), Command: route.Command,
			Target: gateway.ReplicatedReplicaDescriptor{Member: route.Target.Member, Node: route.Target.Node, StoreID: route.Target.StoreID, NodeIncarnation: route.Target.NodeIncarnation,
				Endpoint: distribution.EndpointID(route.Target.Endpoint), NativeEndpoint: distribution.EndpointID(route.Target.NativeEndpoint), ControlEndpoint: distribution.EndpointID(route.Target.ControlEndpoint)}})
		grants = append(grants, grant)
	}
	next, err := gateway.BuildReplicaReplacementSetTransition(current, changes, postRemove)
	if err != nil {
		return true, err
	}
	err = authority.PublishReplicaReplacementSet(ctx, current.Generation(), next, grants, postRemove)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		err = executor.options.Catalog.RetryPending(ctx)
	}
	return true, err
}
