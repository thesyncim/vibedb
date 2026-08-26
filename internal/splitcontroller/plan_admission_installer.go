package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
)

const AbsoluteMaxLocalPlanAdmissionStores = 256

// PlanAdmissionStoreResolver selects only runtime registries already pinned by
// this process's retained manifests. It must return every local replica store
// participating in the plan and must never derive paths from plan strings.
type PlanAdmissionStoreResolver interface {
	ResolveLocalPlanAdmissionStores(context.Context, *Plan) ([]*RuntimeStoreRegistry, error)
}

// PlanAdmissionBinder exposes action/observation capabilities after every
// selected local store has durably settled the exact same admission. Bind is
// idempotent and memory-only; a restart requires the catalog authority to
// replay installation before the shard accepts an action.
type PlanAdmissionBinder interface {
	BindPlanAdmission(context.Context, *Plan, PlanAdmission) error
}

type PlanAdmissionInstaller struct {
	stores PlanAdmissionStoreResolver
	binder PlanAdmissionBinder
	limit  int
}

func NewPlanAdmissionInstaller(
	stores PlanAdmissionStoreResolver,
	binder PlanAdmissionBinder,
	maxLocalStores int,
) (*PlanAdmissionInstaller, error) {
	if stores == nil || binder == nil || maxLocalStores <= 0 ||
		maxLocalStores > AbsoluteMaxLocalPlanAdmissionStores {
		return nil, ErrPlanAdmission
	}
	return &PlanAdmissionInstaller{stores: stores, binder: binder, limit: maxLocalStores}, nil
}

// Install authenticates against the exact catalog image, durably settles the
// compact witness on all local runtimes, and only then publishes executable
// capabilities. A partial disk outcome is harmless: no capability is exposed,
// and the byte-identical retry completes the remaining stores.
func (installer *PlanAdmissionInstaller) Install(
	ctx context.Context,
	catalog *gateway.Snapshot,
	admission PlanAdmission,
) error {
	if installer == nil || ctx == nil || catalog == nil || !validPlanAdmission(admission) {
		return ErrPlanAdmission
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	plan, err := admission.Open(catalog)
	if err != nil {
		return err
	}
	registries, err := installer.stores.ResolveLocalPlanAdmissionStores(ctx, plan)
	if err != nil || len(registries) == 0 || len(registries) > installer.limit {
		return errors.Join(ErrPlanAdmission, err)
	}
	for index, registry := range registries {
		if registry == nil {
			return ErrPlanAdmission
		}
		for prior := 0; prior < index; prior++ {
			if registry == registries[prior] {
				return ErrPlanAdmission
			}
		}
	}
	leases := make([]*RuntimeStoreLease, 0, len(registries))
	defer func() {
		for _, lease := range leases {
			_ = lease.Release()
		}
	}()
	for _, registry := range registries {
		lease, acquireErr := registry.Acquire(admission.Operation)
		if acquireErr != nil {
			return errors.Join(ErrPlanAdmission, acquireErr)
		}
		leases = append(leases, lease)
	}
	for _, lease := range leases {
		if err = PersistPlanAdmission(lease, admission); err != nil {
			return errors.Join(ErrPlanAdmission, err)
		}
	}
	if err = installer.binder.BindPlanAdmission(ctx, plan, admission); err != nil {
		return errors.Join(ErrPlanAdmission, err)
	}
	return nil
}
