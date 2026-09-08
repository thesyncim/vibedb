package gatewayruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

func pgMembershipTransitionError() error {
	catalog := raftservice.CommandFence{
		ReplicaSetVersion: 7, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 4, SchemaGeneration: 5, RelationManifestDigest: [32]byte{6},
		RoutingVersion: 8, RouteGeneration: 9,
	}
	observed := catalog
	observed.ReplicaSetVersion++
	return &gateway.ReplicatedMembershipTransitionError{
		CatalogCommand: catalog, ObservedCommand: observed,
	}
}

func TestPostgreSQLDirectPreparationDoesNotRestartHandledMembershipRefresh(t *testing.T) {
	attempts := 0
	service := &directPoolService{}
	service.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		attempts++
		return nil, errors.Join(
			gateway.ErrDurableSQLNotAdmitted,
			pgMembershipTransitionError(),
			gateway.ErrReplicatedLeader,
			context.DeadlineExceeded,
		)
	}
	pool := testDirectPool(t, service)
	_, err := pool.prepare(t.Context(), durableExecBatchIdentity{}, []gateway.Query{{SQL: "UPDATE docs SET n=n+1"}})
	if err == nil || attempts != 1 {
		t.Fatalf("membership refresh was retried by PG outer loop: attempts=%d err=%v", attempts, err)
	}
	if !errors.Is(err, gateway.ErrReplicatedMembershipTransition) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handled error tree was not preserved: %v", err)
	}
}

func TestPostgreSQLDirectPreparationStopsOnMembershipTransitionWithoutContext(t *testing.T) {
	attempts := 0
	service := &directPoolService{}
	service.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		attempts++
		return nil, errors.Join(
			gateway.ErrDurableSQLNotAdmitted,
			pgMembershipTransitionError(),
			gateway.ErrReplicatedLeader,
		)
	}
	pool := testDirectPool(t, service)
	_, err := pool.prepare(t.Context(), durableExecBatchIdentity{}, []gateway.Query{{SQL: "UPDATE docs SET n=n+1"}})
	if err == nil || attempts != 1 || !errors.Is(err, gateway.ErrReplicatedMembershipTransition) ||
		!errors.Is(err, gateway.ErrReplicatedLeader) {
		t.Fatalf("membership transition was retried or hidden: attempts=%d err=%v", attempts, err)
	}
}

func TestPostgreSQLDirectPreparationKeepsOrdinaryLeaderRetry(t *testing.T) {
	attempts := 0
	service := &directPoolService{}
	service.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, gateway.ErrReplicatedLeader)
		}
		return &gateway.DurableSQLDirectPlan{CatalogGeneration: 1}, nil
	}
	pool := testDirectPool(t, service)
	plan, err := pool.prepare(t.Context(), durableExecBatchIdentity{}, []gateway.Query{{SQL: "UPDATE docs SET n=n+1"}})
	if err != nil || plan == nil || attempts != 3 {
		t.Fatalf("ordinary leader retry changed: attempts=%d plan=%+v err=%v", attempts, plan, err)
	}
}

func TestPostgreSQLDirectPreparationStopsOnChildContextErrors(t *testing.T) {
	for _, terminal := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			attempts := 0
			service := &directPoolService{}
			service.prepare = func(context.Context, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
				attempts++
				return nil, errors.Join(
					gateway.ErrDurableSQLNotAdmitted,
					gateway.ErrReplicatedLeader,
					terminal.err,
				)
			}
			pool := testDirectPool(t, service)
			_, err := pool.prepare(t.Context(), durableExecBatchIdentity{}, []gateway.Query{{SQL: "UPDATE docs SET n=n+1"}})
			if err == nil || attempts != 1 || !errors.Is(err, terminal.err) {
				t.Fatalf("child %s was retried: attempts=%d err=%v", terminal.name, attempts, err)
			}
		})
	}
}
