package gatewayruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

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
		{name: "unauthorized", err: gateway.ErrReplicatedUnauthorized},
		{name: "route", err: gateway.ErrReplicatedRoute},
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
