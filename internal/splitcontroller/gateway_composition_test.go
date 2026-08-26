package splitcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type planAdmissionNodeClientStub struct{ calls int }

func (stub *planAdmissionNodeClientStub) Install(
	context.Context, rafttransport.NodeID, *gateway.Snapshot, PlanAdmission,
) error {
	stub.calls++
	return nil
}

func TestRF3PlanAdmissionCoordinatorRejectsIncompleteSourceRosterBeforeIO(t *testing.T) {
	plan, catalog, _, _ := testPlanWithChildLeaders(t, rf3ChildLeaders())
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	client := new(planAdmissionNodeClientStub)
	coordinator, err := NewRF3PlanAdmissionCoordinator(RF3PlanAdmissionCoordinatorOptions{
		Client: client, MaxConcurrent: 3, MaxAttempts: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = coordinator.AdmitPlan(t.Context(), catalog, plan, admission); !errors.Is(err, ErrPlanAdmission) || client.calls != 0 {
		t.Fatalf("calls=%d err=%v", client.calls, err)
	}
}
