package splitcontroller

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type gatewayCompositionTerminalTest struct {
	calls int
}

func (terminal *gatewayCompositionTerminalTest) RetirePlan(
	context.Context, *gateway.Snapshot, *Plan, Observation,
) error {
	terminal.calls++
	return nil
}

func TestCatalogGatewaySplitActionsRefreshesAfterTerminalRetirement(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	terminal := new(gatewayCompositionTerminalTest)
	refreshCalls := 0
	wantRefreshErr := errors.New("refresh cut unavailable")
	actions := CatalogGatewaySplitActions{
		Terminal: terminal,
		Refresh: func(context.Context) error {
			refreshCalls++
			return wantRefreshErr
		},
	}
	err := actions.ExecuteGatewaySplitAction(t.Context(), plan, Observation{Catalog: catalog}, Action{Kind: ActionComplete})
	if err != nil || terminal.calls != 1 || refreshCalls != 1 {
		t.Fatalf("terminal calls=%d refresh calls=%d err=%v, want one call each and collected Complete", terminal.calls, refreshCalls, err)
	}
}

type gatewayCompositionTerminalErrorTest struct {
	calls int
	err   error
}

func (terminal *gatewayCompositionTerminalErrorTest) RetirePlan(
	context.Context, *gateway.Snapshot, *Plan, Observation,
) error {
	terminal.calls++
	return terminal.err
}

func TestCatalogGatewaySplitActionsCollectsCompleteWhenRetirementFails(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	wantRetireErr := errors.New("stale serving fence")
	terminal := &gatewayCompositionTerminalErrorTest{err: wantRetireErr}
	refreshCalls := 0
	actions := CatalogGatewaySplitActions{
		Terminal: terminal,
		Refresh: func(context.Context) error {
			refreshCalls++
			return errors.New("refresh cut unavailable")
		},
	}
	err := actions.ExecuteGatewaySplitAction(t.Context(), plan, Observation{Catalog: catalog}, Action{Kind: ActionComplete})
	if err != nil || terminal.calls != 1 || refreshCalls != 1 {
		t.Fatalf("terminal calls=%d refresh calls=%d err=%v, want collected Complete despite retirement failure", terminal.calls, refreshCalls, err)
	}
}
