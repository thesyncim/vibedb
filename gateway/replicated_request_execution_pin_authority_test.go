package gateway

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestDurableRequestWaveRefreshCannotBorrowAnotherLiveController(t *testing.T) {
	execution := typedExecutionFixture(t)
	_, _, route := lifecycleRunnerFixture(t)
	execution, _ = bindTypedExecutionPin(t, execution, route)
	lease := execution.ExecutionPinLease
	principal := serviceauthz.Authority{Node: rafttransport.NodeID(lease.Controller), Generation: 1}
	if !durableRequestPinControllerMatches(lease, principal) {
		t.Fatal("current controller rejected")
	}
	foreign := principal
	foreign.Node[0] ^= 0x80
	if durableRequestPinControllerMatches(lease, foreign) {
		t.Fatal("different gateway borrowed live controller at wave refresh")
	}
	principal.Generation = 0
	if durableRequestPinControllerMatches(lease, principal) {
		t.Fatal("invalid principal borrowed controller")
	}
	if durableRequestPinControllerMatches(executionpin.LeaseCertificate{}, foreign) {
		t.Fatal("invalid lease accepted")
	}
}
