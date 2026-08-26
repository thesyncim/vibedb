package gateway

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestDurableExecutionPinSessionIdentityBindsPinAndPrincipalGeneration(t *testing.T) {
	pin := executionpin.PinID{1, 2, 3}
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	want := durableExecutionPinSessionIdentity(pin, principal)
	if want == ([16]byte{}) || durableExecutionPinSessionIdentity(pin, principal) != want {
		t.Fatal("session identity is zero or nondeterministic")
	}
	changedPin := pin
	changedPin[0]++
	changedPrincipal := principal
	changedPrincipal.Generation++
	changedNode := principal
	changedNode.Node[0]++
	if durableExecutionPinSessionIdentity(changedPin, principal) == want ||
		durableExecutionPinSessionIdentity(pin, changedPrincipal) == want ||
		durableExecutionPinSessionIdentity(pin, changedNode) == want {
		t.Fatal("session identity aliases pin or principal authority")
	}
}
