package gateway

import (
	"sync"
	"testing"
	"time"

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

func TestDurableExecutionPinSessionStripesDistributeAndOnlySameIdentitySerializes(t *testing.T) {
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	seen := make([]bool, durableExecutionPinSessionStripes)
	var distinct int
	var first, second [16]byte
	for ordinal := uint64(1); ordinal <= 8192; ordinal++ {
		var pin executionpin.PinID
		for shift := 0; shift < 8; shift++ {
			pin[shift] = byte(ordinal >> (8 * shift))
		}
		identity := durableExecutionPinSessionIdentity(pin, principal)
		stripe := durableExecutionPinSessionStripe(identity)
		if !seen[stripe] {
			seen[stripe] = true
			distinct++
			if first == ([16]byte{}) {
				first = identity
			} else if second == ([16]byte{}) {
				second = identity
			}
		}
	}
	if distinct < 3000 || durableExecutionPinSessionStripe(first) == durableExecutionPinSessionStripe(second) {
		t.Fatalf("distinct stripes = %d/%d", distinct, durableExecutionPinSessionStripes)
	}
	var stripes [durableExecutionPinSessionStripes]sync.Mutex
	stripes[durableExecutionPinSessionStripe(first)].Lock()
	acquired := make(chan struct{})
	go func() {
		stripes[durableExecutionPinSessionStripe(second)].Lock()
		close(acquired)
		stripes[durableExecutionPinSessionStripe(second)].Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("unrelated execution-pin sessions serialized")
	}
	stripes[durableExecutionPinSessionStripe(first)].Unlock()
}

func BenchmarkDurableExecutionPinSessionStripe(b *testing.B) {
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	pin := executionpin.PinID{1, 2, 3}
	identity := durableExecutionPinSessionIdentity(pin, principal)
	b.ReportAllocs()
	for range b.N {
		_ = durableExecutionPinSessionStripe(identity)
	}
}
