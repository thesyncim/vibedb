package servicetls

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestNodeAuthorizerOwnsUniqueExactBinaryNodes(t *testing.T) {
	first, second := rafttransport.NodeID{1}, rafttransport.NodeID{2}
	input := []rafttransport.NodeID{second, first}
	authorizer, err := NewNodeAuthorizer(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = rafttransport.NodeID{3}
	if !authorizer.allows(rafttransport.PeerIdentity{Node: first}) ||
		!authorizer.allows(rafttransport.PeerIdentity{Node: second}) ||
		authorizer.allows(rafttransport.PeerIdentity{Node: rafttransport.NodeID{3}}) {
		t.Fatal("authorizer did not retain the exact detached binary set")
	}
	for _, nodes := range [][]rafttransport.NodeID{nil, {{}}, {first, first}} {
		if _, err := NewNodeAuthorizer(nodes); !errors.Is(err, ErrInvalidProfile) {
			t.Fatalf("nodes=%v err=%v", nodes, err)
		}
	}
}

func TestNodeAuthorizationAndStatsAreAllocationFree(t *testing.T) {
	node := rafttransport.NodeID{1}
	authorizer, err := NewNodeAuthorizer([]rafttransport.NodeID{node})
	if err != nil {
		t.Fatal(err)
	}
	identity := rafttransport.PeerIdentity{Node: node}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !authorizer.allows(identity) {
			panic("identity disappeared")
		}
	}); allocations != 0 {
		t.Fatalf("authorization allocations=%v", allocations)
	}
}

func TestParseNodeIDRequiresExactNonzeroHex(t *testing.T) {
	for _, value := range []string{"", "00", "00000000000000000000000000000000", "zz000000000000000000000000000000"} {
		if _, err := ParseNodeID(value); !errors.Is(err, ErrInvalidProfile) {
			t.Fatalf("value=%q err=%v", value, err)
		}
	}
	if node, err := ParseNodeID("0102030405060708090a0b0c0d0e0f10"); err != nil || node[0] != 1 || node[15] != 16 {
		t.Fatalf("node=%x err=%v", node, err)
	}
}
