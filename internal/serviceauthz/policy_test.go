package serviceauthz

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func authzNode(marker byte) rafttransport.NodeID {
	var node rafttransport.NodeID
	node[0] = marker
	return node
}

func TestGateDenyDefaultRoleSeparationAndRotation(t *testing.T) {
	reader, writer, operator := authzNode(1), authzNode(2), authzNode(3)
	first, err := NewPolicy(7, []Entry{
		{Node: writer, Capabilities: CapabilityDataWrite},
		{Node: reader, Capabilities: CapabilityDataRead},
		{Node: operator, Capabilities: CapabilityTopology | CapabilityMembership | CapabilitySplit |
			CapabilityMove | CapabilityBackup | CapabilityRestore | CapabilityOperator | CapabilitySchema},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := NewGate(first)
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		authority  Authority
		capability Capability
		want       DecisionCode
	}{
		{Authority{reader, 7}, CapabilityDataRead, DecisionAllow},
		{Authority{reader, 7}, CapabilityDataWrite, DecisionDenyCapability},
		{Authority{writer, 7}, CapabilityDataRead, DecisionDenyCapability},
		{Authority{operator, 7}, CapabilityTopology | CapabilityMove, DecisionAllow},
		{Authority{authzNode(9), 7}, CapabilityDataRead, DecisionDenyNoPrincipal},
		{Authority{reader, 6}, CapabilityDataRead, DecisionDenyGeneration},
		{Authority{}, CapabilityDataRead, DecisionDenyInvalid},
	}
	for _, check := range checks {
		if got := gate.CheckAuthority(check.authority, check.capability); got != check.want {
			t.Fatalf("CheckAuthority(%x,%d,%x)=%d want %d", check.authority.Node,
				check.authority.Generation, check.capability, got, check.want)
		}
	}
	second, err := NewPolicy(8, []Entry{{Node: reader, Capabilities: CapabilityDataWrite}})
	if err != nil || gate.Rotate(second) != nil {
		t.Fatalf("rotate err=%v", err)
	}
	if got := gate.CheckAuthority(Authority{reader, 7}, CapabilityDataRead); got != DecisionDenyGeneration {
		t.Fatalf("old generation=%d", got)
	}
	if got := gate.CheckAuthority(Authority{reader, 8}, CapabilityDataWrite); got != DecisionAllow {
		t.Fatalf("new generation=%d", got)
	}
}

func TestAuthorityContextIsExactAndAllocationFreeOnRead(t *testing.T) {
	authority := Authority{Node: authzNode(11), Generation: 19}
	ctx, err := WithAuthority(context.Background(), authority)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := FromContext(ctx); !ok || got != authority {
		t.Fatalf("authority=%+v ok=%t", got, ok)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if got, ok := FromContext(ctx); !ok || got != authority {
			panic("authority changed")
		}
	}); allocations != 0 {
		t.Fatalf("FromContext allocations=%v", allocations)
	}
}

func TestPolicyHotCheckAllocationFree(t *testing.T) {
	entries := make([]Entry, 1024)
	for index := range entries {
		entries[index] = Entry{Node: authzNode(byte(index/256 + 1)), Capabilities: CapabilityDataRead}
		entries[index].Node[1] = byte(index)
	}
	policy, err := NewPolicy(1, entries)
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := NewGate(policy)
	authority := Authority{Node: entries[731].Node, Generation: 1}
	if allocations := testing.AllocsPerRun(1000, func() {
		if gate.CheckAuthority(authority, CapabilityDataRead) != DecisionAllow {
			panic("authorization changed")
		}
	}); allocations != 0 {
		t.Fatalf("CheckAuthority allocations=%v", allocations)
	}
}
