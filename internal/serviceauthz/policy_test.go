package serviceauthz

import (
	"context"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func authzNode(marker byte) rafttransport.NodeID {
	var node rafttransport.NodeID
	node[0] = marker
	return node
}

func TestGateConcurrentRotationNeverRegressesGeneration(t *testing.T) {
	node := authzNode(31)
	for range 100 {
		first, _ := NewPolicy(1, []Entry{{Node: node, Capabilities: CapabilityDataRead}})
		second, _ := NewPolicy(2, []Entry{{Node: node, Capabilities: CapabilityDataWrite}})
		third, _ := NewPolicy(3, []Entry{{Node: node, Capabilities: CapabilitySchema}})
		gate, _ := NewGate(first)
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		go func() { defer wait.Done(); <-start; _ = gate.Rotate(second) }()
		go func() { defer wait.Done(); <-start; _ = gate.Rotate(third) }()
		close(start)
		wait.Wait()
		if gate.Generation() != 3 || gate.Check(node, 3, CapabilitySchema) != DecisionAllow {
			t.Fatalf("publication regressed: generation=%d", gate.Generation())
		}
	}
}

func TestGateDenyDefaultRoleSeparationAndRotation(t *testing.T) {
	reader, writer, schema := authzNode(1), authzNode(2), authzNode(3)
	first, err := NewPolicy(7, []Entry{
		{Node: writer, Capabilities: CapabilityDataWrite},
		{Node: reader, Capabilities: CapabilityDataRead},
		{Node: schema, Capabilities: CapabilitySchema},
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
		{Authority{schema, 7}, CapabilitySchema, DecisionAllow},
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

func TestTransactionRecoveryCapabilityIsIndependent(t *testing.T) {
	recovery, reader, writer, topology := authzNode(20), authzNode(21),
		authzNode(22), authzNode(23)
	policy, err := NewPolicy(11, []Entry{
		{Node: recovery, Capabilities: CapabilityTransactionRecovery},
		{Node: reader, Capabilities: CapabilityDataRead},
		{Node: writer, Capabilities: CapabilityDataWrite},
		{Node: topology, Capabilities: CapabilityTopology},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []rafttransport.NodeID{reader, writer, topology} {
		if got := policy.Check(node, CapabilityTransactionRecovery); got != DecisionDenyCapability {
			t.Fatalf("ordinary capability %x implied transaction recovery: %d", node, got)
		}
	}
	if got := policy.Check(recovery, CapabilityTransactionRecovery); got != DecisionAllow {
		t.Fatalf("transaction recovery denied: %d", got)
	}
	for _, capability := range []Capability{
		CapabilityDataRead, CapabilityDataWrite, CapabilitySchema,
		CapabilityMembership, CapabilityTopology,
	} {
		if got := policy.Check(recovery, capability); got != DecisionDenyCapability {
			t.Fatalf("transaction recovery implied capability %x: %d", capability, got)
		}
	}
}

func TestRequestLedgerCapabilityIsIndependent(t *testing.T) {
	ledger, writer, topology, recovery := authzNode(30), authzNode(31),
		authzNode(32), authzNode(33)
	policy, err := NewPolicy(12, []Entry{
		{Node: ledger, Capabilities: CapabilityRequestLedger},
		{Node: writer, Capabilities: CapabilityDataWrite},
		{Node: topology, Capabilities: CapabilityTopology},
		{Node: recovery, Capabilities: CapabilityTransactionRecovery},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := policy.Check(ledger, CapabilityRequestLedger); got != DecisionAllow {
		t.Fatalf("request ledger denied: %d", got)
	}
	for _, node := range []rafttransport.NodeID{writer, topology, recovery} {
		if got := policy.Check(node, CapabilityRequestLedger); got != DecisionDenyCapability {
			t.Fatalf("ordinary capability %x implied request ledger: %d", node, got)
		}
	}
	for _, capability := range []Capability{
		CapabilityDataRead, CapabilityDataWrite, CapabilitySchema,
		CapabilityMembership, CapabilityTopology, CapabilityTransactionRecovery,
	} {
		if got := policy.Check(ledger, capability); got != DecisionDenyCapability {
			t.Fatalf("request ledger implied capability %x: %d", capability, got)
		}
	}
}

func TestExecutionPinCapabilityIsIndependentFromTopology(t *testing.T) {
	pin, topology, writer := authzNode(40), authzNode(41), authzNode(42)
	policy, err := NewPolicy(12, []Entry{
		{Node: pin, Capabilities: CapabilityExecutionPin},
		{Node: topology, Capabilities: CapabilityTopology},
		{Node: writer, Capabilities: CapabilityDataWrite},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := policy.Check(pin, CapabilityExecutionPin); got != DecisionAllow {
		t.Fatalf("execution-pin operator denied: %d", got)
	}
	for _, capability := range []Capability{
		CapabilityTopology, CapabilityDataRead, CapabilityDataWrite,
		CapabilitySchema, CapabilityMembership, CapabilityTransactionRecovery,
	} {
		if got := policy.Check(pin, capability); got != DecisionDenyCapability {
			t.Fatalf("execution-pin implied capability %x: %d", capability, got)
		}
	}
	for _, node := range []rafttransport.NodeID{topology, writer} {
		if got := policy.Check(node, CapabilityExecutionPin); got != DecisionDenyCapability {
			t.Fatalf("ordinary authority %x implied execution-pin: %d", node, got)
		}
	}
}

func TestBackupCapabilityCannotAcquireServingOrTopologyAuthority(t *testing.T) {
	backup := authzNode(31)
	policy, err := NewPolicy(9, []Entry{{Node: backup, Capabilities: CapabilityBackup}})
	if err != nil {
		t.Fatal(err)
	}
	if policy.Check(backup, CapabilityBackup) != DecisionAllow {
		t.Fatal("backup authority denied")
	}
	for _, capability := range []Capability{CapabilityDataRead, CapabilityDataWrite, CapabilitySchema,
		CapabilityDelegate, CapabilityMembership, CapabilityTopology, CapabilityTransactionRecovery,
		CapabilityRequestLedger, CapabilityExecutionPin} {
		if policy.Check(backup, capability) != DecisionDenyCapability {
			t.Fatalf("backup principal acquired capability %x", capability)
		}
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
