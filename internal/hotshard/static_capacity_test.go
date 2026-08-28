package hotshard

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
)

func testStaticCapacity() StaticCapacityConfig {
	vector := autosplit.CapacityVector{}
	for resource := range autosplit.ResourceCount {
		vector[resource] = 1000
	}
	return StaticCapacityConfig{Format: StaticCapacityFormat, RecorderLanes: 4,
		WindowCapacity: vector, NodeCapacity: vector, MigrationCapacity: 1 << 30,
		ShardMigrationBytes: 384 << 20,
		MaxReceives:         2, Nodes: []StaticCapacityNode{
			{Endpoint: "member-1", FailureDomain: 1},
			{Endpoint: "member-2", FailureDomain: 2},
		}}
}

func TestStaticCapacityConfigCanonicalAndFailClosed(t *testing.T) {
	config := testStaticCapacity()
	raw, err := AppendStaticCapacityConfig(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenStaticCapacityConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := AppendStaticCapacityConfig(nil, opened)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("canonical err=%v", err)
	}
	config.Nodes[1].Endpoint = config.Nodes[0].Endpoint
	if _, err = AppendStaticCapacityConfig(nil, config); err == nil {
		t.Fatal("duplicate endpoint capacity accepted")
	}
	if _, err = OpenStaticCapacityConfig(append(raw, ' ')); err == nil {
		t.Fatal("noncanonical capacity accepted")
	}
}

func TestStaticCapacityProviderUsesOnlyLogicalPulse(t *testing.T) {
	_, source, _ := hotCatalog(t)
	provider := StaticCapacityProvider{Capacity: testStaticCapacity().WindowCapacity, MigrationBytes: 384 << 20}
	pulse := autosplit.Pulse{Source: source, Sequence: 7}
	pulse.Total[autosplit.ResourceRequests] = 19
	set, demand, _, ok := provider.PressureCapacity(pulse)
	if !ok || set.WindowSequence != 7 || demand[autosplit.ResourceRequests] != 19 {
		t.Fatalf("set=%+v demand=%+v ok=%v", set, demand, ok)
	}
}

func TestStaticCapacityPerNodeCeilings(t *testing.T) {
	config := testStaticCapacity()
	legacy, err := AppendStaticCapacityConfig(nil, config)
	if err != nil || bytes.Contains(legacy, []byte(`"capacity"`)) {
		t.Fatalf("homogeneous format changed: %s err=%v", legacy, err)
	}
	larger := config.NodeCapacity
	for resource := range larger {
		larger[resource] *= 2
	}
	config.Nodes[1].Capacity = &larger
	raw, err := AppendStaticCapacityConfig(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenStaticCapacityConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	nodes := opened.NodeCapacities(7)
	if len(nodes) != 2 || nodes[0].Capacity != config.NodeCapacity || nodes[1].Capacity != larger ||
		nodes[0].Used != (autosplit.CapacityVector{}) || nodes[1].Used != (autosplit.CapacityVector{}) {
		t.Fatalf("provisioned ceilings or empty utilization changed: %+v", nodes)
	}
	// Returned observations do not alias the provisioning configuration.
	larger[autosplit.ResourceRequests] = 0
	if nodes[1].Capacity[autosplit.ResourceRequests] != 2000 {
		t.Fatal("capacity evidence aliases configuration")
	}
	if _, err = AppendStaticCapacityConfig(nil, config); err == nil {
		t.Fatal("partially unspecified node capacity accepted")
	}
}
