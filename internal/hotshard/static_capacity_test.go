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
		MaxReceives: 2, Nodes: []StaticCapacityNode{
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
	provider := StaticCapacityProvider{Capacity: testStaticCapacity().WindowCapacity}
	pulse := autosplit.Pulse{Source: source, Sequence: 7}
	pulse.Total[autosplit.ResourceRequests] = 19
	set, demand, _, ok := provider.PressureCapacity(pulse)
	if !ok || set.WindowSequence != 7 || demand[autosplit.ResourceRequests] != 19 {
		t.Fatalf("set=%+v demand=%+v ok=%v", set, demand, ok)
	}
}
