package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/hotshard"
)

func TestGatewayHotShardCapacityRequiresCanonicalBoundedFile(t *testing.T) {
	var capacity autosplit.CapacityVector
	for resource := range autosplit.ResourceCount {
		capacity[resource] = 100
	}
	config := hotshard.StaticCapacityConfig{Format: hotshard.StaticCapacityFormat,
		RecorderLanes: 2, WindowCapacity: capacity, NodeCapacity: capacity,
		MigrationCapacity: 1024, ShardMigrationBytes: 512, MaxReceives: 1,
		Nodes: []hotshard.StaticCapacityNode{{Endpoint: "member-1", FailureDomain: 1}}}
	raw, err := hotshard.AppendStaticCapacityConfig(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "capacity.vibejson")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadGatewayHotShardCapacity(path); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(raw, ' '), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadGatewayHotShardCapacity(path); err == nil {
		t.Fatal("noncanonical capacity file accepted")
	}
}
