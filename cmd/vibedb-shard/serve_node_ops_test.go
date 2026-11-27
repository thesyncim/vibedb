package main

import (
	"testing"
	"time"
)

func TestServeNodeRejectsNegativeOpsDurations(t *testing.T) {
	if code := runServeNode([]string{"-manifest", "/nonexistent", "-frontend-drain-grace-period", "-1s"}); code != 2 {
		t.Fatalf("negative drain grace = %d, want 2", code)
	}
	if code := runServeNode([]string{"-manifest", "/nonexistent", "-slow-query-threshold", "-5ms"}); code != 2 {
		t.Fatalf("negative slow-query threshold = %d, want 2", code)
	}
}

func TestRF3ReloadIgnoresCLIOnlyRuntimeOps(t *testing.T) {
	base, err := parseRF3Manifest([]byte(canonicalRF3Manifest))
	if err != nil {
		t.Fatal(err)
	}
	peerNode := base.groupBundles()[0].Members[0].NodeID
	gw := &rf3ManifestGateway{
		ListenAddress: "127.0.0.1:0",
		ShardPeers: []rf3ManifestGatewayPeer{
			{NodeID: hexEncodeNodeID(peerNode), Address: base.Listeners.Native},
		},
	}
	// Populate all three group members in ShardPeers so rf3EmbeddedGatewayPeers passes.
	gw.ShardPeers = gw.ShardPeers[:0]
	for i, member := range base.groupBundles()[0].Members {
		gw.ShardPeers = append(gw.ShardPeers, rf3ManifestGatewayPeer{
			NodeID:  hexEncodeNodeID(member.NodeID),
			Address: "127.0.0.1:" + string(rune('1'+i)) + "9000",
		})
	}
	currentGW := *gw
	currentGW.RuntimeOps = rf3ServeNodeRuntimeOps{
		MetricsHTTPListenAddress: "127.0.0.1:0",
		FrontendDrainGracePeriod: 5 * time.Second,
		MaxMemoryBytes:           1 << 30,
		MinFreeDiskBytes:         1 << 28,
		DiskWatermarkPath:        "/tmp",
		SlowQueryThreshold:       250 * time.Millisecond,
		ControllerLeaseDir:       "/tmp/leases",
		ControllerLeaseKey:       "vibedb/controller/primary",
		ControllerLeaseTTL:       10 * time.Second,
	}
	nextGW := *gw // RuntimeOps is zero as loaded from disk JSON.

	current := base
	current.Gateway = &currentGW
	next := base
	next.Gateway = &nextGW
	group := base.groupBundles()[0]
	group.Route.Group.GroupID[0]++
	group.WAL.Path += "-new"
	group.SQL.Path += "-new"
	next.Groups = append(base.groupBundles(), group)

	if err := validateRF3GroupAppend(current, next); err != nil {
		t.Fatalf("validateRF3GroupAppend rejected CLI RuntimeOps: %v", err)
	}
	if err := validateRF3GroupTransition(current, next); err != nil {
		t.Fatalf("validateRF3GroupTransition rejected CLI RuntimeOps: %v", err)
	}
	if current.Gateway.RuntimeOps.ControllerLeaseKey != "vibedb/controller/primary" {
		t.Fatal("validate mutated current.Gateway.RuntimeOps")
	}
}

func hexEncodeNodeID(id [16]byte) string {
	const hexDigits = "0123456789abcdef"
	var out [32]byte
	for i, b := range id {
		out[i*2] = hexDigits[b>>4]
		out[i*2+1] = hexDigits[b&0x0f]
	}
	return string(out[:])
}
