//go:build vibedb_rf3_read_authority_lab

// Protocol enrollment and refresh tests run in the explicit laboratory CI
// variant. Standard builds use rf3_read_authority_gate_default_test.go to
// assert that an enabled manifest is refused before runtime setup.
package main

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func testRF3ReadAuthorityConfig() *rf3ManifestReadAuthority {
	return &rf3ManifestReadAuthority{
		Enabled: true, FeatureVersion: rf3ReadAuthorityFeatureVersion,
		PolicyVersion:        rf3ReadAuthorityPolicyVersion,
		MaxGrantMillis:       uint64(rf3ReadAuthorityMaxGrant / time.Millisecond),
		ClockRatePPM:         rf3ReadAuthorityClockRatePPM,
		RoundingMarginMillis: uint64(rf3ReadAuthorityMargin / time.Millisecond),
		Voters:               []uint64{1, 2, 3},
		Capabilities: []rf3ManifestVoterCapability{
			{MemberID: 1, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
			{MemberID: 2, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
			{MemberID: 3, PolicyVersion: rf3ReadAuthorityPolicyVersion, Enabled: true},
		},
	}
}

func testRF3ReadAuthorityGroup() rf3ManifestGroup {
	return rf3ManifestGroup{
		Members: [rf3ManifestMembers]rf3ManifestMember{
			{MemberID: 1, NodeID: rafttransport.NodeID{1}, StoreID: [16]byte{11}, NativeAddress: "127.0.0.1:7501"},
			{MemberID: 2, NodeID: rafttransport.NodeID{2}, StoreID: [16]byte{12}, NativeAddress: "127.0.0.1:7502"},
			{MemberID: 3, NodeID: rafttransport.NodeID{3}, StoreID: [16]byte{13}, NativeAddress: "127.0.0.1:7503"},
		},
		MemberCount: rf3ManifestMembers,
	}
}

func testRF3ReadAuthorityPolicy() raftauthority.ReadAuthorityPolicy {
	config := testRF3ReadAuthorityConfig()
	policy, err := config.rf3Policy()
	if err != nil {
		panic(err)
	}
	return policy
}

func TestRF3ReadAuthorityManifestRequiresExactVoterRoster(t *testing.T) {
	config := testRF3ReadAuthorityConfig()
	group := testRF3ReadAuthorityGroup()
	if err := validateRF3ReadAuthority(config, []rf3ManifestGroup{group}, false); err != nil {
		t.Fatalf("valid RF3 policy rejected: %v", err)
	}
	for name, mutate := range map[string]func(*rf3ManifestReadAuthority, *rf3ManifestGroup){
		"development": func(_ *rf3ManifestReadAuthority, _ *rf3ManifestGroup) {},
		"voter": func(config *rf3ManifestReadAuthority, _ *rf3ManifestGroup) {
			config.Voters[2] = 4
		},
		"store": func(_ *rf3ManifestReadAuthority, group *rf3ManifestGroup) {
			group.Members[1].StoreID = [16]byte{}
		},
		"native": func(_ *rf3ManifestReadAuthority, group *rf3ManifestGroup) {
			group.Members[1].NativeAddress = ""
		},
	} {
		badConfig := *config
		badConfig.Voters = append([]uint64(nil), config.Voters...)
		badConfig.Capabilities = append([]rf3ManifestVoterCapability(nil), config.Capabilities...)
		badGroup := group
		mutate(&badConfig, &badGroup)
		developmentOnly := name == "development"
		if err := validateRF3ReadAuthority(&badConfig, []rf3ManifestGroup{badGroup}, developmentOnly); err == nil {
			t.Fatalf("%s configuration accepted", name)
		}
	}
}

func TestRF3ReadAuthorityPolicyAndMarkerAreImmutable(t *testing.T) {
	root := t.TempDir()
	policy := testRF3ReadAuthorityPolicy()
	if err := ensureRF3ReadAuthorityState(root, policy); err != nil {
		t.Fatal(err)
	}
	if err := ensureRF3ReadAuthorityState(root, policy); err != nil {
		t.Fatalf("same policy was not restartable: %v", err)
	}
	if _, err := os.Stat(rf3ReadAuthorityMarkerPath(root)); err != nil {
		t.Fatalf("policy marker missing: %v", err)
	}
	changed := policy
	changed.MaxGrant += time.Millisecond
	if err := ensureRF3ReadAuthorityState(root, changed); !errors.Is(err, errRF3ReadAuthorityDowngrade) {
		t.Fatalf("changed policy error = %v", err)
	}
	if err := ensureRF3ReadAuthorityDisabled(root); !errors.Is(err, errRF3ReadAuthorityDowngrade) {
		t.Fatalf("disabled restart error = %v", err)
	}
	if err := ensureRF3ReadAuthorityDisabled(t.TempDir()); err != nil {
		t.Fatalf("default-off member rejected without marker: %v", err)
	}
}

func TestRF3ReadAuthorityIncarnationCacheIsMonotonicBoundedAndExpiring(t *testing.T) {
	group := raftmember.GroupKey{GroupID: [16]byte{1}, TopologyRecoveryEpoch: 1}
	target := rf3ReadAuthorityProbeTarget{
		key: rf3ReadAuthorityCacheKey{
			group: group, member: 2, node: rafttransport.NodeID{2},
			store: [16]byte{12}, allocation: 7,
		},
		allocation: 7, address: "127.0.0.1:7502",
	}
	cache := &rf3ReadAuthorityIncarnationCache{
		values: make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets: map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget{
			{group: group, member: 2}: target,
		},
		connections: make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		ttl:         time.Minute,
	}
	cache.Put(group, 2, 9)
	cache.Put(group, 2, 8)
	if incarnation, ok, err := cache.Lookup(group, 2); err != nil || !ok || incarnation != 9 {
		t.Fatalf("lower incarnation replaced cache value: incarnation=%d ok=%v err=%v", incarnation, ok, err)
	}
	cache.Put(group, 2, 10)
	if incarnation, ok, _ := cache.Lookup(group, 2); !ok || incarnation != 10 {
		t.Fatalf("new incarnation not published: %d %v", incarnation, ok)
	}
	cache.mu.Lock()
	value := cache.values[target.key]
	value.seen = time.Now().Add(-2 * time.Minute)
	cache.values[target.key] = value
	cache.mu.Unlock()
	if incarnation, ok, err := cache.Lookup(group, 2); err != nil || ok || incarnation != 0 {
		t.Fatalf("expired incarnation remained usable: incarnation=%d ok=%v err=%v", incarnation, ok, err)
	}
	other := target
	other.address = "127.0.0.1:8502"
	if cache.connection(target) == nil || cache.connection(other) != nil {
		t.Fatal("cache retargeted a physical peer under the same authenticated NodeID")
	}
}

func TestRF3ReadAuthorityRefreshKeepsHealthyPeerProgress(t *testing.T) {
	group := raftmember.GroupKey{GroupID: [16]byte{2}, TopologyRecoveryEpoch: 1}
	bad := rf3ReadAuthorityProbeTarget{key: rf3ReadAuthorityCacheKey{
		group: group, member: 2, node: rafttransport.NodeID{2}, store: [16]byte{22}, allocation: 9,
	}, allocation: 9, address: "bad:7502"}
	good := rf3ReadAuthorityProbeTarget{key: rf3ReadAuthorityCacheKey{
		group: group, member: 3, node: rafttransport.NodeID{3}, store: [16]byte{23}, allocation: 9,
	}, allocation: 9, address: "good:7503"}
	cache := &rf3ReadAuthorityIncarnationCache{
		values: make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets: map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget{
			{group: group, member: 2}: bad,
			{group: group, member: 3}: good,
		},
		connections: make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		ttl:         time.Minute,
	}
	var badProbes, goodProbes atomic.Int32
	cache.probeOverride = func(_ context.Context, target rf3ReadAuthorityProbeTarget) bool {
		if target.key.node == bad.key.node {
			badProbes.Add(1)
			return false
		}
		goodProbes.Add(1)
		cache.Put(target.key.group, target.key.member, 42)
		return true
	}
	cache.refresh(context.Background())
	if badProbes.Load() == 0 || goodProbes.Load() == 0 {
		t.Fatalf("refresh probes bad/good = %d/%d, want both peers attempted", badProbes.Load(), goodProbes.Load())
	}
	if incarnation, ok, err := cache.Lookup(group, 3); err != nil || !ok || incarnation != 42 {
		t.Fatalf("healthy peer cache value = %d ok=%t err=%v, want authenticated refresh", incarnation, ok, err)
	}
}

func TestRF3ReadAuthorityRefreshSkipsPermanentlyRefusedGroup(t *testing.T) {
	refusedGroup := raftmember.GroupKey{GroupID: [16]byte{3}, TopologyRecoveryEpoch: 1}
	healthyGroup := raftmember.GroupKey{GroupID: [16]byte{4}, TopologyRecoveryEpoch: 1}
	refused := rf3ReadAuthorityProbeTarget{key: rf3ReadAuthorityCacheKey{
		group: refusedGroup, member: 1, node: rafttransport.NodeID{2}, store: [16]byte{32}, allocation: 11,
	}, allocation: 11, address: "refused:7502"}
	healthy := rf3ReadAuthorityProbeTarget{key: rf3ReadAuthorityCacheKey{
		group: healthyGroup, member: 1, node: rafttransport.NodeID{2}, store: [16]byte{33}, allocation: 11,
	}, allocation: 11, address: "healthy:7502"}
	cache := &rf3ReadAuthorityIncarnationCache{
		values: make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets: map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget{
			{group: refusedGroup, member: 1}: refused,
			{group: healthyGroup, member: 1}: healthy,
		},
		connections: make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		ttl:         time.Minute,
	}
	var refusedProbes, healthyProbes atomic.Int32
	cache.probeResultOverride = func(_ context.Context, target rf3ReadAuthorityProbeTarget) rf3ReadAuthorityProbeResult {
		if target.key.group == refusedGroup {
			refusedProbes.Add(1)
			return rf3ReadAuthorityProbeGroupRefused
		}
		healthyProbes.Add(1)
		cache.Put(target.key.group, target.key.member, 43)
		return rf3ReadAuthorityProbeSuccess
	}
	cache.refresh(context.Background())
	if refusedProbes.Load() != 1 || healthyProbes.Load() != 1 {
		t.Fatalf("refused/healthy probes = %d/%d, want one each", refusedProbes.Load(), healthyProbes.Load())
	}
	if incarnation, ok, err := cache.Lookup(healthyGroup, 1); err != nil || !ok || incarnation != 43 {
		t.Fatalf("healthy group cache value = %d ok=%t err=%v, want authenticated refresh", incarnation, ok, err)
	}
}

func TestRF3ReadAuthorityRefreshRotatesPastDeadlineLimitedPrefix(t *testing.T) {
	prefixGroup := raftmember.GroupKey{GroupID: [16]byte{5}, TopologyRecoveryEpoch: 1}
	healthyGroup := raftmember.GroupKey{GroupID: [16]byte{6}, TopologyRecoveryEpoch: 1}
	prefix := rf3ReadAuthorityProbeTarget{key: rf3ReadAuthorityCacheKey{
		group: prefixGroup, member: 1, node: rafttransport.NodeID{4}, store: [16]byte{42}, allocation: 12,
	}, allocation: 12, address: "prefix:7504"}
	healthy := rf3ReadAuthorityProbeTarget{key: rf3ReadAuthorityCacheKey{
		group: healthyGroup, member: 1, node: rafttransport.NodeID{4}, store: [16]byte{43}, allocation: 12,
	}, allocation: 12, address: "healthy:7504"}
	cache := &rf3ReadAuthorityIncarnationCache{
		values: make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets: map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget{
			{group: prefixGroup, member: 1}:  prefix,
			{group: healthyGroup, member: 1}: healthy,
		},
		connections:  make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		ttl:          time.Minute,
		probeTimeout: 5 * time.Millisecond,
	}
	var prefixProbes, healthyProbes atomic.Int32
	cache.probeResultOverride = func(ctx context.Context, target rf3ReadAuthorityProbeTarget) rf3ReadAuthorityProbeResult {
		if target.key.group == prefixGroup {
			prefixProbes.Add(1)
			<-ctx.Done()
			return rf3ReadAuthorityProbeTransportFailure
		}
		healthyProbes.Add(1)
		cache.Put(target.key.group, target.key.member, 44)
		return rf3ReadAuthorityProbeSuccess
	}
	cache.refresh(context.Background())
	if healthyProbes.Load() != 0 {
		t.Fatalf("deadline-limited first refresh reached healthy suffix: %d probes", healthyProbes.Load())
	}
	cache.refresh(context.Background())
	if prefixProbes.Load() == 0 || healthyProbes.Load() == 0 {
		t.Fatalf("prefix/healthy probes = %d/%d, want eventual suffix progress", prefixProbes.Load(), healthyProbes.Load())
	}
	if incarnation, ok, err := cache.Lookup(healthyGroup, 1); err != nil || !ok || incarnation != 44 {
		t.Fatalf("healthy group cache value = %d ok=%t err=%v, want authenticated refresh", incarnation, ok, err)
	}
}
