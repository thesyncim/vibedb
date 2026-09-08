//go:build vibedb_rf3_read_authority_lab

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func testRF3ReadAuthorityCache(localIncarnation uint64) *rf3ReadAuthorityIncarnationCache {
	return &rf3ReadAuthorityIncarnationCache{
		values:        make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets:       make(map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget),
		registrations: make(map[uint64]rf3ReadAuthorityRegistration),
		groups:        make(map[raftmember.GroupKey]uint64),
		nodeAddresses: make(map[rafttransport.NodeID]string),
		connections:   make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		localNode:     rafttransport.NodeID{1},
		ttl:           time.Minute,
	}
}

func testRF3ReadAuthorityCacheGroup(index byte, localIncarnation uint64) rf3ReadAuthorityGroupTargets {
	group := raftmember.GroupKey{GroupID: [16]byte{index}, TopologyRecoveryEpoch: 1}
	members := make([]rf3ReadAuthorityProbeTarget, 0, rf3ManifestMembers)
	for member := uint64(1); member <= rf3ManifestMembers; member++ {
		node := rafttransport.NodeID{byte(member)}
		store := [16]byte{byte(10 + member)}
		members = append(members, rf3ReadAuthorityProbeTarget{
			key: rf3ReadAuthorityCacheKey{
				group: group, member: member, node: node, store: store, allocation: 7,
			},
			allocation: 7, address: "127.0.0.1:" + strconv.FormatUint(member, 10),
		})
	}
	return rf3ReadAuthorityGroupTargets{
		group: group, allocation: 7, members: members,
		localMember: 1, localStore: [16]byte{11}, localIncarnation: localIncarnation,
	}
}

func TestRF3ReadAuthorityRegisterGroupsIsAtomicAndSeedsOnlyLocal(t *testing.T) {
	cache := testRF3ReadAuthorityCache(19)
	valid := testRF3ReadAuthorityCacheGroup(1, 19)
	invalid := testRF3ReadAuthorityCacheGroup(2, 23)
	invalid.members[1].key.store = [16]byte{}
	if _, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{valid, invalid}); err == nil {
		t.Fatal("mixed registration unexpectedly succeeded")
	}
	if len(cache.targets) != 0 || len(cache.values) != 0 || len(cache.groups) != 0 {
		t.Fatalf("failed batch changed cache: targets=%d values=%d groups=%d", len(cache.targets), len(cache.values), len(cache.groups))
	}
	registrations, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{valid})
	if err != nil || len(registrations) != 1 {
		t.Fatalf("valid registration = %#v, %v", registrations, err)
	}
	if incarnation, ok, _ := cache.Lookup(valid.group, valid.localMember); !ok || incarnation != 19 {
		t.Fatalf("local durable seed = %d/%t, want 19/true", incarnation, ok)
	}
	if incarnation, ok, _ := cache.Lookup(valid.group, 2); ok || incarnation != 0 {
		t.Fatalf("remote cache was seeded locally: %d/%t", incarnation, ok)
	}
}

func TestRF3ReadAuthorityRegistrationRejectsAddressReplacement(t *testing.T) {
	cache := testRF3ReadAuthorityCache(19)
	first := testRF3ReadAuthorityCacheGroup(1, 19)
	if _, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{first}); err != nil {
		t.Fatal(err)
	}
	replacement := testRF3ReadAuthorityCacheGroup(2, 23)
	replacement.members[1].address = "127.0.0.1:9999"
	if _, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{replacement}); err == nil {
		t.Fatal("NodeID address replacement was accepted")
	}
	if len(cache.groups) != 1 || len(cache.targets) != rf3ManifestMembers {
		t.Fatalf("rejected replacement changed cache: groups=%d targets=%d", len(cache.groups), len(cache.targets))
	}
}

func TestRF3ReadAuthorityLateProbeCannotResurrectRecreatedGroup(t *testing.T) {
	cache := testRF3ReadAuthorityCache(19)
	original := testRF3ReadAuthorityCacheGroup(3, 19)
	survivor := testRF3ReadAuthorityCacheGroup(4, 19)
	old, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{original})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{survivor}); err != nil {
		t.Fatal(err)
	}
	started := make(chan rf3ReadAuthorityProbeTarget, 1)
	release := make(chan struct{})
	var once sync.Once
	cache.probeResultOverride = func(_ context.Context, target rf3ReadAuthorityProbeTarget) rf3ReadAuthorityProbeResult {
		if target.key.group == original.group {
			once.Do(func() { started <- target })
			<-release
			cache.putProbe(target, 47)
		} else {
			cache.putProbe(target, 55)
		}
		return rf3ReadAuthorityProbeSuccess
	}
	done := make(chan struct{})
	go func() {
		cache.refresh(context.Background())
		close(done)
	}()
	stale := <-started
	if err := cache.UnregisterGroups(old); err != nil {
		t.Fatal(err)
	}
	fresh, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{original})
	if err != nil {
		t.Fatal(err)
	}
	if stale == cache.targets[rf3ReadAuthorityGroupMember{group: original.group, member: stale.key.member}] {
		t.Fatal("recreated registration reused the old probe generation")
	}
	close(release)
	<-done
	if incarnation, ok, _ := cache.Lookup(original.group, 2); ok || incarnation != 0 {
		t.Fatalf("late probe resurrected remote incarnation: %d/%t", incarnation, ok)
	}
	if incarnation, ok, _ := cache.Lookup(survivor.group, 2); !ok || incarnation != 55 {
		t.Fatalf("surviving group refresh = %d/%t, want 55/true", incarnation, ok)
	}
	if err := cache.UnregisterGroups(old); err == nil {
		t.Fatal("stale registration removed its successor")
	}
	if err := cache.UnregisterGroups(fresh); err != nil {
		t.Fatal(err)
	}
	if registration, ok := cache.RegistrationFor(survivor.group, survivor.allocation); !ok ||
		cache.UnregisterGroups([]rf3ReadAuthorityRegistration{registration}) != nil {
		t.Fatal("surviving registration did not cleanly unregister")
	}
}

func TestRF3ReadAuthorityRegisterGroupsHonorsTargetBound(t *testing.T) {
	cache := testRF3ReadAuthorityCache(19)
	groups := make([]rf3ReadAuthorityGroupTargets, 0, 1366)
	for index := 1; index <= 1366; index++ {
		group := testRF3ReadAuthorityCacheGroup(byte(index), 19)
		binary.LittleEndian.PutUint64(group.group.GroupID[8:], uint64(index))
		for member := range group.members {
			group.members[member].key.group = group.group
		}
		groups = append(groups, group)
	}
	if _, err := cache.RegisterGroups(groups); !errors.Is(err, errRF3ReadAuthority) {
		t.Fatalf("oversized registration error = %v", err)
	}
	if len(cache.targets) != 0 || len(cache.groups) != 0 {
		t.Fatalf("oversized registration changed cache: targets=%d groups=%d", len(cache.targets), len(cache.groups))
	}
}

func TestRF3ReadAuthorityClosedCacheFailsClosed(t *testing.T) {
	cache := testRF3ReadAuthorityCache(19)
	group := testRF3ReadAuthorityCacheGroup(5, 19)
	registrations, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{group})
	if err != nil || len(registrations) != 1 {
		t.Fatalf("registration = %#v, %v", registrations, err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if incarnation, ok, err := cache.Lookup(group.group, group.localMember); err != nil || ok || incarnation != 0 {
		t.Fatalf("closed local lookup = %d/%t/%v, want 0/false/nil", incarnation, ok, err)
	}
	if registration, ok := cache.RegistrationFor(group.group, group.allocation); ok || registration != (rf3ReadAuthorityRegistration{}) {
		t.Fatalf("closed registration lookup = %#v/%t", registration, ok)
	}
	if _, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{group}); err == nil {
		t.Fatal("closed cache accepted a new registration")
	}
}

func TestRF3ReadAuthorityRetirementRemovesExactRegistration(t *testing.T) {
	cache := testRF3ReadAuthorityCache(19)
	group := testRF3ReadAuthorityCacheGroup(6, 19)
	registrations, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{group})
	if err != nil || len(registrations) != 1 {
		t.Fatalf("registration = %#v, %v", registrations, err)
	}
	if !cache.putProbe(group.members[1], 42) {
		t.Fatal("remote probe did not publish for registered target")
	}
	if err := cache.UnregisterGroups(registrations); err != nil {
		t.Fatal(err)
	}
	if registration, ok := cache.RegistrationFor(group.group, group.allocation); ok || registration != (rf3ReadAuthorityRegistration{}) {
		t.Fatalf("retired registration remains addressable: %#v/%t", registration, ok)
	}
	if incarnation, ok, _ := cache.Lookup(group.group, group.localMember); ok || incarnation != 0 {
		t.Fatalf("retired local value remains addressable: %d/%t", incarnation, ok)
	}
	if len(cache.targets) != 0 || len(cache.values) != 0 || len(cache.groups) != 0 || len(cache.registrations) != 0 {
		t.Fatalf("retirement retained cache state: targets=%d values=%d groups=%d registrations=%d", len(cache.targets), len(cache.values), len(cache.groups), len(cache.registrations))
	}
}

func TestRF3ReadAuthorityReloadAllowsOnlySamePolicySuffix(t *testing.T) {
	policy := testRF3ReadAuthorityConfig()
	group := func(index byte) rf3ManifestGroup {
		result := testRF3ReadAuthorityGroup()
		result.Route = rf3ManifestGroupRoute{
			Group: raftmember.GroupKey{
				ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
				TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3},
				GroupID: [16]byte{index},
			},
			Distribution: "distribution", Shard: "shard", AllocationGeneration: 7,
			MemberID: 1, StoreID: [16]byte{11}, MemberRoot: "/tmp/member-" + strconv.Itoa(int(index)),
		}
		result.WAL.Path = "/tmp/group-" + strconv.Itoa(int(index)) + ".wal"
		result.SQL.Path = "/tmp/group-" + strconv.Itoa(int(index)) + ".sql"
		for member := range result.Members {
			result.Members[member].PeerAddress = "127.0.0.1:75" + strconv.Itoa(member+1)
		}
		return result
	}
	first, second := group(1), group(2)
	current := rf3Manifest{ReadAuthority: policy, Groups: []rf3ManifestGroup{first}}
	next := current
	next.Groups = []rf3ManifestGroup{first, second}
	if err := validateRF3GroupTransition(current, next); err != nil {
		t.Fatalf("same-policy authority suffix rejected: %v", err)
	}
	if err := validateRF3GroupAppend(current, next); err != nil {
		t.Fatalf("same-policy authority append rejected: %v", err)
	}
	changed := *policy
	changed.Voters = append([]uint64(nil), policy.Voters...)
	changed.Capabilities = append([]rf3ManifestVoterCapability(nil), policy.Capabilities...)
	changed.MaxGrantMillis++
	bad := next
	bad.ReadAuthority = &changed
	if err := validateRF3GroupTransition(current, bad); err == nil {
		t.Fatal("changed read-authority policy accepted during reload")
	}
}
