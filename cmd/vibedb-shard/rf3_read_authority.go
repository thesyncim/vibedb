package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3qualification"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

const (
	rf3ReadAuthorityFeatureVersion = uint32(1)
	rf3ReadAuthorityPolicyVersion  = uint32(1)
	rf3ReadAuthorityMaxGrant       = 5 * time.Second
	rf3ReadAuthorityClockRatePPM   = uint32(100_000)
	rf3ReadAuthorityMargin         = time.Millisecond
	rf3ReadAuthorityMarkerBytes    = 8 << 10
	rf3ReadAuthorityCacheEntries   = 4096
	rf3ReadAuthorityCacheTTL       = 15 * time.Second
	rf3ReadAuthorityProbeTimeout   = 2 * time.Second
	rf3ReadAuthorityRefreshWorkers = 8
)

var (
	errRF3ReadAuthority          = errors.New("vibedb-shard: read authority configuration refused")
	errRF3ReadAuthorityDowngrade = errors.New("vibedb-shard: read authority policy marker prevents downgrade")
	errRF3ReadAuthorityState     = errors.New("vibedb-shard: read authority policy marker is invalid")
)

func rf3NativePeerNodes(policy *serviceauthz.Policy) []rafttransport.NodeID {
	if policy == nil {
		return nil
	}
	nodes := policy.NodesWith(serviceauthz.CapabilityDelegate)
	slices.SortFunc(nodes, func(a, b rafttransport.NodeID) int { return bytes.Compare(a[:], b[:]) })
	return slices.Compact(nodes)
}

func rf3ReadAuthorityManifestEqual(left, right *rf3ManifestReadAuthority) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftRaw, leftErr := vibejson.Marshal(left)
	rightRaw, rightErr := vibejson.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

// rf3ReadAuthorityPolicy returns the core protocol policy only after the
// persisted manifest section has passed its fixed-width shape checks.
func (config *rf3ManifestReadAuthority) rf3Policy() (raftauthority.ReadAuthorityPolicy, error) {
	if config == nil {
		return raftauthority.ReadAuthorityPolicy{}, nil
	}
	if !config.Enabled || config.FeatureVersion != rf3ReadAuthorityFeatureVersion ||
		config.PolicyVersion != rf3ReadAuthorityPolicyVersion ||
		config.MaxGrantMillis == 0 || config.RoundingMarginMillis == 0 {
		return raftauthority.ReadAuthorityPolicy{}, errRF3ReadAuthority
	}
	if config.MaxGrantMillis > uint64((24*time.Hour)/time.Millisecond) ||
		config.RoundingMarginMillis > uint64((24*time.Hour)/time.Millisecond) {
		return raftauthority.ReadAuthorityPolicy{}, errRF3ReadAuthority
	}
	if config.MaxGrantMillis != uint64(rf3ReadAuthorityMaxGrant/time.Millisecond) ||
		config.ClockRatePPM != rf3ReadAuthorityClockRatePPM ||
		config.RoundingMarginMillis != uint64(rf3ReadAuthorityMargin/time.Millisecond) {
		// Feature version 1 has one qualified deployment contract. A future
		// contract must use a new policy version and an explicit migration.
		return raftauthority.ReadAuthorityPolicy{}, errRF3ReadAuthority
	}
	policy := raftauthority.ReadAuthorityPolicy{
		Enabled:        true,
		PolicyVersion:  config.PolicyVersion,
		MaxGrant:       time.Duration(config.MaxGrantMillis) * time.Millisecond,
		ClockRatePPM:   config.ClockRatePPM,
		RoundingMargin: time.Duration(config.RoundingMarginMillis) * time.Millisecond,
		Voters:         slices.Clone(config.Voters),
		Capabilities:   make([]raftauthority.VoterCapability, len(config.Capabilities)),
	}
	for index, capability := range config.Capabilities {
		policy.Capabilities[index] = raftauthority.VoterCapability{
			MemberID: capability.MemberID, PolicyVersion: capability.PolicyVersion,
			Enabled: capability.Enabled,
		}
	}
	if err := policy.Validate(); err != nil {
		return raftauthority.ReadAuthorityPolicy{}, errors.Join(errRF3ReadAuthority, err)
	}
	return policy, nil
}

func validateRF3ReadAuthority(
	config *rf3ManifestReadAuthority,
	groups []rf3ManifestGroup,
	developmentOnly bool,
) error {
	if config == nil {
		return nil
	}
	// The enabled section is admitted only by an explicitly tagged laboratory
	// binary. Keep this check in the shared manifest validator so every
	// prepare, serve, reload, and adoption path refuses the policy before it
	// can write a marker or configure a runtime.
	if !rf3qualification.ReadAuthorityEnabled {
		return errRF3ReadAuthority
	}
	policy, err := config.rf3Policy()
	if err != nil || developmentOnly || len(groups) == 0 || len(policy.Voters) != rf3ManifestMembers {
		return errRF3ReadAuthority
	}
	nativeByNode := make(map[rafttransport.NodeID]string, len(policy.Voters))
	nodeByNative := make(map[string]rafttransport.NodeID, len(policy.Voters))
	for _, group := range groups {
		if group.MemberCount != rf3ManifestMembers {
			return errRF3ReadAuthority
		}
		voters := make([]uint64, len(group.Members))
		for index, member := range group.Members[:group.MemberCount] {
			if member.MemberID == 0 || member.NodeID == (rafttransport.NodeID{}) ||
				member.StoreID == ([16]byte{}) || member.NativeAddress == "" {
				return errRF3ReadAuthority
			}
			if prior, found := nativeByNode[member.NodeID]; found && prior != member.NativeAddress {
				return errRF3ReadAuthority
			}
			if prior, found := nodeByNative[member.NativeAddress]; found && prior != member.NodeID {
				return errRF3ReadAuthority
			}
			nativeByNode[member.NodeID], nodeByNative[member.NativeAddress] = member.NativeAddress, member.NodeID
			voters[index] = member.MemberID
		}
		if !slices.Equal(voters, policy.Voters) {
			return errRF3ReadAuthority
		}
	}
	return nil
}

// rf3ReadAuthorityState is a durable downgrade/restart fence. It intentionally
// contains no wall-clock deadline: every enabled startup enters the core's
// suspend-aware quarantine again, while this marker prevents an omitted or
// changed policy from silently bypassing the old promise.
type rf3ReadAuthorityState struct {
	Enabled        bool     `json:"enabled"`
	FeatureVersion uint32   `json:"feature_version"`
	PolicyVersion  uint32   `json:"policy_version"`
	PolicyDigest   string   `json:"policy_digest"`
	Voters         []uint64 `json:"voters"`
}

func rf3ReadAuthorityMarkerPath(memberRoot string) string {
	return filepath.Join(memberRoot, "read-authority.state.vibejson")
}

func rf3ReadAuthorityStateFor(policy raftauthority.ReadAuthorityPolicy) rf3ReadAuthorityState {
	digest := policy.PolicyDigest()
	return rf3ReadAuthorityState{
		Enabled: true, FeatureVersion: rf3ReadAuthorityFeatureVersion,
		PolicyVersion: policy.PolicyVersion, PolicyDigest: hex.EncodeToString(digest[:]),
		Voters: slices.Clone(policy.Voters),
	}
}

func readRF3ReadAuthorityState(path string) (rf3ReadAuthorityState, error) {
	linkInfo, err := os.Lstat(path)
	if err != nil {
		return rf3ReadAuthorityState{}, err
	}
	if !linkInfo.Mode().IsRegular() {
		return rf3ReadAuthorityState{}, errRF3ReadAuthorityState
	}
	file, err := os.Open(path)
	if err != nil {
		return rf3ReadAuthorityState{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > rf3ReadAuthorityMarkerBytes {
		return rf3ReadAuthorityState{}, errors.Join(errRF3ReadAuthorityState, err)
	}
	raw := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, raw); err != nil {
		return rf3ReadAuthorityState{}, errors.Join(errRF3ReadAuthorityState, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return rf3ReadAuthorityState{}, errRF3ReadAuthorityState
	}
	var state rf3ReadAuthorityState
	if err := vibejson.Unmarshal(raw, &state); err != nil {
		return rf3ReadAuthorityState{}, errors.Join(errRF3ReadAuthorityState, err)
	}
	canonical, err := vibejson.Marshal(&state)
	if err != nil || !bytes.Equal(raw, canonical) || !state.Enabled ||
		state.FeatureVersion != rf3ReadAuthorityFeatureVersion || state.PolicyVersion == 0 ||
		len(state.PolicyDigest) != hex.EncodedLen(32) || len(state.Voters) == 0 {
		return rf3ReadAuthorityState{}, errRF3ReadAuthorityState
	}
	if _, err := hex.DecodeString(state.PolicyDigest); err != nil {
		return rf3ReadAuthorityState{}, errRF3ReadAuthorityState
	}
	for index, voter := range state.Voters {
		if voter == 0 || index != 0 && state.Voters[index-1] >= voter {
			return rf3ReadAuthorityState{}, errRF3ReadAuthorityState
		}
	}
	return state, nil
}

func writeRF3ReadAuthorityState(path string, state rf3ReadAuthorityState) error {
	raw, err := vibejson.Marshal(&state)
	if err != nil {
		return errors.Join(errRF3ReadAuthorityState, err)
	}
	if len(raw) > rf3ReadAuthorityMarkerBytes {
		return errRF3ReadAuthorityState
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(raw); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return err
	}
	ok = true
	return nil
}

func syncRF3ReadAuthorityState(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	directory, dirErr := os.Open(filepath.Dir(path))
	if dirErr != nil {
		return errors.Join(syncErr, closeErr, dirErr)
	}
	dirSyncErr := directory.Sync()
	dirCloseErr := directory.Close()
	return errors.Join(syncErr, closeErr, dirSyncErr, dirCloseErr)
}

func ensureRF3ReadAuthorityState(memberRoot string, policy raftauthority.ReadAuthorityPolicy) error {
	if !rf3qualification.ReadAuthorityEnabled {
		return errors.Join(errRF3ReadAuthorityState, errRF3ReadAuthority)
	}
	if !policy.Enabled {
		return errors.Join(errRF3ReadAuthorityState, errRF3ReadAuthority)
	}
	if err := policy.Validate(); err != nil {
		return errors.Join(errRF3ReadAuthorityState, err)
	}
	path := rf3ReadAuthorityMarkerPath(memberRoot)
	want := rf3ReadAuthorityStateFor(policy)
	got, err := readRF3ReadAuthorityState(path)
	if err == nil {
		if got.PolicyVersion != want.PolicyVersion || got.PolicyDigest != want.PolicyDigest ||
			!slices.Equal(got.Voters, want.Voters) {
			return errors.Join(errRF3ReadAuthorityState, errRF3ReadAuthorityDowngrade)
		}
		// A previous process may have written the marker and died before its
		// file or containing directory reached stable storage. Repair both
		// fences before Runtime can enter a new quarantine or make a grant.
		if err := syncRF3ReadAuthorityState(path); err != nil {
			return errors.Join(errRF3ReadAuthorityState, err)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeRF3ReadAuthorityState(path, want); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ensureRF3ReadAuthorityState(memberRoot, policy)
		}
		return err
	}
	return nil
}

func ensureRF3ReadAuthorityDisabled(memberRoot string) error {
	path := rf3ReadAuthorityMarkerPath(memberRoot)
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.Join(errRF3ReadAuthorityState, err)
	}
	return errRF3ReadAuthorityDowngrade
}

type rf3ReadAuthorityCacheKey struct {
	group      raftmember.GroupKey
	member     uint64
	node       rafttransport.NodeID
	store      [16]byte
	allocation uint64
}

type rf3ReadAuthorityProbeTarget struct {
	key        rf3ReadAuthorityCacheKey
	allocation uint64
	address    string
}

type rf3ReadAuthorityGroupMember struct {
	group  raftmember.GroupKey
	member uint64
}

type rf3ReadAuthorityCacheValue struct {
	incarnation uint64
	seen        time.Time
}

type rf3ReadAuthorityProbeConnection struct {
	mu      sync.Mutex
	address string
	conn    rafttransport.PeerConnection
	encoder shardservice.FrameEncoder
}

type rf3ReadAuthorityProbeResult uint8

const (
	rf3ReadAuthorityProbeSuccess rf3ReadAuthorityProbeResult = iota + 1
	rf3ReadAuthorityProbeTransportFailure
	rf3ReadAuthorityProbeGroupRefused
)

// rf3ReadAuthorityIncarnationCache is populated only by authenticated native
// ReplicatedProbe responses. Runtime callbacks are read-only and never dial or
// call an Owner; a miss simply keeps ReadIndex as the safe path.
type rf3ReadAuthorityIncarnationCache struct {
	mu          sync.RWMutex
	values      map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue
	targets     map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget
	connections map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection
	profile     *rafttransport.PeerTLS
	authority   serviceauthz.Authority
	ttl         time.Duration
	cursors     map[rafttransport.NodeID]uint32
	// probeOverride is test-only dependency injection. Production leaves it
	// nil, which selects the authenticated ReplicatedProbe implementation.
	probeOverride func(context.Context, rf3ReadAuthorityProbeTarget) bool
	// probeResultOverride and probeTimeout are test-only dependency injection.
	// Production leaves both at their zero values.
	probeResultOverride func(context.Context, rf3ReadAuthorityProbeTarget) rf3ReadAuthorityProbeResult
	probeTimeout        time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (cache *rf3ReadAuthorityIncarnationCache) probeTargetResult(
	ctx context.Context,
	target rf3ReadAuthorityProbeTarget,
) rf3ReadAuthorityProbeResult {
	if cache.probeResultOverride != nil {
		return cache.probeResultOverride(ctx, target)
	}
	if cache.probeOverride != nil {
		if cache.probeOverride(ctx, target) {
			return rf3ReadAuthorityProbeSuccess
		}
		return rf3ReadAuthorityProbeTransportFailure
	}
	return cache.probeResult(ctx, target)
}

func (cache *rf3ReadAuthorityIncarnationCache) probeTarget(
	ctx context.Context,
	target rf3ReadAuthorityProbeTarget,
) bool {
	return cache.probeTargetResult(ctx, target) == rf3ReadAuthorityProbeSuccess
}

func newRF3ReadAuthorityCache(
	profile *rafttransport.PeerTLS,
	authPolicy *serviceauthz.Policy,
	groups []preparedRF3Group,
	runtimes []*raftmember.Runtime,
	localNode rafttransport.NodeID,
) (*rf3ReadAuthorityIncarnationCache, error) {
	if profile == nil || authPolicy == nil || localNode == (rafttransport.NodeID{}) || len(groups) == 0 || len(groups) != len(runtimes) {
		return nil, errRF3ReadAuthority
	}
	cache := &rf3ReadAuthorityIncarnationCache{
		values:      make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets:     make(map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget),
		connections: make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		cursors:     make(map[rafttransport.NodeID]uint32),
		profile:     profile, authority: serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: authPolicy.Generation()},
		ttl: rf3ReadAuthorityCacheTTL,
	}
	nodeAddresses := make(map[rafttransport.NodeID]string, rf3ReadAuthorityCacheEntries)
	for index := range groups {
		if runtimes[index] == nil {
			return nil, errRF3ReadAuthority
		}
		identity := runtimes[index].Identity()
		group := identity.Group
		if groups[index].manifest.Route.Group != group ||
			groups[index].manifest.Route.AllocationGeneration != identity.AllocationGeneration ||
			identity.MemberID == 0 || identity.StoreID == ([16]byte{}) ||
			identity.AllocationGeneration == 0 || identity.NodeIncarnation == 0 {
			return nil, errRF3ReadAuthority
		}
		for _, member := range groups[index].manifest.memberRoster() {
			if member.MemberID == 0 || member.NodeID == (rafttransport.NodeID{}) || member.StoreID == ([16]byte{}) || member.NativeAddress == "" {
				return nil, errRF3ReadAuthority
			}
			if prior, found := nodeAddresses[member.NodeID]; found && prior != member.NativeAddress {
				return nil, errRF3ReadAuthority
			}
			nodeAddresses[member.NodeID] = member.NativeAddress
			target := rf3ReadAuthorityProbeTarget{
				key:        rf3ReadAuthorityCacheKey{group: group, member: member.MemberID, node: member.NodeID, store: member.StoreID, allocation: identity.AllocationGeneration},
				allocation: identity.AllocationGeneration, address: member.NativeAddress,
			}
			lookupKey := rf3ReadAuthorityGroupMember{group: group, member: member.MemberID}
			if prior, found := cache.targets[lookupKey]; found && prior != target {
				return nil, errRF3ReadAuthority
			}
			if _, found := cache.targets[lookupKey]; !found && len(cache.targets) >= rf3ReadAuthorityCacheEntries {
				return nil, errRF3ReadAuthority
			}
			cache.targets[lookupKey] = target
		}
		localKey := rf3ReadAuthorityGroupMember{group: group, member: identity.MemberID}
		localTarget, found := cache.targets[localKey]
		if !found || localTarget.key.node != localNode || localTarget.key.store != identity.StoreID {
			return nil, errRF3ReadAuthority
		}
		cache.Put(identity.Group, identity.MemberID, identity.NodeIncarnation)
	}
	return cache, nil
}

func (cache *rf3ReadAuthorityIncarnationCache) Put(group raftmember.GroupKey, member, incarnation uint64) {
	if cache == nil || member == 0 || incarnation == 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	lookupKey := rf3ReadAuthorityGroupMember{group: group, member: member}
	target, ok := cache.targets[lookupKey]
	if !ok {
		return
	}
	prior, exists := cache.values[target.key]
	if exists && incarnation < prior.incarnation {
		return
	}
	cache.values[target.key] = rf3ReadAuthorityCacheValue{incarnation: incarnation, seen: time.Now()}
}

func (cache *rf3ReadAuthorityIncarnationCache) Lookup(group raftmember.GroupKey, member uint64) (uint64, bool, error) {
	if cache == nil || member == 0 {
		return 0, false, nil
	}
	cache.mu.RLock()
	lookupKey := rf3ReadAuthorityGroupMember{group: group, member: member}
	target, ok := cache.targets[lookupKey]
	value := cache.values[target.key]
	cache.mu.RUnlock()
	if !ok || value.incarnation == 0 {
		return 0, false, nil
	}
	age := time.Since(value.seen)
	if age < 0 || age > cache.ttl {
		return 0, false, nil
	}
	return value.incarnation, true, nil
}

func (cache *rf3ReadAuthorityIncarnationCache) connection(target rf3ReadAuthorityProbeTarget) *rf3ReadAuthorityProbeConnection {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry := cache.connections[target.key.node]
	if entry != nil && entry.address != target.address {
		// One authenticated connection is shared per physical peer. A manifest
		// that gives the same NodeID two native endpoints is ambiguous; keep the
		// cache miss path safe instead of retargeting an existing TLS session.
		return nil
	}
	if entry == nil {
		entry = &rf3ReadAuthorityProbeConnection{address: target.address}
		cache.connections[target.key.node] = entry
	}
	return entry
}

func (cache *rf3ReadAuthorityIncarnationCache) probeResult(
	ctx context.Context,
	target rf3ReadAuthorityProbeTarget,
) rf3ReadAuthorityProbeResult {
	entry := cache.connection(target)
	if entry == nil {
		return rf3ReadAuthorityProbeTransportFailure
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	probeTimeout := rf3ReadAuthorityProbeTimeout
	if cache.probeTimeout > 0 {
		probeTimeout = cache.probeTimeout
	}
	if entry.conn == nil {
		dialer := net.Dialer{Timeout: probeTimeout}
		raw, err := dialer.DialContext(ctx, "tcp", target.address)
		if err != nil {
			return rf3ReadAuthorityProbeTransportFailure
		}
		deadline := func() time.Time { return time.Now().Add(probeTimeout) }
		entry.conn, err = cache.profile.Client(ctx, raw, target.key.node, rafttransport.TrafficShardNative, deadline)
		if err != nil {
			_ = raw.Close()
			entry.conn = nil
			return rf3ReadAuthorityProbeTransportFailure
		}
	}
	response, err := entry.encoder.RoundTripReplicated(ctx, entry.conn, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Authority: cache.authority,
		Capability: serviceauthz.CapabilityDataRead,
		Fence:      shardservice.ReplicatedFence{Group: target.key.group, AllocationGeneration: target.allocation},
	})
	if err != nil || response == nil {
		_ = entry.conn.Close()
		entry.conn = nil
		return rf3ReadAuthorityProbeTransportFailure
	}
	if response.Kind == shardservice.ReplicatedRefusal {
		if response.HasState && response.State.Fence.Group != target.key.group {
			_ = entry.conn.Close()
			entry.conn = nil
			return rf3ReadAuthorityProbeTransportFailure
		}
		// A refusal is an authenticated answer for this group. Keep the
		// per-node TLS session and let refresh continue with other groups.
		return rf3ReadAuthorityProbeGroupRefused
	}
	if response.Kind != shardservice.ReplicatedHandshake || !response.HasState ||
		response.State.Fence.Group != target.key.group ||
		response.State.Fence.AllocationGeneration != target.allocation ||
		response.State.Fence.MemberID != target.key.member ||
		response.State.Fence.StoreID != target.key.store || response.State.Fence.NodeIncarnation == 0 {
		_ = entry.conn.Close()
		entry.conn = nil
		return rf3ReadAuthorityProbeTransportFailure
	}
	cache.Put(target.key.group, target.key.member, response.State.Fence.NodeIncarnation)
	return rf3ReadAuthorityProbeSuccess
}

func (cache *rf3ReadAuthorityIncarnationCache) rotatePeerTargets(
	node rafttransport.NodeID,
	targets []rf3ReadAuthorityProbeTarget,
) []rf3ReadAuthorityProbeTarget {
	if len(targets) <= 1 {
		return targets
	}
	cache.mu.Lock()
	if cache.cursors == nil {
		cache.cursors = make(map[rafttransport.NodeID]uint32)
	}
	start := int(cache.cursors[node] % uint32(len(targets)))
	if _, found := cache.cursors[node]; !found && len(cache.cursors) >= rf3ReadAuthorityCacheEntries {
		// The target map is already bounded. If the cursor map ever reaches
		// that same bound, keep refresh safe and bounded even for malformed
		// callers that bypass newRF3ReadAuthorityCache.
		start = 0
	} else {
		cache.cursors[node] = uint32(start+1) % uint32(len(targets))
	}
	cache.mu.Unlock()
	rotated := make([]rf3ReadAuthorityProbeTarget, len(targets))
	copy(rotated, targets[start:])
	copy(rotated[len(targets)-start:], targets[:start])
	return rotated
}

func (cache *rf3ReadAuthorityIncarnationCache) refresh(ctx context.Context) {
	cache.mu.RLock()
	targets := make([]rf3ReadAuthorityProbeTarget, 0, len(cache.targets))
	for _, target := range cache.targets {
		targets = append(targets, target)
	}
	cache.mu.RUnlock()
	slices.SortFunc(targets, func(a, b rf3ReadAuthorityProbeTarget) int {
		if a.key.node != b.key.node {
			return bytes.Compare(a.key.node[:], b.key.node[:])
		}
		for _, groups := range [][2][16]byte{
			{a.key.group.ClusterID, b.key.group.ClusterID},
			{a.key.group.ClusterIncarnation, b.key.group.ClusterIncarnation},
			{a.key.group.ShardIncarnation, b.key.group.ShardIncarnation},
			{a.key.group.GroupID, b.key.group.GroupID},
		} {
			if comparison := bytes.Compare(groups[0][:], groups[1][:]); comparison != 0 {
				return comparison
			}
		}
		if a.key.group.TopologyRecoveryEpoch != b.key.group.TopologyRecoveryEpoch {
			if a.key.group.TopologyRecoveryEpoch < b.key.group.TopologyRecoveryEpoch {
				return -1
			}
			return 1
		}
		if a.key.member < b.key.member {
			return -1
		}
		if a.key.member > b.key.member {
			return 1
		}
		return 0
	})
	peerTargets := make(map[rafttransport.NodeID][]rf3ReadAuthorityProbeTarget)
	peerOrder := make([]rafttransport.NodeID, 0)
	for _, target := range targets {
		node := target.key.node
		if _, found := peerTargets[node]; !found {
			peerOrder = append(peerOrder, node)
		}
		peerTargets[node] = append(peerTargets[node], target)
	}
	slices.SortFunc(peerOrder, func(a, b rafttransport.NodeID) int { return bytes.Compare(a[:], b[:]) })
	work := make(chan rafttransport.NodeID)
	workers := min(rf3ReadAuthorityRefreshWorkers, len(peerOrder))
	var wait sync.WaitGroup
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			for node := range work {
				peerTargetsForRefresh := cache.rotatePeerTargets(node, peerTargets[node])
				probeTimeout := rf3ReadAuthorityProbeTimeout
				if cache.probeTimeout > 0 {
					probeTimeout = cache.probeTimeout
				}
				peerCtx, cancel := context.WithTimeout(ctx, probeTimeout)
				refusedGroups := make(map[raftmember.GroupKey]struct{})
			probeTargets:
				for _, target := range peerTargetsForRefresh {
					if _, refused := refusedGroups[target.key.group]; refused {
						continue
					}
					switch cache.probeTargetResult(peerCtx, target) {
					case rf3ReadAuthorityProbeSuccess:
					case rf3ReadAuthorityProbeGroupRefused:
						refusedGroups[target.key.group] = struct{}{}
					case rf3ReadAuthorityProbeTransportFailure:
						break probeTargets
					}
					if peerCtx.Err() != nil {
						break
					}
				}
				cancel()
			}
		}()
	}
send:
	for _, node := range peerOrder {
		select {
		case work <- node:
		case <-ctx.Done():
			break send
		}
	}
	close(work)
	wait.Wait()
}

func (cache *rf3ReadAuthorityIncarnationCache) Start(parent context.Context) {
	if cache == nil || parent == nil || cache.done != nil {
		return
	}
	cache.ctx, cache.cancel = context.WithCancel(parent)
	cache.done = make(chan struct{})
	go func() {
		defer close(cache.done)
		cache.refresh(cache.ctx)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-cache.ctx.Done():
				return
			case <-ticker.C:
				cache.refresh(cache.ctx)
			}
		}
	}()
}

func (cache *rf3ReadAuthorityIncarnationCache) Close() error {
	if cache == nil {
		return nil
	}
	if cache.cancel != nil {
		cache.cancel()
	}
	if cache.done != nil {
		<-cache.done
	}
	cache.mu.Lock()
	connections := make([]*rf3ReadAuthorityProbeConnection, 0, len(cache.connections))
	for _, entry := range cache.connections {
		connections = append(connections, entry)
	}
	cache.mu.Unlock()
	for _, entry := range connections {
		entry.mu.Lock()
		if entry.conn != nil {
			_ = entry.conn.Close()
			entry.conn = nil
		}
		entry.mu.Unlock()
	}
	return nil
}

func configureRF3ReadAuthorities(
	manifest rf3Manifest,
	prepared []preparedRF3Group,
	runtimes []*raftmember.Runtime,
	profile *rafttransport.PeerTLS,
	authPolicy *serviceauthz.Policy,
	localNode rafttransport.NodeID,
) (*rf3ReadAuthorityIncarnationCache, error) {
	if len(prepared) != len(runtimes) {
		return nil, errRF3ReadAuthority
	}
	if manifest.ReadAuthority == nil {
		for _, item := range prepared {
			if err := ensureRF3ReadAuthorityDisabled(item.manifest.Route.MemberRoot); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	policy, err := manifest.ReadAuthority.rf3Policy()
	if err != nil || manifest.DevelopmentOnly || profile == nil || authPolicy == nil ||
		authPolicy.Check(profile.LocalIdentity().Node, serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow ||
		authPolicy.Check(profile.LocalIdentity().Node, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow {
		// The bounded refresh uses the already authenticated embedded gateway
		// principal. Standalone storage-only serving has no delegated probe
		// identity and therefore keeps ReadIndex as its only read path.
		return nil, errRF3ReadAuthority
	}
	if err := validateRF3ReadAuthority(manifest.ReadAuthority, manifest.groupBundles(), manifest.DevelopmentOnly); err != nil {
		return nil, err
	}
	cache, err := newRF3ReadAuthorityCache(profile, authPolicy, prepared, runtimes, localNode)
	if err != nil {
		return nil, err
	}
	// Qualify every clock before mutating any durable policy marker. On an
	// unsupported platform an explicitly requested feature must fail before a
	// partial enrollment can be mistaken for a completed voter rollout.
	clocks := make([]*raftauthority.CheckedClock, len(runtimes))
	for index := range clocks {
		var source raftauthority.ElapsedClock
		source, err = raftauthority.NewQualifiedElapsedClock()
		if err != nil {
			_ = cache.Close()
			return nil, err
		}
		// Linux construction is intentionally cheap and the CLOCK_BOOTTIME
		// syscall occurs on Now. Exercise every source before the first marker
		// write, then reuse the initialized checked clock for Runtime startup.
		clocks[index] = raftauthority.NewCheckedClock(source)
		if _, err = clocks[index].Now(); err != nil {
			_ = cache.Close()
			return nil, err
		}
	}
	for index, runtime := range runtimes {
		item := &prepared[index]
		if err := ensureRF3ReadAuthorityState(item.manifest.Route.MemberRoot, policy); err != nil {
			_ = cache.Close()
			return nil, err
		}
		group := runtime.Identity().Group
		if err := runtime.ConfigureReadAuthority(raftmember.ReadAuthorityOptions{
			Policy: policy, Clock: clocks[index],
			LeaderIncarnation: func(memberID uint64) (uint64, bool, error) {
				return cache.Lookup(group, memberID)
			},
		}); err != nil {
			_ = cache.Close()
			return nil, err
		}
	}
	return cache, nil
}
