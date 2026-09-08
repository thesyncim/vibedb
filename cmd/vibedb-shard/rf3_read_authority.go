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

// inspectRF3ReadAuthorityState validates and repairs an existing marker without
// creating one. The returned bit is the durable pre-startup fact used to
// distinguish a previously qualified policy from a marker that this startup
// may create after its publication/roster preflight.
func inspectRF3ReadAuthorityState(memberRoot string, policy raftauthority.ReadAuthorityPolicy) (bool, error) {
	if !policy.Enabled {
		return false, errors.Join(errRF3ReadAuthorityState, errRF3ReadAuthority)
	}
	if err := policy.Validate(); err != nil {
		return false, errors.Join(errRF3ReadAuthorityState, err)
	}
	path := rf3ReadAuthorityMarkerPath(memberRoot)
	want := rf3ReadAuthorityStateFor(policy)
	got, err := readRF3ReadAuthorityState(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if got.PolicyVersion != want.PolicyVersion || got.PolicyDigest != want.PolicyDigest ||
		!slices.Equal(got.Voters, want.Voters) {
		return false, errors.Join(errRF3ReadAuthorityState, errRF3ReadAuthorityDowngrade)
	}
	// A previous process may have written the marker and died before its file
	// or containing directory reached stable storage. Repair both fences before
	// Runtime can enter a new quarantine or make a grant.
	if err := syncRF3ReadAuthorityState(path); err != nil {
		return false, errors.Join(errRF3ReadAuthorityState, err)
	}
	return true, nil
}

func ensureRF3ReadAuthorityState(memberRoot string, policy raftauthority.ReadAuthorityPolicy) error {
	exists, err := inspectRF3ReadAuthorityState(memberRoot, policy)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	path := rf3ReadAuthorityMarkerPath(memberRoot)
	want := rf3ReadAuthorityStateFor(policy)
	if err := writeRF3ReadAuthorityState(path, want); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ensureRF3ReadAuthorityState(memberRoot, policy)
		}
		return err
	}
	return nil
}

// preflightRF3ReadAuthorityRoster obtains one read-only publication cut before
// a marker can be created. It returns whether the current non-joint voter set
// is the exact persisted policy roster. A changed set is restorable only when
// the caller has already found the exact durable marker and the local runtime
// remains an enrolled voter in the retained policy. Pending replay is allowed
// here so RestoreReadAuthority can install quarantine before that replay; its
// startup-pristine Node check and authority observation keep it fail-closed.
func preflightRF3ReadAuthorityRoster(
	runtime *raftmember.Runtime, policy raftauthority.ReadAuthorityPolicy,
) (bool, error) {
	if runtime == nil || !policy.Enabled {
		return false, errRF3ReadAuthority
	}
	publication, err := runtime.Publication()
	if err != nil || publication.ConfState == nil || publication.ReplicaSetVersion == 0 {
		return false, errors.Join(errRF3ReadAuthority, err)
	}
	confState := publication.ConfState
	if len(confState.GetVoters()) == 0 || len(confState.GetVotersOutgoing()) != 0 ||
		len(confState.GetLearnersNext()) != 0 || confState.GetAutoLeave() {
		return false, errRF3ReadAuthority
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil || observation.Config.Joint {
		return false, errors.Join(errRF3ReadAuthority, err)
	}
	identity := runtime.Identity()
	if !slices.Contains(policy.Voters, identity.MemberID) {
		return false, errRF3ReadAuthority
	}
	return slices.Equal(confState.GetVoters(), policy.Voters), nil
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
	// generation is an in-process registration generation. It is deliberately
	// separate from the durable allocation so a late probe from a removed
	// group cannot publish into a same-key group recreated in this process.
	generation uint64
}

// rf3ReadAuthorityGroupTargets is the complete, already manifest-validated
// native roster for one newly serving group. RegisterGroups validates the
// whole batch before publishing any target or local incarnation.
type rf3ReadAuthorityGroupTargets struct {
	group            raftmember.GroupKey
	allocation       uint64
	members          []rf3ReadAuthorityProbeTarget
	localMember      uint64
	localStore       [16]byte
	localIncarnation uint64
}

// rf3ReadAuthorityRegistration is an opaque cache capability. The group and
// allocation fields are diagnostic identity only; removal requires the exact
// in-process generation returned by RegisterGroups.
type rf3ReadAuthorityRegistration struct {
	group      raftmember.GroupKey
	allocation uint64
	generation uint64
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

// rf3ReadAuthorityIncarnationCache retains exact serving targets for the
// process lifetime. Local values are seeded from the durable Runtime identity;
// remote values are populated only by authenticated native ReplicatedProbe
// responses. Runtime callbacks are read-only and never dial or call an Owner;
// a miss simply keeps ReadIndex as the safe path.
type rf3ReadAuthorityIncarnationCache struct {
	mu             sync.RWMutex
	values         map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue
	targets        map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget
	registrations  map[uint64]rf3ReadAuthorityRegistration
	groups         map[raftmember.GroupKey]uint64
	nodeAddresses  map[rafttransport.NodeID]string
	localNode      rafttransport.NodeID
	nextGeneration uint64
	closed         bool
	connections    map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection
	profile        *rafttransport.PeerTLS
	authority      serviceauthz.Authority
	ttl            time.Duration
	cursors        map[rafttransport.NodeID]uint32
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
	if profile == nil || authPolicy == nil || localNode == (rafttransport.NodeID{}) ||
		len(groups) == 0 || len(groups) != len(runtimes) {
		return nil, errRF3ReadAuthority
	}
	cache := &rf3ReadAuthorityIncarnationCache{
		values:        make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue),
		targets:       make(map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget),
		registrations: make(map[uint64]rf3ReadAuthorityRegistration),
		groups:        make(map[raftmember.GroupKey]uint64),
		nodeAddresses: make(map[rafttransport.NodeID]string, rf3ReadAuthorityCacheEntries),
		connections:   make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection),
		cursors:       make(map[rafttransport.NodeID]uint32),
		localNode:     localNode,
		profile:       profile,
		authority:     serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: authPolicy.Generation()},
		ttl:           rf3ReadAuthorityCacheTTL,
	}
	registrations := make([]rf3ReadAuthorityGroupTargets, 0, len(groups))
	for index := range groups {
		if groups[index].adoptedChild || runtimes[index] == nil {
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
		groupTargets, err := rf3ReadAuthorityGroupTargetsForPrepared(groups[index], identity)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, groupTargets)
	}
	if _, err := cache.RegisterGroups(registrations); err != nil {
		return nil, err
	}
	return cache, nil
}

func rf3ReadAuthorityGroupTargetsForPrepared(
	item preparedRF3Group, identity raftmember.RuntimeIdentity,
) (rf3ReadAuthorityGroupTargets, error) {
	group := identity.Group
	if item.manifest.Route.Group != group ||
		item.manifest.Route.AllocationGeneration != identity.AllocationGeneration ||
		identity.MemberID == 0 || identity.StoreID == ([16]byte{}) ||
		identity.AllocationGeneration == 0 || identity.NodeIncarnation == 0 {
		return rf3ReadAuthorityGroupTargets{}, errRF3ReadAuthority
	}
	members := item.manifest.memberRoster()
	if len(members) != rf3ManifestMembers {
		return rf3ReadAuthorityGroupTargets{}, errRF3ReadAuthority
	}
	targets := make([]rf3ReadAuthorityProbeTarget, 0, len(members))
	for _, member := range members {
		if member.MemberID == 0 || member.NodeID == (rafttransport.NodeID{}) ||
			member.StoreID == ([16]byte{}) || member.NativeAddress == "" {
			return rf3ReadAuthorityGroupTargets{}, errRF3ReadAuthority
		}
		targets = append(targets, rf3ReadAuthorityProbeTarget{
			key: rf3ReadAuthorityCacheKey{
				group: group, member: member.MemberID, node: member.NodeID,
				store: member.StoreID, allocation: identity.AllocationGeneration,
			},
			allocation: identity.AllocationGeneration,
			address:    member.NativeAddress,
		})
	}
	return rf3ReadAuthorityGroupTargets{
		group: group, allocation: identity.AllocationGeneration, members: targets,
		localMember: identity.MemberID, localStore: identity.StoreID,
		localIncarnation: identity.NodeIncarnation,
	}, nil
}

func (cache *rf3ReadAuthorityIncarnationCache) RegisterGroups(
	groups []rf3ReadAuthorityGroupTargets,
) ([]rf3ReadAuthorityRegistration, error) {
	if cache == nil || len(groups) == 0 ||
		len(groups) > rf3ReadAuthorityCacheEntries/rf3ManifestMembers {
		return nil, errRF3ReadAuthority
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return nil, errRF3ReadAuthority
	}

	// Work from detached maps until every group has passed validation. This is
	// what makes a suffix enrollment all-or-nothing even when one target has a
	// bad store, native endpoint, or local incarnation.
	nodeAddresses := make(map[rafttransport.NodeID]string, len(cache.nodeAddresses)+len(groups)*rf3ManifestMembers)
	for node, address := range cache.nodeAddresses {
		nodeAddresses[node] = address
	}
	for _, target := range cache.targets {
		if prior, found := nodeAddresses[target.key.node]; found && prior != target.address {
			return nil, errRF3ReadAuthority
		}
		nodeAddresses[target.key.node] = target.address
	}
	for node, connection := range cache.connections {
		if connection != nil {
			if prior, found := nodeAddresses[node]; found && prior != connection.address {
				return nil, errRF3ReadAuthority
			}
			nodeAddresses[node] = connection.address
		}
	}

	seenGroups := make(map[raftmember.GroupKey]struct{}, len(groups))
	seenTargets := 0
	requestAddresses := make(map[rafttransport.NodeID]string, len(groups)*rf3ManifestMembers)
	for _, group := range groups {
		if group.group == (raftmember.GroupKey{}) || group.allocation == 0 ||
			group.localMember == 0 || group.localStore == ([16]byte{}) ||
			group.localIncarnation == 0 || len(group.members) != rf3ManifestMembers {
			return nil, errRF3ReadAuthority
		}
		if _, duplicate := seenGroups[group.group]; duplicate {
			return nil, errRF3ReadAuthority
		}
		seenGroups[group.group] = struct{}{}
		if _, registered := cache.groups[group.group]; registered || cache.hasTargetsForGroupLocked(group.group) {
			// A registration is immutable. Retrying the exact group after a
			// failed append must first remove its unpublished registration.
			return nil, errRF3ReadAuthority
		}
		memberIDs := make(map[uint64]struct{}, len(group.members))
		nodes := make(map[rafttransport.NodeID]struct{}, len(group.members))
		localFound := false
		for _, target := range group.members {
			if target.key.group != group.group || target.key.allocation != group.allocation ||
				target.allocation != group.allocation || target.key.member == 0 ||
				target.key.node == (rafttransport.NodeID{}) || target.key.store == ([16]byte{}) ||
				target.address == "" {
				return nil, errRF3ReadAuthority
			}
			if _, duplicate := memberIDs[target.key.member]; duplicate {
				return nil, errRF3ReadAuthority
			}
			if _, duplicate := nodes[target.key.node]; duplicate {
				return nil, errRF3ReadAuthority
			}
			memberIDs[target.key.member] = struct{}{}
			nodes[target.key.node] = struct{}{}
			if target.key.member == group.localMember {
				if target.key.node != cache.localNode || target.key.store != group.localStore {
					return nil, errRF3ReadAuthority
				}
				localFound = true
			}
			if prior, found := nodeAddresses[target.key.node]; found && prior != target.address {
				return nil, errRF3ReadAuthority
			}
			if prior, found := requestAddresses[target.key.node]; found && prior != target.address {
				return nil, errRF3ReadAuthority
			}
			nodeAddresses[target.key.node] = target.address
			requestAddresses[target.key.node] = target.address
			seenTargets++
		}
		if !localFound || len(memberIDs) != rf3ManifestMembers {
			return nil, errRF3ReadAuthority
		}
	}
	if len(cache.targets)+seenTargets > rf3ReadAuthorityCacheEntries || len(nodeAddresses) > rf3ReadAuthorityCacheEntries {
		return nil, errRF3ReadAuthority
	}

	registrations := make([]rf3ReadAuthorityRegistration, len(groups))
	// Registration generations are process-local ABA protection. Never wrap
	// them: reusing an old generation could make a delayed probe or stale
	// teardown capability match a later registration.
	if cache.nextGeneration > ^uint64(0)-uint64(len(groups)) {
		return nil, errRF3ReadAuthority
	}
	nextGeneration := cache.nextGeneration
	for index, group := range groups {
		for {
			nextGeneration++
			if nextGeneration == 0 {
				continue
			}
			if _, used := cache.registrations[nextGeneration]; !used {
				break
			}
		}
		registrations[index] = rf3ReadAuthorityRegistration{
			group: group.group, allocation: group.allocation, generation: nextGeneration,
		}
	}

	if cache.values == nil {
		cache.values = make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue)
	}
	if cache.targets == nil {
		cache.targets = make(map[rf3ReadAuthorityGroupMember]rf3ReadAuthorityProbeTarget)
	}
	if cache.registrations == nil {
		cache.registrations = make(map[uint64]rf3ReadAuthorityRegistration)
	}
	if cache.groups == nil {
		cache.groups = make(map[raftmember.GroupKey]uint64)
	}
	cache.nodeAddresses = nodeAddresses
	now := time.Now()
	for index, group := range groups {
		registration := registrations[index]
		cache.registrations[registration.generation] = registration
		cache.groups[group.group] = registration.generation
		for _, original := range group.members {
			target := original
			target.generation = registration.generation
			lookup := rf3ReadAuthorityGroupMember{group: group.group, member: target.key.member}
			cache.targets[lookup] = target
			cache.nodeAddresses[target.key.node] = target.address
			if target.key.member == group.localMember {
				cache.values[target.key] = rf3ReadAuthorityCacheValue{incarnation: group.localIncarnation, seen: now}
			}
		}
	}
	cache.nextGeneration = nextGeneration
	return registrations, nil
}

func (cache *rf3ReadAuthorityIncarnationCache) hasTargetsForGroupLocked(group raftmember.GroupKey) bool {
	for lookup := range cache.targets {
		if lookup.group == group {
			return true
		}
	}
	return false
}

func (cache *rf3ReadAuthorityIncarnationCache) RegistrationFor(
	group raftmember.GroupKey, allocation uint64,
) (rf3ReadAuthorityRegistration, bool) {
	if cache == nil {
		return rf3ReadAuthorityRegistration{}, false
	}
	cache.mu.RLock()
	if cache.closed {
		cache.mu.RUnlock()
		return rf3ReadAuthorityRegistration{}, false
	}
	generation, found := cache.groups[group]
	registration, registered := cache.registrations[generation]
	cache.mu.RUnlock()
	return registration, found && registered && registration.allocation == allocation
}

func (cache *rf3ReadAuthorityIncarnationCache) UnregisterGroups(
	registrations []rf3ReadAuthorityRegistration,
) error {
	if cache == nil || len(registrations) == 0 {
		return errRF3ReadAuthority
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return errRF3ReadAuthority
	}
	seen := make(map[uint64]struct{}, len(registrations))
	for _, registration := range registrations {
		if registration.group == (raftmember.GroupKey{}) || registration.allocation == 0 ||
			registration.generation == 0 {
			return errRF3ReadAuthority
		}
		if _, duplicate := seen[registration.generation]; duplicate {
			return errRF3ReadAuthority
		}
		seen[registration.generation] = struct{}{}
		generation, found := cache.groups[registration.group]
		current, registered := cache.registrations[registration.generation]
		if !found || generation != registration.generation || !registered || current != registration {
			return errRF3ReadAuthority
		}
		targetCount := 0
		for lookup, target := range cache.targets {
			if lookup.group != registration.group {
				continue
			}
			if target.generation != registration.generation {
				return errRF3ReadAuthority
			}
			targetCount++
		}
		if targetCount != rf3ManifestMembers {
			return errRF3ReadAuthority
		}
	}
	for _, registration := range registrations {
		for lookup, target := range cache.targets {
			if lookup.group == registration.group {
				if target.generation != registration.generation {
					return errRF3ReadAuthority
				}
				delete(cache.values, target.key)
				delete(cache.targets, lookup)
			}
		}
		delete(cache.groups, registration.group)
		delete(cache.registrations, registration.generation)
	}
	return nil
}

func (cache *rf3ReadAuthorityIncarnationCache) putValueLocked(
	key rf3ReadAuthorityCacheKey, incarnation uint64, seen time.Time,
) {
	if incarnation == 0 {
		return
	}
	if cache.values == nil {
		cache.values = make(map[rf3ReadAuthorityCacheKey]rf3ReadAuthorityCacheValue)
	}
	prior, exists := cache.values[key]
	if exists && incarnation < prior.incarnation {
		return
	}
	cache.values[key] = rf3ReadAuthorityCacheValue{incarnation: incarnation, seen: seen}
}

// putProbe is the only production path that publishes a remote incarnation.
// The target must still be the exact registration captured before dialing.
func (cache *rf3ReadAuthorityIncarnationCache) putProbe(
	target rf3ReadAuthorityProbeTarget, incarnation uint64,
) bool {
	if cache == nil || incarnation == 0 {
		return false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return false
	}
	current, ok := cache.targets[rf3ReadAuthorityGroupMember{group: target.key.group, member: target.key.member}]
	if !ok || current != target {
		return false
	}
	cache.putValueLocked(target.key, incarnation, time.Now())
	return true
}

func (cache *rf3ReadAuthorityIncarnationCache) Lookup(group raftmember.GroupKey, member uint64) (uint64, bool, error) {
	if cache == nil || member == 0 {
		return 0, false, nil
	}
	cache.mu.RLock()
	if cache.closed {
		cache.mu.RUnlock()
		return 0, false, nil
	}
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
	if cache.closed {
		return nil
	}
	current, found := cache.targets[rf3ReadAuthorityGroupMember{group: target.key.group, member: target.key.member}]
	if !found || current != target {
		return nil
	}
	if cache.connections == nil {
		cache.connections = make(map[rafttransport.NodeID]*rf3ReadAuthorityProbeConnection)
	}
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
	// A response may race retirement and same-key recreation. Never publish
	// through the mutable group/member lookup in that case.
	if !cache.putProbe(target, response.State.Fence.NodeIncarnation) {
		// The authenticated exchange itself succeeded. Treat the now-stale
		// target as completed work so one retired group cannot stop refresh for
		// other groups sharing this physical peer.
		return rf3ReadAuthorityProbeSuccess
	}
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
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil
	}
	cache.closed = true
	cancel, done := cache.cancel, cache.done
	cache.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
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

func newRF3ReadAuthorityClock() (*raftauthority.CheckedClock, error) {
	source, err := raftauthority.NewQualifiedElapsedClock()
	if err != nil {
		return nil, err
	}
	clock := raftauthority.NewCheckedClock(source)
	if _, err := clock.Now(); err != nil {
		return nil, err
	}
	return clock, nil
}

func configureRF3ReadAuthorityGroup(
	manifest rf3Manifest, item preparedRF3Group, runtime *raftmember.Runtime,
	cache *rf3ReadAuthorityIncarnationCache,
) (rf3ReadAuthorityRegistration, error) {
	if cache == nil || manifest.ReadAuthority == nil || runtime == nil || item.adoptedChild {
		return rf3ReadAuthorityRegistration{}, errRF3ReadAuthority
	}
	policy, err := manifest.ReadAuthority.rf3Policy()
	if err != nil {
		return rf3ReadAuthorityRegistration{}, err
	}
	clock, err := newRF3ReadAuthorityClock()
	if err != nil {
		return rf3ReadAuthorityRegistration{}, err
	}
	if err := ensureRF3ReadAuthorityState(item.manifest.Route.MemberRoot, policy); err != nil {
		return rf3ReadAuthorityRegistration{}, err
	}
	identity := runtime.Identity()
	targets, err := rf3ReadAuthorityGroupTargetsForPrepared(item, identity)
	if err != nil {
		return rf3ReadAuthorityRegistration{}, err
	}
	registrations, err := cache.RegisterGroups([]rf3ReadAuthorityGroupTargets{targets})
	if err != nil {
		return rf3ReadAuthorityRegistration{}, err
	}
	registration := registrations[0]
	if err := runtime.ConfigureReadAuthority(raftmember.ReadAuthorityOptions{
		Policy: policy, Clock: clock,
		LeaderIncarnation: func(memberID uint64) (uint64, bool, error) {
			return cache.Lookup(identity.Group, memberID)
		},
	}); err != nil {
		return rf3ReadAuthorityRegistration{}, errors.Join(
			err, cache.UnregisterGroups([]rf3ReadAuthorityRegistration{registration}),
		)
	}
	return registration, nil
}

func configureRF3ReadAuthorities(
	manifest rf3Manifest,
	prepared []preparedRF3Group,
	runtimes []*raftmember.Runtime,
	profile *rafttransport.PeerTLS,
	authPolicy *serviceauthz.Policy,
	localNode rafttransport.NodeID,
) (*rf3ReadAuthorityIncarnationCache, []raftmember.ReadAuthorityEvidence, error) {
	if len(prepared) != len(runtimes) {
		return nil, nil, errRF3ReadAuthority
	}
	// An adopted child is reconstructed from a receipt for this local child
	// replica while its recovered bundle still carries the parent split
	// template. That template cannot authenticate the child's complete native
	// roster. Keep this restart path on ReadIndex and reject any durable marker
	// that would claim the child was enrolled; do this before cache enrollment
	// or creation of a marker for an ordinary group.
	for _, item := range prepared {
		if item.adoptedChild {
			if err := ensureRF3ReadAuthorityDisabled(item.manifest.Route.MemberRoot); err != nil {
				return nil, nil, err
			}
		}
	}
	if manifest.ReadAuthority == nil {
		for _, item := range prepared {
			if err := ensureRF3ReadAuthorityDisabled(item.manifest.Route.MemberRoot); err != nil {
				return nil, nil, err
			}
		}
		return nil, nil, nil
	}
	policy, err := manifest.ReadAuthority.rf3Policy()
	if err != nil || manifest.DevelopmentOnly || profile == nil || authPolicy == nil ||
		authPolicy.Check(profile.LocalIdentity().Node, serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow ||
		authPolicy.Check(profile.LocalIdentity().Node, serviceauthz.CapabilityDataRead) != serviceauthz.DecisionAllow {
		// The bounded refresh uses the already authenticated embedded gateway
		// principal. Standalone storage-only serving has no delegated probe
		// identity and therefore keeps ReadIndex as its only read path.
		return nil, nil, errRF3ReadAuthority
	}
	if err := validateRF3ReadAuthority(manifest.ReadAuthority, manifest.groupBundles(), manifest.DevelopmentOnly); err != nil {
		return nil, nil, err
	}
	configuredPrepared := make([]preparedRF3Group, 0, len(prepared))
	configuredRuntimes := make([]*raftmember.Runtime, 0, len(runtimes))
	for index, item := range prepared {
		if item.adoptedChild {
			continue
		}
		configuredPrepared = append(configuredPrepared, item)
		configuredRuntimes = append(configuredRuntimes, runtimes[index])
	}
	if len(configuredPrepared) == 0 {
		return nil, nil, errRF3ReadAuthority
	}
	cache, err := newRF3ReadAuthorityCache(profile, authPolicy, configuredPrepared, configuredRuntimes, localNode)
	if err != nil {
		return nil, nil, err
	}
	// Qualify every clock before mutating any durable policy marker. On an
	// unsupported platform an explicitly requested feature must fail before a
	// partial enrollment can be mistaken for a completed voter rollout.
	clocks := make([]*raftauthority.CheckedClock, len(configuredRuntimes))
	for index := range clocks {
		// Linux construction is intentionally cheap and the CLOCK_BOOTTIME
		// syscall occurs on Now. Exercise every source before the first marker
		// write, then reuse the initialized checked clock for Runtime startup.
		clocks[index], err = newRF3ReadAuthorityClock()
		if err != nil {
			_ = cache.Close()
			return nil, nil, err
		}
	}
	// Inspect every marker and publication before creating any missing marker.
	// A changed membership cut may use RestoreReadAuthority only when the
	// exact policy marker was already durable before this startup. This keeps a
	// failed cold/mismatched configuration from laundering a newly-created
	// marker into a later restore.
	preexisting := make([]bool, len(configuredRuntimes))
	exactRoster := make([]bool, len(configuredRuntimes))
	for index, runtime := range configuredRuntimes {
		exactRoster[index], err = preflightRF3ReadAuthorityRoster(runtime, policy)
		if err != nil {
			_ = cache.Close()
			return nil, nil, err
		}
	}
	for index := range configuredRuntimes {
		item := &configuredPrepared[index]
		preexisting[index], err = inspectRF3ReadAuthorityState(item.manifest.Route.MemberRoot, policy)
		if err != nil {
			_ = cache.Close()
			return nil, nil, err
		}
		if !exactRoster[index] && !preexisting[index] {
			_ = cache.Close()
			return nil, nil, errRF3ReadAuthority
		}
	}
	for index, item := range configuredPrepared {
		if preexisting[index] {
			continue
		}
		if err := ensureRF3ReadAuthorityState(item.manifest.Route.MemberRoot, policy); err != nil {
			_ = cache.Close()
			return nil, nil, err
		}
	}
	for index, runtime := range configuredRuntimes {
		group := runtime.Identity().Group
		options := raftmember.ReadAuthorityOptions{
			Policy: policy, Clock: clocks[index],
			LeaderIncarnation: func(memberID uint64) (uint64, bool, error) {
				return cache.Lookup(group, memberID)
			},
		}
		var configureErr error
		if exactRoster[index] {
			configureErr = runtime.ConfigureReadAuthority(options)
		} else {
			configureErr = runtime.RestoreReadAuthority(options)
		}
		if configureErr != nil {
			_ = cache.Close()
			return nil, nil, configureErr
		}
	}
	// Include every runtime in startup evidence, including adopted children.
	// Their explicit Disabled status documents the deliberate ReadIndex path.
	startup := make([]raftmember.ReadAuthorityEvidence, 0, len(runtimes))
	for _, runtime := range runtimes {
		startup = append(startup, runtime.ReadAuthorityEvidence())
	}
	return cache, startup, nil
}
