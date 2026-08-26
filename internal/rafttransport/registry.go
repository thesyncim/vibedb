// Package rafttransport provides a bounded static identity registry, a
// current-format ordinary-message frame boundary, and a composable
// authenticated peer-stream foundation for Multi-Raft.
//
// Callers supply connection discovery, enrolled certificates, listener
// ownership, and the exact trusted metadata grant. raftservice publishes each
// durable ConfState cut into the bounded dynamic authority view.
package rafttransport

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// AbsoluteMaxGroups is the process-wide hard ceiling for retained Raft groups.
const AbsoluteMaxGroups = 4096

var (
	ErrInvalidLimits   = errors.New("rafttransport: invalid registry limits")
	ErrRegistryBound   = errors.New("rafttransport: registry bound exceeded")
	ErrInvalidNode     = errors.New("rafttransport: invalid node ID")
	ErrInvalidGroup    = errors.New("rafttransport: invalid group key")
	ErrInvalidMember   = errors.New("rafttransport: invalid member ID")
	ErrInvalidRole     = errors.New("rafttransport: invalid member role")
	ErrReplicaSet      = errors.New("rafttransport: invalid replica-set version")
	ErrDuplicateMember = errors.New("rafttransport: duplicate group member")
	ErrDuplicateNode   = errors.New("rafttransport: duplicate group node")
	ErrLocalMember     = errors.New("rafttransport: group must have exactly one local member")
	ErrGroupNotFound   = errors.New("rafttransport: group not found")
	ErrMemberNotFound  = errors.New("rafttransport: member not found")
	ErrNodeNotFound    = errors.New("rafttransport: node not found")
)

// NodeID identifies one enrolled endpoint. PeerTLS derives this exact binary
// principal from a verified critical certificate extension.
type NodeID [16]byte

// MemberRole is dynamic committed Raft authority. MemberEnrolled carries only
// stable authenticated identity and grants no Raft traffic role.
type MemberRole uint8

const (
	MemberEnrolled MemberRole = 0
	MemberVoter    MemberRole = 1
	MemberLearner  MemberRole = 2
)

// Member binds one Raft member ID to its transport node within one group.
type Member struct {
	Group             raftmember.GroupKey
	ReplicaSetVersion uint64
	MemberID          uint64
	Node              NodeID
	Role              MemberRole
}

// Limits bounds all retained registry state. Both fields are required.
type Limits struct {
	MaxGroups  int
	MaxMembers int
}

type memberKey struct {
	group    raftmember.GroupKey
	memberID uint64
}

type memberRecord struct {
	node NodeID
}

type nodeKey struct {
	group raftmember.GroupKey
	node  NodeID
}

// StaticRegistry keeps immutable member-to-node enrollment plus atomically
// published bounded dynamic ConfState authority. Its methods are concurrent.
//
// The enrollment digest excludes roles and ReplicaSetVersion. Dynamic views
// are advanced only from an exact committed ConfState and metadata grant.
type StaticRegistry struct {
	local        NodeID
	trustDomain  TrustDomain
	limits       Limits
	nodes        map[memberKey]memberRecord
	members      map[nodeKey]uint64
	localMembers map[raftmember.GroupKey]uint64
	digests      map[raftmember.GroupKey][sha256.Size]byte
	authorities  map[raftmember.GroupKey]*authoritySlot
	dynamicMu    sync.Mutex
	dynamic      atomic.Pointer[dynamicEnrollment]
	canonical    frameBufferPool
}

// dynamicEnrollment is a copy-on-write overlay for groups adopted after
// process start. Ordinary frame admission remains lock-free: mutations are
// cold control-plane operations and publish one immutable overlay pointer.
type dynamicEnrollment struct {
	nodes        map[memberKey]memberRecord
	members      map[nodeKey]uint64
	localMembers map[raftmember.GroupKey]uint64
	digests      map[raftmember.GroupKey][sha256.Size]byte
	authorities  map[raftmember.GroupKey]*authoritySlot
	memberCount  int
}

type authorityView struct {
	version       uint64
	roles         map[uint64]MemberRole
	previous      *authorityView
	allowPrevious bool
	// retiredVersion records only the exact authority revoked by source
	// removal. Its roles are deliberately not retained or accepted.
	retiredVersion uint64
	grant          membershipgrant.Grant
	revokedGrant   membershipgrant.Grant
	promotion      *raftmember.DurablePromotionProof
}

type authoritySlot struct{ view atomic.Pointer[authorityView] }

// NewStaticRegistry validates and copies one current static roster before
// publishing it. It accepts members in any order. The caller must derive every
// group, role, and ReplicaSetVersion from the exact trusted bootstrap topology.
func NewStaticRegistry(local NodeID, members []Member, limits Limits) (*StaticRegistry, error) {
	maxMembers, err := validateLimits(limits)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, ErrInvalidGroup
	}
	if len(members) > limits.MaxMembers || len(members) > maxMembers {
		return nil, fmt.Errorf("%w: members %d exceed %d", ErrRegistryBound, len(members), limits.MaxMembers)
	}
	if local == (NodeID{}) {
		return nil, ErrInvalidNode
	}

	registry := &StaticRegistry{
		local:        local,
		limits:       limits,
		nodes:        make(map[memberKey]memberRecord, len(members)),
		members:      make(map[nodeKey]uint64, len(members)),
		localMembers: make(map[raftmember.GroupKey]uint64),
		digests:      make(map[raftmember.GroupKey][sha256.Size]byte),
		authorities:  make(map[raftmember.GroupKey]*authoritySlot),
		canonical:    frameBufferPool{retain: DefaultRetainedFrameBytes},
	}
	registry.dynamic.Store(&dynamicEnrollment{})
	groups := make(map[raftmember.GroupKey]struct{})
	groupMembers := make(map[raftmember.GroupKey]int)
	groupVoters := make(map[raftmember.GroupKey]int)
	groupVersions := make(map[raftmember.GroupKey]uint64)
	detached := slices.Clone(members)
	for i := range members {
		member := members[i]
		if err := validateMember(member); err != nil {
			return nil, fmt.Errorf("member %d: %w", i, err)
		}
		memberDomain := TrustDomain{
			ClusterID:          member.Group.ClusterID,
			ClusterIncarnation: member.Group.ClusterIncarnation,
		}
		if i == 0 {
			registry.trustDomain = memberDomain
		} else if memberDomain != registry.trustDomain {
			return nil, fmt.Errorf("member %d: %w: mixed trust domain", i, ErrInvalidGroup)
		}

		memberKey := memberKey{group: member.Group, memberID: member.MemberID}
		if _, exists := registry.nodes[memberKey]; exists {
			return nil, fmt.Errorf("member %d: %w", i, ErrDuplicateMember)
		}
		nodeKey := nodeKey{group: member.Group, node: member.Node}
		if _, exists := registry.members[nodeKey]; exists {
			return nil, fmt.Errorf("member %d: %w", i, ErrDuplicateNode)
		}
		if _, exists := groups[member.Group]; !exists {
			if len(groups) == limits.MaxGroups {
				return nil, fmt.Errorf("%w: groups exceed %d", ErrRegistryBound, limits.MaxGroups)
			}
			groups[member.Group] = struct{}{}
			groupVersions[member.Group] = member.ReplicaSetVersion
		} else if groupVersions[member.Group] != member.ReplicaSetVersion {
			return nil, fmt.Errorf("member %d: %w", i, ErrReplicaSet)
		}
		if groupMembers[member.Group] == raftmodel.MaxConfStateMembers {
			return nil, fmt.Errorf("%w: group members exceed %d", ErrRegistryBound, raftmodel.MaxConfStateMembers)
		}
		groupMembers[member.Group]++
		if member.Role == MemberVoter {
			groupVoters[member.Group]++
		}

		registry.nodes[memberKey] = memberRecord{node: member.Node}
		registry.members[nodeKey] = member.MemberID
		if member.Node == local {
			registry.localMembers[member.Group] = member.MemberID
		}
	}
	if len(registry.localMembers) != len(groups) {
		return nil, ErrLocalMember
	}
	for group := range groups {
		if groupVoters[group] == 0 {
			return nil, fmt.Errorf("%w: group has no voter", ErrInvalidRole)
		}
	}
	slices.SortFunc(detached, compareMembers)
	for first := 0; first < len(detached); {
		last := first + 1
		for last < len(detached) && detached[last].Group == detached[first].Group {
			last++
		}
		group := detached[first].Group
		registry.digests[group] = rosterDigest(detached[first:last])
		roles := make(map[uint64]MemberRole, last-first)
		for _, member := range detached[first:last] {
			if member.Role != MemberEnrolled {
				roles[member.MemberID] = member.Role
			}
		}
		slot := &authoritySlot{}
		slot.view.Store(&authorityView{version: detached[first].ReplicaSetVersion, roles: roles})
		registry.authorities[group] = slot
		first = last
	}
	return registry, nil
}

func validateLimits(limits Limits) (int, error) {
	if limits.MaxGroups <= 0 || limits.MaxGroups > AbsoluteMaxGroups ||
		limits.MaxMembers <= 0 {
		return 0, fmt.Errorf("%w: %+v", ErrInvalidLimits, limits)
	}
	if limits.MaxGroups > math.MaxInt/raftmodel.MaxConfStateMembers {
		return 0, fmt.Errorf("%w: member capacity overflow", ErrInvalidLimits)
	}
	maxMembers := limits.MaxGroups * raftmodel.MaxConfStateMembers
	if limits.MaxMembers > maxMembers {
		return 0, fmt.Errorf("%w: MaxMembers %d exceeds %d", ErrInvalidLimits, limits.MaxMembers, maxMembers)
	}
	return maxMembers, nil
}

func validateMember(member Member) error {
	if member.Node == (NodeID{}) {
		return ErrInvalidNode
	}
	if member.MemberID == 0 || raft.IsLocalMsgTarget(member.MemberID) {
		return ErrInvalidMember
	}
	if member.Role != MemberEnrolled && member.Role != MemberVoter && member.Role != MemberLearner {
		return ErrInvalidRole
	}
	if member.ReplicaSetVersion == 0 {
		return ErrReplicaSet
	}
	group := member.Group
	if group.ClusterID == ([16]byte{}) ||
		group.ClusterIncarnation == ([16]byte{}) ||
		group.TopologyRecoveryEpoch == 0 ||
		group.ShardIncarnation == ([16]byte{}) ||
		group.GroupID == ([16]byte{}) {
		return ErrInvalidGroup
	}
	return nil
}

// InstallGroup publishes one complete new group only after install has bound
// its local Runtime to the execution owner. publish is a no-fail closure: the
// caller must invoke it from the serialized owner before that owner can run
// the new Runtime. Returning without invoking publish rolls back enrollment
// and leaves the registry byte-identical.
func (registry *StaticRegistry) InstallGroup(
	members []Member,
	install func(publish func()) error,
) error {
	if registry == nil || len(members) == 0 || install == nil {
		return ErrInvalidGroup
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()

	group := members[0].Group
	version := members[0].ReplicaSetVersion
	if group == (raftmember.GroupKey{}) || registry.staticAuthority(group) != nil ||
		registry.dynamicAuthority(group) != nil {
		return ErrDuplicateMember
	}
	current := registry.dynamic.Load()
	if current == nil || len(registry.authorities)+len(current.authorities) >= registry.limits.MaxGroups ||
		registry.staticMemberCount()+current.memberCount+len(members) > registry.limits.MaxMembers ||
		len(members) > raftmodel.MaxConfStateMembers {
		return ErrRegistryBound
	}
	detached := slices.Clone(members)
	seenMembers := make(map[uint64]struct{}, len(detached))
	seenNodes := make(map[NodeID]struct{}, len(detached))
	roles := make(map[uint64]MemberRole, len(detached))
	localMember := uint64(0)
	voters := 0
	for index := range detached {
		member := detached[index]
		if err := validateMember(member); err != nil || member.Group != group ||
			member.ReplicaSetVersion != version ||
			member.Group.ClusterID != registry.trustDomain.ClusterID ||
			member.Group.ClusterIncarnation != registry.trustDomain.ClusterIncarnation {
			return errors.Join(ErrInvalidGroup, err)
		}
		if !registry.knownNode(current, member.Node) {
			// OrdinaryTransport owns a fixed per-node queue/dial set. Dynamic
			// groups may reuse those authenticated nodes, but cannot silently
			// introduce a node the shared transport cannot reach.
			return ErrNodeNotFound
		}
		if _, duplicate := seenMembers[member.MemberID]; duplicate {
			return ErrDuplicateMember
		}
		if _, duplicate := seenNodes[member.Node]; duplicate {
			return ErrDuplicateNode
		}
		seenMembers[member.MemberID] = struct{}{}
		seenNodes[member.Node] = struct{}{}
		if member.Node == registry.local {
			if localMember != 0 {
				return ErrLocalMember
			}
			localMember = member.MemberID
		}
		if member.Role == MemberVoter {
			voters++
		}
		if member.Role != MemberEnrolled {
			roles[member.MemberID] = member.Role
		}
	}
	if localMember == 0 {
		return ErrLocalMember
	}
	if voters == 0 {
		return ErrInvalidRole
	}
	slices.SortFunc(detached, compareMembers)
	next := cloneDynamicEnrollment(current)
	for _, member := range detached {
		next.nodes[memberKey{group: group, memberID: member.MemberID}] = memberRecord{node: member.Node}
		next.members[nodeKey{group: group, node: member.Node}] = member.MemberID
	}
	next.localMembers[group] = localMember
	next.digests[group] = rosterDigest(detached)
	slot := &authoritySlot{}
	slot.view.Store(&authorityView{version: version, roles: roles})
	next.authorities[group] = slot
	next.memberCount += len(detached)
	published := false
	publish := func() {
		if !published {
			registry.dynamic.Store(next)
			published = true
		}
	}
	if err := install(publish); err != nil {
		return err
	}
	if !published {
		return ErrInvalidGroup
	}
	return nil
}

func (registry *StaticRegistry) knownNode(dynamic *dynamicEnrollment, node NodeID) bool {
	for key := range registry.members {
		if key.node == node {
			return true
		}
	}
	if dynamic != nil {
		for key := range dynamic.members {
			if key.node == node {
				return true
			}
		}
	}
	return false
}

// RemoveGroup withdraws one dynamically installed group at the exact point
// chosen by its serialized execution owner. Static bootstrap enrollment is
// immutable. A failed uninstall leaves transport authority untouched.
func (registry *StaticRegistry) RemoveGroup(
	group raftmember.GroupKey,
	uninstall func(withdraw func()) error,
) error {
	if registry == nil || group == (raftmember.GroupKey{}) || uninstall == nil {
		return ErrInvalidGroup
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	if registry.staticAuthority(group) != nil {
		return ErrInvalidGroup
	}
	current := registry.dynamic.Load()
	if current == nil || current.authorities[group] == nil {
		return ErrGroupNotFound
	}
	next := cloneDynamicEnrollment(current)
	removed := 0
	for key := range next.nodes {
		if key.group == group {
			delete(next.nodes, key)
			removed++
		}
	}
	for key := range next.members {
		if key.group == group {
			delete(next.members, key)
		}
	}
	delete(next.localMembers, group)
	delete(next.digests, group)
	delete(next.authorities, group)
	next.memberCount -= removed
	withdrawn := false
	withdraw := func() {
		if !withdrawn {
			registry.dynamic.Store(next)
			withdrawn = true
		}
	}
	if err := uninstall(withdraw); err != nil {
		return err
	}
	if !withdrawn {
		return ErrInvalidGroup
	}
	return nil
}

func (registry *StaticRegistry) staticMemberCount() int { return len(registry.nodes) }

func cloneDynamicEnrollment(current *dynamicEnrollment) *dynamicEnrollment {
	next := &dynamicEnrollment{
		nodes: make(map[memberKey]memberRecord), members: make(map[nodeKey]uint64),
		localMembers: make(map[raftmember.GroupKey]uint64),
		digests:      make(map[raftmember.GroupKey][sha256.Size]byte),
		authorities:  make(map[raftmember.GroupKey]*authoritySlot),
	}
	if current == nil {
		return next
	}
	next.memberCount = current.memberCount
	for key, value := range current.nodes {
		next.nodes[key] = value
	}
	for key, value := range current.members {
		next.members[key] = value
	}
	for key, value := range current.localMembers {
		next.localMembers[key] = value
	}
	for key, value := range current.digests {
		next.digests[key] = value
	}
	for key, value := range current.authorities {
		next.authorities[key] = value
	}
	return next
}

func (registry *StaticRegistry) staticAuthority(group raftmember.GroupKey) *authoritySlot {
	if registry == nil {
		return nil
	}
	return registry.authorities[group]
}

func (registry *StaticRegistry) dynamicAuthority(group raftmember.GroupKey) *authoritySlot {
	if registry == nil {
		return nil
	}
	view := registry.dynamic.Load()
	if view == nil {
		return nil
	}
	return view.authorities[group]
}

func (registry *StaticRegistry) authoritySlot(group raftmember.GroupKey) *authoritySlot {
	if slot := registry.staticAuthority(group); slot != nil {
		return slot
	}
	return registry.dynamicAuthority(group)
}

// InstallTransitionGrant installs an exact catalog-replicated grant at a legal
// committed lifecycle cut. Both identities must already be enrolled. Exact
// reinstall is idempotent; a different live grant always conflicts.
func (registry *StaticRegistry) InstallTransitionGrant(grant membershipgrant.Grant) error {
	if registry == nil || !grant.Valid() {
		return ErrInvalidMember
	}
	slot := registry.authoritySlot(grant.Group)
	if slot == nil {
		return ErrGroupNotFound
	}
	if _, err := registry.Node(grant.Group, grant.SourceMember); err != nil {
		return err
	}
	targetNode, err := registry.Node(grant.Group, grant.TargetMember)
	if err != nil {
		return err
	}
	if [16]byte(targetNode) != grant.TargetNode {
		return ErrReplicaSet
	}
	var initial [3]membershipgrant.RosterMember
	for index, memberID := range grant.InitialVoters {
		node, err := registry.Node(grant.Group, memberID)
		if err != nil {
			return err
		}
		initial[index] = membershipgrant.RosterMember{Member: memberID, Node: [16]byte(node)}
	}
	if membershipgrant.CertifiedRosterDigest(
		grant.Group, grant.InitialReplicaSetVersion, initial,
	) != grant.InitialRosterDigest {
		return ErrReplicaSet
	}
	for {
		current := slot.view.Load()
		if current.grant == grant {
			return nil
		}
		if current.grant != (membershipgrant.Grant{}) ||
			!grantFitsCommittedCut(current, grant) || current.promotion != nil {
			return ErrReplicaSet
		}
		next := *current
		next.grant = grant
		next.revokedGrant = membershipgrant.Grant{}
		if slot.view.CompareAndSwap(current, &next) {
			return nil
		}
	}
}

func grantFitsCommittedCut(current *authorityView, grant membershipgrant.Grant) bool {
	if current == nil || current.version < grant.InitialReplicaSetVersion {
		return false
	}
	if current.version == grant.InitialReplicaSetVersion {
		return exactInitialGrantCut(current.roles, grant)
	}
	return exactLearnerGrantCut(current.roles, grant) ||
		exactPromotedGrantCut(current.roles, grant) ||
		exactCompletedGrantCut(current.roles, grant)
}

func exactInitialGrantCut(roles map[uint64]MemberRole, grant membershipgrant.Grant) bool {
	if len(roles) != len(grant.InitialVoters) || roles[grant.TargetMember] != MemberEnrolled {
		return false
	}
	for _, voter := range grant.InitialVoters {
		if roles[voter] != MemberVoter {
			return false
		}
	}
	return true
}

func exactLearnerGrantCut(roles map[uint64]MemberRole, grant membershipgrant.Grant) bool {
	return exactProgressedGrantCut(roles, grant, MemberLearner)
}

func exactPromotedGrantCut(roles map[uint64]MemberRole, grant membershipgrant.Grant) bool {
	return exactProgressedGrantCut(roles, grant, MemberVoter)
}

func exactProgressedGrantCut(
	roles map[uint64]MemberRole,
	grant membershipgrant.Grant,
	targetRole MemberRole,
) bool {
	if len(roles) != len(grant.InitialVoters)+1 || roles[grant.TargetMember] != targetRole {
		return false
	}
	for _, voter := range grant.InitialVoters {
		if roles[voter] != MemberVoter {
			return false
		}
	}
	return true
}

func exactCompletedGrantCut(roles map[uint64]MemberRole, grant membershipgrant.Grant) bool {
	if len(roles) != len(grant.InitialVoters) || roles[grant.SourceMember] != MemberEnrolled ||
		roles[grant.TargetMember] != MemberVoter {
		return false
	}
	for _, voter := range grant.InitialVoters {
		if voter != grant.SourceMember && roles[voter] != MemberVoter {
			return false
		}
	}
	return true
}

// CurrentTransitionGrant returns one allocation-free detached grant snapshot.
func (registry *StaticRegistry) CurrentTransitionGrant(
	group raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	if registry == nil || group == (raftmember.GroupKey{}) {
		return membershipgrant.Grant{}, false, ErrInvalidGroup
	}
	slot := registry.authoritySlot(group)
	if slot == nil {
		return membershipgrant.Grant{}, false, ErrGroupNotFound
	}
	grant := slot.view.Load().grant
	return grant, grant != (membershipgrant.Grant{}), nil
}

// RevokeTransitionGrant clears only the exact untouched or completed
// transition. Intermediate learner/RF4 cuts remain non-revocable so recovery
// cannot strand a partially changed Raft configuration without its authority.
func (registry *StaticRegistry) RevokeTransitionGrant(expected membershipgrant.Grant) error {
	if registry == nil || !expected.Valid() {
		return ErrInvalidMember
	}
	slot := registry.authoritySlot(expected.Group)
	if slot == nil {
		return ErrGroupNotFound
	}
	for {
		current := slot.view.Load()
		untouched := current.version == expected.InitialReplicaSetVersion &&
			grantFitsCommittedCut(current, expected)
		completed := current.version > expected.InitialReplicaSetVersion &&
			exactCompletedGrantCut(current.roles, expected)
		if (!untouched && !completed) || current.promotion != nil {
			return ErrReplicaSet
		}
		if current.grant == (membershipgrant.Grant{}) {
			if current.revokedGrant == expected {
				return nil
			}
			return ErrReplicaSet
		}
		if current.grant != expected {
			return ErrReplicaSet
		}
		next := *current
		next.grant = membershipgrant.Grant{}
		next.revokedGrant = expected
		if slot.view.CompareAndSwap(current, &next) {
			return nil
		}
	}
}

func (registry *StaticRegistry) PublishCommittedAuthority(
	group raftmember.GroupKey,
	version uint64,
	conf *pb.ConfState,
) error {
	if registry == nil || version == 0 || conf == nil {
		return ErrReplicaSet
	}
	slot := registry.authoritySlot(group)
	if slot == nil {
		return ErrGroupNotFound
	}
	roles, err := registry.rolesFromConf(group, conf)
	if err != nil {
		return err
	}
	for {
		current := slot.view.Load()
		if version == current.version {
			if equalRoles(current.roles, roles) {
				return nil
			}
			return ErrReplicaSet
		}
		if version < current.version || !validAdjacentRoles(current, roles) {
			return ErrReplicaSet
		}
		_, hadSource := current.roles[current.grant.SourceMember]
		_, hasSource := roles[current.grant.SourceMember]
		removed := current.grant.SourceMember != 0 && hadSource && !hasSource
		var previous *authorityView
		var retiredVersion uint64
		if !removed {
			previous = &authorityView{version: current.version, roles: current.roles, grant: current.grant}
		} else {
			retiredVersion = current.version
		}
		next := &authorityView{version: version, roles: roles, grant: current.grant,
			revokedGrant: current.revokedGrant,
			previous:     previous, allowPrevious: !removed, retiredVersion: retiredVersion}
		if slot.view.CompareAndSwap(current, next) {
			return nil
		}
	}
}

// PublishDurablePromotion installs a narrow election witness only after the
// local durable log proves the exact granted target's canonical promotion.
// It does not publish the target as a general voter.
func (registry *StaticRegistry) PublishDurablePromotion(
	group raftmember.GroupKey,
	proof raftmember.DurablePromotionProof,
) error {
	if registry == nil || proof.Version == 0 || proof.TargetMember == 0 {
		return ErrReplicaSet
	}
	slot := registry.authoritySlot(group)
	if slot == nil {
		return ErrGroupNotFound
	}
	for {
		current := slot.view.Load()
		if current.grant.TargetMember != proof.TargetMember ||
			current.grant.Digest() != proof.AuthorizationDigest ||
			current.roles[proof.TargetMember] != MemberLearner ||
			proof.Version <= current.version {
			return ErrReplicaSet
		}
		if current.promotion != nil {
			if *current.promotion == proof {
				return nil
			}
			return ErrReplicaSet
		}
		detached := proof
		next := *current
		next.promotion = &detached
		if slot.view.CompareAndSwap(current, &next) {
			return nil
		}
	}
}

// ClearDurablePromotion revokes a transient election witness when the exact
// entry is no longer present in the durable unapplied suffix.
func (registry *StaticRegistry) ClearDurablePromotion(group raftmember.GroupKey) error {
	if registry == nil {
		return ErrReplicaSet
	}
	slot := registry.authoritySlot(group)
	if slot == nil {
		return ErrGroupNotFound
	}
	for {
		current := slot.view.Load()
		if current.promotion == nil {
			return nil
		}
		next := *current
		next.promotion = nil
		if slot.view.CompareAndSwap(current, &next) {
			return nil
		}
	}
}

func (registry *StaticRegistry) rolesFromConf(
	group raftmember.GroupKey,
	conf *pb.ConfState,
) (map[uint64]MemberRole, error) {
	if len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return nil, ErrReplicaSet
	}
	roles := make(map[uint64]MemberRole, len(conf.GetVoters())+len(conf.GetLearners()))
	for _, member := range conf.GetVoters() {
		if _, err := registry.Node(group, member); err != nil {
			return nil, err
		}
		roles[member] = MemberVoter
	}
	for _, member := range conf.GetLearners() {
		if _, err := registry.Node(group, member); err != nil || roles[member] != MemberEnrolled {
			return nil, ErrReplicaSet
		}
		roles[member] = MemberLearner
	}
	return roles, nil
}

func validAdjacentRoles(current *authorityView, next map[uint64]MemberRole) bool {
	grant := current.grant
	if grant == (membershipgrant.Grant{}) {
		return false
	}
	sourceCurrent, sourceNext := current.roles[grant.SourceMember], next[grant.SourceMember]
	targetCurrent, targetNext := current.roles[grant.TargetMember], next[grant.TargetMember]
	if sourceCurrent != MemberVoter {
		return false
	}
	switch {
	case targetCurrent == MemberEnrolled:
		return targetNext == MemberLearner && sourceNext == MemberVoter &&
			onlyRoleChange(current.roles, next, grant.TargetMember)
	case targetCurrent == MemberLearner:
		return targetNext == MemberVoter && sourceNext == MemberVoter &&
			onlyRoleChange(current.roles, next, grant.TargetMember)
	case targetCurrent == MemberVoter:
		return sourceNext == MemberEnrolled &&
			onlyRoleChange(current.roles, next, grant.SourceMember)
	default:
		return false
	}
}

func onlyRoleChange(current, next map[uint64]MemberRole, member uint64) bool {
	for id, role := range current {
		if id != member && next[id] != role {
			return false
		}
	}
	for id, role := range next {
		if id != member && current[id] != role {
			return false
		}
	}
	return true
}

func equalRoles(left, right map[uint64]MemberRole) bool {
	return len(left) == len(right) && onlyRoleChange(left, right, 0)
}

// LocalNode returns the registry's configured local node ID.
func (registry *StaticRegistry) LocalNode() NodeID {
	if registry == nil {
		return NodeID{}
	}
	return registry.local
}

// TrustDomain returns the single cluster identity bound to every group in the
// immutable registry.
func (registry *StaticRegistry) TrustDomain() TrustDomain {
	if registry == nil {
		return TrustDomain{}
	}
	return registry.trustDomain
}

// LocalMember returns the sole local member ID for group.
func (registry *StaticRegistry) LocalMember(group raftmember.GroupKey) (uint64, error) {
	if registry == nil {
		return 0, ErrGroupNotFound
	}
	memberID, ok := registry.localMembers[group]
	if !ok {
		view := registry.dynamic.Load()
		if view != nil {
			memberID, ok = view.localMembers[group]
		}
	}
	if !ok {
		return 0, ErrGroupNotFound
	}
	return memberID, nil
}

// Node returns the node hosting memberID in group.
func (registry *StaticRegistry) Node(group raftmember.GroupKey, memberID uint64) (NodeID, error) {
	if registry == nil {
		return NodeID{}, ErrMemberNotFound
	}
	key := memberKey{group: group, memberID: memberID}
	record, ok := registry.nodes[key]
	if !ok {
		view := registry.dynamic.Load()
		if view != nil {
			record, ok = view.nodes[key]
		}
	}
	if !ok {
		return NodeID{}, ErrMemberNotFound
	}
	return record.node, nil
}

// Role returns memberID's current committed dynamic role in group.
func (registry *StaticRegistry) Role(group raftmember.GroupKey, memberID uint64) (MemberRole, error) {
	view, ok := registry.currentAuthority(group)
	if !ok {
		return 0, ErrMemberNotFound
	}
	role, ok := view.roles[memberID]
	if !ok {
		return 0, ErrMemberNotFound
	}
	return role, nil
}

// ReplicaSetVersion returns the exact committed authority generation currently
// used to authenticate group traffic.
func (registry *StaticRegistry) ReplicaSetVersion(
	group raftmember.GroupKey,
) (uint64, bool) {
	view, ok := registry.currentAuthority(group)
	if !ok {
		return 0, false
	}
	return view.version, true
}

// Member returns the member hosted by node in group.
func (registry *StaticRegistry) Member(group raftmember.GroupKey, node NodeID) (uint64, error) {
	if registry == nil {
		return 0, ErrNodeNotFound
	}
	key := nodeKey{group: group, node: node}
	memberID, ok := registry.members[key]
	if !ok {
		view := registry.dynamic.Load()
		if view != nil {
			memberID, ok = view.members[key]
		}
	}
	if !ok {
		return 0, ErrNodeNotFound
	}
	return memberID, nil
}

func (registry *StaticRegistry) rosterDigest(group raftmember.GroupKey) ([sha256.Size]byte, bool) {
	if registry == nil {
		return [sha256.Size]byte{}, false
	}
	digest, ok := registry.digests[group]
	if !ok {
		view := registry.dynamic.Load()
		if view != nil {
			digest, ok = view.digests[group]
		}
	}
	return digest, ok
}

func (registry *StaticRegistry) currentAuthority(group raftmember.GroupKey) (*authorityView, bool) {
	if registry == nil {
		return nil, false
	}
	slot := registry.authoritySlot(group)
	if slot == nil {
		return nil, false
	}
	view := slot.view.Load()
	return view, view != nil
}

func (registry *StaticRegistry) authorityAt(
	group raftmember.GroupKey,
	version uint64,
) (*authorityView, bool) {
	current, ok := registry.currentAuthority(group)
	if !ok {
		return nil, false
	}
	if current.version == version {
		return current, true
	}
	if current.allowPrevious && current.previous != nil && current.previous.version == version {
		return current.previous, true
	}
	return nil, false
}

func compareMembers(left, right Member) int {
	if result := compareGroupKeys(left.Group, right.Group); result != 0 {
		return result
	}
	return cmp.Compare(left.MemberID, right.MemberID)
}

func compareGroupKeys(left, right raftmember.GroupKey) int {
	if result := slices.Compare(left.ClusterID[:], right.ClusterID[:]); result != 0 {
		return result
	}
	if result := slices.Compare(left.ClusterIncarnation[:], right.ClusterIncarnation[:]); result != 0 {
		return result
	}
	if result := cmp.Compare(left.TopologyRecoveryEpoch, right.TopologyRecoveryEpoch); result != 0 {
		return result
	}
	if result := slices.Compare(left.ShardIncarnation[:], right.ShardIncarnation[:]); result != 0 {
		return result
	}
	return slices.Compare(left.GroupID[:], right.GroupID[:])
}

func rosterDigest(members []Member) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/raft-transport/stable-enrollment\x00"))
	var fixed [84]byte
	appendGroupKey(fixed[0:72], members[0].Group)
	clear(fixed[72:80])
	binary.BigEndian.PutUint32(fixed[80:84], uint32(len(members)))
	_, _ = hash.Write(fixed[:])
	var encoded [32]byte
	for _, member := range members {
		binary.BigEndian.PutUint64(encoded[0:8], member.MemberID)
		encoded[8] = 0
		clear(encoded[9:16])
		copy(encoded[16:32], member.Node[:])
		_, _ = hash.Write(encoded[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}
