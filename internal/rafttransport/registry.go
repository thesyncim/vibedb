// Package rafttransport provides a bounded static identity registry and a
// current-format ordinary-message frame boundary for Multi-Raft.
//
// It does not implement authentication or a network transport. A future
// authenticated connection supplies the enrolled NodeID presented to frame
// admission.
package rafttransport

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
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

// NodeID identifies one enrolled endpoint. A future authenticated transport
// must independently bind its peer principal to this value.
type NodeID [16]byte

// MemberRole is the immutable static Raft role authorized by a roster.
type MemberRole uint8

const (
	MemberVoter   MemberRole = 1
	MemberLearner MemberRole = 2
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
	role MemberRole
}

type nodeKey struct {
	group raftmember.GroupKey
	node  NodeID
}

// StaticRegistry is an immutable, bounded member-to-node snapshot. Its methods
// are safe for concurrent use after construction.
//
// The roster is trusted caller-supplied bootstrap state. This package does not
// inspect a live Runtime or Host ConfState. Its digest detects mismatched
// static roster bytes; it is not authentication, topology authorization, or
// serving authority.
type StaticRegistry struct {
	local        NodeID
	nodes        map[memberKey]memberRecord
	members      map[nodeKey]uint64
	localMembers map[raftmember.GroupKey]uint64
	digests      map[raftmember.GroupKey][sha256.Size]byte
}

// NewStaticRegistry validates and copies one current static roster before
// publishing it. It accepts members in any order. The caller must derive every
// group, role, and ReplicaSetVersion from the exact trusted bootstrap topology.
func NewStaticRegistry(local NodeID, members []Member, limits Limits) (*StaticRegistry, error) {
	maxMembers, err := validateLimits(limits)
	if err != nil {
		return nil, err
	}
	if len(members) > limits.MaxMembers || len(members) > maxMembers {
		return nil, fmt.Errorf("%w: members %d exceed %d", ErrRegistryBound, len(members), limits.MaxMembers)
	}
	if local == (NodeID{}) {
		return nil, ErrInvalidNode
	}

	registry := &StaticRegistry{
		local:        local,
		nodes:        make(map[memberKey]memberRecord, len(members)),
		members:      make(map[nodeKey]uint64, len(members)),
		localMembers: make(map[raftmember.GroupKey]uint64),
		digests:      make(map[raftmember.GroupKey][sha256.Size]byte),
	}
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

		registry.nodes[memberKey] = memberRecord{node: member.Node, role: member.Role}
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
		registry.digests[detached[first].Group] = rosterDigest(detached[first:last])
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
	if member.Role != MemberVoter && member.Role != MemberLearner {
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

// LocalNode returns the registry's configured local node ID.
func (registry *StaticRegistry) LocalNode() NodeID {
	if registry == nil {
		return NodeID{}
	}
	return registry.local
}

// LocalMember returns the sole local member ID for group.
func (registry *StaticRegistry) LocalMember(group raftmember.GroupKey) (uint64, error) {
	if registry == nil {
		return 0, ErrGroupNotFound
	}
	memberID, ok := registry.localMembers[group]
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
	record, ok := registry.nodes[memberKey{group: group, memberID: memberID}]
	if !ok {
		return NodeID{}, ErrMemberNotFound
	}
	return record.node, nil
}

// Role returns memberID's immutable static role in group.
func (registry *StaticRegistry) Role(group raftmember.GroupKey, memberID uint64) (MemberRole, error) {
	if registry == nil {
		return 0, ErrMemberNotFound
	}
	record, ok := registry.nodes[memberKey{group: group, memberID: memberID}]
	if !ok {
		return 0, ErrMemberNotFound
	}
	return record.role, nil
}

// Member returns the member hosted by node in group.
func (registry *StaticRegistry) Member(group raftmember.GroupKey, node NodeID) (uint64, error) {
	if registry == nil {
		return 0, ErrNodeNotFound
	}
	memberID, ok := registry.members[nodeKey{group: group, node: node}]
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
	return digest, ok
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
	_, _ = hash.Write([]byte("vibedb/raft-transport/static-roster\x00"))
	var fixed [84]byte
	appendGroupKey(fixed[0:72], members[0].Group)
	binary.BigEndian.PutUint64(fixed[72:80], members[0].ReplicaSetVersion)
	binary.BigEndian.PutUint32(fixed[80:84], uint32(len(members)))
	_, _ = hash.Write(fixed[:])
	var encoded [32]byte
	for _, member := range members {
		binary.BigEndian.PutUint64(encoded[0:8], member.MemberID)
		encoded[8] = byte(member.Role)
		clear(encoded[9:16])
		copy(encoded[16:32], member.Node[:])
		_, _ = hash.Write(encoded[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}
