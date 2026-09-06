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
	"context"
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
	// ErrPeerUnauthorized means that a physical endpoint was presented without
	// a committed enrollment proof.  A certificate identity alone is never
	// enough to add a peer to the transport directory.
	ErrPeerUnauthorized   = errors.New("rafttransport: physical peer enrollment is not authorized")
	ErrPeerConflict       = errors.New("rafttransport: physical peer enrollment conflicts")
	ErrPeerKeyMismatch    = errors.New("rafttransport: physical peer service key does not match")
	ErrPeerInUse          = errors.New("rafttransport: physical peer is still referenced")
	ErrPeerRetired        = errors.New("rafttransport: physical peer is retired")
	ErrPeerBusy           = errors.New("rafttransport: physical peer still has queued traffic")
	ErrEnrollmentConflict = errors.New("rafttransport: group member enrollment conflicts")
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
	// MaxPeers bounds physical identities retained by the process.  A zero
	// value uses MaxMembers as the conservative default, keeping old callers
	// source compatible while ensuring a dynamic directory remains bounded.
	MaxPeers int
}

const (
	AbsoluteMaxPeerDirectoryEntries = AbsoluteMaxTransportPeers
	MaxPeerEndpointBytes            = 4096
)

// PeerState is the physical-node lifecycle.  It deliberately carries no Raft
// role: MemberVoter and MemberLearner are derived only from committed
// ConfState authority below.
type PeerState uint8

const (
	PeerEnrolled PeerState = iota + 1
	PeerRetiring
	PeerRetired
)

// PhysicalPeer is the bounded, authenticated endpoint directory record.  The
// endpoint is discovery data and is accepted only when a caller-provided
// EnrollmentVerifier certifies the exact record.  Node is retained as a
// compatibility spelling for small integrations; NodeID is canonical.
type PhysicalPeer struct {
	NodeID      NodeID
	Node        NodeID
	TrustDomain TrustDomain
	Incarnation uint64
	Revision    uint64
	// ServiceKeyDigest binds this physical incarnation to the authenticated
	// leaf public key.  It is intentionally separate from NodeID: a recycled
	// certificate or a new session cannot claim an old directory entry merely
	// by presenting the same logical principal.
	ServiceKeyDigest [sha256.Size]byte
	EnrollmentDigest [sha256.Size]byte
	Endpoint         string
	Address          string
	State            PeerState
}

// PeerRecord and NodeEnrollment are aliases used by callers that refer to the
// directory as a node registry.  They intentionally remain the same exact
// bounded record.
type PeerRecord = PhysicalPeer
type NodeEnrollment = PhysicalPeer

func (peer PhysicalPeer) normalized(domain TrustDomain) (PhysicalPeer, error) {
	if peer.NodeID == (NodeID{}) {
		peer.NodeID = peer.Node
	} else if peer.Node != (NodeID{}) && peer.Node != peer.NodeID {
		return PhysicalPeer{}, ErrInvalidNode
	}
	if peer.NodeID == (NodeID{}) {
		return PhysicalPeer{}, ErrInvalidNode
	}
	if peer.TrustDomain == (TrustDomain{}) {
		peer.TrustDomain = domain
	}
	if peer.TrustDomain != domain || peer.Incarnation == 0 || peer.Revision == 0 {
		return PhysicalPeer{}, ErrPeerConflict
	}
	if peer.Endpoint != "" && peer.Address != "" && peer.Endpoint != peer.Address {
		return PhysicalPeer{}, ErrPeerConflict
	}
	if peer.Endpoint == "" {
		peer.Endpoint = peer.Address
	}
	if len(peer.Endpoint) > MaxPeerEndpointBytes {
		return PhysicalPeer{}, ErrPeerConflict
	}
	if peer.State == 0 {
		peer.State = PeerEnrolled
	}
	if peer.State != PeerEnrolled && peer.State != PeerRetiring && peer.State != PeerRetired {
		return PhysicalPeer{}, ErrPeerConflict
	}
	peer.Node = peer.NodeID
	peer.Address = peer.Endpoint
	return peer, nil
}

// EnrollmentIntent is the small leaf contract between a replicated control
// plane and this package.  The verifier must have read and certified the
// exact committed intent; this package then checks identity, revision, role,
// and roster invariants before publishing a copy-on-write directory cut.
// Group and Member are zero for physical-only enrollment.
type EnrollmentIntent struct {
	Digest               [sha256.Size]byte
	Domain               TrustDomain
	Peer                 PhysicalPeer
	Group                raftmember.GroupKey
	Member               Member
	ExpectedRosterDigest [sha256.Size]byte
	DirectoryRevision    uint64
}

// PeerEnrollment is an alternate name for the leaf intent used by node
// provisioning code.
type PeerEnrollment = EnrollmentIntent

// EnrollmentVerifier is intentionally tiny so gateway can adapt its
// committed GroupEnrollmentIntent without an import cycle.  Implementations
// must not perform network I/O while registry publication is in progress.
type EnrollmentVerifier interface {
	VerifyEnrollment(EnrollmentIntent) error
}

// ContextEnrollmentVerifier is the optional context-aware form used when a
// control-plane adapter must read a committed catalog row or certificate
// authority remotely. The registry invokes it before taking dynamicMu, then
// rechecks the intent's directory revision under that lock.
type ContextEnrollmentVerifier interface {
	EnrollmentVerifier
	VerifyEnrollmentContext(context.Context, EnrollmentIntent) error
}

type EnrollmentVerifierFunc func(EnrollmentIntent) error

func (verify EnrollmentVerifierFunc) VerifyEnrollment(intent EnrollmentIntent) error {
	if verify == nil {
		return ErrPeerUnauthorized
	}
	return verify(intent)
}

// VerifyEnrollmentContext lets the small function adapter participate in a
// context-aware enrollment call while preserving the original synchronous
// verifier contract.
func (verify EnrollmentVerifierFunc) VerifyEnrollmentContext(
	ctx context.Context,
	intent EnrollmentIntent,
) error {
	if ctx == nil {
		return ErrPeerUnauthorized
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if verify == nil {
		return ErrPeerUnauthorized
	}
	return verify(intent)
}

// PeerRetirementProof fences a physical directory cut.  Current authority
// references are checked locally as well; the proof lets a control-plane
// adapter bind retirement to its complete external reference scan.
type PeerRetirementProof struct {
	NodeID            NodeID
	Incarnation       uint64
	Revision          uint64
	DirectoryRevision uint64
	DirectoryDigest   [sha256.Size]byte
}

// MemberRetirementProof authorizes bounded compaction of one historical
// member-to-node mapping after the committed authority no longer references
// it. Physical peer retirement remains a separate operation: the node record
// may still be needed by another group or by the transport drain fence.
type MemberRetirementProof struct {
	Group             raftmember.GroupKey
	MemberID          uint64
	Node              NodeID
	AuthorityVersion  uint64
	RosterDigest      [sha256.Size]byte
	DirectoryRevision uint64
}

// MemberRemovalProof is the control-plane spelling used by some catalog
// adapters. It is the same exact proof and carries no additional authority.
type MemberRemovalProof = MemberRetirementProof

// RosterHandoffProof certifies that every currently authorized endpoint has
// drained the old roster cut. The registry uses it to retire the one adjacent
// compatibility digest before a subsequent enrollment starts a new handoff.
type RosterHandoffProof struct {
	Group             raftmember.GroupKey
	LegacyDigest      [sha256.Size]byte
	CurrentDigest     [sha256.Size]byte
	AuthorityVersion  uint64
	DirectoryRevision uint64
}

// RosterCompatibilityProof is an alternate name for the same exact handoff
// fence used by control-plane adapters.
type RosterCompatibilityProof = RosterHandoffProof

type memberKey struct {
	group    raftmember.GroupKey
	memberID uint64
}

type memberRecord struct {
	node             NodeID
	enrollmentDigest [sha256.Size]byte
	revision         uint64
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
	local             NodeID
	trustDomain       TrustDomain
	limits            Limits
	nodes             map[memberKey]memberRecord
	members           map[nodeKey]uint64
	physical          map[NodeID]PhysicalPeer
	peerDigest        [sha256.Size]byte
	directoryRevision uint64
	localMembers      map[raftmember.GroupKey]uint64
	digests           map[raftmember.GroupKey][sha256.Size]byte
	authorities       map[raftmember.GroupKey]*authoritySlot
	dynamicMu         sync.Mutex
	dynamic           atomic.Pointer[dynamicEnrollment]
	canonical         frameBufferPool
}

// dynamicEnrollment is a copy-on-write overlay for groups adopted after
// process start. Ordinary frame admission remains lock-free: mutations are
// cold control-plane operations and publish one immutable overlay pointer.
type dynamicEnrollment struct {
	nodes   map[memberKey]memberRecord
	members map[nodeKey]uint64
	// retiredMembers hides immutable bootstrap mappings only after an exact
	// committed removal cut. Keeping tombstones bounded by MaxMembers avoids
	// resurrecting a stale member during restart or a mixed directory update.
	retiredMembers map[memberKey]struct{}
	physical       map[NodeID]PhysicalPeer
	// purgedPhysical hides immutable bootstrap records only after the
	// authority has durably tombstoned the node. Dynamic records can be
	// removed outright; the separate set prevents a stale static principal
	// from reappearing through the immutable base map.
	purgedPhysical    map[NodeID]struct{}
	peerDigest        [sha256.Size]byte
	directoryRevision uint64
	// legacyDigests/legacyMembers form one bounded enrollment handoff.  The
	// current roster digest still names the complete member-to-node map, while
	// the legacy cut lets already-authorized members finish in-flight traffic
	// during staggered directory publication.  It is never accepted for a
	// member absent from the old cut.
	legacyDigests map[raftmember.GroupKey][sha256.Size]byte
	legacyMembers map[raftmember.GroupKey]map[uint64]NodeID
	localMembers  map[raftmember.GroupKey]uint64
	digests       map[raftmember.GroupKey][sha256.Size]byte
	authorities   map[raftmember.GroupKey]*authoritySlot
	memberCount   int
	retiredCount  int // retired tombstones that hide immutable mappings
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
		physical:     make(map[NodeID]PhysicalPeer),
		localMembers: make(map[raftmember.GroupKey]uint64),
		digests:      make(map[raftmember.GroupKey][sha256.Size]byte),
		authorities:  make(map[raftmember.GroupKey]*authoritySlot),
		canonical:    frameBufferPool{retain: DefaultRetainedFrameBytes},
	}
	registry.directoryRevision = 1
	registry.dynamic.Store(&dynamicEnrollment{directoryRevision: registry.directoryRevision})
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

		registry.nodes[memberKey] = memberRecord{node: member.Node, revision: 1}
		registry.members[nodeKey] = member.MemberID
		if _, exists := registry.physical[member.Node]; !exists {
			registry.physical[member.Node] = PhysicalPeer{
				NodeID: member.Node, Node: member.Node, TrustDomain: registry.trustDomain,
				Incarnation: 1, Revision: 1, State: PeerEnrolled,
			}
		}
		if member.Node == local {
			registry.localMembers[member.Group] = member.MemberID
		}
	}
	if len(registry.localMembers) != len(groups) {
		return nil, ErrLocalMember
	}
	if len(registry.physical) > peerLimit(limits) {
		return nil, fmt.Errorf("%w: peers exceed %d", ErrRegistryBound, peerLimit(limits))
	}
	for group := range groups {
		if groupVoters[group] == 0 {
			return nil, fmt.Errorf("%w: group has no voter", ErrInvalidRole)
		}
	}
	registry.peerDigest = physicalPeerDigest(registry.physical)
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

// NewStaticRegistryWithPhysicalPeers restores a static roster together with
// the certified physical directory cut that was persisted by the control
// authority. The ordinary constructor intentionally uses only a bootstrap
// principal (incarnation/revision 1 and no endpoint); this form is required
// when a restart must preserve real node incarnations or discovered addresses.
// The supplied slice is trusted recovery state and must cover every member
// node, while it may also contain bounded future physical peers.
func NewStaticRegistryWithPhysicalPeers(
	local NodeID,
	members []Member,
	peers []PhysicalPeer,
	limits Limits,
) (*StaticRegistry, error) {
	return NewStaticRegistryWithDirectory(local, members, peers, 1, limits)
}

// NewStaticRegistryWithDirectory is the revision-fenced form of
// NewStaticRegistryWithPhysicalPeers. DirectoryRevision is the committed
// control-directory cut used by subsequent EnrollmentIntent/retirement proofs.
func NewStaticRegistryWithDirectory(
	local NodeID,
	members []Member,
	peers []PhysicalPeer,
	directoryRevision uint64,
	limits Limits,
) (*StaticRegistry, error) {
	if directoryRevision == 0 {
		return nil, ErrPeerConflict
	}
	registry, err := NewStaticRegistry(local, members, limits)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 || len(peers) > peerLimit(limits) ||
		len(peers) > AbsoluteMaxPeerDirectoryEntries {
		return nil, fmt.Errorf("%w: physical directory has %d entries", ErrRegistryBound, len(peers))
	}
	memberNodes := make(map[NodeID]struct{}, len(members))
	for _, member := range members {
		memberNodes[member.Node] = struct{}{}
	}
	physical := make(map[NodeID]PhysicalPeer, len(peers))
	for index, supplied := range peers {
		peer, normalizeErr := supplied.normalized(registry.trustDomain)
		if normalizeErr != nil {
			return nil, fmt.Errorf("physical peer %d: %w", index, normalizeErr)
		}
		if peer.State == PeerRetiring {
			return nil, fmt.Errorf("physical peer %d: %w", index, ErrPeerConflict)
		}
		if peer.State == PeerRetired {
			if _, member := memberNodes[peer.NodeID]; member {
				return nil, fmt.Errorf("physical peer %d: %w", index, ErrPeerConflict)
			}
		}
		if peer.NodeID == local && peer.State != PeerEnrolled {
			return nil, ErrInvalidNode
		}
		if peer.ServiceKeyDigest == ([sha256.Size]byte{}) {
			return nil, fmt.Errorf("physical peer %d: %w: missing service key digest", index, ErrPeerUnauthorized)
		}
		if _, duplicate := physical[peer.NodeID]; duplicate {
			return nil, ErrPeerConflict
		}
		physical[peer.NodeID] = peer
	}
	for _, member := range members {
		peer, ok := physical[member.Node]
		if !ok || peer.State != PeerEnrolled {
			return nil, fmt.Errorf("member node %x: %w", member.Node, ErrNodeNotFound)
		}
	}
	if localPeer, ok := physical[local]; !ok || localPeer.State != PeerEnrolled {
		return nil, ErrInvalidNode
	}
	registry.physical = physical
	registry.peerDigest = physicalPeerDigest(physical)
	registry.directoryRevision = directoryRevision
	registry.dynamic.Store(&dynamicEnrollment{
		physical:          clonePhysicalPeers(physical),
		peerDigest:        registry.peerDigest,
		directoryRevision: directoryRevision,
	})
	return registry, nil
}

// NewStaticRegistryFromAuthority is a descriptive alias for
// NewStaticRegistryWithDirectory used by restart/replay adapters.
func NewStaticRegistryFromAuthority(
	local NodeID,
	members []Member,
	peers []PhysicalPeer,
	directoryRevision uint64,
	limits Limits,
) (*StaticRegistry, error) {
	return NewStaticRegistryWithDirectory(local, members, peers, directoryRevision, limits)
}

// NewEmptyRegistry creates a node-scoped registry before any Raft group is
// known.  The trust domain is explicit because there is no roster from which
// to infer it.  The local physical identity is trusted as the process owner;
// every remote identity still requires an EnrollmentVerifier proof.
func NewEmptyRegistry(local NodeID, domain TrustDomain, limits Limits) (*StaticRegistry, error) {
	return newEmptyRegistry(local, domain, PhysicalPeer{
		NodeID: local, Node: local, TrustDomain: domain,
		Incarnation: 1, Revision: 1, State: PeerEnrolled,
	}, limits, false)
}

// NewEmptyRegistryWithLocalPeer restores an empty node from the committed
// physical directory when the local incarnation or leaf key is not the
// process-default placeholder. The supplied local record is the certified
// authority cut; it cannot be used to add any remote peer or Raft member.
func NewEmptyRegistryWithLocalPeer(local PhysicalPeer, limits Limits) (*StaticRegistry, error) {
	if local.ServiceKeyDigest == ([sha256.Size]byte{}) {
		return nil, ErrPeerUnauthorized
	}
	node := local.NodeID
	if node == (NodeID{}) {
		node = local.Node
	}
	return newEmptyRegistry(node, local.TrustDomain, local, limits, true)
}

func newEmptyRegistry(
	local NodeID,
	domain TrustDomain,
	localPeer PhysicalPeer,
	limits Limits,
	requireLocalKey bool,
) (*StaticRegistry, error) {
	if _, err := validateLimits(limits); err != nil {
		return nil, err
	}
	if local == (NodeID{}) {
		return nil, ErrInvalidNode
	}
	if domain.ClusterID == ([16]byte{}) || domain.ClusterIncarnation == ([16]byte{}) {
		return nil, ErrInvalidGroup
	}
	normalizedLocal, err := localPeer.normalized(domain)
	if err != nil || normalizedLocal.NodeID != local || normalizedLocal.State != PeerEnrolled {
		return nil, ErrPeerConflict
	}
	if requireLocalKey && normalizedLocal.ServiceKeyDigest == ([sha256.Size]byte{}) {
		return nil, ErrPeerUnauthorized
	}
	registry := &StaticRegistry{
		local: local, trustDomain: domain, limits: limits,
		nodes:   make(map[memberKey]memberRecord),
		members: make(map[nodeKey]uint64),
		physical: map[NodeID]PhysicalPeer{
			local: normalizedLocal,
		},
		localMembers:      make(map[raftmember.GroupKey]uint64),
		digests:           make(map[raftmember.GroupKey][sha256.Size]byte),
		authorities:       make(map[raftmember.GroupKey]*authoritySlot),
		canonical:         frameBufferPool{retain: DefaultRetainedFrameBytes},
		directoryRevision: 1,
	}
	registry.peerDigest = physicalPeerDigest(registry.physical)
	registry.dynamic.Store(&dynamicEnrollment{directoryRevision: registry.directoryRevision})
	return registry, nil
}

// NewNodeRegistry is a descriptive alias for NewEmptyRegistry.
func NewNodeRegistry(local NodeID, domain TrustDomain, limits Limits) (*StaticRegistry, error) {
	return NewEmptyRegistry(local, domain, limits)
}

// NewNodeRegistryWithLocalPeer is the node-registry spelling of
// NewEmptyRegistryWithLocalPeer.
func NewNodeRegistryWithLocalPeer(local PhysicalPeer, limits Limits) (*StaticRegistry, error) {
	return NewEmptyRegistryWithLocalPeer(local, limits)
}

func validTrustDomain(domain TrustDomain) bool {
	return domain.ClusterID != ([16]byte{}) && domain.ClusterIncarnation != ([16]byte{})
}

func (registry *StaticRegistry) currentDirectoryRevision(current *dynamicEnrollment) uint64 {
	if current != nil && current.directoryRevision != 0 {
		return current.directoryRevision
	}
	return registry.directoryRevision
}

func (registry *StaticRegistry) normalizeEnrollment(intent EnrollmentIntent) (EnrollmentIntent, error) {
	if registry == nil {
		return EnrollmentIntent{}, ErrPeerUnauthorized
	}
	if intent.Domain == (TrustDomain{}) {
		intent.Domain = registry.trustDomain
	}
	if intent.Domain != registry.trustDomain || !validTrustDomain(intent.Domain) ||
		intent.Digest == ([sha256.Size]byte{}) || intent.DirectoryRevision == 0 {
		return EnrollmentIntent{}, ErrPeerUnauthorized
	}
	peer, err := intent.Peer.normalized(registry.trustDomain)
	if err != nil {
		return EnrollmentIntent{}, err
	}
	if peer.NodeID != registry.local && peer.ServiceKeyDigest == ([sha256.Size]byte{}) {
		return EnrollmentIntent{}, ErrPeerUnauthorized
	}
	intent.Peer = peer
	intent.Peer.EnrollmentDigest = intent.Digest
	if intent.Peer.State != PeerEnrolled {
		return EnrollmentIntent{}, ErrPeerUnauthorized
	}
	if intent.Group == (raftmember.GroupKey{}) {
		if intent.Member != (Member{}) {
			return EnrollmentIntent{}, ErrInvalidGroup
		}
		return intent, nil
	}
	if err := validateMember(intent.Member); err != nil ||
		intent.Member.Group != intent.Group || intent.Member.Node != peer.NodeID ||
		intent.Member.Role != MemberEnrolled ||
		intent.Group.ClusterID != registry.trustDomain.ClusterID ||
		intent.Group.ClusterIncarnation != registry.trustDomain.ClusterIncarnation {
		if err != nil {
			return EnrollmentIntent{}, err
		}
		return EnrollmentIntent{}, ErrPeerUnauthorized
	}
	if intent.ExpectedRosterDigest == ([sha256.Size]byte{}) {
		return EnrollmentIntent{}, ErrPeerUnauthorized
	}
	return intent, nil
}

func enrollmentVerifier(verifiers []EnrollmentVerifier) (EnrollmentVerifier, error) {
	if len(verifiers) > 1 || (len(verifiers) == 1 && verifiers[0] == nil) {
		return nil, ErrPeerUnauthorized
	}
	if len(verifiers) == 0 {
		return nil, ErrPeerUnauthorized
	}
	return verifiers[0], nil
}

func verifyEnrollment(intent EnrollmentIntent, verifier EnrollmentVerifier) error {
	if verifier == nil {
		return ErrPeerUnauthorized
	}
	if err := verifier.VerifyEnrollment(intent); err != nil {
		return errors.Join(ErrPeerUnauthorized, err)
	}
	return nil
}

func verifyEnrollmentContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	if ctx == nil || verifier == nil {
		return ErrPeerUnauthorized
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if contextual, ok := verifier.(ContextEnrollmentVerifier); ok {
		if err := contextual.VerifyEnrollmentContext(ctx, intent); err != nil {
			return errors.Join(ErrPeerUnauthorized, err)
		}
		return nil
	}
	return verifyEnrollment(intent, verifier)
}

func (registry *StaticRegistry) physicalPeerFrom(
	current *dynamicEnrollment,
	node NodeID,
) (PhysicalPeer, bool) {
	if current != nil {
		if _, purged := current.purgedPhysical[node]; purged {
			return PhysicalPeer{}, false
		}
		if peer, ok := current.physical[node]; ok {
			return peer, true
		}
	}
	peer, ok := registry.physical[node]
	return peer, ok
}

func samePhysicalPeer(left, right PhysicalPeer) bool {
	return samePhysicalIdentity(left, right) &&
		left.EnrollmentDigest == right.EnrollmentDigest
}

func samePhysicalIdentity(left, right PhysicalPeer) bool {
	return left.NodeID == right.NodeID && left.TrustDomain == right.TrustDomain &&
		left.Incarnation == right.Incarnation && left.Revision == right.Revision &&
		left.ServiceKeyDigest == right.ServiceKeyDigest &&
		left.Endpoint == right.Endpoint && left.State == right.State
}

func samePhysicalPrincipal(left, right PhysicalPeer) bool {
	return left.NodeID == right.NodeID && left.TrustDomain == right.TrustDomain &&
		left.Incarnation == right.Incarnation && left.Revision == right.Revision &&
		left.State == right.State &&
		// A legacy static placeholder may acquire its first certified key. A
		// record that already has a key cannot silently rotate it at the same
		// incarnation/revision; rotation must publish a fresh physical record.
		(left.ServiceKeyDigest == right.ServiceKeyDigest ||
			left.ServiceKeyDigest == ([sha256.Size]byte{}))
}

func (registry *StaticRegistry) mergedPhysical(
	current *dynamicEnrollment,
) map[NodeID]PhysicalPeer {
	merged := make(map[NodeID]PhysicalPeer, len(registry.physical))
	for node, peer := range registry.physical {
		if current != nil {
			if _, purged := current.purgedPhysical[node]; purged {
				continue
			}
		}
		merged[node] = peer
	}
	if current != nil {
		for node, peer := range current.physical {
			if _, purged := current.purgedPhysical[node]; purged {
				continue
			}
			merged[node] = peer
		}
	}
	return merged
}

// EnrollPeer publishes one committed physical identity.  It is deliberately
// independent of any group role.  A caller may supply a commit callback when
// a transport queue must be installed in the same publication window.
func (registry *StaticRegistry) EnrollPeer(
	intent EnrollmentIntent,
	verifiers ...EnrollmentVerifier,
) error {
	verifier, err := enrollmentVerifier(verifiers)
	if err != nil {
		return err
	}
	return registry.enrollPeerWithCommitContext(context.Background(), intent, verifier, nil)
}

func (registry *StaticRegistry) EnrollPeerWithCommit(
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
	commit func() error,
) error {
	return registry.enrollPeerWithCommitContext(context.Background(), intent, verifier, commit)
}

// EnrollPeerContext is the context-aware physical enrollment form. Remote
// verification happens before registry locks are acquired.
func (registry *StaticRegistry) EnrollPeerContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	return registry.enrollPeerWithCommitContext(ctx, intent, verifier, nil)
}

// EnrollPeerContextWithCommit combines context-aware verification with the
// queue publication callback used by OrdinaryTransport.
func (registry *StaticRegistry) EnrollPeerContextWithCommit(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
	commit func() error,
) error {
	return registry.enrollPeerWithCommitContext(ctx, intent, verifier, commit)
}

func (registry *StaticRegistry) enrollPeerWithCommitContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
	commit func() error,
) error {
	if ctx == nil {
		return ErrPeerUnauthorized
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	intent, err := registry.normalizeEnrollment(intent)
	if err != nil {
		return err
	}
	if intent.Group != (raftmember.GroupKey{}) {
		return ErrInvalidGroup
	}
	if err := verifyEnrollmentContext(ctx, intent, verifier); err != nil {
		return err
	}
	if intent.Peer.NodeID == registry.local {
		return ErrPeerConflict
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	current := registry.dynamic.Load()
	if current == nil {
		current = &dynamicEnrollment{directoryRevision: registry.directoryRevision}
	}
	if existing, ok := registry.physicalPeerFrom(current, intent.Peer.NodeID); ok {
		if existing.EnrollmentDigest == intent.Digest && samePhysicalPeer(existing, intent.Peer) {
			if commit != nil {
				return commit()
			}
			return nil
		}
		if intent.DirectoryRevision != registry.currentDirectoryRevision(current) {
			return ErrPeerConflict
		}
		// Static bootstrap rosters know the authenticated node but have no
		// endpoint proof.  The first certified directory intent may bind that
		// endpoint in place at the exact current revision.
		if existing.EnrollmentDigest == ([sha256.Size]byte{}) &&
			samePhysicalPrincipal(existing, intent.Peer) &&
			existing.State == PeerEnrolled {
			// Continue below as a bounded copy-on-write update.
		} else {
			if existing.State != PeerRetired || intent.Peer.State != PeerEnrolled ||
				intent.Peer.Incarnation <= existing.Incarnation ||
				intent.Peer.Revision <= existing.Revision {
				return ErrPeerConflict
			}
		}
	} else if intent.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	merged := registry.mergedPhysical(current)
	if _, exists := merged[intent.Peer.NodeID]; !exists &&
		len(merged) >= peerLimit(registry.limits) {
		return ErrRegistryBound
	}
	next := cloneDynamicEnrollment(current)
	if next.physical == nil {
		next.physical = make(map[NodeID]PhysicalPeer)
	}
	delete(next.purgedPhysical, intent.Peer.NodeID)
	next.physical[intent.Peer.NodeID] = intent.Peer
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	merged[intent.Peer.NodeID] = intent.Peer
	next.peerDigest = physicalPeerDigest(merged)
	if commit != nil {
		if err := commit(); err != nil {
			return err
		}
	}
	registry.dynamic.Store(next)
	return nil
}

// EnrollMember atomically adds a stable member-to-node mapping in an existing
// group and, when needed, its physical peer record. The MemberRole must be
// MemberEnrolled; only InstallTransitionGrant plus committed ConfState can
// create voter or learner authority.
func (registry *StaticRegistry) EnrollMember(
	intent EnrollmentIntent,
	verifiers ...EnrollmentVerifier,
) error {
	verifier, err := enrollmentVerifier(verifiers)
	if err != nil {
		return err
	}
	return registry.enrollMemberWithCommitContext(context.Background(), intent, verifier, nil)
}

func (registry *StaticRegistry) EnrollMemberWithCommit(
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
	commit func() error,
) error {
	return registry.enrollMemberWithCommitContext(context.Background(), intent, verifier, commit)
}

// EnrollMemberContext is the context-aware group-member enrollment form.
func (registry *StaticRegistry) EnrollMemberContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
) error {
	return registry.enrollMemberWithCommitContext(ctx, intent, verifier, nil)
}

// EnrollMemberContextWithCommit combines context-aware verification with the
// queue publication callback used by OrdinaryTransport.
func (registry *StaticRegistry) EnrollMemberContextWithCommit(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
	commit func() error,
) error {
	return registry.enrollMemberWithCommitContext(ctx, intent, verifier, commit)
}

func (registry *StaticRegistry) enrollMemberWithCommitContext(
	ctx context.Context,
	intent EnrollmentIntent,
	verifier EnrollmentVerifier,
	commit func() error,
) error {
	if ctx == nil {
		return ErrPeerUnauthorized
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	intent, err := registry.normalizeEnrollment(intent)
	if err != nil {
		return err
	}
	if intent.Group == (raftmember.GroupKey{}) {
		return ErrInvalidGroup
	}
	if err := verifyEnrollmentContext(ctx, intent, verifier); err != nil {
		return err
	}
	if intent.Peer.NodeID == registry.local {
		return ErrPeerConflict
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	current := registry.dynamic.Load()
	if current == nil {
		current = &dynamicEnrollment{directoryRevision: registry.directoryRevision}
	}
	slot := registry.staticAuthority(intent.Group)
	if slot == nil {
		slot = current.authorities[intent.Group]
	}
	if slot == nil {
		return ErrGroupNotFound
	}
	view := slot.view.Load()
	if view == nil || intent.Member.ReplicaSetVersion != view.version {
		return ErrPeerConflict
	}
	memberKey := memberKey{group: intent.Group, memberID: intent.Member.MemberID}
	nodeKey := nodeKey{group: intent.Group, node: intent.Peer.NodeID}
	if existing, ok := registry.nodes[memberKey]; ok {
		if existing.node != intent.Peer.NodeID {
			return ErrEnrollmentConflict
		}
		if currentRecord, dynamicOK := current.nodes[memberKey]; dynamicOK {
			existing = currentRecord
		}
		if existing.node == intent.Peer.NodeID &&
			existing.enrollmentDigest == intent.Digest {
			peer, peerOK := registry.physicalPeerFrom(current, intent.Peer.NodeID)
			if !peerOK || !samePhysicalIdentity(peer, intent.Peer) {
				return ErrEnrollmentConflict
			}
			if commit != nil {
				return commit()
			}
			return nil
		}
		return ErrEnrollmentConflict
	}
	if existing, ok := current.nodes[memberKey]; ok {
		if existing.node == intent.Peer.NodeID &&
			existing.enrollmentDigest == intent.Digest {
			peer, peerOK := registry.physicalPeerFrom(current, intent.Peer.NodeID)
			if !peerOK || !samePhysicalIdentity(peer, intent.Peer) {
				return ErrEnrollmentConflict
			}
			if commit != nil {
				return commit()
			}
			return nil
		}
		return ErrEnrollmentConflict
	}
	if expected, ok := registry.rosterDigest(intent.Group); !ok ||
		expected != intent.ExpectedRosterDigest {
		return ErrEnrollmentConflict
	}
	if intent.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	if _, err := registry.Member(intent.Group, intent.Peer.NodeID); err == nil {
		return ErrDuplicateNode
	}
	if _, ok := registry.localMembers[intent.Group]; ok && intent.Peer.NodeID == registry.local {
		return ErrLocalMember
	}
	if _, ok := current.localMembers[intent.Group]; ok && intent.Peer.NodeID == registry.local {
		return ErrLocalMember
	}
	if len(registry.membersForGroup(intent.Group, current)) >= raftmodel.MaxConfStateMembers ||
		registry.effectiveMemberCount(current) >= registry.limits.MaxMembers {
		return ErrRegistryBound
	}
	if existingPeer, ok := registry.physicalPeerFrom(current, intent.Peer.NodeID); ok {
		if !samePhysicalIdentity(existingPeer, intent.Peer) {
			if existingPeer.EnrollmentDigest == ([sha256.Size]byte{}) &&
				samePhysicalPrincipal(existingPeer, intent.Peer) &&
				existingPeer.State == PeerEnrolled {
				// Bind the first certified endpoint to a static bootstrap peer.
			} else if existingPeer.State != PeerRetired || intent.Peer.State != PeerEnrolled ||
				intent.Peer.Incarnation <= existingPeer.Incarnation ||
				intent.Peer.Revision <= existingPeer.Revision {
				return ErrPeerConflict
			}
		}
	}
	merged := registry.mergedPhysical(current)
	if _, exists := merged[intent.Peer.NodeID]; !exists &&
		len(merged) >= peerLimit(registry.limits) {
		return ErrRegistryBound
	}
	next := cloneDynamicEnrollment(current)
	if next.physical == nil {
		next.physical = make(map[NodeID]PhysicalPeer)
	}
	delete(next.purgedPhysical, intent.Peer.NodeID)
	peer := intent.Peer
	if existingPeer, ok := registry.physicalPeerFrom(current, intent.Peer.NodeID); ok &&
		samePhysicalIdentity(existingPeer, intent.Peer) && existingPeer.State == PeerEnrolled {
		// A physical identity may be certified once and then referenced by
		// several group intents.  Keep its original immutable directory proof;
		// the group-specific digest lives in memberRecord below.
		peer = existingPeer
	}
	next.physical[intent.Peer.NodeID] = peer
	next.nodes[memberKey] = memberRecord{
		node: intent.Peer.NodeID, enrollmentDigest: intent.Digest,
		revision: intent.DirectoryRevision,
	}
	next.members[nodeKey] = intent.Member.MemberID
	priorMembers := registry.membersForGroup(intent.Group, current)
	allMembers := slices.Clone(priorMembers)
	allMembers = append(allMembers, intent.Member)
	slices.SortFunc(allMembers, compareMembers)
	next.digests[intent.Group] = rosterDigest(allMembers)
	if next.legacyDigests == nil {
		next.legacyDigests = make(map[raftmember.GroupKey][sha256.Size]byte)
	}
	if next.legacyMembers == nil {
		next.legacyMembers = make(map[raftmember.GroupKey]map[uint64]NodeID)
	}
	if priorDigest, ok := registry.rosterDigest(intent.Group); ok {
		// A second enrollment may start only after the control plane has
		// certified that the prior adjacent cut drained. Retaining one legacy
		// digest while allowing a new one would skip a mixed update and permit
		// a queued frame from a non-adjacent roster to be accepted.
		if _, exists := next.legacyDigests[intent.Group]; exists {
			return ErrEnrollmentConflict
		}
		next.legacyDigests[intent.Group] = priorDigest
		oldMembers := make(map[uint64]NodeID, len(priorMembers))
		for _, member := range priorMembers {
			oldMembers[member.MemberID] = member.Node
		}
		next.legacyMembers[intent.Group] = oldMembers
	}
	next.memberCount++
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	merged[intent.Peer.NodeID] = peer
	next.peerDigest = physicalPeerDigest(merged)
	if commit != nil {
		if err := commit(); err != nil {
			return err
		}
	}
	registry.dynamic.Store(next)
	return nil
}

func (registry *StaticRegistry) groupHasPendingEnrollment(
	group raftmember.GroupKey,
	current *dynamicEnrollment,
) bool {
	view, ok := registry.currentAuthority(group)
	if !ok || view == nil {
		return false
	}
	for _, member := range registry.membersForGroup(group, current) {
		if _, present := view.roles[member.MemberID]; !present {
			// A completed replacement intentionally leaves the retired source
			// mapping in the bounded history until the control plane proves it
			// can be compacted. It is absent from the current roles, but that is
			// a removal cut rather than a pending enrollment. Dynamic enrollment
			// records carry a nonzero intent digest, so they distinguish a newly
			// enrolled target that has not reached ConfState from static/tombstone
			// history without retaining an unbounded compatibility list.
			record, found := registry.memberRecord(group, member.MemberID, current)
			if found && record.enrollmentDigest != ([sha256.Size]byte{}) {
				return true
			}
		}
	}
	return false
}

func (registry *StaticRegistry) memberRecord(
	group raftmember.GroupKey,
	memberID uint64,
	current *dynamicEnrollment,
) (memberRecord, bool) {
	key := memberKey{group: group, memberID: memberID}
	if registry.memberIsRetired(current, key) {
		return memberRecord{}, false
	}
	if current != nil {
		if record, ok := current.nodes[key]; ok {
			return record, true
		}
	}
	record, ok := registry.nodes[key]
	return record, ok
}

func validateLimits(limits Limits) (int, error) {
	if limits.MaxGroups <= 0 || limits.MaxGroups > AbsoluteMaxGroups ||
		limits.MaxMembers <= 0 || limits.MaxPeers < 0 ||
		limits.MaxPeers > AbsoluteMaxPeerDirectoryEntries {
		return 0, fmt.Errorf("%w: %+v", ErrInvalidLimits, limits)
	}
	if limits.MaxGroups > math.MaxInt/raftmodel.MaxConfStateMembers {
		return 0, fmt.Errorf("%w: member capacity overflow", ErrInvalidLimits)
	}
	maxMembers := limits.MaxGroups * raftmodel.MaxConfStateMembers
	if limits.MaxMembers > maxMembers {
		return 0, fmt.Errorf("%w: MaxMembers %d exceeds %d", ErrInvalidLimits, limits.MaxMembers, maxMembers)
	}
	if limits.MaxPeers == 0 {
		limits.MaxPeers = limits.MaxMembers
	}
	return maxMembers, nil
}

func peerLimit(limits Limits) int {
	if limits.MaxPeers > 0 {
		return limits.MaxPeers
	}
	return limits.MaxMembers
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
		registry.effectiveMemberCount(current)+len(members) > registry.limits.MaxMembers ||
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
		next.nodes[memberKey{group: group, memberID: member.MemberID}] = memberRecord{node: member.Node, revision: 1}
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
	peer, ok := registry.physicalPeerFrom(dynamic, node)
	return ok && peer.State == PeerEnrolled
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
	for key := range next.retiredMembers {
		if key.group == group {
			if _, static := registry.nodes[key]; static {
				next.retiredCount--
			}
			delete(next.retiredMembers, key)
		}
	}
	delete(next.localMembers, group)
	delete(next.digests, group)
	delete(next.legacyDigests, group)
	delete(next.legacyMembers, group)
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

func (registry *StaticRegistry) effectiveMemberCount(current *dynamicEnrollment) int {
	if registry == nil {
		return 0
	}
	retired := 0
	dynamicMembers := 0
	if current != nil {
		retired = current.retiredCount
		dynamicMembers = current.memberCount
	}
	return registry.staticMemberCount() - retired + dynamicMembers
}

func (registry *StaticRegistry) membersForGroup(
	group raftmember.GroupKey,
	current *dynamicEnrollment,
) []Member {
	version, _ := registry.ReplicaSetVersion(group)
	members := make([]Member, 0)
	seen := make(map[uint64]struct{})
	for key, record := range registry.nodes {
		if key.group != group {
			continue
		}
		if registry.memberIsRetired(current, key) {
			continue
		}
		members = append(members, Member{Group: group, ReplicaSetVersion: version,
			MemberID: key.memberID, Node: record.node, Role: MemberEnrolled})
		seen[key.memberID] = struct{}{}
	}
	if current != nil {
		for key, record := range current.nodes {
			if key.group != group {
				continue
			}
			if registry.memberIsRetired(current, key) {
				continue
			}
			if _, exists := seen[key.memberID]; exists {
				continue
			}
			members = append(members, Member{Group: group, ReplicaSetVersion: version,
				MemberID: key.memberID, Node: record.node, Role: MemberEnrolled})
		}
	}
	// The immutable and dynamic maps have deliberately different storage
	// paths, but their roster digest must be identical on every process. Keep
	// the detached view in canonical member-ID order before any caller hashes
	// it or publishes a handoff proof.
	slices.SortFunc(members, compareMembers)
	return members
}

func (registry *StaticRegistry) memberIsRetired(
	current *dynamicEnrollment,
	key memberKey,
) bool {
	if current == nil {
		return false
	}
	_, retired := current.retiredMembers[key]
	return retired
}

func physicalPeerDigest(peers map[NodeID]PhysicalPeer) [sha256.Size]byte {
	ordered := make([]PhysicalPeer, 0, len(peers))
	for _, peer := range peers {
		ordered = append(ordered, peer)
	}
	slices.SortFunc(ordered, func(left, right PhysicalPeer) int {
		return slices.Compare(left.NodeID[:], right.NodeID[:])
	})
	hash := sha256.New()
	// v2 adds the certified leaf-key binding to every directory digest. A
	// restart must never treat a pre-binding cut as the same authority cut.
	_, _ = hash.Write([]byte("vibedb/raft-transport/physical-peer-directory/v2\x00"))
	var scalar [8]byte
	for _, peer := range ordered {
		_, _ = hash.Write(peer.NodeID[:])
		_, _ = hash.Write(peer.TrustDomain.ClusterID[:])
		_, _ = hash.Write(peer.TrustDomain.ClusterIncarnation[:])
		binary.BigEndian.PutUint64(scalar[:], peer.Incarnation)
		_, _ = hash.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], peer.Revision)
		_, _ = hash.Write(scalar[:])
		_, _ = hash.Write(peer.ServiceKeyDigest[:])
		_, _ = hash.Write(peer.EnrollmentDigest[:])
		_, _ = hash.Write([]byte{byte(peer.State)})
		binary.BigEndian.PutUint32(scalar[:4], uint32(len(peer.Endpoint)))
		_, _ = hash.Write(scalar[:4])
		_, _ = hash.Write([]byte(peer.Endpoint))
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func clonePhysicalPeers(peers map[NodeID]PhysicalPeer) map[NodeID]PhysicalPeer {
	copyPeers := make(map[NodeID]PhysicalPeer, len(peers))
	for node, peer := range peers {
		copyPeers[node] = peer
	}
	return copyPeers
}

func cloneDynamicEnrollment(current *dynamicEnrollment) *dynamicEnrollment {
	next := &dynamicEnrollment{
		nodes: make(map[memberKey]memberRecord), members: make(map[nodeKey]uint64),
		retiredMembers: make(map[memberKey]struct{}),
		physical:       make(map[NodeID]PhysicalPeer),
		purgedPhysical: make(map[NodeID]struct{}),
		legacyDigests:  make(map[raftmember.GroupKey][sha256.Size]byte),
		legacyMembers:  make(map[raftmember.GroupKey]map[uint64]NodeID),
		localMembers:   make(map[raftmember.GroupKey]uint64),
		digests:        make(map[raftmember.GroupKey][sha256.Size]byte),
		authorities:    make(map[raftmember.GroupKey]*authoritySlot),
	}
	if current == nil {
		return next
	}
	next.memberCount = current.memberCount
	next.retiredCount = current.retiredCount
	next.peerDigest = current.peerDigest
	next.directoryRevision = current.directoryRevision
	for key, value := range current.nodes {
		next.nodes[key] = value
	}
	for key, value := range current.members {
		next.members[key] = value
	}
	for key := range current.retiredMembers {
		next.retiredMembers[key] = struct{}{}
	}
	for node := range current.purgedPhysical {
		next.purgedPhysical[node] = struct{}{}
	}
	for key, value := range current.physical {
		next.physical[key] = value
	}
	for group, digest := range current.legacyDigests {
		next.legacyDigests[group] = digest
	}
	for group, members := range current.legacyMembers {
		copyMembers := make(map[uint64]NodeID, len(members))
		for member, node := range members {
			copyMembers[member] = node
		}
		next.legacyMembers[group] = copyMembers
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
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	slot := registry.authoritySlot(grant.Group)
	if slot == nil {
		return ErrGroupNotFound
	}
	sourceNode, err := registry.Node(grant.Group, grant.SourceMember)
	if err != nil {
		return err
	}
	if !registry.IsPeerEnrolled(sourceNode) {
		return ErrPeerRetired
	}
	targetNode, err := registry.Node(grant.Group, grant.TargetMember)
	if err != nil {
		return err
	}
	if !registry.IsPeerEnrolled(targetNode) {
		return ErrPeerRetired
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
		if !registry.IsPeerEnrolled(node) {
			return ErrPeerRetired
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
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
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
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	slot := registry.authoritySlot(group)
	if slot == nil {
		return ErrGroupNotFound
	}
	current := slot.view.Load()
	if version == current.version {
		if confMatchesRoles(conf, current.roles) {
			return nil
		}
		return ErrReplicaSet
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

func confMatchesRoles(conf *pb.ConfState, roles map[uint64]MemberRole) bool {
	if conf == nil || len(conf.GetVotersOutgoing()) != 0 ||
		len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() ||
		len(conf.GetVoters())+len(conf.GetLearners()) != len(roles) {
		return false
	}
	var previous uint64
	for index, member := range conf.GetVoters() {
		if (index != 0 && member <= previous) || roles[member] != MemberVoter {
			return false
		}
		previous = member
	}
	previous = 0
	for index, member := range conf.GetLearners() {
		if (index != 0 && member <= previous) || roles[member] != MemberLearner {
			return false
		}
		previous = member
	}
	return true
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
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
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
		targetNode, err := registry.Node(group, proof.TargetMember)
		if err != nil || !registry.IsPeerEnrolled(targetNode) {
			return ErrPeerRetired
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
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
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
		node, err := registry.Node(group, member)
		if err != nil {
			return nil, err
		}
		if !registry.IsPeerEnrolled(node) {
			return nil, ErrPeerRetired
		}
		roles[member] = MemberVoter
	}
	for _, member := range conf.GetLearners() {
		node, err := registry.Node(group, member)
		if err != nil || roles[member] != MemberEnrolled {
			return nil, ErrReplicaSet
		}
		if !registry.IsPeerEnrolled(node) {
			return nil, ErrPeerRetired
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

// PhysicalPeer returns the current authenticated directory record.  A
// retired record remains visible until its stable group mappings can be
// forgotten by the authoritative owner; this preserves restart diagnostics
// without keeping a transport worker alive.
func (registry *StaticRegistry) PhysicalPeer(node NodeID) (PhysicalPeer, error) {
	if registry == nil || node == (NodeID{}) {
		return PhysicalPeer{}, ErrNodeNotFound
	}
	peer, ok := registry.physicalPeerFrom(registry.dynamic.Load(), node)
	if !ok {
		return PhysicalPeer{}, ErrNodeNotFound
	}
	return peer, nil
}

// Peer is a concise lookup alias for PhysicalPeer.
func (registry *StaticRegistry) Peer(node NodeID) (PhysicalPeer, error) {
	return registry.PhysicalPeer(node)
}

// IsPeerEnrolled reports whether node is currently eligible for an
// authenticated ordinary stream.  It does not inspect or grant group roles.
func (registry *StaticRegistry) IsPeerEnrolled(node NodeID) bool {
	peer, err := registry.PhysicalPeer(node)
	return err == nil && peer.State == PeerEnrolled
}

// VerifyPeerBinding checks the credential that authenticated one connection
// against the current physical directory cut.  The check is deliberately
// independent of group authority: enrollment only makes the endpoint
// discoverable, while ConfState admission still decides whether it may carry
// Raft traffic.
//
// Legacy static manifests authorize CA-authenticated node identities and do
// not contain leaf-key pins. Their records retain a zero ServiceKeyDigest;
// TLS still verifies the trust domain and node identity before this check.
// Restored and dynamically enrolled directory records require a nonzero pin,
// which every connection (including pooled streams) must match exactly.
func (registry *StaticRegistry) VerifyPeerBinding(
	identity PeerIdentity,
	serviceKeyDigest [sha256.Size]byte,
) error {
	if registry == nil || !validPeerIdentity(identity) ||
		identity.TrustDomain != registry.trustDomain {
		return ErrPeerUnauthorized
	}
	peer, err := registry.PhysicalPeer(identity.Node)
	if err != nil || peer.State != PeerEnrolled || peer.TrustDomain != identity.TrustDomain {
		return ErrPeerUnauthorized
	}
	if peer.ServiceKeyDigest == ([sha256.Size]byte{}) {
		return nil
	}
	if serviceKeyDigest == ([sha256.Size]byte{}) ||
		serviceKeyDigest != peer.ServiceKeyDigest {
		return ErrPeerKeyMismatch
	}
	return nil
}

// VerifyPeerConnectionBinding is the convenience form used by authenticated
// service adapters.  It performs no network operation and is safe to call
// before every request on a pooled PeerConnection.
func (registry *StaticRegistry) VerifyPeerConnectionBinding(
	connection PeerConnection,
) error {
	if connection == nil {
		return ErrPeerUnauthorized
	}
	return registry.VerifyPeerBinding(connection.PeerIdentity(), connection.PeerKeyDigest())
}

// PeerDirectory returns a bounded, node-ID-sorted detached directory cut.
func (registry *StaticRegistry) PeerDirectory() []PhysicalPeer {
	if registry == nil {
		return nil
	}
	merged := registry.mergedPhysical(registry.dynamic.Load())
	peers := make([]PhysicalPeer, 0, len(merged))
	for _, peer := range merged {
		peers = append(peers, peer)
	}
	slices.SortFunc(peers, func(left, right PhysicalPeer) int {
		return slices.Compare(left.NodeID[:], right.NodeID[:])
	})
	return peers
}

// PeerDirectoryDigest identifies the exact physical directory cut.  It is
// intentionally distinct from the group roster digest carried in Raft frames.
func (registry *StaticRegistry) PeerDirectoryDigest() [sha256.Size]byte {
	if registry == nil {
		return [sha256.Size]byte{}
	}
	current := registry.dynamic.Load()
	if current != nil && current.peerDigest != ([sha256.Size]byte{}) {
		return current.peerDigest
	}
	return registry.peerDigest
}

// PeerDirectoryRevision returns the monotonic revision used to fence a
// committed enrollment or retirement against concurrent control changes.
func (registry *StaticRegistry) PeerDirectoryRevision() uint64 {
	if registry == nil {
		return 0
	}
	return registry.currentDirectoryRevision(registry.dynamic.Load())
}

// CanRetirePeer checks all currently retained group authority views.  It is a
// local safety check; a control-plane caller should additionally bind its
// complete catalog/gateway reference scan in PeerRetirementProof.
func (registry *StaticRegistry) CanRetirePeer(node NodeID) error {
	if registry == nil {
		return ErrNodeNotFound
	}
	peer, err := registry.PhysicalPeer(node)
	if err != nil {
		return err
	}
	if peer.State == PeerRetired {
		return nil
	}
	if registry.peerInUse(node) {
		return ErrPeerInUse
	}
	return nil
}

func (registry *StaticRegistry) peerInUse(node NodeID) bool {
	if registry == nil {
		return false
	}
	for key, record := range registry.nodes {
		if record.node != node {
			continue
		}
		if registry.memberIsRetired(registry.dynamic.Load(), key) {
			continue
		}
		if view, ok := registry.currentAuthority(key.group); ok {
			if _, active := view.roles[key.memberID]; active {
				return true
			}
			if view.grant.SourceMember == key.memberID || view.grant.TargetMember == key.memberID {
				return true
			}
			if view.previous != nil {
				if _, active := view.previous.roles[key.memberID]; active {
					return true
				}
			}
		}
	}
	current := registry.dynamic.Load()
	if current != nil {
		for key, record := range current.nodes {
			if record.node != node {
				continue
			}
			if registry.memberIsRetired(current, key) {
				continue
			}
			if view, ok := registry.currentAuthority(key.group); ok {
				if _, active := view.roles[key.memberID]; active {
					return true
				}
				if view.grant.SourceMember == key.memberID || view.grant.TargetMember == key.memberID {
					return true
				}
				if view.previous != nil {
					if _, active := view.previous.roles[key.memberID]; active {
						return true
					}
				}
			}
		}
	}
	return false
}

// RetirePhysicalPeer marks one physical identity retired at an exact
// directory revision.  The optional commit callback is used by
// OrdinaryTransport to fence its queue before the new directory cut becomes
// visible. No callback is invoked while a network operation is in progress.
func (registry *StaticRegistry) RetirePhysicalPeer(
	proof PeerRetirementProof,
	commit ...func() error,
) error {
	if registry == nil || proof.NodeID == (NodeID{}) || proof.NodeID == registry.local || proof.Revision == 0 ||
		proof.Incarnation == 0 || proof.DirectoryRevision == 0 {
		return ErrPeerConflict
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	current := registry.dynamic.Load()
	if current == nil {
		current = &dynamicEnrollment{directoryRevision: registry.directoryRevision}
	}
	peer, ok := registry.physicalPeerFrom(current, proof.NodeID)
	if !ok {
		return ErrNodeNotFound
	}
	if peer.Incarnation != proof.Incarnation || peer.Revision != proof.Revision ||
		proof.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	if proof.DirectoryDigest != ([sha256.Size]byte{}) &&
		proof.DirectoryDigest != registry.PeerDirectoryDigest() {
		return ErrPeerConflict
	}
	if peer.State == PeerRetired {
		if len(commit) > 1 {
			return ErrPeerConflict
		}
		if len(commit) == 1 && commit[0] != nil {
			return commit[0]()
		}
		return nil
	}
	if registry.peerInUse(proof.NodeID) {
		return ErrPeerInUse
	}
	next := cloneDynamicEnrollment(current)
	retired := peer
	retired.State = PeerRetired
	next.physical[proof.NodeID] = retired
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	if len(commit) > 1 {
		return ErrPeerConflict
	}
	next.peerDigest = physicalPeerDigest(registry.mergedPhysical(next))
	if len(commit) == 1 && commit[0] != nil {
		if err := commit[0](); err != nil {
			return err
		}
	}
	registry.dynamic.Store(next)
	return nil
}

// ForgetRetiredPeer compacts a retired physical record only after the
// committed control directory has persisted its tombstone and all group
// mappings/transport queues are gone. The exact retirement proof fences the
// operation; a future enrollment must present a newer authority-certified
// identity at a fresh directory revision.
func (registry *StaticRegistry) ForgetRetiredPeer(proof PeerRetirementProof) error {
	if registry == nil || proof.NodeID == (NodeID{}) || proof.NodeID == registry.local ||
		proof.Incarnation == 0 || proof.Revision == 0 || proof.DirectoryRevision == 0 {
		return ErrPeerConflict
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	current := registry.dynamic.Load()
	if current == nil {
		return ErrNodeNotFound
	}
	peer, ok := registry.physicalPeerFrom(current, proof.NodeID)
	if !ok {
		return ErrNodeNotFound
	}
	if peer.State != PeerRetired || peer.Incarnation != proof.Incarnation ||
		peer.Revision != proof.Revision ||
		proof.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	if proof.DirectoryDigest == ([sha256.Size]byte{}) ||
		proof.DirectoryDigest != registry.PeerDirectoryDigest() {
		return ErrPeerConflict
	}
	if registry.peerInUse(proof.NodeID) {
		return ErrPeerInUse
	}
	next := cloneDynamicEnrollment(current)
	delete(next.physical, proof.NodeID)
	if _, immutable := registry.physical[proof.NodeID]; immutable {
		next.purgedPhysical[proof.NodeID] = struct{}{}
	}
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	next.peerDigest = physicalPeerDigest(registry.mergedPhysical(next))
	registry.dynamic.Store(next)
	return nil
}

// PurgeRetiredPeer is the control-plane wording for ForgetRetiredPeer.
func (registry *StaticRegistry) PurgeRetiredPeer(proof PeerRetirementProof) error {
	return registry.ForgetRetiredPeer(proof)
}

// CompleteRosterHandoff retires the one adjacent compatibility digest after a
// control-plane fanout has observed queue drain from every authorized voter.
// The registry cannot infer that remote fact locally, so the caller supplies a
// certified exact authority/roster/directory cut. No network work is performed
// under dynamicMu and no arbitrary stale digest becomes acceptable.
func (registry *StaticRegistry) CompleteRosterHandoff(
	proof RosterHandoffProof,
) error {
	if registry == nil || proof.Group == (raftmember.GroupKey{}) ||
		proof.LegacyDigest == ([sha256.Size]byte{}) ||
		proof.CurrentDigest == ([sha256.Size]byte{}) ||
		proof.AuthorityVersion == 0 || proof.DirectoryRevision == 0 {
		return ErrEnrollmentConflict
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	current := registry.dynamic.Load()
	if current == nil {
		return ErrEnrollmentConflict
	}
	slot := registry.authoritySlot(proof.Group)
	if slot == nil {
		return ErrGroupNotFound
	}
	view := slot.view.Load()
	if view == nil || view.version != proof.AuthorityVersion ||
		proof.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	if current.legacyDigests[proof.Group] != proof.LegacyDigest {
		return ErrEnrollmentConflict
	}
	currentDigest, ok := registry.rosterDigest(proof.Group)
	if !ok || currentDigest != proof.CurrentDigest {
		return ErrEnrollmentConflict
	}
	if registry.groupHasPendingEnrollment(proof.Group, current) {
		return ErrEnrollmentConflict
	}
	next := cloneDynamicEnrollment(current)
	delete(next.legacyDigests, proof.Group)
	delete(next.legacyMembers, proof.Group)
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	registry.dynamic.Store(next)
	return nil
}

// AcknowledgeRosterHandoff is the control-plane wording for
// CompleteRosterHandoff.
func (registry *StaticRegistry) AcknowledgeRosterHandoff(
	proof RosterHandoffProof,
) error {
	return registry.CompleteRosterHandoff(proof)
}

// RetireMember compacts one group mapping after the committed authority has
// removed it and any transition grant has been revoked. The exact roster,
// authority, and directory revisions fence the operation against a concurrent
// enrollment or decommission. It leaves the physical peer record intact so a
// separate all-groups reference scan can decide when to retire its queue.
func (registry *StaticRegistry) RetireMember(
	proof MemberRetirementProof,
	commit ...func() error,
) error {
	if registry == nil || proof.Group == (raftmember.GroupKey{}) ||
		proof.MemberID == 0 || proof.Node == (NodeID{}) ||
		proof.AuthorityVersion == 0 || proof.RosterDigest == ([sha256.Size]byte{}) ||
		proof.DirectoryRevision == 0 || len(commit) > 1 {
		return ErrEnrollmentConflict
	}
	registry.dynamicMu.Lock()
	defer registry.dynamicMu.Unlock()
	current := registry.dynamic.Load()
	if current == nil {
		current = &dynamicEnrollment{directoryRevision: registry.directoryRevision}
	}
	slot := registry.authoritySlot(proof.Group)
	if slot == nil {
		return ErrGroupNotFound
	}
	view := slot.view.Load()
	if view == nil || view.version != proof.AuthorityVersion ||
		proof.DirectoryRevision != registry.currentDirectoryRevision(current) {
		return ErrPeerConflict
	}
	currentDigest, ok := registry.rosterDigest(proof.Group)
	if !ok || currentDigest != proof.RosterDigest {
		return ErrEnrollmentConflict
	}
	key := memberKey{group: proof.Group, memberID: proof.MemberID}
	record, ok := registry.memberRecord(proof.Group, proof.MemberID, current)
	if !ok {
		return ErrMemberNotFound
	}
	if record.node != proof.Node {
		return ErrEnrollmentConflict
	}
	if proof.Node == registry.local {
		return ErrLocalMember
	}
	if _, active := view.roles[proof.MemberID]; active ||
		view.grant.SourceMember == proof.MemberID || view.grant.TargetMember == proof.MemberID ||
		(view.previous != nil && view.previous.roles[proof.MemberID] != MemberEnrolled) ||
		(view.promotion != nil && view.promotion.TargetMember == proof.MemberID) {
		return ErrPeerInUse
	}
	if _, handoffPending := current.legacyDigests[proof.Group]; handoffPending {
		return ErrEnrollmentConflict
	}

	next := cloneDynamicEnrollment(current)
	if _, already := next.retiredMembers[key]; already {
		if len(commit) == 1 && commit[0] != nil {
			return commit[0]()
		}
		return nil
	}
	if _, dynamic := next.nodes[key]; dynamic {
		delete(next.nodes, key)
		delete(next.members, nodeKey{group: proof.Group, node: proof.Node})
		if next.memberCount <= 0 {
			return ErrEnrollmentConflict
		}
		next.memberCount--
	} else if _, immutable := registry.nodes[key]; immutable {
		next.retiredMembers[key] = struct{}{}
		next.retiredCount++
	} else {
		return ErrMemberNotFound
	}
	activeMembers := registry.membersForGroup(proof.Group, next)
	if len(activeMembers) == 0 {
		return ErrLocalMember
	}
	if next.digests == nil {
		next.digests = make(map[raftmember.GroupKey][sha256.Size]byte)
	}
	next.digests[proof.Group] = rosterDigest(activeMembers)
	if next.legacyDigests == nil {
		next.legacyDigests = make(map[raftmember.GroupKey][sha256.Size]byte)
	}
	if next.legacyMembers == nil {
		next.legacyMembers = make(map[raftmember.GroupKey]map[uint64]NodeID)
	}
	next.legacyDigests[proof.Group] = currentDigest
	oldMembers := make(map[uint64]NodeID, len(activeMembers)+1)
	for _, member := range registry.membersForGroup(proof.Group, current) {
		oldMembers[member.MemberID] = member.Node
	}
	next.legacyMembers[proof.Group] = oldMembers
	next.directoryRevision = registry.currentDirectoryRevision(current) + 1
	if len(commit) == 1 && commit[0] != nil {
		if err := commit[0](); err != nil {
			return err
		}
	}
	registry.dynamic.Store(next)
	return nil
}

// LocalMember returns the sole local member ID for group.
func (registry *StaticRegistry) LocalMember(group raftmember.GroupKey) (uint64, error) {
	if registry == nil {
		return 0, ErrGroupNotFound
	}
	view := registry.dynamic.Load()
	memberID, ok := uint64(0), false
	if view != nil {
		memberID, ok = view.localMembers[group]
	}
	if !ok {
		memberID, ok = registry.localMembers[group]
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
	view := registry.dynamic.Load()
	if registry.memberIsRetired(view, key) {
		return NodeID{}, ErrMemberNotFound
	}
	record, ok := memberRecord{}, false
	if view != nil {
		record, ok = view.nodes[key]
	}
	if !ok {
		record, ok = registry.nodes[key]
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
	view := registry.dynamic.Load()
	memberID, ok := uint64(0), false
	if view != nil {
		memberID, ok = view.members[key]
	}
	if !ok {
		memberID, ok = registry.members[key]
	}
	if ok && registry.memberIsRetired(view, memberKey{group: group, memberID: memberID}) {
		return 0, ErrNodeNotFound
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
	view := registry.dynamic.Load()
	digest, ok := [sha256.Size]byte{}, false
	if view != nil {
		digest, ok = view.digests[group]
	}
	if !ok {
		digest, ok = registry.digests[group]
	}
	return digest, ok
}

// RosterDigest returns the exact stable member-to-node enrollment digest for
// one group. It is a detached cut for control-plane ACKs; it does not grant a
// role and must not be used in place of Current ConfState authority.
func (registry *StaticRegistry) RosterDigest(group raftmember.GroupKey) ([sha256.Size]byte, bool) {
	return registry.rosterDigest(group)
}

// outboundRosterDigest chooses the handoff digest for an already-authorized
// pair while a new physical/member mapping is being rolled through the
// cluster.  The legacy cut is restricted to member IDs and node identities
// that were present before enrollment; traffic involving the new target must
// use the current complete roster and remains blocked until ConfState grants a
// role to that member.
func (registry *StaticRegistry) outboundRosterDigest(
	group raftmember.GroupKey,
	from, to uint64,
	current [sha256.Size]byte,
) [sha256.Size]byte {
	if registry == nil {
		return current
	}
	view := registry.dynamic.Load()
	if view == nil {
		return current
	}
	legacy, ok := view.legacyDigests[group]
	if !ok || legacy == ([sha256.Size]byte{}) {
		return current
	}
	members := view.legacyMembers[group]
	if members == nil {
		return current
	}
	if _, ok := members[from]; !ok {
		return current
	}
	if _, ok := members[to]; !ok {
		return current
	}
	return legacy
}

// acceptsRosterDigest accepts the current complete roster or one bounded
// adjacent handoff cut.  A legacy digest is valid only when both endpoints
// were present in that exact old member-to-node mapping and the authenticated
// source/destination still match those identities.  This keeps staggered
// enrollment from partitioning existing traffic without accepting arbitrary
// stale endpoint claims.
func (registry *StaticRegistry) acceptsRosterDigest(
	group raftmember.GroupKey,
	digest [sha256.Size]byte,
	from, to uint64,
	source, destination NodeID,
	current [sha256.Size]byte,
) bool {
	if digest == current {
		return true
	}
	if registry == nil {
		return false
	}
	view := registry.dynamic.Load()
	if view == nil || view.legacyDigests[group] != digest {
		return false
	}
	members := view.legacyMembers[group]
	return members != nil && members[from] == source && members[to] == destination
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
