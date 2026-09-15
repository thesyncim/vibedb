package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

const (
	rf3EnrollmentPeerReceiptFile     = "enrolled-peer-receipts.vibejson"
	rf3EnrollmentPeerReceiptVersion  = 1
	maxRF3EnrollmentPeerReceipts     = maxRF3ManifestGroups * 4
	maxRF3EnrollmentPeerReceiptBytes = 256 << 10
)

var errRF3EnrollmentPeerReceipt = errors.New("vibedb-shard: invalid durable enrolled-peer receipt")

// validateRF3EnrollmentGrant checks the full authority carried by an
// enrollment request against the voter registry after that request's member
// mapping has been committed. The catalog controller is authenticated by the
// control service, but the serving node still binds the grant to its own
// current roster before accepting a durable endpoint receipt.
func validateRF3EnrollmentGrant(registry *rafttransport.StaticRegistry, intent rafttransport.EnrollmentIntent) error {
	grant := intent.Grant
	if registry == nil || !grant.Valid() || grant.Group != intent.Group ||
		grant.Digest() != intent.Digest || grant.TargetMember != intent.Member.MemberID ||
		[16]byte(grant.TargetNode) != intent.Peer.NodeID ||
		grant.InitialReplicaSetVersion != intent.Member.ReplicaSetVersion {
		return errRF3EnrollmentPeerReceipt
	}
	version, found := registry.ReplicaSetVersion(intent.Group)
	if !found || version != grant.InitialReplicaSetVersion {
		return errRF3EnrollmentPeerReceipt
	}
	if !registry.AcceptsRosterDigest(intent.Group, intent.ExpectedRosterDigest) {
		return errRF3EnrollmentPeerReceipt
	}
	var initial [3]membershipgrant.RosterMember
	for index, memberID := range grant.InitialVoters {
		node, err := registry.Node(intent.Group, memberID)
		if err != nil {
			return errors.Join(errRF3EnrollmentPeerReceipt, err)
		}
		initial[index] = membershipgrant.RosterMember{Member: memberID, Node: [16]byte(node)}
	}
	if membershipgrant.CertifiedRosterDigest(
		grant.Group, grant.InitialReplicaSetVersion, initial,
	) != grant.InitialRosterDigest {
		return errRF3EnrollmentPeerReceipt
	}
	return nil
}

// rf3EnrollmentPeerReceipt is the source-local, authenticated endpoint
// witness for a dynamic group member.  The grant is retained in full so a
// source can reconstruct the endpoint after the active grant file is removed
// at final retirement.  Its digest is the exact EnrollmentIntent digest sent
// by the catalog controller and is checked before the receipt is used.
type rf3EnrollmentPeerReceipt struct {
	Group                raftmember.GroupKey   `json:"group"`
	MemberID             uint64                `json:"member_id"`
	ReplicaSetVersion    uint64                `json:"replica_set_version"`
	NodeID               rafttransport.NodeID  `json:"node_id"`
	NodeIncarnation      uint64                `json:"node_incarnation"`
	NodeRevision         uint64                `json:"node_revision"`
	ServiceKeyDigest     [32]byte              `json:"service_key_digest"`
	PeerAddress          string                `json:"peer_address"`
	ExpectedRosterDigest [32]byte              `json:"expected_roster_digest"`
	DirectoryRevision    uint64                `json:"directory_revision"`
	EnrollmentDigest     [32]byte              `json:"enrollment_digest"`
	Grant                membershipgrant.Grant `json:"grant"`
	ReceiptDigest        [32]byte              `json:"receipt_digest"`
}

type rf3EnrollmentPeerDirectory struct {
	Version  uint8                      `json:"version"`
	Receipts []rf3EnrollmentPeerReceipt `json:"receipts"`
}

type rf3EnrollmentPeerStore struct {
	mu       sync.Mutex
	path     string
	receipts []rf3EnrollmentPeerReceipt
}

func openRF3EnrollmentPeerStore(root string) (*rf3EnrollmentPeerStore, error) {
	if root == "" {
		return nil, nil
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, errRF3EnrollmentPeerReceipt
	}
	path := filepath.Join(root, rf3EnrollmentPeerReceiptFile)
	store := &rf3EnrollmentPeerStore{path: path}
	raw, err := readRF3BoundedFile(path, maxRF3EnrollmentPeerReceiptBytes)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, errors.Join(errRF3EnrollmentPeerReceipt, err)
	}
	var directory rf3EnrollmentPeerDirectory
	if err = vibejson.Unmarshal(raw, &directory); err != nil ||
		directory.Version != rf3EnrollmentPeerReceiptVersion ||
		len(directory.Receipts) > maxRF3EnrollmentPeerReceipts {
		return nil, errors.Join(errRF3EnrollmentPeerReceipt, err)
	}
	if err = validateRF3EnrollmentPeerDirectory(raw, directory); err != nil {
		return nil, err
	}
	store.receipts = slices.Clone(directory.Receipts)
	return store, nil
}

func validateRF3EnrollmentPeerDirectory(raw []byte, directory rf3EnrollmentPeerDirectory) error {
	canonical, err := vibejson.Marshal(&directory)
	if err != nil || !bytes.Equal(raw, canonical) {
		return errors.Join(errRF3EnrollmentPeerReceipt, err)
	}
	for index := range directory.Receipts {
		if err := directory.Receipts[index].validate(); err != nil {
			return errors.Join(errRF3EnrollmentPeerReceipt, err)
		}
		if index > 0 && compareRF3EnrollmentPeerReceipt(directory.Receipts[index-1], directory.Receipts[index]) >= 0 {
			return errRF3EnrollmentPeerReceipt
		}
	}
	return nil
}

func compareRF3EnrollmentPeerReceipt(left, right rf3EnrollmentPeerReceipt) int {
	if result := bytes.Compare(left.Group.ClusterID[:], right.Group.ClusterID[:]); result != 0 {
		return result
	}
	if result := bytes.Compare(left.Group.ClusterIncarnation[:], right.Group.ClusterIncarnation[:]); result != 0 {
		return result
	}
	if left.Group.TopologyRecoveryEpoch < right.Group.TopologyRecoveryEpoch {
		return -1
	}
	if left.Group.TopologyRecoveryEpoch > right.Group.TopologyRecoveryEpoch {
		return 1
	}
	if result := bytes.Compare(left.Group.ShardIncarnation[:], right.Group.ShardIncarnation[:]); result != 0 {
		return result
	}
	if result := bytes.Compare(left.Group.GroupID[:], right.Group.GroupID[:]); result != 0 {
		return result
	}
	if left.MemberID < right.MemberID {
		return -1
	}
	if left.MemberID > right.MemberID {
		return 1
	}
	return bytes.Compare(left.EnrollmentDigest[:], right.EnrollmentDigest[:])
}

func (store *rf3EnrollmentPeerStore) snapshot() []rf3EnrollmentPeerReceipt {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return slices.Clone(store.receipts)
}

// sameRF3EnrollmentPeerReceiptIdentity compares the authenticated endpoint
// witness and its grant. DirectoryRevision is a registry-local CAS fence; it
// is rebuilt when a serving process restarts and therefore is not part of the
// per-group enrollment identity. ReceiptDigest is derived from that fence and
// is excluded for the same reason.
func sameRF3EnrollmentPeerReceiptIdentity(left, right rf3EnrollmentPeerReceipt) bool {
	left.DirectoryRevision = 0
	right.DirectoryRevision = 0
	left.ReceiptDigest = [32]byte{}
	right.ReceiptDigest = [32]byte{}
	return left == right
}

func (store *rf3EnrollmentPeerStore) record(intent rafttransport.EnrollmentIntent, grant membershipgrant.Grant) error {
	if store == nil {
		return errRF3EnrollmentPeerReceipt
	}
	receipt, err := newRF3EnrollmentPeerReceipt(intent, grant)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, prior := range store.receipts {
		if prior.Group != receipt.Group || prior.MemberID != receipt.MemberID {
			continue
		}
		if prior.EnrollmentDigest == receipt.EnrollmentDigest {
			if !sameRF3EnrollmentPeerReceiptIdentity(prior, receipt) {
				return errRF3EnrollmentPeerReceipt
			}
			return nil
		}
	}
	if len(store.receipts) >= maxRF3EnrollmentPeerReceipts {
		return errRF3EnrollmentPeerReceipt
	}
	next := append(slices.Clone(store.receipts), receipt)
	slices.SortFunc(next, compareRF3EnrollmentPeerReceipt)
	directory := rf3EnrollmentPeerDirectory{Version: rf3EnrollmentPeerReceiptVersion, Receipts: next}
	raw, err := vibejson.Marshal(&directory)
	if err != nil || len(raw) == 0 || len(raw) > maxRF3EnrollmentPeerReceiptBytes {
		return errors.Join(errRF3EnrollmentPeerReceipt, err)
	}
	if err = writeRF3DurableMarker(store.path, raw); err != nil {
		return errors.Join(errRF3EnrollmentPeerReceipt, err)
	}
	store.receipts = next
	return nil
}

func newRF3EnrollmentPeerReceipt(intent rafttransport.EnrollmentIntent, grant membershipgrant.Grant) (rf3EnrollmentPeerReceipt, error) {
	receipt := rf3EnrollmentPeerReceipt{
		Group: intent.Group, MemberID: intent.Member.MemberID,
		ReplicaSetVersion: intent.Member.ReplicaSetVersion,
		NodeID:            intent.Peer.NodeID, NodeIncarnation: intent.Peer.Incarnation,
		NodeRevision: intent.Peer.Revision, ServiceKeyDigest: intent.Peer.ServiceKeyDigest,
		PeerAddress: intent.Peer.Endpoint, ExpectedRosterDigest: intent.ExpectedRosterDigest,
		DirectoryRevision: intent.DirectoryRevision, EnrollmentDigest: intent.Digest,
		Grant: grant,
	}
	if intent.Domain != (rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}) ||
		intent.Member.Group != intent.Group || intent.Member.Node != intent.Peer.NodeID ||
		intent.Member.Role != rafttransport.MemberEnrolled || intent.Peer.TrustDomain != intent.Domain ||
		intent.Peer.State != rafttransport.PeerEnrolled || intent.Peer.EnrollmentDigest != intent.Digest ||
		!grant.Valid() || grant.Group != intent.Group || grant.TargetMember != intent.Member.MemberID ||
		grant.TargetNode != [16]byte(intent.Peer.NodeID) || grant.InitialReplicaSetVersion != intent.Member.ReplicaSetVersion ||
		grant.Digest() != intent.Digest {
		return rf3EnrollmentPeerReceipt{}, errRF3EnrollmentPeerReceipt
	}
	receipt.ReceiptDigest = receipt.computedDigest()
	if err := receipt.validate(); err != nil {
		return rf3EnrollmentPeerReceipt{}, err
	}
	return receipt, nil
}

func (receipt rf3EnrollmentPeerReceipt) validate() error {
	domain := rafttransport.TrustDomain{ClusterID: receipt.Group.ClusterID, ClusterIncarnation: receipt.Group.ClusterIncarnation}
	if receipt.Group == (raftmember.GroupKey{}) || receipt.Group.ClusterID == ([16]byte{}) ||
		receipt.Group.ClusterIncarnation == ([16]byte{}) || receipt.Group.TopologyRecoveryEpoch == 0 ||
		receipt.Group.ShardIncarnation == ([16]byte{}) || receipt.Group.GroupID == ([16]byte{}) ||
		receipt.MemberID == 0 || receipt.ReplicaSetVersion == 0 || receipt.NodeID == (rafttransport.NodeID{}) ||
		receipt.NodeIncarnation == 0 || receipt.NodeRevision == 0 || receipt.ServiceKeyDigest == ([32]byte{}) ||
		receipt.ExpectedRosterDigest == ([32]byte{}) || receipt.DirectoryRevision == 0 ||
		receipt.EnrollmentDigest == ([32]byte{}) || validateRF3Address(receipt.PeerAddress, false) != nil ||
		!receipt.Grant.Valid() || receipt.Grant.Group != receipt.Group ||
		receipt.Grant.TargetMember != receipt.MemberID || receipt.Grant.TargetNode != [16]byte(receipt.NodeID) ||
		receipt.Grant.InitialReplicaSetVersion != receipt.ReplicaSetVersion || receipt.Grant.Digest() != receipt.EnrollmentDigest ||
		domain == (rafttransport.TrustDomain{}) || receipt.ReceiptDigest == ([32]byte{}) ||
		receipt.computedDigest() != receipt.ReceiptDigest {
		return errRF3EnrollmentPeerReceipt
	}
	return nil
}

func (receipt rf3EnrollmentPeerReceipt) computedDigest() [32]byte {
	copyOfReceipt := receipt
	copyOfReceipt.ReceiptDigest = [32]byte{}
	raw, err := vibejson.Marshal(&copyOfReceipt)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(append([]byte("vibedb/rf3/enrolled-peer-receipt/v1\x00"), raw...))
}

func (receipt rf3EnrollmentPeerReceipt) physicalPeer() rafttransport.PhysicalPeer {
	domain := rafttransport.TrustDomain{ClusterID: receipt.Group.ClusterID, ClusterIncarnation: receipt.Group.ClusterIncarnation}
	return rafttransport.PhysicalPeer{
		NodeID: receipt.NodeID, Node: receipt.NodeID, TrustDomain: domain,
		Incarnation: receipt.NodeIncarnation, Revision: receipt.NodeRevision,
		ServiceKeyDigest: receipt.ServiceKeyDigest, EnrollmentDigest: receipt.EnrollmentDigest,
		Endpoint: receipt.PeerAddress, Address: receipt.PeerAddress, State: rafttransport.PeerEnrolled,
	}
}

func sameRF3PhysicalPeerIdentity(left, right rafttransport.PhysicalPeer) bool {
	return left.NodeID == right.NodeID && left.Node == right.Node && left.TrustDomain == right.TrustDomain &&
		left.Incarnation == right.Incarnation && left.Revision == right.Revision &&
		left.ServiceKeyDigest == right.ServiceKeyDigest && left.Endpoint == right.Endpoint &&
		left.Address == right.Address && left.State == right.State
}

func rf3MembershipGrantForGroup(manifest rf3Manifest, group raftmember.GroupKey) (membershipgrant.Grant, bool, error) {
	if group == (raftmember.GroupKey{}) {
		return membershipgrant.Grant{}, false, errRF3EnrollmentPeerReceipt
	}
	var path string
	for _, bundle := range manifest.groupBundles() {
		if bundle.Route.Group != group {
			continue
		}
		if path != "" && path != bundle.Route.MembershipGrantPath {
			return membershipgrant.Grant{}, false, errRF3EnrollmentPeerReceipt
		}
		path = bundle.Route.MembershipGrantPath
	}
	if path == "" {
		return membershipgrant.Grant{}, false, errRF3EnrollmentPeerReceipt
	}
	return readRF3MembershipGrant(path)
}

// rf3DynamicEnrollmentTarget selects the one dynamic member present in the
// durable ConfState and proves its physical endpoint with a source receipt.
// A missing or ambiguous receipt is a startup error: member IDs alone never
// authorize a peer endpoint.
func rf3DynamicEnrollmentTarget(
	manifest rf3Manifest, group raftmember.GroupKey, publication raftmodel.Publication,
	receipts []rf3EnrollmentPeerReceipt,
) (*rf3ManifestEnrolledTarget, rafttransport.PhysicalPeer, error) {
	if manifest.EnrolledTarget != nil || publication.ConfState == nil {
		return nil, rafttransport.PhysicalPeer{}, nil
	}
	base := make(map[uint64]struct{}, len(manifest.memberRoster()))
	for _, member := range manifest.memberRoster() {
		base[member.MemberID] = struct{}{}
	}
	dynamicMembers := make(map[uint64]struct{}, 1)
	for _, member := range append(slices.Clone(publication.ConfState.GetVoters()), publication.ConfState.GetLearners()...) {
		if _, isBase := base[member]; !isBase {
			dynamicMembers[member] = struct{}{}
		}
	}
	if len(dynamicMembers) == 0 {
		return nil, rafttransport.PhysicalPeer{}, nil
	}
	if len(dynamicMembers) != 1 {
		return nil, rafttransport.PhysicalPeer{}, errRF3EnrollmentPeerReceipt
	}
	var memberID uint64
	for member := range dynamicMembers {
		memberID = member
	}
	var currentGrant membershipgrant.Grant
	grantFound := false
	if manifest.Route.MembershipGrantPath != "" {
		var err error
		currentGrant, grantFound, err = readRF3MembershipGrant(manifest.Route.MembershipGrantPath)
		if err != nil {
			return nil, rafttransport.PhysicalPeer{}, err
		}
	}
	var selected *rf3EnrollmentPeerReceipt
	for index := range receipts {
		receipt := &receipts[index]
		if receipt.Group != group || receipt.MemberID != memberID {
			continue
		}
		if err := receipt.validate(); err != nil {
			return nil, rafttransport.PhysicalPeer{}, err
		}
		if grantFound && currentGrant.TargetMember == memberID && receipt.Grant != currentGrant {
			continue
		}
		if selected != nil {
			return nil, rafttransport.PhysicalPeer{}, errRF3EnrollmentPeerReceipt
		}
		selected = receipt
	}
	if selected == nil {
		return nil, rafttransport.PhysicalPeer{}, fmt.Errorf("%w: dynamic member %d has no authenticated endpoint receipt", errRF3EnrollmentPeerReceipt, memberID)
	}
	if grantFound && currentGrant.TargetMember == memberID && selected.Grant != currentGrant {
		return nil, rafttransport.PhysicalPeer{}, errRF3EnrollmentPeerReceipt
	}
	if err := selected.validateAgainstManifest(manifest, group); err != nil {
		return nil, rafttransport.PhysicalPeer{}, err
	}
	target := &rf3ManifestEnrolledTarget{
		MemberID: selected.MemberID, NodeID: selected.NodeID,
		NodeIncarnation: selected.NodeIncarnation, PeerAddress: selected.PeerAddress,
	}
	return target, selected.physicalPeer(), nil
}

func (receipt rf3EnrollmentPeerReceipt) validateAgainstManifest(manifest rf3Manifest, group raftmember.GroupKey) error {
	if err := receipt.validate(); err != nil || receipt.Group != group {
		return errRF3EnrollmentPeerReceipt
	}
	if len(manifest.memberRoster()) != rf3ManifestMembers {
		return errRF3EnrollmentPeerReceipt
	}
	authority := coldRF3GrantAuthority{group: group, target: rf3ManifestEnrolledTarget{MemberID: receipt.MemberID, NodeID: receipt.NodeID}}
	copy(authority.members[:], manifest.memberRoster())
	if err := authority.InstallTransitionGrant(receipt.Grant); err != nil {
		return errors.Join(errRF3EnrollmentPeerReceipt, err)
	}
	stable := make([]rafttransport.Member, len(manifest.memberRoster()))
	for index, member := range manifest.memberRoster() {
		stable[index] = rafttransport.Member{Group: group, ReplicaSetVersion: receipt.Grant.InitialReplicaSetVersion,
			MemberID: member.MemberID, Node: member.NodeID, Role: rafttransport.MemberVoter}
	}
	rosterDigest, err := rafttransport.StableRosterDigest(stable)
	if err != nil || rosterDigest != receipt.ExpectedRosterDigest {
		return errRF3EnrollmentPeerReceipt
	}
	return nil
}
