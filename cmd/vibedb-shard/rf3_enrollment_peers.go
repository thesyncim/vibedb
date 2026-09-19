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
	if !registry.AcceptsEnrollmentRosterDigest(intent.Group, intent.ExpectedRosterDigest,
		intent.Member.MemberID, intent.Peer.NodeID, grant.InitialVoters) {
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

// rf3RecoveredEnrollmentRoster reconstructs endpoints from the authenticated
// enrollment chain, and roles only from durable Raft membership. Historical
// receipts prove identities; they do not accumulate live member mappings.
func rf3RecoveredEnrollmentRoster(
	manifest rf3Manifest, group raftmember.GroupKey, localMember uint64,
	publication raftmodel.Publication, receipts []rf3EnrollmentPeerReceipt,
) (rf3EnrollmentRoster, error) {
	var result rf3EnrollmentRoster
	conf := publication.ConfState
	if raftmodel.ValidateConfState(conf, publication.ReplicaSetVersion) != nil ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() ||
		(!manifest.DevelopmentOnly && (len(conf.GetVoters()) < 3 || len(conf.GetVoters()) > 4 ||
			len(conf.GetLearners()) > 1 || len(conf.GetVoters())+len(conf.GetLearners()) > 4)) {
		return result, fmt.Errorf("%w: unsupported durable membership cut", errRF3EnrollmentPeerReceipt)
	}
	known := make(map[uint64]rf3ManifestMember)
	for _, member := range manifest.memberRoster() {
		known[member.MemberID] = member
	}
	if target := manifest.EnrolledTarget; target != nil {
		known[target.MemberID] = rf3ManifestMember{MemberID: target.MemberID, NodeID: target.NodeID, PeerAddress: target.PeerAddress}
	}
	chain := make([]rf3EnrollmentPeerReceipt, 0)
	for _, receipt := range receipts {
		if receipt.Group == group {
			chain = append(chain, receipt)
		}
	}
	slices.SortFunc(chain, func(a, b rf3EnrollmentPeerReceipt) int {
		if a.ReplicaSetVersion < b.ReplicaSetVersion {
			return -1
		}
		if a.ReplicaSetVersion > b.ReplicaSetVersion {
			return 1
		}
		return compareRF3EnrollmentPeerReceipt(a, b)
	})
	witnesses := make(map[uint64]rf3EnrollmentPeerReceipt, len(chain))
	for index, receipt := range chain {
		if err := receipt.validate(); err != nil {
			return result, err
		}
		if _, exists := witnesses[receipt.MemberID]; exists || index > 0 && chain[index-1].ReplicaSetVersion == receipt.ReplicaSetVersion {
			return result, fmt.Errorf("%w: ambiguous enrollment chain", errRF3EnrollmentPeerReceipt)
		}
		var certified [3]membershipgrant.RosterMember
		stable := make([]rafttransport.Member, 3)
		for i, id := range receipt.Grant.InitialVoters {
			member, found := known[id]
			if !found || member.NodeID == receipt.NodeID {
				return result, fmt.Errorf("%w: unresolved initial voter %d", errRF3EnrollmentPeerReceipt, id)
			}
			certified[i] = membershipgrant.RosterMember{Member: id, Node: member.NodeID}
			stable[i] = rafttransport.Member{Group: group, ReplicaSetVersion: receipt.ReplicaSetVersion, MemberID: id, Node: member.NodeID, Role: rafttransport.MemberVoter}
		}
		digest, err := rafttransport.StableRosterDigest(stable)
		if err != nil || digest != receipt.ExpectedRosterDigest ||
			membershipgrant.CertifiedRosterDigest(group, receipt.ReplicaSetVersion, certified) != receipt.Grant.InitialRosterDigest {
			return result, fmt.Errorf("%w: initial roster differs from enrollment grant", errRF3EnrollmentPeerReceipt)
		}
		member := rf3ManifestMember{MemberID: receipt.MemberID, NodeID: receipt.NodeID, PeerAddress: receipt.PeerAddress}
		if prior, exists := known[member.MemberID]; exists && prior != member {
			return result, errRF3EnrollmentPeerReceipt
		}
		known[member.MemberID], witnesses[member.MemberID] = member, receipt
	}
	required := make(map[uint64]struct{}, 4)
	for _, id := range conf.GetVoters() {
		required[id] = struct{}{}
	}
	for _, id := range conf.GetLearners() {
		required[id] = struct{}{}
	}
	required[localMember] = struct{}{}
	if target := manifest.EnrolledTarget; target != nil {
		required[target.MemberID] = struct{}{}
	}
	if manifest.Route.MembershipGrantPath != "" {
		grant, found, err := readRF3MembershipGrant(manifest.Route.MembershipGrantPath)
		if err != nil {
			return result, err
		}
		if found {
			if grant.Group != group {
				return result, errRF3EnrollmentPeerReceipt
			}
			for _, id := range grant.InitialVoters {
				required[id] = struct{}{}
			}
			required[grant.TargetMember] = struct{}{}
			if receipt, exists := witnesses[grant.TargetMember]; exists && receipt.Grant != grant {
				return result, errRF3EnrollmentPeerReceipt
			}
		}
	}
	ids := make([]uint64, 0, len(required))
	for id := range required {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	result.endpoints = make(map[rafttransport.NodeID]string, len(ids))
	for _, id := range ids {
		member, found := known[id]
		if !found {
			return result, fmt.Errorf("%w: member %d has no authenticated endpoint", errRF3EnrollmentPeerReceipt, id)
		}
		if _, duplicate := result.endpoints[member.NodeID]; duplicate {
			return result, fmt.Errorf("%w: physical node has two live members", errRF3EnrollmentPeerReceipt)
		}
		role := rafttransport.MemberEnrolled
		if slices.Contains(conf.GetVoters(), id) {
			role = rafttransport.MemberVoter
		} else if slices.Contains(conf.GetLearners(), id) {
			role = rafttransport.MemberLearner
		}
		result.members = append(result.members, rafttransport.Member{Group: group, ReplicaSetVersion: publication.ReplicaSetVersion, MemberID: id, Node: member.NodeID, Role: role})
		result.endpoints[member.NodeID] = member.PeerAddress
		if receipt, dynamic := witnesses[id]; dynamic {
			result.peers = append(result.peers, receipt.physicalPeer())
			if role == rafttransport.MemberVoter || result.dynamicMember == 0 {
				result.dynamicMember = id
			}
		}
		if id == localMember {
			result.native = role == rafttransport.MemberVoter || manifest.EnrolledTarget != nil && id == manifest.EnrolledTarget.MemberID
		}
	}
	return result, nil
}

type rf3EnrollmentRoster struct {
	members       []rafttransport.Member
	endpoints     map[rafttransport.NodeID]string
	peers         []rafttransport.PhysicalPeer
	dynamicMember uint64
	native        bool
}
