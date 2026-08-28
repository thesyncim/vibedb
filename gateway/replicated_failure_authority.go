package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	// FailureConfirmationRevisions is a replicated revision window, not a
	// process-local duration. A stopped or partitioned gateway cannot advance it.
	FailureConfirmationRevisions      uint64 = 3
	maxReplicatedFailureRecordBytes          = 8 << 10
	replicatedFailurePageCount               = 64
	maxReplicatedFailureGroupsPerPage        = 64
	maxReplicatedFailurePageBytes            = 32 << 10
)

var ErrReplicatedFailureAuthority = errors.New("gateway: invalid replicated failure authority")

// ReplicaHealthAttestation carries an identity obtained from an authenticated
// shard-control stream. The authority accepts it only when member, node, and
// incarnation are the exact current catalog tuple.
type ReplicaHealthAttestation struct {
	Member            uint64
	Node              rafttransport.NodeID
	NodeIncarnation   uint64
	CatalogGeneration uint64
	ReplicaSetVersion uint64
	LeaderMember      uint64
	LeaderTerm        uint64
	CommitIndex       uint64
	Failed            bool
}

// ReplicaHealthRevision is one quorum observation of one serving replica.
// Revision is monotonically advanced through the catalog Raft group. All
// attestations describe this exact leader/term/RSV/commit cut.
type ReplicaHealthRevision struct {
	Distribution       distribution.DistributionName
	Shard              distribution.ShardID
	Group              raftmember.GroupKey
	CatalogGeneration  uint64
	ReplicaSetVersion  uint64
	Revision           uint64
	LeaderMember       uint64
	LeaderTerm         uint64
	CommitIndex        uint64
	SuspectMember      uint64
	SuspectNode        rafttransport.NodeID
	SuspectIncarnation uint64
	Attestations       []ReplicaHealthAttestation
}

// ReplicatedFailureConfirmation is the detached confirmation vocabulary used
// by a cmd adapter. Every confirmation was reopened from one canonical RF3
// control-plane row; it is not reconstructed from local timeouts.
type ReplicatedFailureConfirmation struct {
	Member            uint64
	FirstRevision     uint64
	ConfirmedRevision uint64
	LeaderTerm        uint64
	ReplicaSetVersion uint64
	CommitIndex       uint64
}

type ReplicatedFailureCertificate struct {
	Distribution       distribution.DistributionName
	Shard              distribution.ShardID
	Group              raftmember.GroupKey
	CatalogGeneration  uint64
	ReplicaSetVersion  uint64
	LeaderMember       uint64
	LeaderTerm         uint64
	CommitIndex        uint64
	FirstRevision      uint64
	ConfirmedRevision  uint64
	SuspectMember      uint64
	SuspectNode        rafttransport.NodeID
	SuspectIncarnation uint64
	Confirmations      []ReplicatedFailureConfirmation
}

// ReplicaHealthRevisionSink and ReplicaFailureCertificateSource are the two
// narrow seams consumed by the shipped health loop. Keeping collection and
// certificate consumption behind interfaces lets cmd wire authenticated shard
// streams without acquiring access to the authority's replicated session.
type ReplicaHealthRevisionSink interface {
	PublishReplicaHealthRevision(context.Context, ReplicaHealthRevision) error
}

// ReplicaHealthRevisionAuthority adds the restart-safe sequence source needed
// by a health collector. A process never invents a revision from wall-clock
// time or process-local memory: it reopens the last catalog-Raft value and the
// existing publish CAS admits exactly the next value.
type ReplicaHealthRevisionAuthority interface {
	ReplicaHealthRevisionSink
	ReadReplicaHealthRevision(context.Context, raftmember.GroupKey, uint64) (uint64, error)
}

// ReplicaHealthRevisionStatusSource lets a collector avoid rewriting an
// already healthy identity. Failure confirmations still advance every round;
// a healthy observation must durably clear any preceding failure window.
type ReplicaHealthRevisionStatusSource interface {
	ReadReplicaHealthRevisionStatus(context.Context, raftmember.GroupKey, uint64) (ReplicaHealthRevisionStatus, error)
}

type ReplicaHealthRevisionStatus struct {
	Revision, CatalogGeneration, ReplicaSetVersion uint64
	SuspectNode                                    rafttransport.NodeID
	SuspectIncarnation                             uint64
	Healthy                                        bool
}

func (status ReplicaHealthRevisionStatus) AlreadyHealthy(revision ReplicaHealthRevision) bool {
	return status.Revision != 0 && status.Healthy && len(revision.Attestations) != 0 && !revision.Attestations[0].Failed &&
		status.CatalogGeneration == revision.CatalogGeneration && status.ReplicaSetVersion == revision.ReplicaSetVersion &&
		status.SuspectNode == revision.SuspectNode && status.SuspectIncarnation == revision.SuspectIncarnation
}

type ReplicaFailureCertificateSource interface {
	VisitReplicaFailureCertificates(context.Context, *Snapshot,
		func(ReplicatedFailureCertificate) error) error
}

// ReadReplicaHealthRevision returns the last committed revision for an exact
// group/member identity. Zero means no revision has been committed. Stale
// catalog generations intentionally retain their sequence so a new process
// cannot resurrect an old revision number after restart.
func (authority *ReplicatedCatalogAuthority) ReadReplicaHealthRevision(
	ctx context.Context, group raftmember.GroupKey, suspect uint64,
) (uint64, error) {
	status, err := authority.ReadReplicaHealthRevisionStatus(ctx, group, suspect)
	return status.Revision, err
}

func (authority *ReplicatedCatalogAuthority) ReadReplicaHealthRevisionStatus(
	ctx context.Context, group raftmember.GroupKey, suspect uint64,
) (ReplicaHealthRevisionStatus, error) {
	if authority == nil || ctx == nil || !validMembershipGrantGroup(group) || suspect == 0 {
		return ReplicaHealthRevisionStatus{}, ErrReplicatedFailureAuthority
	}
	key, _ := replicatedFailureKeys(group, suspect)
	result, err := authority.readRaw(ctx, key[:], maxReplicatedFailureRecordBytes)
	if err != nil || !result.Found {
		return ReplicaHealthRevisionStatus{}, err
	}
	record, err := openReplicatedFailureRecord(result.Value)
	if err != nil || openPersistedMembershipGrantGroup(record.Group) != group ||
		record.SuspectMember != suspect {
		return ReplicaHealthRevisionStatus{}, errors.Join(err, ErrReplicatedFailureAuthority)
	}
	return ReplicaHealthRevisionStatus{Revision: record.Revision, CatalogGeneration: record.CatalogGeneration,
		ReplicaSetVersion: record.ReplicaSetVersion, SuspectNode: record.SuspectNode,
		SuspectIncarnation: record.SuspectIncarnation, Healthy: record.FirstRevision == 0}, nil
}

type replicatedFailureRecord struct {
	Group              persistedMembershipGrantGroup  `json:"group"`
	CatalogGeneration  uint64                         `json:"catalog_generation"`
	ReplicaSetVersion  uint64                         `json:"replica_set_version"`
	Revision           uint64                         `json:"revision"`
	FirstRevision      uint64                         `json:"first_revision"`
	LeaderMember       uint64                         `json:"leader_member"`
	LeaderTerm         uint64                         `json:"leader_term"`
	CommitIndex        uint64                         `json:"commit_index"`
	SuspectMember      uint64                         `json:"suspect_member"`
	SuspectNode        [16]byte                       `json:"suspect_node"`
	SuspectIncarnation uint64                         `json:"suspect_incarnation"`
	Confirmations      []persistedFailureConfirmation `json:"confirmations"`
}

type persistedFailureConfirmation struct {
	Member          uint64   `json:"member"`
	Node            [16]byte `json:"node"`
	NodeIncarnation uint64   `json:"node_incarnation"`
}

type replicatedFailurePage struct {
	Identities []persistedFailureIdentity `json:"identities"`
}

type persistedFailureIdentity struct {
	Group   persistedMembershipGrantGroup `json:"group"`
	Suspect uint64                        `json:"suspect"`
}

// PublishReplicaHealthRevision is the sink for the shipped health loop. It
// atomically CAS-advances one per-group/per-suspect revision and its bounded
// retention page. Insufficient, stale, reordered, or unauthenticated evidence
// cannot consume a revision.
func (authority *ReplicatedCatalogAuthority) PublishReplicaHealthRevision(
	ctx context.Context, revision ReplicaHealthRevision,
) error {
	if authority == nil || authority.session == nil || ctx == nil {
		return ErrReplicatedFailureAuthority
	}
	catalog, err := authority.Read(ctx)
	if err != nil || !validReplicaHealthRevision(catalog, revision) {
		return errors.Join(err, ErrReplicatedFailureAuthority)
	}
	ctx, err = authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	recordKey, pageKey := replicatedFailureKeys(revision.Group, revision.SuspectMember)
	current, err := authority.readRaw(ctx, recordKey[:], maxReplicatedFailureRecordBytes)
	if err != nil {
		return err
	}
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedFailurePageBytes)
	if err != nil {
		return err
	}
	var prior replicatedFailureRecord
	exactRetry := false
	if current.Found {
		prior, err = openReplicatedFailureRecord(current.Value)
		if err != nil || openPersistedMembershipGrantGroup(prior.Group) != revision.Group ||
			prior.SuspectMember != revision.SuspectMember ||
			(revision.Revision != prior.Revision && revision.Revision != prior.Revision+1) {
			return errors.Join(err, ErrReplicatedCatalogConflict)
		}
		exactRetry = revision.Revision == prior.Revision
	} else if revision.Revision != 1 {
		return ErrReplicatedCatalogConflict
	}
	identities, err := openOptionalReplicatedFailurePage(pageKey.bucket(), page)
	if err != nil {
		return err
	}
	identity := persistedFailureIdentity{Group: persistMembershipGrantGroup(revision.Group), Suspect: revision.SuspectMember}
	position, found := findReplicatedFailureIdentity(identities, identity)
	if !found {
		if len(identities) >= maxReplicatedFailureGroupsPerPage {
			return ErrReplicatedCatalogConflict
		}
		identities = append(identities, persistedFailureIdentity{})
		copy(identities[position+1:], identities[position:])
		identities[position] = identity
	}
	record := makeReplicatedFailureRecord(revision, prior)
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedFailureRecord(authority.scratch, record)
	if err != nil {
		return err
	}
	recordBytes := len(authority.scratch)
	if exactRetry {
		if !found || !bytes.Equal(current.Value, authority.scratch[:recordBytes]) {
			return ErrReplicatedCatalogConflict
		}
		return nil
	}
	authority.scratch, err = appendReplicatedFailurePage(authority.scratch, pageKey.bucket(), identities)
	if err != nil {
		return err
	}
	mutations := []NativeMutation{{Kind: replication.MutationPutAbsentOrEqual, Key: recordKey[:], Value: authority.scratch[:recordBytes]}}
	if current.Found {
		digest := sha256.Sum256(current.Value)
		mutations[0].Kind = replication.MutationPutDigestEqual
		mutations[0].ExpectedValueLength = uint64(len(current.Value))
		mutations[0].ExpectedValueDigest = replication.Digest(digest)
	}
	pageMutation := NativeMutation{Kind: replication.MutationPutAbsentOrEqual, Key: pageKey[:], Value: authority.scratch[recordBytes:]}
	if page.Found {
		digest := sha256.Sum256(page.Value)
		pageMutation.Kind = replication.MutationPutDigestEqual
		pageMutation.ExpectedValueLength = uint64(len(page.Value))
		pageMutation.ExpectedValueDigest = replication.Digest(digest)
	}
	mutations = append(mutations, pageMutation)
	result, err := authority.session.MutateBatch(ctx, mutations)
	if err != nil {
		if authority.session.Status().Pending {
			return errors.Join(ErrReplicatedCatalogPending, err)
		}
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedFailureAuthority
	}
	return nil
}

// VisitReplicaFailureCertificates is the restart-safe source for cmd health
// orchestration. It scans the current catalog (not an unbounded process list)
// and emits only byte-canonical records whose exact catalog identity is still
// current and whose replicated failure window is complete.
func (authority *ReplicatedCatalogAuthority) VisitReplicaFailureCertificates(
	ctx context.Context, catalog *Snapshot, visit func(ReplicatedFailureCertificate) error,
) error {
	if authority == nil || ctx == nil || catalog == nil || visit == nil {
		return ErrReplicatedFailureAuthority
	}
	descriptors := catalog.ReplicatedShardDescriptors()
	for index := range descriptors {
		if err := ctx.Err(); err != nil {
			return err
		}
		descriptor := descriptors[index]
		for _, suspect := range descriptor.Replicas {
			key, _ := replicatedFailureKeys(descriptor.Group, suspect.Member)
			result, err := authority.readRaw(ctx, key[:], maxReplicatedFailureRecordBytes)
			if err != nil {
				return err
			}
			if !result.Found {
				continue
			}
			record, err := openReplicatedFailureRecord(result.Value)
			if err != nil {
				return err
			}
			certificate, ok := recordFailureCertificate(catalog, descriptor, suspect, record)
			if ok {
				if err = visit(certificate); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// DeleteReplicaHealthRecord provides exact-revision GC. A concurrent health
// advance wins the CAS and cannot be erased. Page occupancy is removed in the
// same replicated mutation, keeping retained metadata bounded and restartable.
func (authority *ReplicatedCatalogAuthority) DeleteReplicaHealthRecord(
	ctx context.Context, group raftmember.GroupKey, suspect, expectedRevision uint64,
) error {
	if authority == nil || authority.session == nil || ctx == nil || !validMembershipGrantGroup(group) || suspect == 0 || expectedRevision == 0 {
		return ErrReplicatedFailureAuthority
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	recordKey, pageKey := replicatedFailureKeys(group, suspect)
	current, err := authority.readRaw(ctx, recordKey[:], maxReplicatedFailureRecordBytes)
	if err != nil {
		return err
	}
	if !current.Found {
		return nil
	}
	record, err := openReplicatedFailureRecord(current.Value)
	if err != nil || record.Revision != expectedRevision {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	page, err := authority.readRaw(ctx, pageKey[:], maxReplicatedFailurePageBytes)
	if err != nil || !page.Found {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	identities, err := openReplicatedFailurePage(pageKey.bucket(), page.Value)
	if err != nil {
		return err
	}
	identity := persistedFailureIdentity{Group: persistMembershipGrantGroup(group), Suspect: suspect}
	position, found := findReplicatedFailureIdentity(identities, identity)
	if !found {
		return ErrReplicatedCatalogConflict
	}
	copy(identities[position:], identities[position+1:])
	identities = identities[:len(identities)-1]
	authority.scratch = authority.scratch[:0]
	authority.scratch, err = appendReplicatedFailurePage(authority.scratch, pageKey.bucket(), identities)
	if err != nil {
		return err
	}
	recordDigest, pageDigest := sha256.Sum256(current.Value), sha256.Sum256(page.Value)
	result, err := authority.session.MutateBatch(ctx, []NativeMutation{
		{Kind: replication.MutationDeleteDigestEqual, Key: recordKey[:], ExpectedValueLength: uint64(len(current.Value)), ExpectedValueDigest: replication.Digest(recordDigest)},
		{Kind: replication.MutationPutDigestEqual, Key: pageKey[:], Value: authority.scratch, ExpectedValueLength: uint64(len(page.Value)), ExpectedValueDigest: replication.Digest(pageDigest)},
	})
	if err != nil {
		return err
	}
	if result.Completion.ResultCode == replicatedstate.ResultIndexConflict {
		return ErrReplicatedCatalogConflict
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied {
		return ErrReplicatedFailureAuthority
	}
	return nil
}

func validReplicaHealthRevision(catalog *Snapshot, revision ReplicaHealthRevision) bool {
	if catalog == nil || revision.CatalogGeneration != catalog.Generation() || revision.Revision == 0 ||
		revision.ReplicaSetVersion == 0 || revision.LeaderMember == 0 || revision.LeaderTerm == 0 ||
		revision.CommitIndex == 0 || revision.SuspectMember == 0 || revision.SuspectIncarnation == 0 ||
		len(revision.Attestations) < ServingReplicaCount/2+1 || len(revision.Attestations) > ServingReplicaCount {
		return false
	}
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	route, ok := catalog.ResolveReplicatedRoute(revision.Distribution, revision.Shard, workspace[:0])
	if !ok || route.Group != revision.Group || route.Command.ReplicaSetVersion != revision.ReplicaSetVersion {
		return false
	}
	suspect, ok := endpointByMember(route.Replicas, revision.SuspectMember)
	if !ok || suspect.Node != revision.SuspectNode || suspect.NodeIncarnation != revision.SuspectIncarnation {
		return false
	}
	failed := revision.Attestations[0].Failed
	previous := uint64(0)
	leader := false
	for _, attestation := range revision.Attestations {
		endpoint, found := endpointByMember(route.Replicas, attestation.Member)
		if !found || (failed && attestation.Member == revision.SuspectMember) || attestation.Member <= previous ||
			attestation.Failed != failed || endpoint.Node != attestation.Node || endpoint.NodeIncarnation != attestation.NodeIncarnation ||
			attestation.CatalogGeneration != revision.CatalogGeneration ||
			attestation.ReplicaSetVersion != revision.ReplicaSetVersion ||
			attestation.LeaderMember != revision.LeaderMember || attestation.LeaderTerm != revision.LeaderTerm ||
			attestation.CommitIndex != revision.CommitIndex {
			return false
		}
		previous = attestation.Member
		leader = leader || attestation.Member == revision.LeaderMember
	}
	return leader
}

func endpointByMember(replicas []ReplicatedEndpoint, member uint64) (ReplicatedEndpoint, bool) {
	for _, replica := range replicas {
		if replica.Member == member {
			return replica, true
		}
	}
	return ReplicatedEndpoint{}, false
}

func makeReplicatedFailureRecord(revision ReplicaHealthRevision, prior replicatedFailureRecord) replicatedFailureRecord {
	failed := revision.Attestations[0].Failed
	first := uint64(0)
	if failed {
		first = revision.Revision
		if prior.FirstRevision != 0 && prior.CatalogGeneration == revision.CatalogGeneration &&
			prior.ReplicaSetVersion == revision.ReplicaSetVersion && prior.LeaderMember == revision.LeaderMember &&
			prior.LeaderTerm == revision.LeaderTerm && prior.CommitIndex <= revision.CommitIndex &&
			prior.SuspectNode == revision.SuspectNode && prior.SuspectIncarnation == revision.SuspectIncarnation {
			first = prior.FirstRevision
		}
	}
	record := replicatedFailureRecord{
		Group: persistMembershipGrantGroup(revision.Group), CatalogGeneration: revision.CatalogGeneration,
		ReplicaSetVersion: revision.ReplicaSetVersion, Revision: revision.Revision, FirstRevision: first,
		LeaderMember: revision.LeaderMember, LeaderTerm: revision.LeaderTerm, CommitIndex: revision.CommitIndex,
		SuspectMember: revision.SuspectMember, SuspectNode: revision.SuspectNode,
		SuspectIncarnation: revision.SuspectIncarnation,
	}
	if failed {
		record.Confirmations = make([]persistedFailureConfirmation, len(revision.Attestations))
		for index, attestation := range revision.Attestations {
			record.Confirmations[index] = persistedFailureConfirmation{Member: attestation.Member, Node: attestation.Node, NodeIncarnation: attestation.NodeIncarnation}
		}
	}
	return record
}

func recordFailureCertificate(catalog *Snapshot, descriptor ReplicatedShardDescriptor, suspect ReplicatedReplicaDescriptor, record replicatedFailureRecord) (ReplicatedFailureCertificate, bool) {
	if record.CatalogGeneration != catalog.Generation() || openPersistedMembershipGrantGroup(record.Group) != descriptor.Group ||
		record.ReplicaSetVersion != descriptor.Command.ReplicaSetVersion || record.SuspectMember != suspect.Member ||
		record.SuspectNode != suspect.Node || record.SuspectIncarnation != suspect.NodeIncarnation || record.FirstRevision == 0 ||
		record.Revision < record.FirstRevision || record.Revision-record.FirstRevision+1 < FailureConfirmationRevisions {
		return ReplicatedFailureCertificate{}, false
	}
	certificate := ReplicatedFailureCertificate{Distribution: descriptor.Distribution, Shard: descriptor.Shard, Group: descriptor.Group,
		CatalogGeneration: record.CatalogGeneration, ReplicaSetVersion: record.ReplicaSetVersion, LeaderMember: record.LeaderMember,
		LeaderTerm: record.LeaderTerm, CommitIndex: record.CommitIndex, FirstRevision: record.FirstRevision,
		ConfirmedRevision: record.Revision, SuspectMember: record.SuspectMember, SuspectNode: record.SuspectNode,
		SuspectIncarnation: record.SuspectIncarnation, Confirmations: make([]ReplicatedFailureConfirmation, len(record.Confirmations))}
	for index, confirmation := range record.Confirmations {
		certificate.Confirmations[index] = ReplicatedFailureConfirmation{Member: confirmation.Member, FirstRevision: record.FirstRevision,
			ConfirmedRevision: record.Revision, LeaderTerm: record.LeaderTerm, ReplicaSetVersion: record.ReplicaSetVersion, CommitIndex: record.CommitIndex}
	}
	return certificate, true
}

func appendReplicatedFailureRecord(dst []byte, record replicatedFailureRecord) ([]byte, error) {
	if !validReplicatedFailureRecord(record) {
		return dst, ErrReplicatedFailureAuthority
	}
	payload, err := vibejson.Marshal(&record)
	if err != nil {
		return dst, err
	}
	identifier := replicatedFailureDocumentID(openPersistedMembershipGrantGroup(record.Group), record.SuspectMember)
	return appendControlPlaneDocument(dst, identifier[:], payload, maxReplicatedFailureRecordBytes)
}

func openReplicatedFailureRecord(raw []byte) (replicatedFailureRecord, error) {
	if len(raw) == 0 || len(raw) > maxReplicatedFailureRecordBytes {
		return replicatedFailureRecord{}, ErrReplicatedFailureAuthority
	}
	identifier, payload, ok := openFixedControlPlaneDocument(raw, len("health/")+64)
	if !ok || !bytes.Equal(identifier[:len("health/")], []byte("health/")) {
		return replicatedFailureRecord{}, ErrReplicatedFailureAuthority
	}
	var record replicatedFailureRecord
	if err := vibejson.Unmarshal(payload, &record); err != nil || !validReplicatedFailureRecord(record) {
		return replicatedFailureRecord{}, ErrReplicatedFailureAuthority
	}
	expected := replicatedFailureDocumentID(openPersistedMembershipGrantGroup(record.Group), record.SuspectMember)
	canonical, err := appendReplicatedFailureRecord(nil, record)
	if err != nil || !bytes.Equal(identifier, expected[:]) || !bytes.Equal(raw, canonical) {
		return replicatedFailureRecord{}, errors.Join(err, ErrReplicatedFailureAuthority)
	}
	return record, nil
}

func validReplicatedFailureRecord(record replicatedFailureRecord) bool {
	group := openPersistedMembershipGrantGroup(record.Group)
	if !validMembershipGrantGroup(group) || record.CatalogGeneration == 0 || record.ReplicaSetVersion == 0 || record.Revision == 0 ||
		record.LeaderMember == 0 || record.LeaderTerm == 0 || record.CommitIndex == 0 || record.SuspectMember == 0 || record.SuspectIncarnation == 0 ||
		len(record.Confirmations) > ServingReplicaCount {
		return false
	}
	if record.FirstRevision == 0 {
		return len(record.Confirmations) == 0
	}
	if record.FirstRevision > record.Revision || len(record.Confirmations) < ServingReplicaCount/2+1 {
		return false
	}
	previous := uint64(0)
	leader := false
	for _, confirmation := range record.Confirmations {
		if confirmation.Member <= previous || confirmation.Member == record.SuspectMember || confirmation.NodeIncarnation == 0 {
			return false
		}
		previous = confirmation.Member
		leader = leader || confirmation.Member == record.LeaderMember
	}
	return leader
}

func replicatedFailureDocumentID(group raftmember.GroupKey, suspect uint64) [71]byte {
	digest := replicatedFailureDigest(group, suspect)
	var id [71]byte
	copy(id[:], []byte("health/"))
	for index, value := range digest {
		id[7+index*2], id[8+index*2] = lowerHex[value>>4], lowerHex[value&15]
	}
	return id
}

func replicatedFailureDigest(group raftmember.GroupKey, suspect uint64) [32]byte {
	var raw [104]byte
	offset := copy(raw[:], []byte("vibedb/failure-health\x00"))
	offset += copy(raw[offset:], group.ClusterID[:])
	offset += copy(raw[offset:], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(raw[offset:], group.TopologyRecoveryEpoch)
	offset += 8
	offset += copy(raw[offset:], group.ShardIncarnation[:])
	offset += copy(raw[offset:], group.GroupID[:])
	binary.BigEndian.PutUint64(raw[offset:], suspect)
	offset += 8
	return sha256.Sum256(raw[:offset])
}

type replicatedFailureRecordKey [71 + 3]byte
type replicatedFailurePageKey [15 + 3]byte

func replicatedFailureKeys(group raftmember.GroupKey, suspect uint64) (replicatedFailureRecordKey, replicatedFailurePageKey) {
	digest := replicatedFailureDigest(group, suspect)
	id := replicatedFailureDocumentID(group, suspect)
	pageID := replicatedFailurePageID(digest[0] & (replicatedFailurePageCount - 1))
	var record replicatedFailureRecordKey
	var page replicatedFailurePageKey
	copy(record[:], fixedControlPlaneKey(id[:]))
	copy(page[:], fixedControlPlaneKey(pageID[:]))
	return record, page
}

func (key replicatedFailurePageKey) bucket() byte {
	// Privately constructed ASCII IDs end in two hex digits and an 'x'.
	high, highOK := lowerHexNibble(key[len(key)-5])
	low, lowOK := lowerHexNibble(key[len(key)-4])
	if !highOK || !lowOK || high<<4|low >= replicatedFailurePageCount {
		panic("gateway: invalid replica health page key")
	}
	return high<<4 | low
}

func replicatedFailurePageID(index byte) [15]byte {
	var id [15]byte
	copy(id[:], []byte("health/page/"))
	id[12], id[13], id[14] = lowerHex[index>>4], lowerHex[index&15], 'x'
	return id
}

func appendReplicatedFailurePage(dst []byte, index byte, identities []persistedFailureIdentity) ([]byte, error) {
	if index >= replicatedFailurePageCount || len(identities) > maxReplicatedFailureGroupsPerPage {
		return dst, ErrReplicatedFailureAuthority
	}
	page := replicatedFailurePage{Identities: append([]persistedFailureIdentity(nil), identities...)}
	for ordinal, identity := range identities {
		group := openPersistedMembershipGrantGroup(identity.Group)
		_, pageKey := replicatedFailureKeys(group, identity.Suspect)
		if !validMembershipGrantGroup(group) || identity.Suspect == 0 || pageKey.bucket() != index ||
			ordinal > 0 && compareReplicatedFailureIdentity(identities[ordinal-1], identity) >= 0 {
			return dst, ErrReplicatedFailureAuthority
		}
	}
	raw, err := vibejson.Marshal(&page)
	if err != nil {
		return dst, err
	}
	id := replicatedFailurePageID(index)
	return appendControlPlaneDocument(dst, id[:], raw, maxReplicatedFailurePageBytes)
}

func openOptionalReplicatedFailurePage(index byte, result ReplicatedPointResult) ([]persistedFailureIdentity, error) {
	if !result.Found {
		return nil, nil
	}
	return openReplicatedFailurePage(index, result.Value)
}

func openReplicatedFailurePage(index byte, raw []byte) ([]persistedFailureIdentity, error) {
	if index >= replicatedFailurePageCount || len(raw) == 0 || len(raw) > maxReplicatedFailurePageBytes {
		return nil, ErrReplicatedFailureAuthority
	}
	id := replicatedFailurePageID(index)
	payload, err := openTypedControlPlaneDocument(raw, id[:], maxReplicatedFailurePageBytes)
	if err != nil {
		return nil, err
	}
	var page replicatedFailurePage
	if err = vibejson.Unmarshal(payload, &page); err != nil || len(page.Identities) > maxReplicatedFailureGroupsPerPage {
		return nil, ErrReplicatedFailureAuthority
	}
	for ordinal, identity := range page.Identities {
		group := openPersistedMembershipGrantGroup(identity.Group)
		_, pageKey := replicatedFailureKeys(group, identity.Suspect)
		if !validMembershipGrantGroup(group) || identity.Suspect == 0 || pageKey.bucket() != index ||
			ordinal > 0 && compareReplicatedFailureIdentity(page.Identities[ordinal-1], identity) >= 0 {
			return nil, ErrReplicatedFailureAuthority
		}
	}
	canonical, err := appendReplicatedFailurePage(nil, index, page.Identities)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, ErrReplicatedFailureAuthority
	}
	return page.Identities, nil
}

func compareReplicatedFailureIdentity(left, right persistedFailureIdentity) int {
	if order := compareMembershipGrantGroup(openPersistedMembershipGrantGroup(left.Group), openPersistedMembershipGrantGroup(right.Group)); order != 0 {
		return order
	}
	if left.Suspect < right.Suspect {
		return -1
	}
	if left.Suspect > right.Suspect {
		return 1
	}
	return 0
}

func findReplicatedFailureIdentity(identities []persistedFailureIdentity, target persistedFailureIdentity) (int, bool) {
	index := sort.Search(len(identities), func(index int) bool { return compareReplicatedFailureIdentity(identities[index], target) >= 0 })
	return index, index < len(identities) && compareReplicatedFailureIdentity(identities[index], target) == 0
}
