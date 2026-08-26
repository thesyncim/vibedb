package rebalance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	// ErrFailureEvidence means a replacement was requested without one exact,
	// quorum-confirmed failure cut from the currently serving Raft membership.
	ErrFailureEvidence = errors.New("rebalance: failed replica evidence is not quorum certified")
	// ErrReplacementUnavailable means the failure is certified but no candidate
	// satisfies the current membership, epoch, and node/store anti-affinity cut.
	ErrReplacementUnavailable = errors.New("rebalance: no eligible replica replacement")
)

// MinimumFailureConfirmationEpochs prevents a single reachability sample from
// authorizing removal. Epochs are durable health-authority revisions, not wall
// clock ticks; the authority must advance them through its replicated quorum.
const MinimumFailureConfirmationEpochs uint64 = 3

// FailureConfirmation is one current voter's acknowledgement of the exact
// replicated failure record. Entries must be strictly member ordered so the
// same certificate has one representation and duplicate votes cannot count.
type FailureConfirmation struct {
	Member            uint64
	FirstFailureEpoch uint64
	ConfirmedEpoch    uint64
	LeaderTerm        uint64
	ReplicaSetVersion uint64
	CommitIndex       uint64
}

// FailureQuorumCertificate is a detached proof produced by a replicated
// health authority. CommitIndex is the Raft-committed failure record; local
// probes and process-local timers cannot construct a valid certificate.
type FailureQuorumCertificate struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	Group        raftmember.GroupKey

	CatalogGeneration uint64
	ReplicaSetVersion uint64
	LeaderTerm        uint64
	CommitIndex       uint64
	FirstFailureEpoch uint64
	ConfirmedEpoch    uint64
	SuspectMember     uint64
	Confirmations     []FailureConfirmation
}

// HealthyReplica is an epoch-fenced donor observation. A donor must be a
// current voter, active through the certificate epoch, and applied through the
// committed failure record. Applied is used only to choose the freshest safe
// donor; member ID is the stable tie-break.
type HealthyReplica struct {
	Member            uint64
	LeaderTerm        uint64
	ReplicaSetVersion uint64
	HealthyThrough    uint64
	Applied           uint64
	RecentActive      bool
}

// ReplacementCandidate is one already authenticated placement candidate.
// HealthEpoch and TopologyRecoveryEpoch prevent an old capacity observation
// from being reused after either health or topology authority advances.
type ReplacementCandidate struct {
	Member                uint64
	Node                  rafttransport.NodeID
	StoreID               [16]byte
	NodeIncarnation       uint64
	Endpoint              distribution.EndpointID
	TopologyRecoveryEpoch uint64
	HealthEpoch           uint64
	Load                  uint64
}

// FailedReplicaPlanningCut contains one exact catalog/Raft/evidence cut. The
// candidate slice has no participant-count ceiling: selection is O(n), uses
// constant workspace, and never sorts or copies the pool.
type FailedReplicaPlanningCut struct {
	Catalog     *gateway.Snapshot
	Publication raftmodel.Publication
	Leader      raftmember.RuntimeStatus
	Certificate FailureQuorumCertificate
	Healthy     []HealthyReplica
	Candidates  []ReplacementCandidate
}

// FailedReplicaMoveIntent is the immutable output handed to a durable
// controller journal. Intent is the canonical replica-move record; Evidence
// binds the exact quorum certificate that authorized its creation, and
// Placement binds the authenticated node/store/incarnation selected for the
// target member and endpoint carried by Intent.
type FailedReplicaMoveIntent struct {
	Operation OperationID
	Evidence  [sha256.Size]byte
	Placement [sha256.Size]byte
	Plan      *Plan
	Intent    []byte
}

// FailedReplicaMoveSink persists the exact intent before any move execution.
// Implementations must make retries of the same operation/evidence/placement
// idempotent and reject a different witness at the same operation identity.
type FailedReplicaMoveSink interface {
	SubmitFailedReplicaMove(context.Context, FailedReplicaMoveIntent) error
}

// PlanFailedReplicaReplacement validates the failure certificate, chooses a
// healthy snapshot donor and replacement in deterministic constant workspace,
// and constructs the existing crash-recoverable replica move intent.
func PlanFailedReplicaReplacement(cut FailedReplicaPlanningCut) (FailedReplicaMoveIntent, error) {
	certificate := cut.Certificate
	if cut.Catalog == nil || cut.Publication.ConfState == nil ||
		certificate.CatalogGeneration != cut.Catalog.Generation() ||
		certificate.ReplicaSetVersion != cut.Publication.ReplicaSetVersion ||
		invalidGroup(certificate.Group) || certificate.Distribution == "" ||
		certificate.Shard == "" || !validFailureLeader(cut.Leader, cut.Publication, certificate) {
		return FailedReplicaMoveIntent{}, ErrFailureEvidence
	}
	var workspace [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := cut.Catalog.ResolveReplicatedRoute(
		certificate.Distribution, certificate.Shard, workspace[:0],
	)
	if !ok || route.Group != certificate.Group ||
		route.Command.ReplicaSetVersion != certificate.ReplicaSetVersion ||
		!exactServingVoters(route.Replicas, cut.Publication.ConfState.GetVoters()) ||
		!validFailureQuorum(route.Replicas, cut.Leader.MemberID, certificate) {
		return FailedReplicaMoveIntent{}, ErrFailureEvidence
	}
	source, ok := replicaByMember(route.Replicas, certificate.SuspectMember)
	if !ok {
		return FailedReplicaMoveIntent{}, ErrFailureEvidence
	}
	donor, ok := selectSnapshotDonor(route.Replicas, cut.Healthy, certificate)
	if !ok {
		return FailedReplicaMoveIntent{}, ErrFailureEvidence
	}
	target, ok := selectReplacementCandidate(cut.Catalog, route.Replicas, cut.Candidates, certificate)
	if !ok {
		return FailedReplicaMoveIntent{}, ErrReplacementUnavailable
	}
	plan, err := PlanReplicaMove(cut.Catalog, cut.Publication, MoveRequest{
		Distribution:         certificate.Distribution,
		Shard:                certificate.Shard,
		Group:                certificate.Group,
		RetiringMember:       certificate.SuspectMember,
		SnapshotSourceMember: donor,
		TargetMember:         target.Member,
		Source:               distribution.EndpointID(source.Endpoint),
		Target:               target.Endpoint,
		RetiringReplica: ReplicaIdentity{
			Member: source.Member, Node: source.Node, StoreID: source.StoreID,
			NodeIncarnation: source.NodeIncarnation,
			ControlEndpoint: distribution.EndpointID(source.ControlEndpoint),
		},
	})
	if err != nil {
		return FailedReplicaMoveIntent{}, err
	}
	intent, err := AppendReplicaMoveIntent(nil, cut.Catalog, plan)
	if err != nil {
		return FailedReplicaMoveIntent{}, err
	}
	return FailedReplicaMoveIntent{
		Operation: plan.OperationID(), Evidence: failureEvidenceDigest(certificate),
		Placement: replacementPlacementDigest(target), Plan: plan, Intent: intent,
	}, nil
}

// ScheduleFailedReplicaReplacement persists one planned replacement through
// the injected durable authority. It performs no membership or network work.
func ScheduleFailedReplicaReplacement(
	ctx context.Context, cut FailedReplicaPlanningCut, sink FailedReplicaMoveSink,
) (FailedReplicaMoveIntent, error) {
	if ctx == nil || sink == nil {
		return FailedReplicaMoveIntent{}, ErrFailureEvidence
	}
	intent, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		return FailedReplicaMoveIntent{}, err
	}
	if err = sink.SubmitFailedReplicaMove(ctx, intent); err != nil {
		return intent, err
	}
	return intent, nil
}

func validFailureLeader(
	leader raftmember.RuntimeStatus,
	publication raftmodel.Publication,
	certificate FailureQuorumCertificate,
) bool {
	if certificate.ReplicaSetVersion == 0 || certificate.LeaderTerm == 0 ||
		certificate.CommitIndex == 0 || certificate.FirstFailureEpoch == 0 ||
		certificate.ConfirmedEpoch < certificate.FirstFailureEpoch ||
		certificate.ConfirmedEpoch-certificate.FirstFailureEpoch+1 < MinimumFailureConfirmationEpochs ||
		certificate.SuspectMember == 0 || leader.MemberID == 0 ||
		leader.MemberID != leader.LeaderID || leader.MemberID == certificate.SuspectMember ||
		leader.Term != certificate.LeaderTerm || leader.Commit < certificate.CommitIndex ||
		leader.Applied < certificate.CommitIndex || publication.Applied < certificate.CommitIndex ||
		publication.ReplicaSetVersion == 0 ||
		publication.ReplicaSetVersion > publication.Applied {
		return false
	}
	return true
}

func validFailureQuorum(
	replicas []gateway.ReplicatedEndpoint,
	leader uint64,
	certificate FailureQuorumCertificate,
) bool {
	quorum := len(replicas)/2 + 1
	if len(certificate.Confirmations) < quorum {
		return false
	}
	previous := uint64(0)
	leaderConfirmed := false
	for _, confirmation := range certificate.Confirmations {
		if confirmation.Member <= previous || confirmation.Member == certificate.SuspectMember ||
			confirmation.FirstFailureEpoch != certificate.FirstFailureEpoch ||
			confirmation.ConfirmedEpoch != certificate.ConfirmedEpoch ||
			confirmation.LeaderTerm != certificate.LeaderTerm ||
			confirmation.ReplicaSetVersion != certificate.ReplicaSetVersion ||
			confirmation.CommitIndex != certificate.CommitIndex {
			return false
		}
		if _, ok := replicaByMember(replicas, confirmation.Member); !ok {
			return false
		}
		leaderConfirmed = leaderConfirmed || confirmation.Member == leader
		previous = confirmation.Member
	}
	return leaderConfirmed
}

func exactServingVoters(replicas []gateway.ReplicatedEndpoint, voters []uint64) bool {
	if len(replicas) != len(voters) {
		return false
	}
	for _, replica := range replicas {
		found := false
		for _, voter := range voters {
			if voter == replica.Member {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func replicaByMember(
	replicas []gateway.ReplicatedEndpoint, member uint64,
) (gateway.ReplicatedEndpoint, bool) {
	for _, replica := range replicas {
		if replica.Member == member {
			return replica, true
		}
	}
	return gateway.ReplicatedEndpoint{}, false
}

func selectSnapshotDonor(
	replicas []gateway.ReplicatedEndpoint,
	healthy []HealthyReplica,
	certificate FailureQuorumCertificate,
) (uint64, bool) {
	selected := HealthyReplica{}
	for _, candidate := range healthy {
		if candidate.Member == certificate.SuspectMember || !candidate.RecentActive ||
			candidate.LeaderTerm != certificate.LeaderTerm ||
			candidate.ReplicaSetVersion != certificate.ReplicaSetVersion ||
			candidate.HealthyThrough < certificate.ConfirmedEpoch ||
			candidate.Applied < certificate.CommitIndex {
			continue
		}
		if _, ok := replicaByMember(replicas, candidate.Member); !ok {
			continue
		}
		if selected.Member == 0 || candidate.Applied > selected.Applied ||
			candidate.Applied == selected.Applied && candidate.Member < selected.Member {
			selected = candidate
		}
	}
	return selected.Member, selected.Member != 0
}

func selectReplacementCandidate(
	catalog *gateway.Snapshot,
	replicas []gateway.ReplicatedEndpoint,
	candidates []ReplacementCandidate,
	certificate FailureQuorumCertificate,
) (ReplacementCandidate, bool) {
	selected := ReplacementCandidate{}
	for _, candidate := range candidates {
		if !validReplacementCandidate(catalog, replicas, candidate, certificate) {
			continue
		}
		if selected.Member == 0 || candidate.Load < selected.Load ||
			candidate.Load == selected.Load && replacementCandidateLess(candidate, selected) {
			selected = candidate
		}
	}
	if selected.Member == 0 {
		return ReplacementCandidate{}, false
	}
	// Conflicting current descriptions of the chosen member/node/store/address
	// are an authority split, not extra candidates. Reject them without a map or
	// a count-dependent allocation; exact duplicate observations are harmless.
	for _, candidate := range candidates {
		if candidate == selected ||
			!validReplacementCandidate(catalog, replicas, candidate, certificate) {
			continue
		}
		if candidate.Member == selected.Member || candidate.Node == selected.Node ||
			candidate.StoreID == selected.StoreID || candidate.Endpoint == selected.Endpoint {
			return ReplacementCandidate{}, false
		}
	}
	return selected, true
}

func validReplacementCandidate(
	catalog *gateway.Snapshot,
	replicas []gateway.ReplicatedEndpoint,
	candidate ReplacementCandidate,
	certificate FailureQuorumCertificate,
) bool {
	if candidate.Member == 0 || candidate.Node == (rafttransport.NodeID{}) ||
		candidate.StoreID == ([16]byte{}) || candidate.NodeIncarnation == 0 ||
		candidate.Endpoint == "" ||
		candidate.TopologyRecoveryEpoch != certificate.Group.TopologyRecoveryEpoch ||
		candidate.HealthEpoch < certificate.ConfirmedEpoch {
		return false
	}
	if _, err := catalog.Address(candidate.Endpoint); err != nil {
		return false
	}
	for _, replica := range replicas {
		if candidate.Member == replica.Member || candidate.Node == replica.Node ||
			candidate.StoreID == replica.StoreID || string(candidate.Endpoint) == replica.Endpoint {
			return false
		}
	}
	return true
}

func replacementCandidateLess(left, right ReplacementCandidate) bool {
	if left.Member != right.Member {
		return left.Member < right.Member
	}
	if order := bytes.Compare(left.Node[:], right.Node[:]); order != 0 {
		return order < 0
	}
	if order := bytes.Compare(left.StoreID[:], right.StoreID[:]); order != 0 {
		return order < 0
	}
	return left.Endpoint < right.Endpoint
}

func failureEvidenceDigest(certificate FailureQuorumCertificate) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/rebalance/failure-quorum-certificate\x00"))
	writeOperationString(hash, string(certificate.Distribution))
	writeOperationString(hash, string(certificate.Shard))
	for _, value := range [...][16]byte{
		certificate.Group.ClusterID, certificate.Group.ClusterIncarnation,
		certificate.Group.ShardIncarnation, certificate.Group.GroupID,
	} {
		_, _ = hash.Write(value[:])
	}
	for _, value := range [...]uint64{
		certificate.Group.TopologyRecoveryEpoch, certificate.CatalogGeneration,
		certificate.ReplicaSetVersion, certificate.LeaderTerm, certificate.CommitIndex,
		certificate.FirstFailureEpoch, certificate.ConfirmedEpoch, certificate.SuspectMember,
		uint64(len(certificate.Confirmations)),
	} {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], value)
		_, _ = hash.Write(raw[:])
	}
	for _, confirmation := range certificate.Confirmations {
		for _, value := range [...]uint64{
			confirmation.Member, confirmation.FirstFailureEpoch, confirmation.ConfirmedEpoch,
			confirmation.LeaderTerm, confirmation.ReplicaSetVersion, confirmation.CommitIndex,
		} {
			var raw [8]byte
			binary.LittleEndian.PutUint64(raw[:], value)
			_, _ = hash.Write(raw[:])
		}
	}
	var result [sha256.Size]byte
	_ = hash.Sum(result[:0])
	return result
}

func replacementPlacementDigest(candidate ReplacementCandidate) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/rebalance/replacement-placement\x00"))
	_, _ = hash.Write(candidate.Node[:])
	_, _ = hash.Write(candidate.StoreID[:])
	for _, value := range [...]uint64{
		candidate.Member, candidate.NodeIncarnation, candidate.TopologyRecoveryEpoch,
		candidate.HealthEpoch, candidate.Load,
	} {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], value)
		_, _ = hash.Write(raw[:])
	}
	writeOperationString(hash, string(candidate.Endpoint))
	var result [sha256.Size]byte
	_ = hash.Sum(result[:0])
	return result
}
