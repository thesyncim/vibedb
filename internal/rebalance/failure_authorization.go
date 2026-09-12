package rebalance

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

const maxFailureAuthorizationBytes = 4 << 10

var failureAuthorizationMagic = [4]byte{'V', 'F', 'A', 1}

type failedReplicaAuthorization struct {
	certificate FailureQuorumCertificate
	publication raftmodel.Publication
	leader      raftmember.RuntimeStatus
	donor       HealthyReplica
	candidate   ReplacementCandidate
}

// FailedReplicaAuthorizationDigests returns the exact evidence and placement
// witnesses embedded in a scheduler-created plan. Proactive/manual moves have
// no failure authorization and return ok=false.
func (plan *Plan) FailedReplicaAuthorizationDigests() (
	evidence [32]byte, placement [32]byte, ok bool,
) {
	if plan == nil || len(plan.failureAuthorization) == 0 {
		return evidence, placement, false
	}
	authorization, err := openFailureAuthorization(plan.failureAuthorization)
	if err != nil || !validFailedReplicaAuthorization(plan, authorization) {
		return evidence, placement, false
	}
	return failureEvidenceDigest(authorization.certificate),
		replacementPlacementDigest(authorization.candidate), true
}

func authorizeFailedReplicaMove(
	plan *Plan, cut FailedReplicaPlanningCut, donor HealthyReplica, candidate ReplacementCandidate,
) error {
	if plan == nil || len(plan.failureAuthorization) != 0 {
		return ErrFailureEvidence
	}
	authorization := failedReplicaAuthorization{
		certificate: cloneFailureCertificate(cut.Certificate),
		publication: raftmodel.Publication{
			Applied: cut.Publication.Applied, ReplicaSetVersion: cut.Publication.ReplicaSetVersion,
			ConfState: cloneConfState(cut.Publication.ConfState),
		},
		leader: cut.Leader, donor: donor, candidate: candidate,
	}
	if !validFailedReplicaAuthorization(plan, authorization) {
		return ErrFailureEvidence
	}
	raw, err := appendFailureAuthorization(nil, authorization)
	if err != nil {
		return err
	}
	plan.failureAuthorization = raw
	plan.operation = replicaMoveOperationID(plan)
	// Failure evidence is part of the operation identity. Keep the still-local
	// transition ownership on that final identity before either is persisted.
	if plan.transitionReady {
		plan.transition.Key.OperationID = [32]byte(plan.operation)
	}
	if plan.operation == (OperationID{}) {
		return ErrFailureEvidence
	}
	return nil
}

func restoreFailedReplicaAuthorization(plan *Plan, raw []byte) error {
	if plan == nil || len(raw) == 0 || len(plan.failureAuthorization) != 0 {
		return ErrFailureEvidence
	}
	authorization, err := openFailureAuthorization(raw)
	if err != nil || !validFailedReplicaAuthorization(plan, authorization) {
		return errors.Join(err, ErrFailureEvidence)
	}
	plan.failureAuthorization = bytes.Clone(raw)
	plan.operation = replicaMoveOperationID(plan)
	// Failure evidence is part of the operation identity. Keep the still-local
	// transition ownership on that final identity before either is persisted.
	if plan.transitionReady {
		plan.transition.Key.OperationID = [32]byte(plan.operation)
	}
	return nil
}

func validFailedReplicaAuthorization(plan *Plan, auth failedReplicaAuthorization) bool {
	if plan == nil || plan.initialConf == nil || auth.publication.ConfState == nil {
		return false
	}
	request, certificate := plan.request, auth.certificate
	if certificate.Distribution != request.Distribution || certificate.Shard != request.Shard ||
		certificate.Group != request.Group || certificate.CatalogGeneration != plan.catalogGeneration ||
		certificate.SuspectMember != request.RetiringMember ||
		auth.donor.Member != request.SnapshotSourceMember ||
		auth.candidate.Member != request.TargetMember || auth.candidate.Endpoint != request.Target ||
		!equalSimpleConfState(auth.publication.ConfState, plan.initialConf) ||
		!validFailureLeader(auth.leader, auth.publication, certificate) ||
		!validAuthorizationQuorum(plan.initialConf.GetVoters(), auth.leader.MemberID, certificate) {
		return false
	}
	if !auth.donor.RecentActive || auth.donor.Member == certificate.SuspectMember ||
		auth.donor.LeaderTerm != certificate.LeaderTerm ||
		auth.donor.ReplicaSetVersion != certificate.ReplicaSetVersion ||
		auth.donor.HealthyThrough < certificate.ConfirmedEpoch ||
		auth.donor.Applied < certificate.CommitIndex ||
		!memberInSorted(plan.initialConf.GetVoters(), auth.donor.Member) {
		return false
	}
	return auth.candidate.Node != (rafttransport.NodeID{}) &&
		auth.candidate.StoreID != ([16]byte{}) && auth.candidate.NodeIncarnation != 0 &&
		auth.candidate.TopologyRecoveryEpoch == request.Group.TopologyRecoveryEpoch &&
		auth.candidate.HealthEpoch >= certificate.ConfirmedEpoch
}

func validAuthorizationQuorum(
	voters []uint64, leader uint64, certificate FailureQuorumCertificate,
) bool {
	if len(voters) != gateway.ServingReplicaCount ||
		len(certificate.Confirmations) < len(voters)/2+1 ||
		len(certificate.Confirmations) > len(voters) {
		return false
	}
	previous := uint64(0)
	leaderConfirmed := false
	for _, confirmation := range certificate.Confirmations {
		if confirmation.Member <= previous || confirmation.Member == certificate.SuspectMember ||
			!memberInSorted(voters, confirmation.Member) ||
			confirmation.FirstFailureEpoch != certificate.FirstFailureEpoch ||
			confirmation.ConfirmedEpoch != certificate.ConfirmedEpoch ||
			confirmation.LeaderTerm != certificate.LeaderTerm ||
			confirmation.ReplicaSetVersion != certificate.ReplicaSetVersion ||
			confirmation.CommitIndex != certificate.CommitIndex {
			return false
		}
		leaderConfirmed = leaderConfirmed || confirmation.Member == leader
		previous = confirmation.Member
	}
	return leaderConfirmed
}

func equalSimpleConfState(left, right *pb.ConfState) bool {
	return left != nil && right != nil &&
		equalMembers(left.GetVoters(), right.GetVoters()) &&
		len(left.GetLearners()) == 0 && len(right.GetLearners()) == 0 &&
		len(left.GetVotersOutgoing()) == 0 && len(right.GetVotersOutgoing()) == 0 &&
		len(left.GetLearnersNext()) == 0 && len(right.GetLearnersNext()) == 0 &&
		!left.GetAutoLeave() && !right.GetAutoLeave()
}

func equalMembers(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func cloneFailureCertificate(value FailureQuorumCertificate) FailureQuorumCertificate {
	value.Confirmations = append([]FailureConfirmation(nil), value.Confirmations...)
	return value
}

func cloneConfState(value *pb.ConfState) *pb.ConfState {
	if value == nil {
		return nil
	}
	return &pb.ConfState{Voters: append([]uint64(nil), value.GetVoters()...)}
}

func appendFailureAuthorization(dst []byte, auth failedReplicaAuthorization) ([]byte, error) {
	if !validFailureAuthorizationShape(auth) {
		return dst, ErrFailureEvidence
	}
	start := len(dst)
	dst = append(dst, failureAuthorizationMagic[:]...)
	dst = appendBoundedString(dst, string(auth.certificate.Distribution))
	dst = appendBoundedString(dst, string(auth.certificate.Shard))
	for _, id := range [...][16]byte{auth.certificate.Group.ClusterID,
		auth.certificate.Group.ClusterIncarnation, auth.certificate.Group.ShardIncarnation,
		auth.certificate.Group.GroupID} {
		dst = append(dst, id[:]...)
	}
	dst = appendUint64s(dst, auth.certificate.Group.TopologyRecoveryEpoch,
		auth.certificate.CatalogGeneration, auth.certificate.ReplicaSetVersion,
		auth.certificate.LeaderTerm, auth.certificate.CommitIndex,
		auth.certificate.FirstFailureEpoch, auth.certificate.ConfirmedEpoch,
		auth.certificate.SuspectMember)
	dst = append(dst, byte(len(auth.certificate.Confirmations)))
	for _, confirmation := range auth.certificate.Confirmations {
		dst = appendUint64s(dst, confirmation.Member, confirmation.FirstFailureEpoch,
			confirmation.ConfirmedEpoch, confirmation.LeaderTerm,
			confirmation.ReplicaSetVersion, confirmation.CommitIndex)
	}
	dst = appendUint64s(dst, auth.publication.Applied, auth.publication.ReplicaSetVersion)
	dst = append(dst, byte(len(auth.publication.ConfState.GetVoters())))
	dst = appendUint64s(dst, auth.publication.ConfState.GetVoters()...)
	dst = appendUint64s(dst, auth.leader.MemberID, auth.leader.LeaderID, auth.leader.Term,
		auth.leader.Commit, auth.leader.Applied)
	dst = appendUint64s(dst, auth.donor.Member, auth.donor.LeaderTerm,
		auth.donor.ReplicaSetVersion, auth.donor.HealthyThrough, auth.donor.Applied)
	if auth.donor.RecentActive {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = appendUint64s(dst, auth.candidate.Member)
	dst = append(dst, auth.candidate.Node[:]...)
	dst = append(dst, auth.candidate.StoreID[:]...)
	dst = appendUint64s(dst, auth.candidate.NodeIncarnation,
		auth.candidate.TopologyRecoveryEpoch, auth.candidate.HealthEpoch, auth.candidate.Load)
	dst = appendBoundedString(dst, string(auth.candidate.Endpoint))
	if len(dst)-start > maxFailureAuthorizationBytes {
		return dst[:start], ErrFailureEvidence
	}
	return dst, nil
}

func openFailureAuthorization(raw []byte) (failedReplicaAuthorization, error) {
	decoder := failureAuthorizationDecoder{raw: raw}
	if len(raw) == 0 || len(raw) > maxFailureAuthorizationBytes ||
		!bytes.Equal(decoder.take(4), failureAuthorizationMagic[:]) {
		return failedReplicaAuthorization{}, ErrFailureEvidence
	}
	var auth failedReplicaAuthorization
	auth.certificate.Distribution = distribution.DistributionName(decoder.text())
	auth.certificate.Shard = distribution.ShardID(decoder.text())
	for _, target := range []*[16]byte{&auth.certificate.Group.ClusterID,
		&auth.certificate.Group.ClusterIncarnation, &auth.certificate.Group.ShardIncarnation,
		&auth.certificate.Group.GroupID} {
		copy(target[:], decoder.take(16))
	}
	auth.certificate.Group.TopologyRecoveryEpoch = decoder.u64()
	auth.certificate.CatalogGeneration = decoder.u64()
	auth.certificate.ReplicaSetVersion = decoder.u64()
	auth.certificate.LeaderTerm = decoder.u64()
	auth.certificate.CommitIndex = decoder.u64()
	auth.certificate.FirstFailureEpoch = decoder.u64()
	auth.certificate.ConfirmedEpoch = decoder.u64()
	auth.certificate.SuspectMember = decoder.u64()
	confirmationCount := decoder.count(gateway.ServingReplicaCount)
	auth.certificate.Confirmations = make([]FailureConfirmation, confirmationCount)
	for index := range auth.certificate.Confirmations {
		confirmation := &auth.certificate.Confirmations[index]
		confirmation.Member, confirmation.FirstFailureEpoch = decoder.u64(), decoder.u64()
		confirmation.ConfirmedEpoch, confirmation.LeaderTerm = decoder.u64(), decoder.u64()
		confirmation.ReplicaSetVersion, confirmation.CommitIndex = decoder.u64(), decoder.u64()
	}
	auth.publication.Applied, auth.publication.ReplicaSetVersion = decoder.u64(), decoder.u64()
	voterCount := decoder.count(gateway.ServingReplicaCount)
	auth.publication.ConfState = &pb.ConfState{Voters: make([]uint64, voterCount)}
	for index := range auth.publication.ConfState.Voters {
		auth.publication.ConfState.Voters[index] = decoder.u64()
	}
	auth.leader.MemberID, auth.leader.LeaderID = decoder.u64(), decoder.u64()
	auth.leader.Term, auth.leader.Commit, auth.leader.Applied = decoder.u64(), decoder.u64(), decoder.u64()
	auth.donor.Member, auth.donor.LeaderTerm = decoder.u64(), decoder.u64()
	auth.donor.ReplicaSetVersion, auth.donor.HealthyThrough = decoder.u64(), decoder.u64()
	auth.donor.Applied = decoder.u64()
	auth.donor.RecentActive = decoder.byte() == 1
	auth.candidate.Member = decoder.u64()
	copy(auth.candidate.Node[:], decoder.take(16))
	copy(auth.candidate.StoreID[:], decoder.take(16))
	auth.candidate.NodeIncarnation, auth.candidate.TopologyRecoveryEpoch = decoder.u64(), decoder.u64()
	auth.candidate.HealthEpoch, auth.candidate.Load = decoder.u64(), decoder.u64()
	auth.candidate.Endpoint = distribution.EndpointID(decoder.text())
	if decoder.err || decoder.offset != len(raw) || !validFailureAuthorizationShape(auth) {
		return failedReplicaAuthorization{}, ErrFailureEvidence
	}
	canonical, err := appendFailureAuthorization(nil, auth)
	if err != nil || !bytes.Equal(canonical, raw) {
		return failedReplicaAuthorization{}, ErrFailureEvidence
	}
	return auth, nil
}

func validFailureAuthorizationShape(auth failedReplicaAuthorization) bool {
	return auth.certificate.Distribution != "" && auth.certificate.Shard != "" &&
		len(auth.certificate.Confirmations) <= gateway.ServingReplicaCount &&
		auth.publication.ConfState != nil &&
		len(auth.publication.ConfState.GetVoters()) <= gateway.ServingReplicaCount &&
		auth.candidate.Endpoint != ""
}

func appendUint64s(dst []byte, values ...uint64) []byte {
	for _, value := range values {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], value)
		dst = append(dst, raw[:]...)
	}
	return dst
}

func appendBoundedString(dst []byte, value string) []byte {
	var raw [2]byte
	binary.LittleEndian.PutUint16(raw[:], uint16(len(value)))
	dst = append(dst, raw[:]...)
	return append(dst, value...)
}

type failureAuthorizationDecoder struct {
	raw    []byte
	offset int
	err    bool
}

func (decoder *failureAuthorizationDecoder) take(size int) []byte {
	if decoder.err || size < 0 || size > len(decoder.raw)-decoder.offset {
		decoder.err = true
		return nil
	}
	value := decoder.raw[decoder.offset : decoder.offset+size]
	decoder.offset += size
	return value
}
func (decoder *failureAuthorizationDecoder) byte() byte {
	raw := decoder.take(1)
	if len(raw) != 1 {
		return 0
	}
	return raw[0]
}
func (decoder *failureAuthorizationDecoder) u64() uint64 {
	raw := decoder.take(8)
	if len(raw) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(raw)
}
func (decoder *failureAuthorizationDecoder) count(maximum int) int {
	value := int(decoder.byte())
	if value > maximum {
		decoder.err = true
		return 0
	}
	return value
}
func (decoder *failureAuthorizationDecoder) text() string {
	raw := decoder.take(2)
	if len(raw) != 2 {
		return ""
	}
	size := int(binary.LittleEndian.Uint16(raw))
	return string(decoder.take(size))
}
