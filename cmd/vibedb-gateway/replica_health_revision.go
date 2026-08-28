package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
)

// gatewayReplicaHealthRevisionController is only an evidence publisher. It
// cannot schedule, reconfigure, or remove a replica. Each pass uses at most
// one RF3 worth of concurrent control streams and processes groups in stable
// catalog order, bounding sockets and temporary storage independently of
// cluster size.
type gatewayReplicaHealthRevisionController struct {
	catalog      gatewayReplicaCatalogReader
	observations gatewayReplicaObservationClient
	authority    gateway.ReplicaHealthRevisionAuthority
}

type gatewayReplicaHealthRevisionPass struct {
	Groups    uint64
	Certified uint64
	Suspects  uint64
	Published uint64
	Unchanged uint64
}

func newGatewayReplicaHealthRevisionController(
	catalog gatewayReplicaCatalogReader,
	observations gatewayReplicaObservationClient,
	authority gateway.ReplicaHealthRevisionAuthority,
) (*gatewayReplicaHealthRevisionController, error) {
	if catalog == nil || observations == nil || authority == nil {
		return nil, errGatewayReplicaHealth
	}
	return &gatewayReplicaHealthRevisionController{
		catalog: catalog, observations: observations, authority: authority,
	}, nil
}

func (controller *gatewayReplicaHealthRevisionController) RunPass(
	ctx context.Context,
) (gatewayReplicaHealthRevisionPass, error) {
	if controller == nil || ctx == nil || controller.catalog == nil ||
		controller.observations == nil || controller.authority == nil {
		return gatewayReplicaHealthRevisionPass{}, errGatewayReplicaHealth
	}
	catalog, err := controller.catalog.Read(ctx)
	if err != nil || catalog == nil {
		return gatewayReplicaHealthRevisionPass{}, errors.Join(err, errGatewayReplicaHealth)
	}
	descriptors := catalog.ReplicatedShardDescriptors()
	slices.SortFunc(descriptors, compareHealthDescriptors)
	var pass gatewayReplicaHealthRevisionPass
	var failures error
	for index := range descriptors {
		if err := ctx.Err(); err != nil {
			return pass, errors.Join(failures, err)
		}
		pass.Groups++
		revisions, observeErr := controller.observeGroup(ctx, catalog.Generation(), descriptors[index])
		if observeErr != nil {
			failures = errors.Join(failures, observeErr)
			continue
		}
		pass.Certified++
		for revisionIndex := range revisions {
			revision := &revisions[revisionIndex]
			var current uint64
			var readErr error
			if source, ok := controller.authority.(gateway.ReplicaHealthRevisionStatusSource); ok {
				var status gateway.ReplicaHealthRevisionStatus
				status, readErr = source.ReadReplicaHealthRevisionStatus(ctx, revision.Group, revision.SuspectMember)
				current = status.Revision
				if readErr == nil && status.AlreadyHealthy(*revision) {
					pass.Unchanged++
					continue
				}
			} else {
				current, readErr = controller.authority.ReadReplicaHealthRevision(ctx, revision.Group, revision.SuspectMember)
			}
			if readErr != nil || current == ^uint64(0) {
				failures = errors.Join(failures, readErr, errGatewayReplicaHealth)
				continue
			}
			revision.Revision = current + 1
			if revision.Attestations[0].Failed {
				pass.Suspects++
			}
			if publishErr := controller.authority.PublishReplicaHealthRevision(ctx, *revision); publishErr != nil {
				// A concurrent publisher wins through the authority's exact CAS.
				// The next interval reopens its revision; no local retry storm or
				// timeout-derived evidence is created here.
				failures = errors.Join(failures, publishErr)
				continue
			}
			pass.Published++
		}
	}
	return pass, failures
}

type gatewayHealthCut struct {
	member      uint64
	endpoint    gateway.ReplicatedReplicaDescriptor
	observation replicacontrol.Observation
	err         error
}

type gatewayHealthAgreement struct {
	leader uint64
	term   uint64
	commit uint64
	rsv    uint64
}

func (controller *gatewayReplicaHealthRevisionController) observeGroup(
	ctx context.Context, catalogGeneration uint64, descriptor gateway.ReplicatedShardDescriptor,
) ([]gateway.ReplicaHealthRevision, error) {
	if len(descriptor.Replicas) != gateway.ServingReplicaCount ||
		descriptor.Command.ReplicaSetVersion == 0 || catalogGeneration == 0 {
		return nil, errGatewayReplicaHealth
	}
	operation, step := replicaHealthRoundIdentity(catalogGeneration, descriptor)
	results := make(chan gatewayHealthCut, gateway.ServingReplicaCount)
	for index := range descriptor.Replicas {
		endpoint := descriptor.Replicas[index]
		go func() {
			request := replicacontrol.Request{
				Operation: operation, Step: step, Group: descriptor.Group,
				TargetMember:              endpoint.Member,
				ExpectedReplicaSetVersion: descriptor.Command.ReplicaSetVersion,
			}
			observation, err := controller.observations.Observe(ctx, endpoint.Node, request)
			results <- gatewayHealthCut{member: endpoint.Member, endpoint: endpoint,
				observation: observation, err: err}
		}()
	}
	cuts := make([]gatewayHealthCut, 0, gateway.ServingReplicaCount)
	for range descriptor.Replicas {
		cut := <-results
		if cut.err == nil && validHealthCut(cut, descriptor.Command.ReplicaSetVersion) {
			cuts = append(cuts, cut)
		}
	}
	agreement, reporters, ok := quorumHealthAgreement(cuts)
	if !ok {
		return nil, errGatewayReplicaHealth
	}
	// A full healthy RF3 cut clears any prior suspicion for every member. With
	// exactly a quorum, only the absent/nonagreeing member advances as failed.
	allAgree := len(reporters) == gateway.ServingReplicaCount
	revisions := make([]gateway.ReplicaHealthRevision, 0, gateway.ServingReplicaCount)
	for _, suspect := range descriptor.Replicas {
		if !allAgree && healthReporterPosition(reporters, suspect.Member) >= 0 {
			continue
		}
		attestations := make([]gateway.ReplicaHealthAttestation, 0, gateway.ServingReplicaCount)
		for _, reporter := range reporters {
			if !allAgree && reporter.member == suspect.Member {
				continue
			}
			attestations = append(attestations, gateway.ReplicaHealthAttestation{
				Member: reporter.member, Node: reporter.endpoint.Node,
				NodeIncarnation:   reporter.endpoint.NodeIncarnation,
				CatalogGeneration: catalogGeneration,
				ReplicaSetVersion: agreement.rsv, LeaderMember: agreement.leader,
				LeaderTerm: agreement.term, CommitIndex: agreement.commit,
				Failed: !allAgree,
			})
		}
		if len(attestations) < gateway.ServingReplicaCount/2+1 {
			continue
		}
		revisions = append(revisions, gateway.ReplicaHealthRevision{
			Distribution: descriptor.Distribution, Shard: descriptor.Shard, Group: descriptor.Group,
			CatalogGeneration: catalogGeneration, ReplicaSetVersion: agreement.rsv,
			LeaderMember: agreement.leader, LeaderTerm: agreement.term, CommitIndex: agreement.commit,
			SuspectMember: suspect.Member, SuspectNode: suspect.Node,
			SuspectIncarnation: suspect.NodeIncarnation, Attestations: attestations,
		})
	}
	if (!allAgree && len(revisions) != 1) || (allAgree && len(revisions) != gateway.ServingReplicaCount) {
		return nil, errGatewayReplicaHealth
	}
	return revisions, nil
}

func validHealthCut(cut gatewayHealthCut, rsv uint64) bool {
	status := cut.observation.Status
	return cut.member != 0 && cut.endpoint.Member == cut.member && status.MemberID == cut.member &&
		status.LeaderID != 0 && status.Term != 0 && status.Commit != 0 &&
		cut.observation.Publication.ReplicaSetVersion == rsv &&
		status.Applied == cut.observation.Publication.Applied && status.Applied <= status.Commit
}

func quorumHealthAgreement(cuts []gatewayHealthCut) (gatewayHealthAgreement, []gatewayHealthCut, bool) {
	for _, candidate := range cuts {
		status := candidate.observation.Status
		agreement := gatewayHealthAgreement{leader: status.LeaderID, term: status.Term,
			commit: status.Commit, rsv: candidate.observation.Publication.ReplicaSetVersion}
		reporters := make([]gatewayHealthCut, 0, gateway.ServingReplicaCount)
		leaderAnswered := false
		for _, cut := range cuts {
			other := cut.observation.Status
			if other.LeaderID == agreement.leader && other.Term == agreement.term &&
				other.Commit == agreement.commit &&
				cut.observation.Publication.ReplicaSetVersion == agreement.rsv {
				reporters = append(reporters, cut)
				leaderAnswered = leaderAnswered || (cut.member == agreement.leader && other.MemberID == other.LeaderID)
			}
		}
		if leaderAnswered && len(reporters) >= gateway.ServingReplicaCount/2+1 {
			slices.SortFunc(reporters, func(left, right gatewayHealthCut) int {
				return compareUint64(left.member, right.member)
			})
			return agreement, reporters, true
		}
	}
	return gatewayHealthAgreement{}, nil, false
}

func healthReporterPosition(reporters []gatewayHealthCut, member uint64) int {
	return slices.IndexFunc(reporters, func(reporter gatewayHealthCut) bool { return reporter.member == member })
}

func replicaHealthRoundIdentity(
	catalogGeneration uint64, descriptor gateway.ReplicatedShardDescriptor,
) ([32]byte, [32]byte) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/replica-health-round\x00"))
	_, _ = hash.Write(descriptor.Group.ClusterID[:])
	_, _ = hash.Write(descriptor.Group.ClusterIncarnation[:])
	_, _ = hash.Write(descriptor.Group.ShardIncarnation[:])
	_, _ = hash.Write(descriptor.Group.GroupID[:])
	var numbers [24]byte
	binary.BigEndian.PutUint64(numbers[0:8], descriptor.Group.TopologyRecoveryEpoch)
	binary.BigEndian.PutUint64(numbers[8:16], catalogGeneration)
	binary.BigEndian.PutUint64(numbers[16:24], descriptor.Command.ReplicaSetVersion)
	_, _ = hash.Write(numbers[:])
	var operation [32]byte
	_ = hash.Sum(operation[:0])
	hash.Reset()
	_, _ = hash.Write([]byte("vibedb/replica-health-observe\x00"))
	_, _ = hash.Write(operation[:])
	var step [32]byte
	_ = hash.Sum(step[:0])
	return operation, step
}

func compareHealthDescriptors(left, right gateway.ReplicatedShardDescriptor) int {
	if compared := compareBytes(left.Group.ClusterID[:], right.Group.ClusterID[:]); compared != 0 {
		return compared
	}
	if compared := compareBytes(left.Group.ClusterIncarnation[:], right.Group.ClusterIncarnation[:]); compared != 0 {
		return compared
	}
	if left.Group.TopologyRecoveryEpoch != right.Group.TopologyRecoveryEpoch {
		return compareUint64(left.Group.TopologyRecoveryEpoch, right.Group.TopologyRecoveryEpoch)
	}
	if compared := compareBytes(left.Group.ShardIncarnation[:], right.Group.ShardIncarnation[:]); compared != 0 {
		return compared
	}
	return compareBytes(left.Group.GroupID[:], right.Group.GroupID[:])
}

func compareBytes(left, right []byte) int {
	return slices.Compare(left, right)
}

func compareUint64(left, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
