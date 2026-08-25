package multiraft

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestThreeRealHostsTransferLeaderThroughAuthenticatedTransportAndContinueApply(t *testing.T) {
	const replicas = 3
	identities := realTransferIdentities()
	voters := []uint64{identities[0].MemberID, identities[1].MemberID, identities[2].MemberID}
	hosts := make([]*Host, replicas)
	registries := make([]*rafttransport.StaticRegistry, 0, replicas)
	members := make([]rafttransport.Member, replicas)
	nodes := [replicas]rafttransport.NodeID{{1}, {2}, {3}}
	var commandIdentity sqldriver.ReplicatedShardStoreIdentity

	for index := range replicas {
		runtime, base, _ := newRealTransferRuntime(t, identities[index], voters)
		host, err := NewHost(testHostLimits())
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Add(runtime); err != nil {
			t.Fatal(err)
		}
		hosts[index] = host
		t.Cleanup(func() { _ = host.Close() })
		if index == 1 {
			commandIdentity = base
		}
		members[index] = rafttransport.Member{
			Group: runtime.Identity().Group, ReplicaSetVersion: 1,
			MemberID: identities[index].MemberID, Node: nodes[index], Role: rafttransport.MemberVoter,
		}
	}
	for index := range replicas {
		registry, err := rafttransport.NewStaticRegistry(nodes[index], members, rafttransport.Limits{
			MaxGroups: 1, MaxMembers: replicas,
		})
		if err != nil {
			t.Fatal(err)
		}
		registries = append(registries, registry)
	}

	cluster := realTransferCluster{
		t: t, hosts: hosts, registries: registries,
		group:       members[0].Group,
		memberIndex: map[uint64]int{voters[0]: 0, voters[1]: 1, voters[2]: 2},
	}
	group := members[0].Group
	if err := hosts[0].RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool { return cluster.allAppliedWithLeader(group, voters[0], 2) })

	if err := hosts[0].TransferLeader(group, voters[1]); err != nil {
		t.Fatal(err)
	}
	cluster.duplicateTimeoutNow = true
	cluster.driveUntil(func() bool {
		return cluster.allAppliedWithLeader(group, voters[1], 2) && cluster.oldLeaderSteppedDown(group)
	})
	if !cluster.timeoutNowSeen || !cluster.duplicateRejected {
		t.Fatalf("leader-transfer transport evidence: seen=%t duplicate-rejected=%t",
			cluster.timeoutNowSeen, cluster.duplicateRejected)
	}

	before, err := hosts[1].Status(group)
	if err != nil {
		t.Fatal(err)
	}
	command := realTransferSessionOpen(commandIdentity)
	if err := hosts[1].EnqueueProposal(group, command); err != nil {
		t.Fatal(err)
	}
	cluster.driveUntil(func() bool {
		return cluster.allAppliedWithLeader(group, voters[1], before.Applied+1)
	})
	for index, host := range hosts {
		publication, err := host.Publication(group)
		if err != nil || publication.Applied < before.Applied+1 {
			t.Fatalf("member %d publication = %+v, %v", voters[index], publication, err)
		}
	}
}

type realTransferCluster struct {
	t                   *testing.T
	hosts               []*Host
	registries          []*rafttransport.StaticRegistry
	memberIndex         map[uint64]int
	inactive            map[int]bool
	duplicateTimeoutNow bool
	timeoutNowSeen      bool
	duplicateRejected   bool
	pendingDuplicate    int
	syncAuthority       bool
	group               raftmember.GroupKey
	promotionTarget     uint64
	pausePromotion      bool
	promotionPaused     bool
	promotionVoteSeen   bool
	holdTargetUntilVote bool
}

func (cluster *realTransferCluster) driveUntil(done func() bool) {
	cluster.t.Helper()
	step := 0
	doneReached, idleStep := cluster.driveUntilIdle(done, &step)
	if doneReached {
		return
	}
	cluster.t.Fatalf("cluster became idle before condition at step %d: %s",
		idleStep, cluster.diagnostic())
}

// driveUntilIdle advances the deterministic scheduler until either done is
// observed or one exact protocol-idle cut is reached. Unlike driveUntil it does
// not classify idle as failure: callers that deliberately supply a logical
// clock input can first observe the quiescent cut and then inject that input.
// Exhausting the deterministic step bound remains fatal in every caller.
func (cluster *realTransferCluster) driveUntilIdle(
	done func() bool,
	step *int,
) (doneReached bool, idleStep int) {
	cluster.t.Helper()
	for *step < 100000 {
		if done() {
			return true, -1
		}
		current := *step
		*step++
		if !cluster.driveRound(current) {
			return false, current
		}
	}
	cluster.t.Fatalf("cluster drive did not converge: %s", cluster.diagnostic())
	return false, -1
}

// driveRound executes one deterministic scheduler turn and routes every
// resulting real Raft message. A false result is an observable protocol-idle
// boundary; callers may then inject an explicit logical clock input.
func (cluster *realTransferCluster) driveRound(step int) bool {
	cluster.t.Helper()
	progressed := false
	for index, host := range cluster.hosts {
		if cluster.inactive[index] {
			continue
		}
		if cluster.holdTargetUntilVote &&
			index == cluster.memberIndex[cluster.promotionTarget] &&
			!cluster.promotionVoteSeen {
			continue
		}
		_, consumed, err := host.RunOne()
		if err != nil {
			if cluster.pendingDuplicate != 0 && index == 1 && strings.Contains(err.Error(), "leader-transfer") {
				cluster.pendingDuplicate--
				cluster.duplicateRejected = true
			} else {
				cluster.t.Fatalf("host %d RunOne step %d: %v", index, step, err)
			}
		}
		progressed = progressed || consumed
		if cluster.syncAuthority && consumed {
			publication, publishErr := host.Publication(cluster.group)
			if publishErr != nil {
				cluster.t.Fatal(publishErr)
			}
			if publishErr = cluster.registries[index].PublishCommittedAuthority(
				cluster.group, publication.ReplicaSetVersion,
				publication.ConfState); publishErr != nil {
				cluster.t.Fatal(publishErr)
			}
			if cluster.promotionTarget != 0 {
				proof, found, proofErr := host.DurablePromotion(cluster.group,
					cluster.promotionTarget)
				if proofErr != nil {
					cluster.t.Fatal(proofErr)
				}
				if found {
					if proofErr = cluster.registries[index].PublishDurablePromotion(
						cluster.group, proof); proofErr != nil {
						cluster.t.Fatal(proofErr)
					}
					if cluster.pausePromotion && index == cluster.memberIndex[cluster.promotionTarget] {
						status, statusErr := host.Status(cluster.group)
						publication, publicationErr := host.Publication(cluster.group)
						if statusErr != nil || publicationErr != nil {
							cluster.t.Fatal(errors.Join(statusErr, publicationErr))
						}
						if status.Commit >= proof.Version && publication.ReplicaSetVersion < proof.Version {
							cluster.pausePromotion = false
							cluster.promotionPaused = true
							cluster.inactive[index] = true
						}
					}
				} else if proofErr = cluster.registries[index].ClearDurablePromotion(
					cluster.group); proofErr != nil {
					cluster.t.Fatal(proofErr)
				}
			}
		}
		for {
			outbound, ok := host.PopOutbound()
			if !ok {
				break
			}
			progressed = true
			cluster.route(index, outbound)
		}
	}
	return progressed
}

// driveUntilWithLeaderTicks advances an exact condition across protocol-idle
// boundaries by ticking only the currently observed leader. Each tick is a
// real Raft input and every heartbeat/response is routed through the
// authenticated transport; no commit or promotion witness is synthesized.
func (cluster *realTransferCluster) driveUntilWithLeaderTicks(done func() bool) {
	cluster.t.Helper()
	step := 0
	for step < 100000 {
		doneReached, idleStep := cluster.driveUntilIdle(done, &step)
		if doneReached {
			return
		}
		leader := -1
		for index, host := range cluster.hosts {
			if cluster.inactive[index] {
				continue
			}
			status, err := host.Status(cluster.group)
			if err != nil {
				cluster.t.Fatal(err)
			}
			if status.MemberID == status.LeaderID && status.LeaderID != 0 {
				if leader >= 0 {
					cluster.t.Fatalf("multiple leaders at protocol-idle step %d: %s",
						idleStep, cluster.diagnostic())
				}
				leader = index
			}
		}
		if leader < 0 {
			cluster.t.Fatalf("no leader at protocol-idle step %d: %s",
				idleStep, cluster.diagnostic())
		}
		if err := cluster.hosts[leader].RequestTick(cluster.group); err != nil {
			cluster.t.Fatal(err)
		}
		step++
	}
	cluster.t.Fatalf("cluster drive with leader ticks did not converge: %s",
		cluster.diagnostic())
}

// driveUntilWithActiveVoterTicks advances an election after the former leader
// has been isolated. Followers correctly retain the old leader lease until
// ElectionTick logical ticks have elapsed, so an immediate explicit campaign
// can stop at a protocol-idle pre-vote cut. At each such cut this driver ticks
// every active voter exactly once, never an inactive former leader or learner.
// The caller supplies a small protocol-derived tick-round bound; scheduler work
// retains the ordinary deterministic step bound in driveUntilIdle.
func (cluster *realTransferCluster) driveUntilWithActiveVoterTicks(
	done func() bool,
	maxTickRounds int,
) {
	cluster.t.Helper()
	if maxTickRounds <= 0 {
		cluster.t.Fatal("active-voter tick bound must be positive")
	}
	step := 0
	tickRound := 0
	for {
		doneReached, idleStep := cluster.driveUntilIdle(done, &step)
		if doneReached {
			return
		}
		if tickRound == maxTickRounds {
			cluster.t.Fatalf("cluster election did not converge after %d active-voter tick rounds: %s",
				maxTickRounds, cluster.diagnostic())
		}
		ticked := 0
		for index, host := range cluster.hosts {
			if cluster.inactive[index] {
				continue
			}
			status, statusErr := host.Status(cluster.group)
			publication, publicationErr := host.Publication(cluster.group)
			if statusErr != nil || publicationErr != nil {
				cluster.t.Fatal(errors.Join(statusErr, publicationErr))
			}
			if !confHasVoter(publication.ConfState, status.MemberID) {
				continue
			}
			if err := host.RequestTick(cluster.group); err != nil {
				cluster.t.Fatalf("tick active voter host %d at idle step %d round %d: %v",
					index, idleStep, tickRound, err)
			}
			ticked++
		}
		if ticked == 0 {
			cluster.t.Fatalf("no active voter at protocol-idle step %d tick round %d: %s",
				idleStep, tickRound, cluster.diagnostic())
		}
		tickRound++
		step++
	}
}

func (cluster *realTransferCluster) diagnostic() string {
	if cluster == nil {
		return "nil cluster"
	}
	var result strings.Builder
	for index, host := range cluster.hosts {
		if index != 0 {
			result.WriteString("; ")
		}
		status, statusErr := host.Status(cluster.group)
		publication, publicationErr := host.Publication(cluster.group)
		version, versionFound := uint64(0), false
		if index < len(cluster.registries) && cluster.registries[index] != nil {
			version, versionFound = cluster.registries[index].ReplicaSetVersion(cluster.group)
		}
		fmt.Fprintf(&result,
			"host=%d inactive=%t status={member=%d leader=%d term=%d commit=%d applied=%d err=%v} "+
				"publication={applied=%d version=%d conf=%v err=%v} authority={version=%d found=%t}",
			index, cluster.inactive[index], status.MemberID, status.LeaderID, status.Term,
			status.Commit, status.Applied, statusErr, publication.Applied,
			publication.ReplicaSetVersion, publication.ConfState, publicationErr,
			version, versionFound)
	}
	return result.String()
}

func (cluster *realTransferCluster) route(senderIndex int, outbound raftmember.OutboundMessage) {
	cluster.t.Helper()
	receiverIndex, ok := cluster.memberIndex[outbound.To]
	if !ok {
		cluster.t.Fatalf("unknown outbound destination %d", outbound.To)
	}
	if cluster.inactive[receiverIndex] {
		return
	}
	sender := cluster.registries[senderIndex]
	receiver := cluster.registries[receiverIndex]
	frame, destination, err := sender.EncodeOutbound(nil, outbound)
	if errors.Is(err, rafttransport.ErrUnauthorized) {
		publication, publicationErr := cluster.hosts[senderIndex].Publication(outbound.Group)
		if publicationErr == nil && !confHasMember(publication.ConfState, outbound.From) {
			return
		}
	}
	if err != nil || destination != receiver.LocalNode() {
		cluster.t.Fatalf("encode %s %d->%d = %x, %v",
			outbound.Message.GetType(), outbound.From, outbound.To, destination, err)
	}
	deliver := func() {
		inbound, err := receiver.DecodeInbound(rafttransport.PeerIdentity{
			TrustDomain: receiver.TrustDomain(), Node: sender.LocalNode(),
		}, frame)
		if err != nil {
			cluster.t.Fatalf("decode %s %d->%d: %v", outbound.Message.GetType(), outbound.From, outbound.To, err)
		}
		if err := cluster.hosts[receiverIndex].AdoptMessage(inbound.Group, inbound.Message); err != nil {
			cluster.t.Fatalf("adopt %s %d->%d: %v", outbound.Message.GetType(), outbound.From, outbound.To, err)
		}
		if !cluster.promotionVoteSeen && outbound.To == cluster.promotionTarget &&
			(outbound.Message.GetType() == pb.MsgVote || outbound.Message.GetType() == pb.MsgPreVote) {
			role, roleErr := receiver.Role(outbound.Group, cluster.promotionTarget)
			if roleErr != nil || role != rafttransport.MemberLearner {
				cluster.t.Fatalf("promotion election admitted after target publication: role=%d err=%v",
					role, roleErr)
			}
			cluster.promotionVoteSeen = true
		}
	}
	deliver()
	if outbound.Message.GetType() == pb.MsgTimeoutNow {
		cluster.timeoutNowSeen = true
		if cluster.duplicateTimeoutNow {
			cluster.duplicateTimeoutNow = false
			cluster.pendingDuplicate++
			deliver()
		}
	}
}

func confHasMember(conf *pb.ConfState, member uint64) bool {
	if conf == nil {
		return false
	}
	for _, values := range [][]uint64{conf.GetVoters(), conf.GetLearners(),
		conf.GetVotersOutgoing(), conf.GetLearnersNext()} {
		for _, candidate := range values {
			if candidate == member {
				return true
			}
		}
	}
	return false
}

func confHasVoter(conf *pb.ConfState, member uint64) bool {
	if conf == nil {
		return false
	}
	for _, values := range [][]uint64{conf.GetVoters(), conf.GetVotersOutgoing()} {
		for _, candidate := range values {
			if candidate == member {
				return true
			}
		}
	}
	return false
}

func (cluster *realTransferCluster) allAppliedWithLeader(
	group raftmember.GroupKey,
	leader, minimumApplied uint64,
) bool {
	for _, host := range cluster.hosts {
		status, err := host.Status(group)
		if err != nil || status.LeaderID != leader || status.Applied < minimumApplied {
			return false
		}
	}
	return true
}

func (cluster *realTransferCluster) oldLeaderSteppedDown(group raftmember.GroupKey) bool {
	status, err := cluster.hosts[0].Status(group)
	return err == nil && status.RaftState == raft.StateFollower
}

func newRealTransferRuntime(
	t *testing.T,
	identity raftstore.Identity,
	voters []uint64,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity, func() *raftmember.Runtime) {
	return newRealTransferRuntimeWithLearners(t, identity, voters, nil)
}

func newRealTransferRuntimeWithLearners(
	t *testing.T,
	identity raftstore.Identity,
	voters, learners []uint64,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity, func() *raftmember.Runtime) {
	t.Helper()
	index, term := uint64(1), uint64(1)
	walPath := filepath.Join(t.TempDir(), "member.wal")
	sqlPath := filepath.Join(t.TempDir(), "member.vdb")
	options := raftstore.Options{
		MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 1024, MaxEntries: 8192, MaxLiveBytes: 2 * raftstore.MinimumReadyLiveBytes,
	}
	wal, err := raftstore.Create(walPath, identity, realTransferWALKey(), raftstore.Bootstrap{
		TopologyRecoveryEpoch: 29,
		Snapshot: &pb.Snapshot{
			Data: []byte("multiraft-real-transfer-bootstrap"),
			Metadata: &pb.SnapshotMetadata{
				Index: &index, Term: &term, ConfState: &pb.ConfState{
					Voters: append([]uint64(nil), voters...), Learners: append([]uint64(nil), learners...),
				},
			},
		},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sqldriver.InitializeShardStore(sqlPath, sqldriver.ShardStoreBinding{
		Distribution:         distribution.DistributionName(identity.Distribution),
		Shard:                distribution.ShardID(identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(identity.AllocationGeneration),
	})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		_ = database.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	prepared, err := session.Prepare(context.Background(), `CREATE TABLE docs (PRIMARY KEY (id))`)
	if err == nil {
		_, err = prepared.Exec(context.Background(), nil)
	}
	if prepared != nil {
		err = errors.Join(err, prepared.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		_ = database.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	authority := realTransferAuthority()
	base, err := raftmember.BindPreparedSQL(wal, database, authority, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		_ = database.Close()
		_ = wal.Close()
		t.Skipf("strict allocation unsupported: %v", err)
	}
	if err != nil {
		_ = database.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	apply, applyID, err := raftmember.OpenPreparedApply(wal, database, authority, base, sqldriver.ReplicatedApplyOptions{
		MaxSessions: 16, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 256, MaxBytes: 64 << 20},
		Placement: sqldriver.ReplicatedPlacementProfile{
			Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	})
	if err != nil {
		_ = database.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err == nil {
		_, err = apply.InstallSnapshot(bootstrap)
	}
	if err != nil {
		_ = apply.Close()
		_ = database.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	runtime, err := raftmember.AdoptRuntime(wal, database, apply)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		} else {
			_ = apply.Close()
			_ = database.Close()
			_ = wal.Close()
		}
		t.Fatal(err)
	}
	reopen := func() *raftmember.Runtime {
		t.Helper()
		reopenedWAL, openErr := raftstore.Open(walPath, identity, 29,
			realTransferWALKey(), options)
		if openErr != nil {
			t.Fatal(openErr)
		}
		reopenedDB, reopenedApply, openErr := raftmember.OpenBoundSQLWithApply(
			sqlPath, reopenedWAL, authority, base, applyID)
		if openErr != nil {
			_ = reopenedWAL.Close()
			t.Fatal(openErr)
		}
		restarted, openErr := raftmember.AdoptRuntime(reopenedWAL, reopenedDB, reopenedApply)
		if openErr != nil {
			if restarted != nil {
				_ = restarted.Close()
			} else {
				_ = reopenedApply.Close()
				_ = reopenedDB.Close()
				_ = reopenedWAL.Close()
			}
			t.Fatal(openErr)
		}
		return restarted
	}
	return runtime, base, reopen
}

func realTransferIdentities() [4]raftstore.Identity {
	var shared raftstore.Identity
	shared.Distribution = "orders"
	shared.Shard = "0000-7fff"
	shared.AllocationGeneration = 1
	fill := func(id *[16]byte, seed byte) {
		for index := range id {
			id[index] = seed + byte(index)
		}
	}
	fill(&shared.ClusterID, 1)
	fill(&shared.ClusterIncarnation, 21)
	fill(&shared.ShardIncarnation, 41)
	fill(&shared.GroupID, 61)
	var identities [4]raftstore.Identity
	for index := range identities {
		identities[index] = shared
		identities[index].MemberID = uint64(index + 1)
		fill(&identities[index].StoreID, byte(81+index*20))
	}
	return identities
}

func realTransferWALKey() raftstore.Key {
	key := raftstore.Key{ID: "multiraft-real-transfer", Wrapped: []byte("opaque-test-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	return key
}

func realTransferAuthority() sqldriver.ReplicatedAuthorityProfile {
	return sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 31, ProtectionEpoch: 37, OwnershipEpoch: 41,
		SchemaGeneration: 43, RoutingVersion: 47, RouteGeneration: 53,
	}
}

func realTransferSessionOpen(identity sqldriver.ReplicatedShardStoreIdentity) []byte {
	binding := identity.Binding
	command := replication.Command{
		Kind:                   replication.CommandSessionOpen,
		ClusterID:              replication.ID128(binding.ClusterID),
		ClusterIncarnation:     replication.ID128(binding.ClusterIncarnation),
		TopologyRecoveryEpoch:  binding.TopologyRecoveryEpoch,
		Distribution:           binding.Distribution,
		Shard:                  binding.Shard,
		AllocationGeneration:   binding.AllocationGeneration,
		ShardIncarnation:       replication.ID128(binding.ShardIncarnation),
		GroupID:                replication.ID128(binding.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{3},
		ClientSequence:         1,
		Fingerprint:            sha256.Sum256([]byte("multiraft/real-transfer/session-open")),
		NextDeadlineUnixNano:   2_000_000_000_000_000_000,
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		panic(err)
	}
	return encoded
}
