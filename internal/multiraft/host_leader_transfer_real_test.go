package multiraft

import (
	"context"
	"crypto/sha256"
	"errors"
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
		runtime, base := newRealTransferRuntime(t, identities[index], voters)
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
	duplicateTimeoutNow bool
	timeoutNowSeen      bool
	duplicateRejected   bool
	pendingDuplicate    int
}

func (cluster *realTransferCluster) driveUntil(done func() bool) {
	cluster.t.Helper()
	for step := 0; step < 100000; step++ {
		if done() {
			return
		}
		progressed := false
		for index, host := range cluster.hosts {
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
			for {
				outbound, ok := host.PopOutbound()
				if !ok {
					break
				}
				progressed = true
				cluster.route(index, outbound)
			}
		}
		if !progressed {
			cluster.t.Fatalf("cluster became idle before condition at step %d", step)
		}
	}
	cluster.t.Fatal("cluster drive did not converge")
}

func (cluster *realTransferCluster) route(senderIndex int, outbound raftmember.OutboundMessage) {
	cluster.t.Helper()
	receiverIndex, ok := cluster.memberIndex[outbound.To]
	if !ok {
		cluster.t.Fatalf("unknown outbound destination %d", outbound.To)
	}
	sender := cluster.registries[senderIndex]
	receiver := cluster.registries[receiverIndex]
	frame, destination, err := sender.EncodeOutbound(nil, outbound)
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
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity) {
	t.Helper()
	index, term := uint64(1), uint64(1)
	wal, err := raftstore.Create(filepath.Join(t.TempDir(), "member.wal"), identity, realTransferWALKey(), raftstore.Bootstrap{
		TopologyRecoveryEpoch: 29,
		Snapshot: &pb.Snapshot{
			Data: []byte("multiraft-real-transfer-bootstrap"),
			Metadata: &pb.SnapshotMetadata{
				Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: append([]uint64(nil), voters...)},
			},
		},
	}, raftstore.Options{
		MaxFileBytes: 256 << 20, MaxRecordBytes: raftstore.DefaultMaxRecordBytes,
		MaxRecords: 1024, MaxEntries: 8192, MaxLiveBytes: 2 * raftstore.MinimumReadyLiveBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	database, err := sqldriver.InitializeShardStore(filepath.Join(t.TempDir(), "member.vdb"), sqldriver.ShardStoreBinding{
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
	apply, _, err := raftmember.OpenPreparedApply(wal, database, authority, base, sqldriver.ReplicatedApplyOptions{
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
	return runtime, base
}

func realTransferIdentities() [3]raftstore.Identity {
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
	var identities [3]raftstore.Identity
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
