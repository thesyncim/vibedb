package gateway

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
)

type distributedMetricsTestConnection struct {
	net.Conn
	peer rafttransport.PeerIdentity
}

func (connection *distributedMetricsTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.peer
}
func (*distributedMetricsTestConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

type distributedMetricsTestProvider struct {
	identity raftmember.RuntimeIdentity
	cut      raftservice.ProgressMetricsSnapshot
}

func (provider distributedMetricsTestProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return provider.cut
}
func (provider distributedMetricsTestProvider) GroupProgressMetrics(group raftmember.GroupKey) (raftmember.RuntimeIdentity, raftservice.ProgressMetricsSnapshot, bool) {
	return provider.identity, provider.cut, group == provider.identity.Group
}

type distributedMetricsTestOpener struct {
	service *servicemetrics.Service
	peer    rafttransport.PeerIdentity
}

func (opener distributedMetricsTestOpener) OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	server, client := net.Pipe()
	go func() {
		_ = opener.service.Serve(context.Background(), &distributedMetricsTestConnection{Conn: server, peer: opener.peer})
	}()
	return &distributedMetricsTestConnection{Conn: client}, nil
}

func TestDistributedMetricsAuthenticatedExactGroupRefresh(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	node := rafttransport.NodeID{6}
	peer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}}
	cut := raftservice.ProgressMetricsSnapshot{ProposalCommands: 10, ProposalBytes: 11,
		AppliedEntries: 12, ReadyPersisted: 13, SnapshotsFinished: 14, ReadCompletions: 15, Faults: 16}
	service, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider:     distributedMetricsTestProvider{identity: raftmember.RuntimeIdentity{Group: group, MemberID: 7}, cut: cut},
		Authorize:    func(identity rafttransport.PeerIdentity) bool { return identity == peer },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := NewDistributedMetrics(distributedMetricsTestOpener{service: service, peer: peer},
		[]ReplicatedRoute{{Group: group, Replicas: []ReplicatedEndpoint{{Member: 7, Node: node}}}})
	if err != nil || metrics.RefreshOne(t.Context(), 0) != nil {
		t.Fatalf("new/refresh err=%v", err)
	}
	samples, aggregate, err := metrics.SnapshotInto(make([]DistributedMetricsSample, 0, metrics.Len()))
	if err != nil || len(samples) != 2 || samples[0].Group != group || samples[0].Member != 7 ||
		samples[0].Node != node || samples[0].Cut != cut || samples[0].Reads != 1 || samples[0].Faults != 0 ||
		!samples[1].NodeAggregate || samples[1].Node != node || aggregate.Cut != cut || aggregate.Samples != 2 || aggregate.Reads != 1 {
		t.Fatalf("samples=%+v aggregate=%+v err=%v", samples, aggregate, err)
	}
}

func TestDistributedMetricsRejectsBoundsDuplicatesAndWrongMember(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	if _, err := NewDistributedMetrics(nil, nil); err == nil {
		t.Fatal("nil configuration accepted")
	}
	opener := distributedMetricsTestOpener{}
	if _, err := NewDistributedMetrics(opener, []ReplicatedRoute{{Group: group,
		Replicas: []ReplicatedEndpoint{{Member: 1, Node: rafttransport.NodeID{1}}, {Member: 1, Node: rafttransport.NodeID{2}}}}}); err == nil {
		t.Fatal("duplicate member accepted")
	}
}

func BenchmarkDistributedMetricsSnapshotInto(b *testing.B) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	routes := []ReplicatedRoute{{Group: group, Replicas: []ReplicatedEndpoint{
		{Member: 1, Node: rafttransport.NodeID{1}}, {Member: 2, Node: rafttransport.NodeID{2}},
		{Member: 3, Node: rafttransport.NodeID{3}},
	}}}
	metrics, err := NewDistributedMetrics(distributedMetricsTestOpener{}, routes)
	if err != nil {
		b.Fatal(err)
	}
	workspace := make([]DistributedMetricsSample, 0, metrics.Len())
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := metrics.SnapshotInto(workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDistributedMetricsAggregateSaturatesOverflow(t *testing.T) {
	base := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	other := base
	other.GroupID[0] = 6
	metrics, err := NewDistributedMetrics(distributedMetricsTestOpener{}, []ReplicatedRoute{
		{Group: base, Replicas: []ReplicatedEndpoint{{Member: 1, Node: rafttransport.NodeID{1}}}},
		{Group: other, Replicas: []ReplicatedEndpoint{{Member: 2, Node: rafttransport.NodeID{1}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics.slots[0].values[2].Store(math.MaxUint64)
	metrics.slots[1].values[2].Store(1)
	aggregate, err := metrics.Aggregate()
	if err != nil || !aggregate.Overflow || aggregate.Cut.AppliedEntries != math.MaxUint64 {
		t.Fatalf("aggregate=%+v err=%v", aggregate, err)
	}
}
