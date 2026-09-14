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
func (*distributedMetricsTestConnection) PeerKeyDigest() [32]byte { return [32]byte{} }
func (*distributedMetricsTestConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

type distributedMetricsTestProvider struct {
	identity raftmember.RuntimeIdentity
	groups   map[raftmember.GroupKey]uint64
	cut      raftservice.ProgressMetricsSnapshot
	budget   servicemetrics.MigrationBudgetSnapshot
}

func (provider distributedMetricsTestProvider) ProgressMetrics() raftservice.ProgressMetricsSnapshot {
	return provider.cut
}
func (provider distributedMetricsTestProvider) MigrationBudgetMetrics() servicemetrics.MigrationBudgetSnapshot {
	return provider.budget
}
func (provider distributedMetricsTestProvider) GroupProgressMetrics(group raftmember.GroupKey) (raftmember.RuntimeIdentity, raftservice.ProgressMetricsSnapshot, bool) {
	if provider.groups != nil {
		member, found := provider.groups[group]
		return raftmember.RuntimeIdentity{Group: group, MemberID: member}, provider.cut, found && member != 0
	}
	return provider.identity, provider.cut, group == provider.identity.Group
}

type distributedMetricsTestOpener struct {
	service       *servicemetrics.Service
	peer          rafttransport.PeerIdentity
	endpointCalls *[]ReplicatedEndpoint
}

func (opener distributedMetricsTestOpener) OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	server, client := net.Pipe()
	go func() {
		_ = opener.service.Serve(context.Background(), &distributedMetricsTestConnection{Conn: server, peer: opener.peer})
	}()
	return &distributedMetricsTestConnection{Conn: client}, nil
}

func (opener distributedMetricsTestOpener) OpenShardControlEndpoint(ctx context.Context, endpoint ReplicatedEndpoint) (rafttransport.PeerConnection, error) {
	if endpoint.Node == (rafttransport.NodeID{}) || endpoint.NodeIncarnation == 0 || endpoint.ControlAddress == "" {
		return nil, ErrDistributedMetrics
	}
	if opener.endpointCalls != nil {
		*opener.endpointCalls = append(*opener.endpointCalls, endpoint)
	}
	return opener.OpenShardControl(ctx, endpoint.Node)
}

func TestDistributedMetricsAuthenticatedExactGroupRefresh(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	node := rafttransport.NodeID{6}
	peer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}}
	cut := raftservice.ProgressMetricsSnapshot{ProposalCommands: 10, ProposalBytes: 11,
		AppliedEntries: 12, ReadyPersisted: 13, SnapshotsFinished: 14, ReadCompletions: 15, Faults: 16}
	wantBudget := servicemetrics.MigrationBudgetSnapshot{ThrottledCalls: 17, ThrottledBytes: 18, PeakActive: 2, MaxActive: 3}
	service, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider:     distributedMetricsTestProvider{identity: raftmember.RuntimeIdentity{Group: group, MemberID: 7}, cut: cut, budget: wantBudget},
		Authorize:    func(identity rafttransport.PeerIdentity) bool { return identity == peer },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := NewDistributedMetrics(distributedMetricsTestOpener{service: service, peer: peer},
		[]ReplicatedRoute{{Group: group, Replicas: []ReplicatedEndpoint{{Member: 7, Node: node}}}})
	if err != nil || metrics.RefreshOne(t.Context(), 0) != nil || metrics.RefreshOne(t.Context(), 1) != nil {
		t.Fatalf("new/refresh err=%v", err)
	}
	samples, aggregate, err := metrics.SnapshotInto(make([]DistributedMetricsSample, 0, metrics.Len()))
	if err != nil || len(samples) != 2 || samples[0].Group != group || samples[0].Member != 7 ||
		samples[0].Node != node || samples[0].Cut != cut || samples[0].Reads != 1 || samples[0].Faults != 0 ||
		!samples[1].NodeAggregate || samples[1].Node != node || samples[1].Budget != wantBudget ||
		aggregate.Cut != cut || aggregate.Budget != wantBudget || aggregate.Samples != 2 || aggregate.Reads != 2 {
		t.Fatalf("samples=%+v aggregate=%+v err=%v", samples, aggregate, err)
	}
}

func TestDistributedMetricsBudgetCountsOneAuthenticatedNodeAggregate(t *testing.T) {
	base := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 3,
		ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	second := base
	second.GroupID[0]++
	node := rafttransport.NodeID{6}
	peer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}}
	wantBudget := servicemetrics.MigrationBudgetSnapshot{ThrottledCalls: 17, ThrottledBytes: 18, PeakActive: 2, MaxActive: 3}
	authorized := true
	service, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider: distributedMetricsTestProvider{
			groups: map[raftmember.GroupKey]uint64{base: 7, second: 8},
			budget: wantBudget,
		},
		Authorize:    func(identity rafttransport.PeerIdentity) bool { return authorized && identity == peer },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := NewDistributedMetrics(distributedMetricsTestOpener{service: service, peer: peer}, []ReplicatedRoute{
		{Group: base, Replicas: []ReplicatedEndpoint{{Member: 7, Node: node}}},
		{Group: second, Replicas: []ReplicatedEndpoint{{Member: 8, Node: node}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := range metrics.Len() {
		if err := metrics.RefreshOne(t.Context(), index); err != nil {
			t.Fatalf("refresh %d: %v", index, err)
		}
	}
	authorized = false
	if err := metrics.RefreshOne(t.Context(), metrics.Len()-1); err == nil {
		t.Fatal("unauthorized aggregate refresh accepted")
	}
	samples, aggregate, err := metrics.SnapshotInto(make([]DistributedMetricsSample, 0, metrics.Len()))
	if err != nil || len(samples) != 3 || aggregate.Budget != wantBudget || samples[2].Budget != wantBudget ||
		samples[2].Faults != 1 || samples[2].Reads != 2 {
		t.Fatalf("samples=%+v aggregate=%+v err=%v", samples, aggregate, err)
	}
	for index := range 2 {
		if samples[index].Budget != (servicemetrics.MigrationBudgetSnapshot{}) {
			t.Fatalf("group sample %d carried node budget: %+v", index, samples[index].Budget)
		}
	}
}

func TestDistributedMetricsAddsCurrentTargetBudgetWithoutManifestMutation(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	source := ReplicatedEndpoint{Member: 7, Node: rafttransport.NodeID{6}, NodeIncarnation: 11, ControlAddress: "source-control"}
	target := ReplicatedEndpoint{Node: rafttransport.NodeID{8}, NodeIncarnation: 22, ControlAddress: "target-control"}
	peer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}}
	wantBudget := servicemetrics.MigrationBudgetSnapshot{ThrottledCalls: 17, ThrottledBytes: 18, PeakActive: 2, MaxActive: 3}
	service, err := servicemetrics.NewService(servicemetrics.ServiceOptions{
		Provider:     distributedMetricsTestProvider{identity: raftmember.RuntimeIdentity{Group: group, MemberID: source.Member}, budget: wantBudget},
		Authorize:    func(identity rafttransport.PeerIdentity) bool { return identity == peer },
		ReadDeadline: func() time.Time { return time.Now().Add(time.Second) }, WriteDeadline: func() time.Time { return time.Now().Add(time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := make([]ReplicatedEndpoint, 0, 4)
	metrics, err := NewDistributedMetrics(distributedMetricsTestOpener{service: service, peer: peer, endpointCalls: &calls}, []ReplicatedRoute{
		{Group: group, Replicas: []ReplicatedEndpoint{source}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Len() != 2 {
		t.Fatalf("initial metrics samples=%d, want group plus source aggregate", metrics.Len())
	}
	if err := metrics.RefreshOne(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	before, err := metrics.Aggregate()
	if err != nil || before.Budget != wantBudget {
		t.Fatalf("before target aggregate=%+v err=%v", before, err)
	}

	if err := metrics.UpdateNodeAggregates([]ReplicatedEndpoint{source, target, target}); err != nil {
		t.Fatalf("register current authenticated directory: %v", err)
	}
	if metrics.Len() != 3 {
		t.Fatalf("updated metrics samples=%d, want group plus two current aggregates", metrics.Len())
	}
	for index := range metrics.Len() {
		if err := metrics.RefreshOne(t.Context(), index); err != nil {
			t.Fatalf("refresh sample %d: %v", index, err)
		}
	}
	_, after, err := metrics.SnapshotInto(make([]DistributedMetricsSample, 0, metrics.Len()))
	if err != nil || after.Budget.ThrottledCalls != wantBudget.ThrottledCalls*2 ||
		after.Budget.ThrottledBytes != wantBudget.ThrottledBytes*2 || after.Budget.PeakActive != wantBudget.PeakActive ||
		after.Budget.MaxActive != wantBudget.MaxActive {
		t.Fatalf("after target aggregate=%+v err=%v", after, err)
	}
	foundTarget := false
	for index := range metrics.Len() {
		sample, sampleErr := metrics.SnapshotAt(index)
		if sampleErr != nil {
			t.Fatal(sampleErr)
		}
		if sample.Node == target.Node {
			foundTarget = sample.NodeAggregate && sample.Budget == wantBudget
		}
	}
	if !foundTarget {
		t.Fatalf("target aggregate budget was not visible: calls=%+v", calls)
	}
	if len(calls) != 3 || calls[2] != target {
		t.Fatalf("endpoint calls=%+v, want source then source/target exact identities", calls)
	}

	if err := metrics.UpdateNodeAggregates([]ReplicatedEndpoint{source}); err != nil {
		t.Fatalf("prune stale target: %v", err)
	}
	if metrics.Len() != 2 {
		t.Fatalf("pruned metrics samples=%d, want 2", metrics.Len())
	}
	if err := metrics.UpdateNodeAggregates([]ReplicatedEndpoint{source, target}); err != nil {
		t.Fatalf("re-add target identity: %v", err)
	}
	if metrics.Len() != 3 || len(metrics.slots) != 3 {
		t.Fatalf("re-added metrics len=%d backing slots=%d, want 3/3", metrics.Len(), len(metrics.slots))
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
