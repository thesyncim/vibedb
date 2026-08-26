package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func clusterDrainDigest(value byte) [sha256.Size]byte {
	var digest [sha256.Size]byte
	digest[0] = value
	return digest
}

func clusterDrainTrust() rafttransport.TrustDomain {
	return rafttransport.TrustDomain{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
	}
}

func clusterDrainMembers(count int) []ClusterCatalogDrainMember {
	members := make([]ClusterCatalogDrainMember, count)
	for index := range members {
		members[index].Node[0] = byte(index >> 8)
		members[index].Node[1] = byte(index)
		members[index].Node[15] = 1
		members[index].Incarnation = uint64(index + 1)
	}
	return members
}

func clusterDrainFence(t testing.TB, count int) ClusterCatalogDrainFence {
	t.Helper()
	fence, err := NewClusterCatalogDrainFence(
		clusterDrainDigest(3), 41, clusterDrainDigest(4), clusterDrainTrust(),
		clusterDrainMembers(count),
	)
	if err != nil {
		t.Fatalf("NewClusterCatalogDrainFence: %v", err)
	}
	return fence
}

func clusterDrainPeer(fence ClusterCatalogDrainFence, member ClusterCatalogDrainMember) rafttransport.PeerIdentity {
	return rafttransport.PeerIdentity{TrustDomain: fence.TrustDomain(), Node: member.Node}
}

func TestClusterCatalogDrainCollectClosesLocalOldGenerationAdmission(t *testing.T) {
	holder := NewCatalogHolder(testSnapshot(t, 40))
	old := holder.pinCurrent()
	if !holder.PublishNewer(testSnapshot(t, 41)) {
		t.Fatal("failed to publish drain generation")
	}
	fence := clusterDrainFence(t, 1)
	member, _ := fence.Member(0)
	type result struct {
		ack ClusterCatalogDrainAck
		err error
	}
	resultChannel := make(chan result, 1)
	go func() {
		ack, err := CollectClusterCatalogDrainAck(context.Background(), holder, fence, member)
		resultChannel <- result{ack: ack, err: err}
	}()
	select {
	case got := <-resultChannel:
		t.Fatalf("collect returned before old lease release: %+v %v", got.ack, got.err)
	case <-time.After(20 * time.Millisecond):
	}
	old.release()
	select {
	case got := <-resultChannel:
		if got.err != nil || got.ack.FenceDigest != fence.Digest() ||
			got.ack.Member != member || got.ack.CurrentGeneration != 41 {
			t.Fatalf("collected ack = %+v, %v", got.ack, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("collect did not observe old lease release")
	}
}

func TestClusterCatalogDrainAuthenticatedReplayAndCrashRecoveryBeyond64Gateways(t *testing.T) {
	fence := clusterDrainFence(t, 257)
	machine, err := NewClusterCatalogDrainMachine(fence)
	if err != nil {
		t.Fatalf("NewClusterCatalogDrainMachine: %v", err)
	}
	for index := 0; index < 256; index++ {
		member, _ := fence.Member(index)
		ack := ClusterCatalogDrainAck{
			FenceDigest: fence.Digest(), Member: member, CurrentGeneration: 41,
		}
		complete, applyErr := machine.ApplyAuthenticated(clusterDrainPeer(fence, member), ack)
		if applyErr != nil || complete {
			t.Fatalf("apply %d = %t, %v", index, complete, applyErr)
		}
	}
	member, _ := fence.Member(0)
	replay := ClusterCatalogDrainAck{FenceDigest: fence.Digest(), Member: member, CurrentGeneration: 99}
	if complete, applyErr := machine.ApplyAuthenticated(clusterDrainPeer(fence, member), replay); applyErr != nil || complete {
		t.Fatalf("idempotent replay = %t, %v", complete, applyErr)
	}
	checkpoint, err := AppendClusterCatalogDrainState(nil, machine)
	if err != nil {
		t.Fatalf("AppendClusterCatalogDrainState: %v", err)
	}
	recovered, err := OpenClusterCatalogDrainState(checkpoint)
	if err != nil {
		t.Fatalf("OpenClusterCatalogDrainState: %v", err)
	}
	if acknowledged, required := recovered.Progress(); acknowledged != 256 || required != 257 {
		t.Fatalf("recovered progress = %d/%d", acknowledged, required)
	}
	last, _ := fence.Member(256)
	lastAck := ClusterCatalogDrainAck{
		FenceDigest: fence.Digest(), Member: last, CurrentGeneration: 42,
	}
	if complete, applyErr := recovered.ApplyAuthenticated(clusterDrainPeer(fence, last), lastAck); applyErr != nil || !complete {
		t.Fatalf("last apply = %t, %v", complete, applyErr)
	}
	certificate, complete := recovered.Certificate()
	if !complete || certificate == ([sha256.Size]byte{}) {
		t.Fatalf("certificate = %x, %t", certificate, complete)
	}
	finalCheckpoint, err := AppendClusterCatalogDrainState(nil, recovered)
	if err != nil {
		t.Fatalf("append complete state: %v", err)
	}
	final, err := OpenClusterCatalogDrainState(finalCheckpoint)
	if err != nil {
		t.Fatalf("open complete state: %v", err)
	}
	if finalCertificate, ok := final.Certificate(); !ok || finalCertificate != certificate {
		t.Fatalf("recovered certificate = %x, %t; want %x", finalCertificate, ok, certificate)
	}
}

func TestClusterCatalogDrainRejectsUnboundPeerFenceAndGeneration(t *testing.T) {
	fence := clusterDrainFence(t, 2)
	machine, _ := NewClusterCatalogDrainMachine(fence)
	member, _ := fence.Member(0)
	ack := ClusterCatalogDrainAck{
		FenceDigest: fence.Digest(), Member: member, CurrentGeneration: 41,
	}
	wrongTrust := clusterDrainPeer(fence, member)
	wrongTrust.TrustDomain.ClusterID[0]++
	if _, err := machine.ApplyAuthenticated(wrongTrust, ack); !errors.Is(err, ErrClusterCatalogDrainAuth) {
		t.Fatalf("wrong trust error = %v", err)
	}
	wrongNode := clusterDrainPeer(fence, member)
	wrongNode.Node[0]++
	if _, err := machine.ApplyAuthenticated(wrongNode, ack); !errors.Is(err, ErrClusterCatalogDrainAuth) {
		t.Fatalf("wrong node error = %v", err)
	}
	ack.FenceDigest[0]++
	if _, err := machine.ApplyAuthenticated(clusterDrainPeer(fence, member), ack); !errors.Is(err, ErrClusterCatalogDrainAck) {
		t.Fatalf("wrong fence error = %v", err)
	}
	ack.FenceDigest = fence.Digest()
	ack.CurrentGeneration = 40
	if _, err := machine.ApplyAuthenticated(clusterDrainPeer(fence, member), ack); !errors.Is(err, ErrClusterCatalogDrainAck) {
		t.Fatalf("old generation error = %v", err)
	}
	if acknowledged, _ := machine.Progress(); acknowledged != 0 {
		t.Fatalf("rejected acknowledgements advanced progress to %d", acknowledged)
	}
}

func TestClusterCatalogDrainAckCanonicalCodec(t *testing.T) {
	fence := clusterDrainFence(t, 1)
	member, _ := fence.Member(0)
	want := ClusterCatalogDrainAck{
		FenceDigest: fence.Digest(), Member: member, CurrentGeneration: 57,
	}
	raw, err := AppendClusterCatalogDrainAck([]byte{9}, want)
	if err != nil || len(raw) != 1+ClusterCatalogDrainAckBytes {
		t.Fatalf("AppendClusterCatalogDrainAck = %d, %v", len(raw), err)
	}
	got, err := OpenClusterCatalogDrainAck(raw[1:])
	if err != nil || got != want {
		t.Fatalf("OpenClusterCatalogDrainAck = %+v, %v", got, err)
	}
	reencoded, err := AppendClusterCatalogDrainAck(nil, got)
	if err != nil || !bytes.Equal(reencoded, raw[1:]) {
		t.Fatal("ack decode/re-encode was not byte-identical")
	}
	for _, invalid := range [][]byte{
		raw[1 : len(raw)-1], append(bytes.Clone(raw[1:]), 0),
	} {
		if _, openErr := OpenClusterCatalogDrainAck(invalid); !errors.Is(openErr, ErrClusterCatalogDrainAck) {
			t.Fatalf("invalid ack error = %v", openErr)
		}
	}
	corrupt := bytes.Clone(raw[1:])
	corrupt[50]++
	if _, openErr := OpenClusterCatalogDrainAck(corrupt); !errors.Is(openErr, ErrClusterCatalogDrainAck) {
		t.Fatalf("corrupt ack error = %v", openErr)
	}
}

func TestClusterCatalogDrainStateRejectsNonCanonicalAndCorruptSnapshots(t *testing.T) {
	fence := clusterDrainFence(t, 9)
	machine, _ := NewClusterCatalogDrainMachine(fence)
	member, _ := fence.Member(0)
	ack := ClusterCatalogDrainAck{
		FenceDigest: fence.Digest(), Member: member, CurrentGeneration: 41,
	}
	if _, err := machine.ApplyAuthenticated(clusterDrainPeer(fence, member), ack); err != nil {
		t.Fatalf("ApplyAuthenticated: %v", err)
	}
	raw, err := AppendClusterCatalogDrainState(nil, machine)
	if err != nil {
		t.Fatalf("AppendClusterCatalogDrainState: %v", err)
	}
	for _, invalid := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, openErr := OpenClusterCatalogDrainState(invalid); !errors.Is(openErr, ErrClusterCatalogDrainState) {
			t.Fatalf("invalid state error = %v", openErr)
		}
	}
	corrupt := bytes.Clone(raw)
	corrupt[48]++
	if _, openErr := OpenClusterCatalogDrainState(corrupt); !errors.Is(openErr, ErrClusterCatalogDrainState) {
		t.Fatalf("corrupt state error = %v", openErr)
	}
	// Set a spare bit beyond the nine-member roster and recompute the checksum;
	// canonical validation must reject it independently of corruption checking.
	nonCanonical := bytes.Clone(raw)
	bitsetOffset := clusterCatalogDrainHeaderBytes + fence.MemberCount()*24
	nonCanonical[bitsetOffset+1] |= 0x80
	checksumOffset := len(nonCanonical) - sha256.Size
	checksum := sha256.Sum256(nonCanonical[:checksumOffset])
	copy(nonCanonical[checksumOffset:], checksum[:])
	if _, openErr := OpenClusterCatalogDrainState(nonCanonical); !errors.Is(openErr, ErrClusterCatalogDrainState) {
		t.Fatalf("noncanonical spare bits error = %v", openErr)
	}
}

func TestClusterCatalogDrainFenceOwnsCanonicalRoster(t *testing.T) {
	members := clusterDrainMembers(3)
	members[0], members[2] = members[2], members[0]
	fence, err := NewClusterCatalogDrainFence(
		clusterDrainDigest(3), 41, clusterDrainDigest(4), clusterDrainTrust(), members,
	)
	if err != nil {
		t.Fatalf("NewClusterCatalogDrainFence: %v", err)
	}
	members[0] = ClusterCatalogDrainMember{}
	first, _ := fence.Member(0)
	if first.Node == (rafttransport.NodeID{}) || !fence.valid() {
		t.Fatal("fence retained caller-owned mutable roster")
	}
	duplicate := clusterDrainMembers(2)
	duplicate[1].Node = duplicate[0].Node
	if _, err = NewClusterCatalogDrainFence(
		clusterDrainDigest(3), 41, clusterDrainDigest(4), clusterDrainTrust(), duplicate,
	); !errors.Is(err, ErrClusterCatalogDrainFence) {
		t.Fatalf("duplicate node error = %v", err)
	}
}

func TestClusterCatalogDrainConcurrentApplyAndCheckpoint(t *testing.T) {
	fence := clusterDrainFence(t, 257)
	machine, _ := NewClusterCatalogDrainMachine(fence)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := 0; index < fence.MemberCount(); index++ {
			member, _ := fence.Member(index)
			ack := ClusterCatalogDrainAck{
				FenceDigest: fence.Digest(), Member: member, CurrentGeneration: 41,
			}
			if _, err := machine.ApplyAuthenticated(clusterDrainPeer(fence, member), ack); err != nil {
				t.Errorf("ApplyAuthenticated %d: %v", index, err)
				return
			}
		}
	}()
	for {
		raw, err := AppendClusterCatalogDrainState(nil, machine)
		if err != nil {
			t.Fatalf("AppendClusterCatalogDrainState: %v", err)
		}
		if _, err = OpenClusterCatalogDrainState(raw); err != nil {
			t.Fatalf("OpenClusterCatalogDrainState: %v", err)
		}
		select {
		case <-done:
			if !machine.Complete() {
				t.Fatal("machine incomplete after all acknowledgements")
			}
			return
		default:
		}
	}
}

type clusterDrainCollector struct {
	limit  int
	fences []ClusterCatalogDrainFence
}

func (collector *clusterDrainCollector) CollectClusterCatalogDrain(
	_ context.Context,
	fence ClusterCatalogDrainFence,
	accept func(rafttransport.PeerIdentity, ClusterCatalogDrainAck) error,
) error {
	collector.fences = append(collector.fences, fence)
	count := fence.MemberCount()
	if collector.limit != 0 && collector.limit < count {
		count = collector.limit
	}
	for index := 0; index < count; index++ {
		member, _ := fence.Member(index)
		if err := accept(clusterDrainPeer(fence, member), ClusterCatalogDrainAck{
			FenceDigest: fence.Digest(), Member: member,
			CurrentGeneration: fence.Generation(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func TestClusterCatalogDrainCoordinatorBindsExactMoveG1AndG2Cuts(t *testing.T) {
	collector := new(clusterDrainCollector)
	coordinator, err := NewClusterCatalogDrainCoordinator(
		clusterDrainTrust(), clusterDrainMembers(3), collector,
	)
	if err != nil {
		t.Fatalf("NewClusterCatalogDrainCoordinator: %v", err)
	}
	g1 := ClusterCatalogDrainRequest{
		Operation: clusterDrainDigest(7), Step: clusterDrainDigest(8),
		Generation: 42, CatalogDigest: clusterDrainDigest(9),
	}
	g1Certificate, err := coordinator.CertifyClusterCatalogDrain(context.Background(), g1)
	if err != nil || !g1Certificate.ValidFor(g1) {
		t.Fatalf("G+1 certificate=%+v err=%v", g1Certificate, err)
	}
	g2 := g1
	g2.Step = clusterDrainDigest(10)
	g2.Generation++
	g2.CatalogDigest = clusterDrainDigest(11)
	g2Certificate, err := coordinator.CertifyClusterCatalogDrain(context.Background(), g2)
	if err != nil || !g2Certificate.ValidFor(g2) {
		t.Fatalf("G+2 certificate=%+v err=%v", g2Certificate, err)
	}
	if g1Certificate.FenceDigest == g2Certificate.FenceDigest ||
		g1Certificate.Proof == g2Certificate.Proof || len(collector.fences) != 2 {
		t.Fatalf("G+1 and G+2 were not distinct: g1=%+v g2=%+v", g1Certificate, g2Certificate)
	}
	mutated := g1
	mutated.Step[0]++
	if g1Certificate.ValidFor(mutated) || g1Certificate.ValidFor(g2) {
		t.Fatal("certificate crossed an exact move-step/catalog cut")
	}
}

func TestClusterCatalogDrainCoordinatorRejectsIncompleteRoster(t *testing.T) {
	collector := &clusterDrainCollector{limit: 2}
	coordinator, err := NewClusterCatalogDrainCoordinator(
		clusterDrainTrust(), clusterDrainMembers(3), collector,
	)
	if err != nil {
		t.Fatal(err)
	}
	request := ClusterCatalogDrainRequest{
		Operation: clusterDrainDigest(7), Step: clusterDrainDigest(8),
		Generation: 42, CatalogDigest: clusterDrainDigest(9),
	}
	if _, err = coordinator.CertifyClusterCatalogDrain(
		context.Background(), request,
	); !errors.Is(err, ErrClusterCatalogDrainState) {
		t.Fatalf("incomplete roster error = %v", err)
	}
}
