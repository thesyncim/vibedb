package shardservice

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestRestoreServingControlRequiresLiveDedicatedAuthorityAndExactReplica(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	node, store := rafttransport.NodeID{6}, [16]byte{7}
	identity := raftmember.RuntimeIdentity{Group: group, MemberID: 2, StoreID: store, NodeIncarnation: 1}
	gate, err := NewRestoreServingGate(identity, node)
	if err != nil {
		t.Fatal(err)
	}
	state := raftservice.ServingState{Identity: identity}
	if gate.Allows(state) {
		t.Fatal("fresh process served before live catalog grant")
	}
	grant := restoreServingGrantFixture(t, group, 2, node, store, 1)
	controller := rafttransport.PeerIdentity{TrustDomain: rafttransport.TrustDomain{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}, Node: rafttransport.NodeID{9}}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: controller.Node,
		Capabilities: serviceauthz.CapabilityRestoreActivate}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewRestoreServingControlService(gate, policy, deadline, deadline)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	opener := membershipGrantOpenFunc(func(_ context.Context, target rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		if target != node {
			return nil, ErrRestoreServingControl
		}
		clientSide, serverSide := net.Pipe()
		go func() {
			serveDone <- service.Serve(context.Background(), &membershipGrantTestConnection{
				Conn: serverSide, identity: controller, class: rafttransport.TrafficShardControl})
		}()
		return &membershipGrantTestConnection{Conn: clientSide,
			identity: rafttransport.PeerIdentity{TrustDomain: controller.TrustDomain, Node: node},
			class:    rafttransport.TrafficShardControl}, nil
	})
	client, err := NewRestoreServingControlClient(opener, deadline, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Install(t.Context(), node, grant); err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nil {
		t.Fatal(err)
	}
	if !gate.Allows(state) {
		t.Fatal("exact live grant did not open restored serving")
	}
	// A new process-local gate starts closed even beside the same durable root.
	restarted, err := NewRestoreServingGate(identity, node)
	if err != nil || restarted.Allows(state) {
		t.Fatalf("restarted gate open=%t err=%v", restarted.Allows(state), err)
	}
}

func TestRestoreServingControlRejectsOtherCapabilitiesAndStaleReplica(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	node, store := rafttransport.NodeID{6}, [16]byte{7}
	identity := raftmember.RuntimeIdentity{Group: group, MemberID: 2, StoreID: store, NodeIncarnation: 1}
	gate, _ := NewRestoreServingGate(identity, node)
	stale := restoreServingGrantFixture(t, group, 2, node, store, 2)
	if err := gate.Install(stale); !errors.Is(err, ErrRestoreServingControl) {
		t.Fatalf("stale incarnation err=%v", err)
	}
	for _, capability := range []serviceauthz.Capability{serviceauthz.CapabilityBackup,
		serviceauthz.CapabilityTopology, serviceauthz.CapabilityMembership} {
		policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{Node: rafttransport.NodeID{9},
			Capabilities: capability}})
		if err != nil {
			t.Fatal(err)
		}
		deadline := func() time.Time { return time.Now().Add(time.Second) }
		if _, err = NewRestoreServingControlService(gate, policy, deadline, deadline); !errors.Is(
			err, ErrRestoreServingControl) {
			t.Fatalf("capability=%d err=%v", capability, err)
		}
	}
}

func TestRestoreServingControlRegistryRejectsDuplicateGroups(t *testing.T) {
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	node := rafttransport.NodeID{6}
	gate, err := NewRestoreServingGate(raftmember.RuntimeIdentity{
		Group: group, MemberID: 1, StoreID: [16]byte{7}, NodeIncarnation: 1,
	}, node)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{
		Node: rafttransport.NodeID{9}, Capabilities: serviceauthz.CapabilityRestoreActivate,
	}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	if _, err = NewRestoreServingControlRegistryService(
		[]*RestoreServingGate{gate, gate}, policy, deadline, deadline,
	); !errors.Is(err, ErrRestoreServingControl) {
		t.Fatalf("duplicate group err=%v", err)
	}
}

func restoreServingGrantFixture(t *testing.T, group raftmember.GroupKey, member uint64,
	node rafttransport.NodeID, store [16]byte, incarnation uint64,
) clusterrestore.ServingGrant {
	t.Helper()
	raw := make([]byte, clusterrestore.ServingGrantBytes)
	discriminator := clusterrestore.ServingGrantDiscriminator()
	copy(raw[:8], discriminator[:])
	raw[8], raw[40] = 1, 2
	copy(raw[72:88], group.ClusterID[:])
	copy(raw[88:104], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(raw[104:112], group.TopologyRecoveryEpoch)
	copy(raw[112:128], group.ShardIncarnation[:])
	copy(raw[128:144], group.GroupID[:])
	binary.BigEndian.PutUint64(raw[144:152], member)
	copy(raw[152:168], node[:])
	copy(raw[168:184], store[:])
	binary.BigEndian.PutUint64(raw[184:192], incarnation)
	digest := sha256.Sum256(raw[:192])
	copy(raw[192:], digest[:])
	grant, err := clusterrestore.OpenServingGrant(raw)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}
