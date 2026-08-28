package snapshottransfer

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestGroupBootstrapControlRegistryRoutesExactGroup(t *testing.T) {
	request1, identity1, source := bootstrapControlFixture()
	request2 := request1
	request2.Operation[0]++
	request2.Step[0]++
	request2.Descriptor.Group.GroupID[0]++
	request2.Descriptor.Group.ShardIncarnation[0]++
	request2.Descriptor.TargetStore[0]++
	request2.Descriptor.TargetIncarnation++
	identity2 := identity1
	identity2.Group = request2.Descriptor.Group
	identity2.StoreID = request2.Descriptor.TargetStore
	identity2.NodeIncarnation = request2.Descriptor.TargetIncarnation
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	installs := [2]int{}
	service := func(index int, request BootstrapRequest, identity raftmember.RuntimeIdentity) *BootstrapControlService {
		installer := &testBootstrapInstaller{identity: identity}
		result, err := NewBootstrapControlService(BootstrapControlOptions{
			Journal:      &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)},
			Receiver:     bootstrapReceiveFunc(func(context.Context, rafttransport.NodeID, Descriptor) error { return nil }),
			Installer:    installer,
			Releaser:     BootstrapArtifactReleaseFunc(func(context.Context, BootstrapRequest, raftmember.RuntimeIdentity) error { return nil }),
			Authorize:    func(_ rafttransport.PeerIdentity, got BootstrapRequest) bool { return got == request },
			SourceNode:   func(Descriptor) (rafttransport.NodeID, bool) { return source, true },
			ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		original := result.installer
		result.installer = bootstrapInstallCounter{BootstrapInstaller: original, count: &installs[index]}
		return result
	}
	completed := make(map[raftmember.GroupKey]int)
	domain := rafttransport.TrustDomain{ClusterID: request1.Descriptor.Group.ClusterID,
		ClusterIncarnation: request1.Descriptor.Group.ClusterIncarnation}
	registry, err := NewGroupBootstrapControlRegistry(GroupBootstrapControlRegistryOptions{
		TrustDomain: domain, ReadDeadline: deadline, MaxConnections: 2,
		Services: []GroupBootstrapControlService{
			{Group: request1.Descriptor.Group, Service: service(0, request1, identity1)},
			{Group: request2.Descriptor.Group, Service: service(1, request2, identity2)},
		}, Complete: func(group raftmember.GroupKey) { completed[group]++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{9}}
	for _, request := range []BootstrapRequest{request2, request1} {
		client, server := net.Pipe()
		done := make(chan error, 1)
		go func() {
			done <- registry.Serve(t.Context(), &testPeerConn{Conn: server, identity: peer,
				class: rafttransport.TrafficShardControl})
		}()
		if err = WriteBootstrapRequest(client, request); err != nil {
			t.Fatal(err)
		}
		response, readErr := ReadBootstrapResponse(client)
		_ = client.Close()
		if serveErr := <-done; readErr != nil || serveErr != nil || response.Request != request {
			t.Fatalf("response=%+v read=%v serve=%v", response, readErr, serveErr)
		}
	}
	if installs != [2]int{1, 1} || completed[request1.Descriptor.Group] != 1 ||
		completed[request2.Descriptor.Group] != 1 {
		t.Fatalf("installs=%v completed=%v", installs, completed)
	}
}

type bootstrapInstallCounter struct {
	BootstrapInstaller
	count *int
}

func (counter bootstrapInstallCounter) InstallPublishedLearner(
	ctx context.Context, descriptor Descriptor,
) (raftmember.RuntimeIdentity, error) {
	*counter.count++
	return counter.BootstrapInstaller.InstallPublishedLearner(ctx, descriptor)
}
