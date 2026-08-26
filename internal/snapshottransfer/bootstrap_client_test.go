package snapshottransfer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type bootstrapOpenFunc func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (open bootstrapOpenFunc) OpenShardControl(
	ctx context.Context,
	node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return open(ctx, node)
}

func TestBootstrapControlClientExecutesExactAuthenticatedRequest(t *testing.T) {
	request, identity, source := bootstrapControlFixture()
	target := rafttransport.NodeID{8}
	controller := rafttransport.PeerIdentity{Node: rafttransport.NodeID{9}, TrustDomain: bootstrapTrustDomain(request)}
	targetIdentity := rafttransport.PeerIdentity{Node: target, TrustDomain: bootstrapTrustDomain(request)}
	journal := &memoryBootstrapJournal{records: make(map[[32]byte]BootstrapRecord)}
	installer := &testBootstrapInstaller{identity: identity}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewBootstrapControlService(BootstrapControlOptions{
		Journal:   journal,
		Receiver:  bootstrapReceiveFunc(func(context.Context, rafttransport.NodeID, Descriptor) error { return nil }),
		Installer: installer,
		Authorize: func(peer rafttransport.PeerIdentity, got BootstrapRequest) bool {
			return peer == controller && got == request
		},
		SourceNode:   func(Descriptor) (rafttransport.NodeID, bool) { return source, true },
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	opens := 0
	opener := bootstrapOpenFunc(func(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		opens++
		if node != target {
			return nil, ErrBootstrapConflict
		}
		clientSide, serverSide := net.Pipe()
		go func() {
			serveDone <- service.Serve(context.Background(), &testPeerConn{
				Conn: serverSide, identity: controller, class: rafttransport.TrafficShardControl,
			})
		}()
		return &testPeerConn{
			Conn: clientSide, identity: targetIdentity, class: rafttransport.TrafficShardControl,
		}, nil
	})
	client, err := NewBootstrapControlClient(BootstrapControlClientOptions{
		Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := client.Execute(context.Background(), target, request)
	serveErr := <-serveDone
	if err != nil || serveErr != nil || opens != 1 || record.Request != request ||
		record.Identity != identity || record.State != BootstrapComplete {
		t.Fatalf("record=%+v opens=%d err=%v serveErr=%v", record, opens, err, serveErr)
	}
}

func TestBootstrapControlClientRejectsMismatchedTerminalEcho(t *testing.T) {
	request, identity, _ := bootstrapControlFixture()
	target := rafttransport.NodeID{8}
	peer := rafttransport.PeerIdentity{Node: target, TrustDomain: bootstrapTrustDomain(request)}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	tests := []struct {
		name   string
		mutate func(*BootstrapRequest, *raftmember.RuntimeIdentity)
	}{
		{name: "operation", mutate: func(request *BootstrapRequest, _ *raftmember.RuntimeIdentity) {
			request.Operation[0]++
		}},
		{name: "step", mutate: func(request *BootstrapRequest, _ *raftmember.RuntimeIdentity) {
			request.Step[0]++
		}},
		{name: "descriptor", mutate: func(request *BootstrapRequest, identity *raftmember.RuntimeIdentity) {
			request.Descriptor.TargetIncarnation++
			identity.NodeIncarnation++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan error, 1)
			opener := bootstrapOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
				clientSide, serverSide := net.Pipe()
				go func() {
					defer serverSide.Close()
					_, err := ReadBootstrapRequest(serverSide)
					if err == nil {
						mismatch, mismatchIdentity := request, identity
						test.mutate(&mismatch, &mismatchIdentity)
						err = WriteBootstrapResponse(serverSide, BootstrapRecord{
							Request: mismatch, Revision: 2, State: BootstrapComplete, Identity: mismatchIdentity,
						})
					}
					done <- err
				}()
				return &testPeerConn{Conn: clientSide, identity: peer, class: rafttransport.TrafficShardControl}, nil
			})
			client, err := NewBootstrapControlClient(BootstrapControlClientOptions{
				Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = client.Execute(context.Background(), target, request); !errors.Is(err, ErrBootstrapConflict) {
				t.Fatalf("mismatched echo err=%v", err)
			}
			if serverErr := <-done; serverErr != nil {
				t.Fatal(serverErr)
			}
		})
	}
}

func TestBootstrapControlClientDoesNotBlindRetryAfterWrite(t *testing.T) {
	request, _, _ := bootstrapControlFixture()
	target := rafttransport.NodeID{8}
	peer := rafttransport.PeerIdentity{Node: target, TrustDomain: bootstrapTrustDomain(request)}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	opens := 0
	received := make(chan error, 1)
	opener := bootstrapOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		opens++
		clientSide, serverSide := net.Pipe()
		go func() {
			_, err := ReadBootstrapRequest(serverSide)
			received <- err
			_ = serverSide.Close()
		}()
		return &testPeerConn{Conn: clientSide, identity: peer, class: rafttransport.TrafficShardControl}, nil
	})
	client, err := NewBootstrapControlClient(BootstrapControlClientOptions{
		Opener: opener, ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Execute(context.Background(), target, request); !errors.Is(err, ErrBootstrapOutcomeUnknown) {
		t.Fatalf("response-loss err=%v", err)
	}
	if receiveErr := <-received; receiveErr != nil || opens != 1 {
		t.Fatalf("received err=%v opens=%d", receiveErr, opens)
	}
}

func TestBootstrapControlClientOpenFailureIsDefiniteAndPeerFencePrecedesWrite(t *testing.T) {
	request, _, _ := bootstrapControlFixture()
	target := rafttransport.NodeID{8}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	dialFault := errors.New("dial refused")
	opens := 0
	client, err := NewBootstrapControlClient(BootstrapControlClientOptions{
		Opener: bootstrapOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			opens++
			return nil, dialFault
		}), ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Execute(context.Background(), target, request); !errors.Is(err, dialFault) ||
		errors.Is(err, ErrBootstrapOutcomeUnknown) || opens != 1 {
		t.Fatalf("dial err=%v opens=%d", err, opens)
	}

	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	wrongPeer := rafttransport.PeerIdentity{Node: rafttransport.NodeID{7}, TrustDomain: bootstrapTrustDomain(request)}
	client, err = NewBootstrapControlClient(BootstrapControlClientOptions{
		Opener: bootstrapOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			return &testPeerConn{Conn: clientSide, identity: wrongPeer, class: rafttransport.TrafficShardControl}, nil
		}), ReadDeadline: deadline, WriteDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Execute(context.Background(), target, request); !errors.Is(err, ErrBootstrapUnauthorized) {
		t.Fatalf("wrong peer err=%v", err)
	}
	_ = serverSide.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	var one [1]byte
	if count, readErr := serverSide.Read(one[:]); count != 0 || readErr == nil {
		t.Fatalf("request bytes reached wrong peer: count=%d err=%v", count, readErr)
	}
}

func TestBootstrapControlClientRequiresBoundedDeadlines(t *testing.T) {
	request, _, _ := bootstrapControlFixture()
	target := rafttransport.NodeID{8}
	peer := rafttransport.PeerIdentity{Node: target, TrustDomain: bootstrapTrustDomain(request)}
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	client, err := NewBootstrapControlClient(BootstrapControlClientOptions{
		Opener: bootstrapOpenFunc(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			return &testPeerConn{Conn: clientSide, identity: peer, class: rafttransport.TrafficShardControl}, nil
		}),
		ReadDeadline:  func() time.Time { return time.Time{} },
		WriteDeadline: func() time.Time { return time.Time{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Execute(context.Background(), target, request); !errors.Is(err, ErrBootstrapControl) {
		t.Fatalf("zero deadline err=%v", err)
	}
	_ = serverSide.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	var one [1]byte
	if count, readErr := serverSide.Read(one[:]); count != 0 || readErr == nil {
		t.Fatalf("zero deadline wrote request: count=%d err=%v", count, readErr)
	}
}

func bootstrapTrustDomain(request BootstrapRequest) rafttransport.TrustDomain {
	return rafttransport.TrustDomain{
		ClusterID:          request.Descriptor.Group.ClusterID,
		ClusterIncarnation: request.Descriptor.Group.ClusterIncarnation,
	}
}
