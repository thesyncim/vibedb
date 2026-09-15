package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/shardservice"
)

type sourceTopologyTestNative struct{ gateway.ReplicatedRoundTripper }

func (*sourceTopologyTestNative) ProbeReplicated(context.Context, gateway.ReplicatedRoute,
	gateway.ReplicatedEndpoint, serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	return nil, gateway.ErrReplicatedRoute
}

type sourceTopologyTestNoProbe struct{ gateway.ReplicatedRoundTripper }

func TestSourceTopologyUsesExistingSemanticOrAuthenticatedTransport(t *testing.T) {
	semantic := new(sourceTopologyTestNative)
	pool := new(gateway.AuthenticatedReplicatedClient)
	runtime := &Runtime{config: Config{Transport: semantic}, replicatedPool: pool}
	if selected, err := runtime.sourceTopologyNativeClient(); err != nil || selected != semantic {
		t.Fatalf("injected semantic transport was bypassed: selected=%T err=%v", selected, err)
	}
	runtime.config.Transport = new(sourceTopologyTestNoProbe)
	if selected, err := runtime.sourceTopologyNativeClient(); selected != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported semantic transport fell back to network pool: selected=%T err=%v", selected, err)
	}
	runtime.config.Transport = nil
	if selected, err := runtime.sourceTopologyNativeClient(); err != nil || selected != pool {
		t.Fatalf("existing authenticated pool not reused: selected=%T err=%v", selected, err)
	}
	runtime.replicatedPool = nil
	if selected, err := runtime.sourceTopologyNativeClient(); selected != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing transport accepted: selected=%T err=%v", selected, err)
	}
}

func TestSourceTopologyCannotOpenWithoutCommittedDirectory(t *testing.T) {
	for _, runtime := range []*Runtime{nil, {}, {replicatedPool: new(gateway.AuthenticatedReplicatedClient)}} {
		if err := runtime.openSourceTopologyService(gatewayReplicaControlManifest{}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("incomplete gateway authority opened source control: %v", err)
		}
	}
}

type sourceTopologyUnreadCatalog struct {
	splitcontroller.SourceTopologyCatalog
}

type sourceTopologyTestConnection struct {
	net.Conn
	reader *bytes.Reader
	peer   rafttransport.PeerIdentity
	key    [32]byte
}

func (connection *sourceTopologyTestConnection) Read(dst []byte) (int, error) {
	return connection.reader.Read(dst)
}
func (*sourceTopologyTestConnection) Close() error                    { return nil }
func (*sourceTopologyTestConnection) SetReadDeadline(time.Time) error { return nil }
func (connection *sourceTopologyTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.peer
}
func (connection *sourceTopologyTestConnection) PeerKeyDigest() [32]byte { return connection.key }
func (*sourceTopologyTestConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficGatewayControl
}

func TestSourceTopologyGatewayDispatchPreservesCurrentPeerBinding(t *testing.T) {
	peer := rafttransport.PeerIdentity{TrustDomain: rafttransport.TrustDomain{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}, Node: rafttransport.NodeID{3}}
	key := [32]byte{4}
	cut := serviceauthz.ServiceDirectoryCut{Revision: 1, PolicyGeneration: 1, TrustDomain: peer.TrustDomain,
		Bindings: []serviceauthz.ServiceBinding{{Principal: peer.Node, PhysicalNode: peer.Node,
			PhysicalIncarnation: 1, KeyDigest: key, Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive}}}
	directory, err := serviceauthz.NewServiceDirectoryGate(cut)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := splitcontroller.NewSourceTopologyService(splitcontroller.SourceTopologyServiceOptions{
		// A truncated frame must never reach any catalog or native method.
		Catalog: new(sourceTopologyUnreadCatalog), Directory: directory, TrustDomain: peer.TrustDomain,
		Native: new(sourceTopologyTestNative), Authority: serviceauthz.Authority{Node: rafttransport.NodeID{5}, Generation: 1},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{sourceTopologyService: service, controlReadDeadline: deadline}
	discriminator := splitcontroller.SourceTopologyRequestDiscriminator()
	request := func(key [32]byte) error {
		return runtime.serveGatewayControlConnection(t.Context(), &sourceTopologyTestConnection{
			reader: bytes.NewReader(discriminator[:]), peer: peer, key: key})
	}
	// The discriminator is consumed by dispatch and replayed to the selected
	// parser. UnexpectedEOF proves it read those bytes before the missing body.
	if err := request(key); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("closed source protocol was not dispatched with its prefix: %v", err)
	}
	if err := request([32]byte{6}); !errors.Is(err, splitcontroller.ErrSourceTopology) {
		t.Fatalf("dispatch lost the authenticated SPKI binding: %v", err)
	}
	for _, lifecycle := range []serviceauthz.ServiceLifecycle{serviceauthz.ServiceDraining, serviceauthz.ServiceDecommissioned} {
		cut.Revision++
		cut.Bindings[0].Lifecycle = lifecycle
		if err := directory.ApplyCommittedCut(cut); err != nil {
			t.Fatal(err)
		}
	}
	if err := request(key); !errors.Is(err, splitcontroller.ErrSourceTopology) {
		t.Fatalf("removed service identity retained source RPC access: %v", err)
	}
}
