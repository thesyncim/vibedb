package gatewayruntime

import (
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestFrontendDrainGracePeriodForceClosesLingeringConnections(t *testing.T) {
	id := FrontendDrainIdentity{
		NodeID:                  rafttransport.NodeID{1},
		Incarnation:             1,
		GatewayNodeID:           rafttransport.NodeID{2},
		GatewayIncarnation:      1,
		GatewayServiceKeyDigest: replication.Digest{3},
		SessionID:               [16]byte{4},
		SessionRevision:         1,
		NodeRevision:            1,
		DirectoryRevision:       1,
		CatalogGeneration:       1,
	}
	frontend := newFrontendAdmission(id, false, false)
	frontend.setGracePeriod(25 * time.Millisecond)
	defer frontend.stopGraceTimer()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	token, ok := frontend.admitNative()
	if !ok {
		t.Fatal("admitNative failed before drain")
	}
	tracked := &frontendTrackedConn{
		Conn:     serverConn,
		token:    token,
		scope:    serviceauthz.FrontendScopeNative,
		frontend: frontend,
		release:  func() { frontend.releaseNative(token) },
	}
	frontend.trackNativeConn(token, tracked)

	// Begin drain: SafeToStop must initially be false while connection is open.
	ack := frontend.begin(nil)
	if ack.SafeToStop || ack.ActiveNativeConnections != 1 {
		t.Fatalf("initial drain ack = %+v, want SafeToStop=false ActiveNativeConnections=1", ack)
	}

	// Wait past the 25ms grace period: lingering connection must be force-closed
	// and SafeToStop must become true.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ack = frontend.status()
		if ack.SafeToStop && ack.ActiveNativeConnections == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grace period did not force-close lingering connection: %+v", frontend.status())
}
