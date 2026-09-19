package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/internal/servicemetrics"
	"github.com/thesyncim/vibedb/internal/shardcontrol"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	publicshardcontrol "github.com/thesyncim/vibedb/shardcontrol"
	"github.com/thesyncim/vibedb/shardservice"
)

type rf3ControlBufferConnection struct {
	net.Conn
	input    *bytes.Reader
	output   bytes.Buffer
	identity rafttransport.PeerIdentity
}

func (c *rf3ControlBufferConnection) Read(p []byte) (int, error)               { return c.input.Read(p) }
func (c *rf3ControlBufferConnection) Write(p []byte) (int, error)              { return c.output.Write(p) }
func (*rf3ControlBufferConnection) Close() error                               { return nil }
func (*rf3ControlBufferConnection) SetReadDeadline(time.Time) error            { return nil }
func (*rf3ControlBufferConnection) SetWriteDeadline(time.Time) error           { return nil }
func (c *rf3ControlBufferConnection) PeerIdentity() rafttransport.PeerIdentity { return c.identity }
func (*rf3ControlBufferConnection) PeerKeyDigest() [32]byte                    { return [32]byte{} }
func (*rf3ControlBufferConnection) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

type rf3ControlRouteProbe struct {
	calls int
	seen  [8]byte
}

func (handler *rf3ControlRouteProbe) Serve(_ context.Context, conn rafttransport.PeerConnection) error {
	handler.calls++
	_, err := io.ReadFull(conn, handler.seen[:])
	return err
}

// The literal protocol list is the supported physical-node grammar, including
// services needed only after an initially empty node becomes a donor. Every
// route must reach its handler with the original discriminator still present.
func TestRF3ControlServicesShareCompleteNodeGrammar(t *testing.T) {
	var probes [23]rf3ControlRouteProbe
	services := rf3ControlServices{
		membership: &probes[0], observation: &probes[1], metrics: &probes[2], capacity: &probes[3],
		preparation: &probes[4], enrollment: &probes[5], backup: &probes[6], source: &probes[7],
		action: &probes[8], split: &probes[9], schema: &probes[10], schemaBuild: &probes[11],
		planObservation: &probes[12], admission: &probes[13], tail: &probes[14], terminal: &probes[15],
		childPrepare: &probes[16], restoreServing: &probes[17], nodeInfo: &probes[18], nodeControl: &probes[19], bootstrap: &probes[20], preparedAck: &probes[21], canonicalSource: &probes[22],
	}
	mux, err := services.mux()
	if err != nil {
		t.Fatal(err)
	}
	protocols := []struct {
		name          string
		discriminator [8]byte
		handler       int
	}{
		{"membership", shardservice.MembershipGrantRequestDiscriminator(), 0},
		{"replica-observation-and-health", replicacontrol.RequestDiscriminator(), 1},
		{"metrics", servicemetrics.RequestDiscriminator(), 2},
		{"capacity", replicacontrol.CapacityRequestDiscriminator(), 3},
		{"donor-preparation", nodecontrol.PreparationSourceRequestDiscriminator(), 4},
		{"peer-enrollment", rafttransport.EnrollmentRequestDiscriminator(), 5},
		{"backup", clusterbackup.LiveRequestDiscriminator(), 6},
		{"snapshot-source", snapshottransfer.SourceControlRequestDiscriminator(), 7},
		{"ownership-and-source-retirement", replicaaction.RequestDiscriminator(), 8},
		{"split-action", publicshardcontrol.RequestDiscriminator(), 9},
		{"schema-install", schemainstall.RequestDiscriminator(), 10},
		{"schema-build", schemainstall.BuildRequestDiscriminator(), 11},
		{"schema-resume", schemainstall.BuildResumeRequestDiscriminator(), 11},
		{"schema-shadow", schemainstall.BuildShadowRequestDiscriminator(), 11},
		{"split-observation", splitcontroller.PlanObservationRequestDiscriminator(), 12},
		{"split-admission", splitcontroller.PlanAdmissionRequestDiscriminator(), 13},
		{"split-tail", splitcontroller.TailStreamRequestDiscriminator(), 14},
		{"split-retirement", splitcontroller.TerminalRetirementRequestDiscriminator(), 15},
		{"split-child-prepare", splitcontroller.ChildPrepareRequestDiscriminator(), 16},
		{"restore-serving", shardservice.RestoreServingRequestDiscriminator(), 17},
		{"joining-node-info", nodecontrol.NodeInfoRequestDiscriminator(), 18},
		{"node-enrollment", nodecontrol.RequestDiscriminator(), 19},
		{"snapshot-bootstrap", snapshottransfer.BootstrapRequestDiscriminator(), 20},
		{"frontend-drain-prepared-ack", frontenddrain.PreparedAckDiscriminator, 21},
		{"frontend-drain-canonical-source", frontenddrain.PreparedAckCutReadDiscriminator, 22},
	}
	if len(protocols) > shardcontrol.MaxRoutes {
		t.Fatal("physical grammar exceeds mux bound")
	}
	for _, test := range protocols {
		t.Run(test.name, func(t *testing.T) {
			handler := &probes[test.handler]
			before := handler.calls
			conn := &rf3ControlBufferConnection{input: bytes.NewReader(test.discriminator[:])}
			if err := mux.Serve(context.Background(), conn); err != nil {
				t.Fatal(err)
			}
			if handler.calls != before+1 || handler.seen != test.discriminator {
				t.Fatalf("wrong handler or lost discriminator: %+v", handler)
			}
		})
	}
	unknown := &rf3ControlBufferConnection{input: bytes.NewReader([]byte("unknown!"))}
	if err := mux.Serve(context.Background(), unknown); !errors.Is(err, shardcontrol.ErrMux) {
		t.Fatalf("unknown service=%v", err)
	}
}
