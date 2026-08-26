package splitcontroller

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type planAdmissionTestOpener struct {
	service *PlanAdmissionService
	peer    rafttransport.PeerIdentity
}

func (opener planAdmissionTestOpener) OpenShardControl(
	ctx context.Context, _ rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	client, server := net.Pipe()
	go func() {
		_ = opener.service.Serve(ctx, &planObservationTestConnection{
			Conn: server, peer: opener.peer, class: rafttransport.TrafficShardControl,
		})
	}()
	return &planObservationTestConnection{
		Conn: client, peer: opener.peer, class: rafttransport.TrafficShardControl,
	}, nil
}

func TestPlanAdmissionTransportAuthenticatesInstallsAndReceipts(t *testing.T) {
	plan, catalog, _, _ := testPlan(t)
	admission, err := NewPlanAdmission(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	request, err := AppendPlanAdmissionRequest(nil, catalog, admission)
	if err != nil {
		t.Fatal(err)
	}
	envelope, total, err := openPlanAdmissionRequestHeader(request[:planAdmissionRequestHeader])
	if err != nil || total != len(request) || envelope.Operation != admission.Operation {
		t.Fatalf("envelope=%+v total=%d err=%v", envelope, total, err)
	}
	openedCatalog, openedAdmission, err := openPlanAdmissionRequest(request, envelope)
	if err != nil || openedCatalog.Generation() != catalog.Generation() ||
		openedAdmission.PlanDigest != admission.PlanDigest {
		t.Fatalf("admission=%+v err=%v", openedAdmission, err)
	}

	root := filepath.Join(t.TempDir(), "split-runtime")
	if err = os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	registry, err := OpenRuntimeStoreRegistry(root, testManifestDigest("transport"), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	binder := new(planAdmissionBinderStub)
	installer, err := NewPlanAdmissionInstaller(
		planAdmissionStoresStub{stores: []*RuntimeStoreRegistry{registry}}, binder, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	controller := rafttransport.PeerIdentity{Node: rafttransport.NodeID{7}}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewPlanAdmissionService(PlanAdmissionServiceOptions{
		Installer: installer,
		Authorize: func(peer rafttransport.PeerIdentity, got PlanAdmissionEnvelope) bool {
			return peer.Node == controller.Node && got == envelope
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: MaxPlanAdmissionRequestBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- service.Serve(t.Context(), &planObservationTestConnection{
			Conn: server, peer: controller, class: rafttransport.TrafficShardControl,
		})
	}()
	if _, err = client.Write(request); err != nil {
		t.Fatal(err)
	}
	var response [planAdmissionResponseBytes]byte
	if _, err = io.ReadFull(client, response[:]); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err = <-done; err != nil || binder.calls != 1 || !validPlanAdmissionResponse(response[:], admission) {
		t.Fatalf("calls=%d response=%x err=%v", binder.calls, response[:16], err)
	}
	clientRuntime, err := NewPlanAdmissionClient(PlanAdmissionClientOptions{
		Opener:       planAdmissionTestOpener{service: service, peer: controller},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 1,
		MaxInflightBytes: MaxPlanAdmissionRequestBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = clientRuntime.Install(t.Context(), rafttransport.NodeID{9}, catalog, admission); err != nil || binder.calls != 2 {
		t.Fatalf("client install calls=%d err=%v", binder.calls, err)
	}

	corrupt := bytes.Clone(request)
	corrupt[len(corrupt)-1] ^= 1
	if _, _, err = openPlanAdmissionRequest(corrupt, envelope); err == nil {
		t.Fatal("corrupt request accepted")
	}
}
