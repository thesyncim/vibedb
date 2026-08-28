package replication

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/routegate"
)

func TestRouteGateCommandRoundTripAndZeroAllocation(t *testing.T) {
	gate := routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 9,
		Identity: routegate.Identity(sha256.Sum256([]byte("request"))),
		Binding:  routegate.Binding(sha256.Sum256([]byte("recipe"))),
	}
	var gateStorage [routegate.CommandBytes]byte
	gateBytes, err := routegate.AppendCommand(gateStorage[:0], gate)
	if err != nil {
		t.Fatal(err)
	}
	command := testCommand()
	command.Kind = CommandRouteGate
	command.Batches = nil
	command.RouteGate = gateBytes
	first, err := AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	storage := make([]byte, 0, len(first))
	encoded, err := AppendCommand(storage[:0], command)
	if err != nil {
		t.Fatal(err)
	}
	view, err := OpenCommand(encoded)
	if err != nil || view.Kind() != CommandRouteGate {
		t.Fatalf("OpenCommand = %d, %v", view.Kind(), err)
	}
	opened, err := view.OpenRouteGate()
	if err != nil || opened != gate {
		t.Fatalf("OpenRouteGate = %+v, %v", opened, err)
	}
	physical, ok := RouteGatePhysicalWitness(view)
	if !ok || physical == (Digest{}) {
		t.Fatal("route-gate physical witness unavailable")
	}
	changed := command
	changed.RouteGeneration++
	changedBytes, err := AppendCommand(nil, changed)
	if err != nil {
		t.Fatal(err)
	}
	changedView, err := OpenCommand(changedBytes)
	if err != nil {
		t.Fatal(err)
	}
	changedPhysical, ok := RouteGatePhysicalWitness(changedView)
	if !ok || changedPhysical == physical {
		t.Fatal("route-gate physical witness ignored a route fence")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		bytes, appendErr := AppendCommand(storage[:0], command)
		if appendErr != nil {
			panic(appendErr)
		}
		openedView, openErr := OpenCommand(bytes)
		if openErr != nil {
			panic(openErr)
		}
		if _, openErr = openedView.OpenRouteGate(); openErr != nil {
			panic(openErr)
		}
		if _, witnessOK := RouteGatePhysicalWitness(openedView); !witnessOK {
			panic("missing physical witness")
		}
	}); allocs != 0 {
		t.Fatalf("Append/Open route-gate allocations = %v", allocs)
	}
}

func TestMaxRouteGateCommandBytesMatchesActualCodec(t *testing.T) {
	command := testCommand()
	command.Kind, command.Batches = CommandRouteGate, nil
	command.Tenant = []byte(strings.Repeat("t", MaxIdentityBytes))
	command.Distribution = strings.Repeat("d", MaxIdentityBytes)
	command.Shard = strings.Repeat("s", MaxIdentityBytes)
	command.RouteGate, _ = routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: routegate.Identity(sha256.Sum256([]byte("max-request"))),
		Binding:  routegate.Binding(sha256.Sum256([]byte("max-binding"))),
	})
	encoded, err := AppendCommand(nil, command)
	if err != nil || len(encoded) != MaxRouteGateCommandBytes ||
		MaxRouteGateCommandBytes != 1113 {
		t.Fatalf("max route-gate command = %d/%d, %v", len(encoded), MaxRouteGateCommandBytes, err)
	}
}
