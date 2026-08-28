package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// The lifecycle unit fixture isolates ledger crash cuts. Actual native session
// opening, sequence admission and durable retry are covered separately against
// a real replicated state machine.
func (proposer *lifecycleRunnerProposer) prepareAcquire(_ context.Context, route ReplicatedRoute, wave DurableRequestWave, head requestledger.HeadRecord) ([]byte, requestledger.Digest, error) {
	identity, err := requestledger.DeriveRouteGateIdentity(head.KeyDigest, head.RequestDigest, head.PlanRoot, head.ContinuationDigest, wave.PinID, head.NextStepOrdinal)
	if err != nil {
		return nil, requestledger.Digest{}, err
	}
	clientID, err := durableRouteSessionIdentity(identity, route, wave.Tenant)
	if err != nil {
		return nil, requestledger.Digest{}, err
	}
	session := &NativeSession{route: route, distribution: string(route.Distribution), shard: string(route.Shard), tenant: wave.Tenant,
		clientID: clientID, retryHome: wave.Identity.RetryHome, epoch: 2, nextSequence: 2, ackThrough: 1,
		proposalCapability: serviceauthz.CapabilityDataWrite}
	return appendDurableRequestRouteGateCommand(nil, route, wave, head.KeyDigest, head.RequestDigest,
		head.PlanRoot, head.ContinuationDigest, head.NextStepOrdinal, routegate.OperationAcquireShared, session)
}

func (proposer *lifecycleRunnerProposer) prepareRelease(ctx context.Context, route ReplicatedRoute, wave DurableRequestWave, pin requestledger.RoutePinRecord) ([]byte, error) {
	driver := nativeDurableRequestRouteGateSessions{executor: &ReplicatedExecutor{}}
	return driver.prepareRelease(ctx, route, wave, pin)
}

func (proposer *lifecycleRunnerProposer) cleanup(context.Context, ReplicatedRoute, DurableRequestWave, requestledger.RoutePinRecord) error {
	return nil
}

type routeSessionDropClient struct {
	base    *routeSessionMachineClient
	drop    replication.CommandKind
	dropped []byte
}

func (client *routeSessionDropClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	response, err := client.base.DoReplicated(ctx, endpoint, request)
	if err != nil || len(request.Command) == 0 {
		return response, err
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	if client.drop != 0 && command.Kind() == client.drop {
		client.drop = 0
		client.dropped = bytes.Clone(request.Command)
		return nil, errors.New("lost committed session response")
	}
	return response, nil
}

func TestDurableRequestRouteSessionLedgerRecovery(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		name := "singleton"
		if checkpoint {
			name = "checkpoint-batch"
		}
		t.Run(name, func(t *testing.T) { testDurableRequestRouteSessionLedgerRecovery(t, checkpoint) })
	}
}

func testDurableRequestRouteSessionLedgerRecovery(t *testing.T, checkpoint bool) {
	route, machineClient, reopen := newRouteSessionMachineWithCheckpoint(t, checkpoint)
	client := &routeSessionDropClient{base: machineClient}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	wave, head, _ := lifecycleRunnerFixture(t)
	wave.GateEpoch = 999 // Must not be confused with the shard's gate epoch.
	driver := &nativeDurableRequestRouteGateSessions{executor: executor}
	acquire, physical, err := driver.prepareAcquire(ctx, route, wave, head)
	if err != nil {
		t.Fatal(err)
	}
	// No gate side effect may precede the replicated route intent. An Open
	// response lost before that intent is safe to reconstruct identically.
	again, _, err := driver.prepareAcquire(ctx, route, wave, head)
	if err != nil || !bytes.Equal(acquire, again) {
		t.Fatalf("open replay changed acquire: %v", err)
	}
	outer, err := replication.OpenCommand(acquire)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := outer.OpenRouteGate()
	if err != nil || gate.Epoch != 1 || outer.ClientEpoch != 2 || outer.ClientSequence != 2 || outer.AckThrough != 1 {
		t.Fatalf("wrong session/gate identities: %+v %v", gate, err)
	}
	status, err := machineClient.machine.RouteGateStatus()
	if err != nil || status.ActivePins != 0 {
		t.Fatalf("acquired before ledger intent: %+v %v", status, err)
	}
	pin, err := requestledger.NewRoutePinAcquiring(head, wave.PinID, wave.Binding, physical, acquire)
	if err != nil {
		t.Fatal(err)
	}
	client.drop = replication.CommandRouteGate
	if _, err := executor.Propose(ctx, route, pin.Command); err == nil {
		t.Fatal("acknowledged lost acquire response")
	}
	reopen()
	result, err := executor.Propose(ctx, route, pin.Command)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.RecordVerifiedRoutePinAcquired(pin, pin.Revision+1, result.Completion)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh gateway has only the replicated intent/proof, no local journal.
	driver = &nativeDurableRequestRouteGateSessions{executor: executor}
	release, err := driver.prepareRelease(ctx, route, wave, pin)
	if err != nil {
		t.Fatal(err)
	}
	again, err = driver.prepareRelease(ctx, route, wave, pin)
	if err != nil || !bytes.Equal(release, again) {
		t.Fatalf("release reconstruction changed bytes: %v", err)
	}
	releaseView, err := replication.OpenCommand(release)
	if err != nil || releaseView.ClientEpoch != outer.ClientEpoch || releaseView.ClientSequence != 3 || releaseView.AckThrough != 2 {
		t.Fatalf("release session sequence changed: %v", err)
	}
	releaseGate, err := releaseView.OpenRouteGate()
	if err != nil || releaseGate.Epoch != gate.Epoch || releaseGate.Identity != gate.Identity || releaseGate.Binding != gate.Binding {
		t.Fatalf("release changed acquired gate identity: %v", err)
	}
	pin, err = requestledger.BeginRoutePinRelease(pin, pin.Revision+1, release)
	if err != nil {
		t.Fatal(err)
	}
	client.drop = replication.CommandRouteGate
	if _, err := executor.Propose(ctx, route, pin.Command); err == nil {
		t.Fatal("acknowledged lost release response")
	}
	reopen()
	result, err = executor.Propose(ctx, route, pin.Command)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = requestledger.RecordVerifiedRoutePinReleased(pin, pin.Revision+1, result.Completion)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []replication.CommandKind{replication.CommandSessionRetire, replication.CommandSessionRelease} {
		client.drop = kind
		if err := driver.cleanup(ctx, route, wave, pin); err == nil {
			t.Fatalf("acknowledged lost cleanup response %d", kind)
		}
		reopen()
		driver = &nativeDurableRequestRouteGateSessions{executor: executor}
	}
	if err := driver.cleanup(ctx, route, wave, pin); err != nil {
		t.Fatal(err)
	}
	if err := driver.cleanup(ctx, route, wave, pin); err != nil {
		t.Fatalf("cleanup is not idempotent: %v", err)
	}
	status, err = machineClient.machine.RouteGateStatus()
	if err != nil || status.ActivePins != 0 || status.ReleasedPins != 1 {
		t.Fatalf("gate leaked: %+v %v", status, err)
	}
	if _, err := machineClient.machine.LookupCompletion(acquire); !errors.Is(err, replicatedstate.ErrRetryRetired) {
		t.Fatalf("session retry window not reclaimed: %v", err)
	}
}
