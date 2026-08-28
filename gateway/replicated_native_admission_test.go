package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/shardservice"
)

type admissionReadClient struct {
	states map[string]shardservice.ReplicatedMemberState
	ready  time.Time
	reads  int
}

func (client *admissionReadClient) DoReplicated(_ context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	state := client.states[endpoint.Address]
	response := &shardservice.ReplicatedResponse{HasState: true, State: state}
	if request.Operation == shardservice.ReplicatedProbe {
		response.Kind = shardservice.ReplicatedHandshake
		return response, nil
	}
	client.reads++
	if time.Now().Before(client.ready) {
		response.Kind, response.Refusal = shardservice.ReplicatedRefusal, shardservice.ReplicatedRefusalAdmissionBound
		return response, nil
	}
	response.ReadApplied = state.Applied
	var err error
	if request.Operation == shardservice.ReplicatedExecutionPinRead {
		response.Kind = shardservice.ReplicatedExecutionPinReadResult
		response.Value, err = shardservice.AppendReplicatedExecutionPinReadValue(nil, shardservice.ReplicatedExecutionPinReadValue{})
	} else {
		response.Kind = shardservice.ReplicatedRequestLedgerReadResult
		response.Value, err = shardservice.AppendReplicatedRequestLedgerReadValue(nil, shardservice.ReplicatedRequestLedgerReadValue{})
	}
	return response, err
}

func TestDurableReadsBackOffTransientAdmissionPressure(t *testing.T) {
	for _, pin := range []bool{false, true} {
		route, _, states := testReplicatedRouteCommand(t)
		client := &admissionReadClient{states: states, ready: time.Now().Add(80 * time.Millisecond)}
		executor, err := NewReplicatedExecutor(client, 5, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if pin {
			read := ReplicatedExecutionPinRead{MinimumApplied: 1}
			read.Pin[0] = 1
			_, err = executor.ReadExecutionPin(t.Context(), route, read)
		} else {
			read := ReplicatedRequestLedgerRead{Key: lifecycleKey(), Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1, MaxBytes: uint32(replicatedstate.RequestLedgerReadMaxBytes(replicatedstate.RequestLedgerReadHead))}
			read.ExpectedRangeIdentity[0] = 1
			_, err = executor.ReadRequestLedger(t.Context(), route, read)
		}
		if err != nil || client.reads > 5 {
			t.Fatalf("pin=%t reads=%d err=%v", pin, client.reads, err)
		}
	}
}
