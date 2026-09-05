//go:build linux

package gatewayruntime

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type hotFinalVoterStub struct {
	probes, reads int
	probeErr      error
	behind        bool
}

func (c *hotFinalVoterStub) ProbeReplicated(context.Context, gateway.ReplicatedRoute, gateway.ReplicatedEndpoint, serviceauthz.Capability) (*shardservice.ReplicatedResponse, error) {
	c.probes++
	if c.probes == 1 && c.probeErr != nil {
		return nil, c.probeErr
	}
	return &shardservice.ReplicatedResponse{HasState: true}, nil
}
func (c *hotFinalVoterStub) DoReplicated(_ context.Context, _ gateway.ReplicatedEndpoint, r *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	c.reads++
	if c.behind && c.reads == 1 {
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalReadBehind}, nil
	}
	return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedReadFound, ReadApplied: r.MinimumApplied, Value: []byte("exact-row")}, nil
}

func TestHotMutationFinalVoterRetriesOnlyClosedStreamAndCatchup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	client := &hotFinalVoterStub{probeErr: io.EOF, behind: true}
	response, err := hotMutationReadFinalVoter(ctx, client, gateway.ReplicatedRoute{}, gateway.ReplicatedEndpoint{}, []byte("key"), 17)
	if err != nil || response == nil || response.ReadApplied != 17 || string(response.Value) != "exact-row" || client.probes != 3 || client.reads != 2 {
		t.Fatalf("final read: %+v %v probes=%d reads=%d", response, err, client.probes, client.reads)
	}
	denied := errors.New("permanent authorization failure")
	client = &hotFinalVoterStub{probeErr: denied}
	if _, err := hotMutationReadFinalVoter(ctx, client, gateway.ReplicatedRoute{}, gateway.ReplicatedEndpoint{}, nil, 17); !errors.Is(err, denied) || client.probes != 1 || client.reads != 0 {
		t.Fatalf("permanent refusal retried: %v", err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	client = &hotFinalVoterStub{}
	if _, err := hotMutationReadFinalVoter(canceled, client, gateway.ReplicatedRoute{}, gateway.ReplicatedEndpoint{}, nil, 17); !errors.Is(err, context.Canceled) || client.probes != 0 {
		t.Fatalf("deadline ignored: %v", err)
	}
}
