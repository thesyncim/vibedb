package gatewayruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestGatewayDevTableRegistrationWaitsForServingFence(t *testing.T) {
	calls := 0
	err := registerGatewayDevTable(t.Context(), func(context.Context) error {
		calls++
		switch calls {
		case 1:
			return gateway.ErrReplicatedLeader
		case 2:
			return gateway.ErrReplicatedReadBehind
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("registration calls=%d err=%v", calls, err)
	}
}

func TestGatewayDevTableRegistrationDoesNotRetryRefusal(t *testing.T) {
	calls := 0
	err := registerGatewayDevTable(t.Context(), func(context.Context) error {
		calls++
		return gateway.ErrInvalidCatalog
	})
	if !errors.Is(err, gateway.ErrInvalidCatalog) || calls != 1 {
		t.Fatalf("registration calls=%d err=%v", calls, err)
	}
}

func TestGatewayDevTableRegistrationDoesNotHideJoinedRefusalOrUnknown(t *testing.T) {
	for _, terminal := range []error{gateway.ErrReplicatedRoute, gateway.ErrReplicatedUnauthorized,
		raftservice.ErrServingFence, gateway.ErrReplicatedReadBufferBound,
		gateway.ErrReplicatedCatalogPending, raftservice.ErrOutcomeUnknown,
		&gateway.ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalRequestLedgerReadMalformed},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		failure := errors.Join(gateway.ErrReplicatedLeader, terminal)
		calls, diagnostics := 0, 0
		err := registerGatewayDevTable(ctx, func(context.Context) error { calls++; return failure },
			func(string, ...any) { diagnostics++ })
		cancel()
		if err != failure || calls != 1 || diagnostics != 1 {
			t.Fatalf("registration hid or retried failure: err=%v calls=%d diagnostics=%d", err, calls, diagnostics)
		}
	}
}

func TestGatewayDevTableRegistrationHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	err := registerGatewayDevTable(ctx, func(context.Context) error {
		calls++
		cancel()
		return gateway.ErrReplicatedLeader
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, gateway.ErrReplicatedLeader) || calls != 1 {
		t.Fatalf("registration calls=%d err=%v", calls, err)
	}
	err = registerGatewayDevTable(ctx, func(context.Context) error {
		calls++
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("canceled registration calls=%d err=%v", calls, err)
	}
}
