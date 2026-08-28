package gateway

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// Session release reclaims retry slots, not class-independent authority
// bindings. This is a capacity contract, not a passing long-running serving
// qualification: internal callers must reuse bounded stable client identities.
func TestNativeDurableRouteSessionAuthorityCapacityIsNotReclaimedByRelease(t *testing.T) {
	route, client, reopen := newRouteSessionMachine(t) // Eight stable identities.
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	wave, _, _ := lifecycleRunnerFixture(t)
	driver := nativeDurableRequestRouteGateSessions{executor: executor}
	for i := byte(1); i <= 8; i++ {
		session, err := driver.session(route, wave, requestledger.Digest{i})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.Open(ctx, math.MaxInt64); err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if _, err := session.Retire(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Release(ctx); err != nil {
			t.Fatal(err)
		}
		state, err := client.machine.SessionCapacityState()
		if err != nil || state.SessionCount != 0 || state.SessionSlotCount != 0 || state.AuthorityBindingCount != uint64(i) {
			t.Fatalf("release %d capacity=%+v err=%v", i, state, err)
		}
	}
	reopen()
	session, err := driver.session(route, wave, requestledger.Digest{9})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(ctx, math.MaxInt64); !errors.Is(err, replicatedstate.ErrAdmissionBound) {
		t.Fatalf("fresh identity bypassed durable authority budget: %v", err)
	}
}
