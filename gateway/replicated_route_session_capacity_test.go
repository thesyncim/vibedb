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

// Coordination sessions reclaim their class-scoped binding atomically with
// release. Ordinary client identities keep their permanent authority fence.
func TestNativeDurableRouteSessionsReclaimCapacityAcrossRequests(t *testing.T) {
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
	for i := byte(1); i <= 32; i++ {
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
		if err != nil || state.SessionCount != 0 || state.SessionSlotCount != 0 || state.AuthorityBindingCount != 0 {
			t.Fatalf("release %d capacity=%+v err=%v", i, state, err)
		}
		reopen()
	}
	reopen()
	session, err := driver.session(route, wave, requestledger.Digest{9})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(ctx, math.MaxInt64); err != nil {
		t.Fatalf("fresh request exhausted reclaimed capacity: %v", err)
	}
}

func TestScopedRouteSessionsRecoverFullLegacyCapacityWithoutChangingClientFences(t *testing.T) {
	route, client, reopen := newRouteSessionMachine(t)
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
		session.scopedCoordination = false // Retained pre-upgrade client identities.
		if _, err = session.Open(ctx, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		if _, err = session.Retire(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = session.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	reopen()
	var active []*NativeSession
	for i := byte(1); i <= 8; i++ {
		session, err := driver.session(route, wave, requestledger.Digest{i})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = session.Open(ctx, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		if _, err = session.Put(ctx, []byte("forbidden"), []byte(`{"id":"forbidden"}`)); err == nil {
			t.Fatal("scoped session acquired data mutation authority")
		}
		active = append(active, session)
	}
	reopen()
	excess, err := driver.session(route, wave, requestledger.Digest{9})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = excess.Open(ctx, math.MaxInt64); !errors.Is(err, replicatedstate.ErrAdmissionBound) {
		t.Fatalf("active session bound lost: %v", err)
	}
	for _, session := range active {
		if _, err = session.Retire(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err = session.Release(ctx); err != nil {
			t.Fatal(err)
		}
	}
	reopen()
	capacity, err := client.machine.SessionCapacityState()
	if err != nil || capacity.AuthorityBindingCount != 8 || capacity.SessionCount != 0 || capacity.SessionSlotCount != 0 {
		t.Fatalf("legacy fences or bounded cleanup changed: %+v %v", capacity, err)
	}
}
