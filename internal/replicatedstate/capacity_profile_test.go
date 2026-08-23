package replicatedstate

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

func TestSessionCapacityStateTracksDurableCutAndPoison(t *testing.T) {
	fixture := newMachineFixture(t)
	want := SessionCapacityState{}
	if got, err := fixture.machine.SessionCapacityState(); err != nil || got != want {
		t.Fatalf("uninitialized capacity state = %+v, %v; want %+v", got, err, want)
	}

	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	want = SessionCapacityState{Initialized: true, Applied: 1}
	if got, err := fixture.machine.SessionCapacityState(); err != nil || got != want {
		t.Fatalf("bootstrap capacity state = %+v, %v; want %+v", got, err, want)
	}

	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("key"), Value: []byte(`{"value":1}`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	want = SessionCapacityState{
		Initialized: true, Applied: 3, SessionCount: 1, SessionSlotCount: 2,
		SessionEpochHighWater: 2,
	}
	if got, err := fixture.machine.SessionCapacityState(); err != nil || got != want {
		t.Fatalf("applied capacity state = %+v, %v; want %+v", got, err, want)
	}

	if _, err := fixture.machine.ApplyNormal(normalMeta(3), nil); err == nil {
		t.Fatal("conflicting replay did not poison machine")
	}
	if got, err := fixture.machine.SessionCapacityState(); got != (SessionCapacityState{}) ||
		!errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("poisoned capacity state = %+v, %v; want zero, ErrApplyPoisoned", got, err)
	}
}
