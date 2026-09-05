package raftmember

import (
	"errors"
	"testing"
	"time"
)

func TestRuntimeEnsureReadAuthorityRoundWaitsUntilRenewalDue(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 176)
	runtime := fixture.fixture.runtime
	token := startReadAuthorityRoundWithQuorum(t, fixture)

	initial := runtime.ReadAuthorityRoundMetrics()
	if initial.RoundsStarted != 1 {
		t.Fatalf("initial rounds started = %d, want one actual round", initial.RoundsStarted)
	}
	usable, err := fixture.policy.UsableDuration()
	if err != nil {
		t.Fatal(err)
	}
	lead := usable / 4
	if lead < fixture.policy.RoundingMargin {
		lead = fixture.policy.RoundingMargin
	}

	for attempt := 0; attempt < 8; attempt++ {
		if err := runtime.EnsureReadAuthorityRound(); err != nil {
			t.Fatalf("EnsureReadAuthorityRound before due (attempt %d): %v", attempt, err)
		}
	}
	if got := runtime.ReadAuthorityRoundMetrics().RoundsStarted; got != 1 {
		t.Fatalf("rounds started before due = %d, want one", got)
	}
	if runtime.authority.renewal != nil {
		t.Fatal("repeated valid reads opened a renewal before the due window")
	}

	fixture.clock.now = token.ExpiresAt - lead - time.Nanosecond
	if err := runtime.EnsureReadAuthorityRound(); err != nil {
		t.Fatalf("EnsureReadAuthorityRound just before due: %v", err)
	}
	if got := runtime.ReadAuthorityRoundMetrics().RoundsStarted; got != 1 {
		t.Fatalf("rounds started just before due = %d, want one", got)
	}

	fixture.clock.now = token.ExpiresAt - lead
	if err := runtime.EnsureReadAuthorityRound(); err != nil {
		t.Fatalf("EnsureReadAuthorityRound at due: %v", err)
	}
	if got := runtime.ReadAuthorityRoundMetrics().RoundsStarted; got != 2 {
		t.Fatalf("rounds started at due = %d, want one renewal", got)
	}
	if runtime.authority.renewal == nil {
		t.Fatal("due renewal was not retained as the single pending candidate")
	}
	if err := runtime.EnsureReadAuthorityRound(); !errors.Is(err, ErrAuthorityRoundActive) {
		t.Fatalf("EnsureReadAuthorityRound with pending renewal = %v, want ErrAuthorityRoundActive", err)
	}
	if got := runtime.ReadAuthorityRoundMetrics().RoundsStarted; got != 2 {
		t.Fatalf("rounds started with pending renewal = %d, want two", got)
	}
}
