package durable

import (
	"fmt"
	"testing"
)

func requirePrimaryJournalAdmissionPanic(
	t *testing.T, name string, fn func(),
) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("%s did not panic", name)
		}
	}()
	fn()
}

func requirePrimaryJournalAdmissionPhase(
	t *testing.T,
	admission *primaryJournalAdmission,
	phase primaryJournalAdmissionPhase,
) primaryJournalAdmissionObservation {
	t.Helper()
	observation := admission.observe()
	if observation.phase != phase {
		t.Fatalf(
			"admission phase = %d at epoch %d, want %d",
			observation.phase, observation.epoch, phase,
		)
	}
	return observation
}

func requirePrimaryJournalAdmissionBatch(
	t *testing.T,
	admission *primaryJournalAdmission,
	ticket primaryJournalAdmissionTicket,
	want ...*primaryJournalAdmissionRequest,
) {
	t.Helper()
	batch, ok := admission.cohortBatch(ticket)
	if !ok {
		t.Fatalf("cohort batch unavailable for ticket %+v", ticket)
	}
	if len(batch) != len(want) {
		t.Fatalf("cohort batch length = %d, want %d", len(batch), len(want))
	}
	for index := range want {
		if batch[index] != want[index] {
			t.Fatalf(
				"cohort batch[%d] = request %d, want request %d",
				index, batch[index].ordinal, want[index].ordinal,
			)
		}
	}
}

func requirePrimaryJournalAdmissionCompletion(
	t *testing.T,
	completion *primaryJournalAdmissionCompletion,
	want ...*primaryJournalAdmissionRequest,
) []*primaryJournalAdmissionRequest {
	t.Helper()
	got := completion.detached()
	if len(got) != len(want) {
		t.Fatalf("completion length = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf(
				"completion[%d] = request %d, want request %d",
				index, got[index].ordinal, want[index].ordinal,
			)
		}
	}
	return got
}

// A1: a lone caller is assigned the established baseline role and returns the
// engine directly to idle. No follower request, detached batch, or background
// handoff is created.
func TestPrimaryJournalAdmissionSingletonUsesBaselinePilot(t *testing.T) {
	var admission primaryJournalAdmission
	ticket, ok := admission.tryStartInitialPilot()
	if !ok {
		t.Fatal("initial caller did not claim baseline pilot")
	}
	if ticket.role != primaryJournalAdmissionBaselineRole || ticket.epoch == 0 {
		t.Fatalf("initial ticket = %+v, want non-zero baseline ticket", ticket)
	}
	requirePrimaryJournalAdmissionPhase(
		t, &admission, primaryJournalAdmissionBaselinePilot,
	)

	decision, ok := admission.seal(ticket)
	if !ok {
		t.Fatal("baseline pilot could not seal its epoch")
	}
	if decision != (primaryJournalAdmissionDecision{}) {
		t.Fatalf("singleton seal decision = %+v, want no baton", decision)
	}
	observation := requirePrimaryJournalAdmissionPhase(
		t, &admission, primaryJournalAdmissionIdle,
	)
	if observation.epoch != ticket.epoch {
		t.Fatalf("idle epoch = %d, want completed epoch %d", observation.epoch, ticket.epoch)
	}
	if admission.count != 0 || admission.batchCount != 0 {
		t.Fatalf(
			"singleton retained queue=%d batch=%d",
			admission.count, admission.batchCount,
		)
	}
	var allocationAdmission primaryJournalAdmission
	if allocs := testing.AllocsPerRun(1000, func() {
		localTicket, started := allocationAdmission.tryStartInitialPilot()
		if !started {
			panic("singleton pilot claim failed")
		}
		if _, sealed := allocationAdmission.seal(localTicket); !sealed {
			panic("singleton pilot seal failed")
		}
	}); allocs != 0 {
		t.Fatalf("singleton admission allocations = %v, want 0", allocs)
	}
}

// A2: a follower may spend bounded private work after observing an open pilot.
// If that pilot and later epochs finish first, binding under the mutex promotes
// it into the current idle epoch rather than stranding it in the observed one.
func TestPrimaryJournalAdmissionLateFollowerCannotABAOrStrand(t *testing.T) {
	var admission primaryJournalAdmission
	first, ok := admission.tryStartInitialPilot()
	if !ok {
		t.Fatal("first pilot claim failed")
	}

	observed := make(chan primaryJournalAdmissionObservation, 1)
	release := make(chan struct{})
	admitted := make(chan primaryJournalAdmissionDecision, 1)
	late := &primaryJournalAdmissionRequest{ordinal: 99}
	go func() {
		observation := admission.observe()
		observed <- observation
		<-release
		admitted <- admission.admitPrepared(late, observation)
	}()

	staleObservation := <-observed
	if staleObservation.epoch != first.epoch ||
		staleObservation.phase != primaryJournalAdmissionBaselinePilot {
		t.Fatalf("late follower observed %+v, want first pilot", staleObservation)
	}
	if _, ok := admission.seal(first); !ok {
		t.Fatal("first pilot seal failed")
	}

	// Complete two more idle -> pilot -> idle cycles. Returning to the same idle
	// phase is the ABA shape; the monotonically advancing epoch distinguishes it.
	for cycle := 0; cycle < 2; cycle++ {
		ticket, started := admission.tryStartInitialPilot()
		if !started {
			t.Fatalf("cycle %d pilot claim failed", cycle)
		}
		if _, sealed := admission.seal(ticket); !sealed {
			t.Fatalf("cycle %d pilot seal failed", cycle)
		}
	}
	close(release)
	decision := <-admitted
	if decision.closed || decision.queued ||
		decision.role != primaryJournalAdmissionBaselineRole ||
		decision.leader != late || !decision.stale {
		t.Fatalf("late follower decision = %+v, want stale baseline baton", decision)
	}
	if decision.ticket.epoch <= staleObservation.epoch {
		t.Fatalf(
			"late follower rebound to epoch %d after observing %d",
			decision.ticket.epoch, staleObservation.epoch,
		)
	}
	lateFinished, ok := admission.seal(decision.ticket)
	if !ok {
		t.Fatal("late follower baseline pilot could not seal")
	}
	requirePrimaryJournalAdmissionCompletion(
		t, &lateFinished.completed, late,
	)
	requirePrimaryJournalAdmissionPhase(t, &admission, primaryJournalAdmissionIdle)

	// A delayed former leader cannot seal the later epoch.
	if _, ok := admission.seal(first); ok {
		t.Fatal("stale first ticket sealed a later epoch")
	}

	// The same stale observation must bind behind a newer active pilot instead
	// of being promoted into, or queued against, the old epoch.
	current, started := admission.tryStartInitialPilot()
	if !started {
		t.Fatal("current pilot claim failed")
	}
	behind := &primaryJournalAdmissionRequest{ordinal: 100}
	queued := admission.admitPrepared(behind, staleObservation)
	if !queued.queued || !queued.stale || queued.closed {
		t.Fatalf("stale follower behind current pilot = %+v", queued)
	}
	next, sealed := admission.seal(current)
	if !sealed || next.role != primaryJournalAdmissionBaselineRole ||
		next.leader != behind || next.ticket.epoch <= current.epoch {
		t.Fatalf("current pilot baton = %+v sealed=%v", next, sealed)
	}
	behindFinished, sealed := admission.seal(next.ticket)
	if !sealed {
		t.Fatal("rebound follower seal failed")
	}
	requirePrimaryJournalAdmissionCompletion(
		t, &behindFinished.completed, behind,
	)
	requirePrimaryJournalAdmissionPhase(t, &admission, primaryJournalAdmissionIdle)
}

// A3: sealing detaches one immutable cohort. Arrivals while it is being applied
// remain in the next queue and receive a caller baton only after the first
// cohort seals; the first cohort never grows to include them.
func TestPrimaryJournalAdmissionFiniteSnapshotBaton(t *testing.T) {
	var admission primaryJournalAdmission
	pilot, ok := admission.tryStartInitialPilot()
	if !ok {
		t.Fatal("pilot claim failed")
	}
	first := []*primaryJournalAdmissionRequest{
		{ordinal: 1}, {ordinal: 2}, {ordinal: 3},
	}
	observation := admission.observe()
	for _, request := range first {
		decision := admission.admitPrepared(request, observation)
		if !decision.queued || decision.closed || decision.role != primaryJournalAdmissionNoRole {
			t.Fatalf("first cohort admission %d = %+v", request.ordinal, decision)
		}
	}

	cohort, ok := admission.seal(pilot)
	if !ok || cohort.role != primaryJournalAdmissionCohortRole ||
		cohort.leader != first[0] {
		t.Fatalf("first cohort baton = %+v ok=%v", cohort, ok)
	}
	requirePrimaryJournalAdmissionBatch(t, &admission, cohort.ticket, first...)

	second := []*primaryJournalAdmissionRequest{
		{ordinal: 4}, {ordinal: 5},
	}
	cohortObservation := admission.observe()
	for _, request := range second {
		decision := admission.admitPrepared(request, cohortObservation)
		if !decision.queued || decision.closed {
			t.Fatalf("next cohort admission %d = %+v", request.ordinal, decision)
		}
	}
	// Later arrivals cannot change the detached snapshot.
	requirePrimaryJournalAdmissionBatch(t, &admission, cohort.ticket, first...)

	next, ok := admission.seal(cohort.ticket)
	if !ok || next.role != primaryJournalAdmissionCohortRole ||
		next.leader != second[0] {
		t.Fatalf("second cohort baton = %+v ok=%v", next, ok)
	}
	firstCompletion := requirePrimaryJournalAdmissionCompletion(
		t, &next.completed, first...,
	)
	if &next.completed.requests[0] == &admission.batch[0] {
		t.Fatal("completion pointer array aliases reusable engine batch")
	}
	requirePrimaryJournalAdmissionBatch(t, &admission, next.ticket, second...)
	finished, ok := admission.seal(next.ticket)
	if !ok {
		t.Fatal("second cohort seal failed")
	}
	requirePrimaryJournalAdmissionCompletion(t, &finished.completed, second...)
	// The first returned value owns its pointer array. Clearing/reusing the
	// engine batch for the next phase cannot mutate the earlier completion.
	for index := range first {
		if firstCompletion[index] != first[index] {
			t.Fatalf("first completion changed at %d after batch reuse", index)
		}
	}
	requirePrimaryJournalAdmissionPhase(t, &admission, primaryJournalAdmissionIdle)
}

// A4: an untouched pressure suffix is older than requests admitted during the
// cohort. It is prepended, and its first member is forced into the baseline
// role so the established fold/checkpoint path resolves pressure before later
// requests are reconsidered.
func TestPrimaryJournalAdmissionPressureSuffixPrependsArrivals(t *testing.T) {
	var admission primaryJournalAdmission
	pilot, ok := admission.tryStartInitialPilot()
	if !ok {
		t.Fatal("pilot claim failed")
	}
	batch := []*primaryJournalAdmissionRequest{
		{ordinal: 1}, {ordinal: 2}, {ordinal: 3},
	}
	observation := admission.observe()
	for _, request := range batch {
		if decision := admission.admitPrepared(request, observation); !decision.queued {
			t.Fatalf("batch admission %d = %+v", request.ordinal, decision)
		}
	}
	cohort, ok := admission.seal(pilot)
	if !ok || cohort.role != primaryJournalAdmissionCohortRole {
		t.Fatalf("cohort baton = %+v ok=%v", cohort, ok)
	}

	arrivals := []*primaryJournalAdmissionRequest{
		{ordinal: 4}, {ordinal: 5},
	}
	cohortObservation := admission.observe()
	for _, request := range arrivals {
		if decision := admission.admitPrepared(request, cohortObservation); !decision.queued {
			t.Fatalf("arrival admission %d = %+v", request.ordinal, decision)
		}
	}

	// Request 1 is the successful prefix. Requests 2 and 3 must move before
	// arrivals 4 and 5, with request 2 taking the pressure-resolving baseline.
	pressure, ok := admission.sealPressure(cohort.ticket, 1)
	if !ok || pressure.role != primaryJournalAdmissionBaselineRole ||
		pressure.leader != batch[1] {
		t.Fatalf("pressure baton = %+v ok=%v", pressure, ok)
	}
	requirePrimaryJournalAdmissionCompletion(t, &pressure.completed, batch[0])
	if &pressure.completed.requests[0] == &admission.batch[0] {
		t.Fatal("pressure completion pointer array aliases engine batch")
	}
	if got := pressure.completed.detached(); len(got) != 1 || got[0].ordinal != 1 {
		t.Fatalf("transition-before-signal completion = %v", got)
	}
	if owned := admission.count + admission.batchCount + 1; owned != 4 {
		t.Fatalf("engine ownership after pressure = %d, want suffix+arrivals=4", owned)
	}
	if admission.count != 3 {
		t.Fatalf("pressure queue count = %d, want 3", admission.count)
	}
	wantQueued := []*primaryJournalAdmissionRequest{batch[2], arrivals[0], arrivals[1]}
	for index, want := range wantQueued {
		if got := admission.queue[index]; got != want {
			t.Fatalf(
				"pressure queue[%d] = request %d, want request %d",
				index, got.ordinal, want.ordinal,
			)
		}
	}

	// A still-later arrival remains after the complete retained suffix.
	later := &primaryJournalAdmissionRequest{ordinal: 6}
	if decision := admission.admitPrepared(later, admission.observe()); !decision.queued {
		t.Fatalf("later arrival decision = %+v", decision)
	}
	next, ok := admission.seal(pressure.ticket)
	if !ok || next.role != primaryJournalAdmissionCohortRole ||
		next.leader != batch[2] {
		t.Fatalf("post-pressure cohort baton = %+v ok=%v", next, ok)
	}
	requirePrimaryJournalAdmissionBatch(
		t, &admission, next.ticket,
		batch[2], arrivals[0], arrivals[1], later,
	)
	if _, ok := admission.seal(next.ticket); !ok {
		t.Fatal("post-pressure cohort seal failed")
	}
	requirePrimaryJournalAdmissionPhase(t, &admission, primaryJournalAdmissionIdle)
}

func TestPrimaryJournalAdmissionPressureBoundaryBatons(t *testing.T) {
	tests := []struct {
		name       string
		suffixAt   int
		wantLeader int
		selfBaton  bool
		completed  int
	}{
		{name: "entire-batch", suffixAt: 0, wantLeader: 0, selfBaton: true},
		{name: "middle", suffixAt: 1, wantLeader: 1, completed: 1},
		{name: "last-member", suffixAt: 2, wantLeader: 2, completed: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var admission primaryJournalAdmission
			pilot, ok := admission.tryStartInitialPilot()
			if !ok {
				t.Fatal("pilot claim failed")
			}
			requests := [3]primaryJournalAdmissionRequest{
				{ordinal: 1}, {ordinal: 2}, {ordinal: 3},
			}
			observation := admission.observe()
			for index := range requests {
				if decision := admission.admitPrepared(
					&requests[index], observation,
				); !decision.queued {
					t.Fatalf("admission %d = %+v", index, decision)
				}
			}
			cohort, ok := admission.seal(pilot)
			if !ok || cohort.role != primaryJournalAdmissionCohortRole {
				t.Fatalf("cohort baton = %+v ok=%v", cohort, ok)
			}
			pressure, ok := admission.sealPressure(
				cohort.ticket, test.suffixAt,
			)
			if !ok || pressure.role != primaryJournalAdmissionBaselineRole ||
				pressure.leader != &requests[test.wantLeader] ||
				pressure.selfBaton != test.selfBaton {
				t.Fatalf("pressure baton = %+v ok=%v", pressure, ok)
			}
			if test.selfBaton && pressure.leader != cohort.leader {
				t.Fatal("self baton did not return the current cohort leader")
			}
			wantCompleted := make(
				[]*primaryJournalAdmissionRequest, test.completed,
			)
			for index := range wantCompleted {
				wantCompleted[index] = &requests[index]
			}
			completed := requirePrimaryJournalAdmissionCompletion(
				t, &pressure.completed, wantCompleted...,
			)
			if owned := admission.count + admission.batchCount + 1; owned+len(completed) != len(requests) {
				t.Fatalf(
					"pressure ownership engine=%d completed=%d want=%d",
					owned, len(completed), len(requests),
				)
			}

			next, sealed := admission.seal(pressure.ticket)
			if !sealed {
				t.Fatal("pressure baseline seal failed")
			}
			if next.role == primaryJournalAdmissionCohortRole {
				if _, sealed := admission.seal(next.ticket); !sealed {
					t.Fatal("retained suffix cohort seal failed")
				}
			} else if next.role == primaryJournalAdmissionBaselineRole {
				if _, sealed := admission.seal(next.ticket); !sealed {
					t.Fatal("retained suffix baseline seal failed")
				}
			}
			requirePrimaryJournalAdmissionPhase(
				t, &admission, primaryJournalAdmissionIdle,
			)
		})
	}
}

func TestPrimaryJournalAdmissionPopulationBounds(t *testing.T) {
	t.Run("initial-plus-32", func(t *testing.T) {
		var admission primaryJournalAdmission
		pilot, ok := admission.tryStartInitialPilot()
		if !ok {
			t.Fatal("pilot claim failed")
		}
		var requests [primaryJournalAdmissionLimit + 1]primaryJournalAdmissionRequest
		observation := admission.observe()
		for index := 0; index < primaryJournalAdmissionLimit; index++ {
			if decision := admission.admitPrepared(
				&requests[index], observation,
			); !decision.queued {
				t.Fatalf("admission %d = %+v", index, decision)
			}
		}
		requirePrimaryJournalAdmissionPanic(t, "33rd follower", func() {
			admission.admitPrepared(
				&requests[primaryJournalAdmissionLimit], observation,
			)
		})
		cohort, sealed := admission.seal(pilot)
		if !sealed || cohort.role != primaryJournalAdmissionCohortRole {
			t.Fatalf("full cohort = %+v sealed=%v", cohort, sealed)
		}
		requirePrimaryJournalAdmissionBatch(
			t, &admission, cohort.ticket,
			func() []*primaryJournalAdmissionRequest {
				want := make([]*primaryJournalAdmissionRequest, len(requests)-1)
				for index := range want {
					want[index] = &requests[index]
				}
				return want
			}()...,
		)
	})

	t.Run("prepared-plus-31", func(t *testing.T) {
		var admission primaryJournalAdmission
		var requests [primaryJournalAdmissionLimit + 1]primaryJournalAdmissionRequest
		pilot := admission.admitPrepared(&requests[0], admission.observe())
		if pilot.role != primaryJournalAdmissionBaselineRole {
			t.Fatalf("prepared pilot = %+v", pilot)
		}
		observation := admission.observe()
		for index := 1; index < primaryJournalAdmissionLimit; index++ {
			if decision := admission.admitPrepared(
				&requests[index], observation,
			); !decision.queued {
				t.Fatalf("admission %d = %+v", index, decision)
			}
		}
		requirePrimaryJournalAdmissionPanic(t, "prepared 33rd context", func() {
			admission.admitPrepared(
				&requests[primaryJournalAdmissionLimit], observation,
			)
		})
		next, sealed := admission.seal(pilot.ticket)
		if !sealed || next.role != primaryJournalAdmissionCohortRole {
			t.Fatalf("prepared population baton = %+v sealed=%v", next, sealed)
		}
	})

	for _, batchSize := range []int{2, 16, primaryJournalAdmissionLimit} {
		t.Run(fmt.Sprintf("batch-%d-plus-queue-%d", batchSize,
			primaryJournalAdmissionLimit-batchSize), func(t *testing.T) {
			var admission primaryJournalAdmission
			var requests [primaryJournalAdmissionLimit + 1]primaryJournalAdmissionRequest
			pilot, ok := admission.tryStartInitialPilot()
			if !ok {
				t.Fatal("pilot claim failed")
			}
			observation := admission.observe()
			for index := 0; index < batchSize; index++ {
				admission.admitPrepared(&requests[index], observation)
			}
			cohort, sealed := admission.seal(pilot)
			if !sealed || cohort.role != primaryJournalAdmissionCohortRole {
				t.Fatalf("cohort = %+v sealed=%v", cohort, sealed)
			}
			cohortObservation := admission.observe()
			for index := batchSize; index < primaryJournalAdmissionLimit; index++ {
				if decision := admission.admitPrepared(
					&requests[index], cohortObservation,
				); !decision.queued {
					t.Fatalf("arrival %d = %+v", index, decision)
				}
			}
			requirePrimaryJournalAdmissionPanic(t, "33rd owned context", func() {
				admission.admitPrepared(
					&requests[primaryJournalAdmissionLimit], cohortObservation,
				)
			})
		})
	}
}

func TestPrimaryJournalAdmissionEpochExhaustionDoesNotWrap(t *testing.T) {
	maxEpoch := ^uint64(0) >> primaryJournalAdmissionPhaseBits
	var admission primaryJournalAdmission
	admission.epoch = maxEpoch - 1
	admission.phase = primaryJournalAdmissionIdle
	admission.word.Store(packPrimaryJournalAdmissionWord(
		primaryJournalAdmissionIdle, admission.epoch,
	))
	stale := primaryJournalAdmissionTicket{
		role: primaryJournalAdmissionBaselineRole, epoch: admission.epoch,
	}
	pilot, ok := admission.tryStartInitialPilot()
	if !ok || pilot.epoch != maxEpoch {
		t.Fatalf("maximum pilot ticket = %+v ok=%v", pilot, ok)
	}
	if _, ok := admission.seal(stale); ok {
		t.Fatal("pre-maximum stale ticket sealed maximum epoch")
	}
	if _, ok := admission.seal(pilot); !ok {
		t.Fatal("maximum epoch seal failed")
	}
	requirePrimaryJournalAdmissionPanic(t, "epoch overflow", func() {
		admission.tryStartInitialPilot()
	})
	observation := admission.observe()
	if observation.phase != primaryJournalAdmissionIdle ||
		observation.epoch != maxEpoch || admission.activeRole != primaryJournalAdmissionNoRole {
		t.Fatalf("state after epoch panic = %+v active=%d", observation, admission.activeRole)
	}
}

func TestPrimaryJournalAdmissionCloseDrainsPassiveOwnership(t *testing.T) {
	t.Run("baseline", func(t *testing.T) {
		var admission primaryJournalAdmission
		pilot, _ := admission.tryStartInitialPilot()
		requests := [2]primaryJournalAdmissionRequest{{ordinal: 1}, {ordinal: 2}}
		observation := admission.observe()
		for index := range requests {
			admission.admitPrepared(&requests[index], observation)
		}
		drain := admission.closeAndDrain()
		if drain.active.role != primaryJournalAdmissionBaselineRole ||
			drain.active.ticket != pilot || drain.active.leader != nil ||
			drain.active.prepared || drain.active.batchCount != 0 {
			t.Fatalf("active baseline = %+v", drain.active)
		}
		got := drain.detached()
		if len(got) != 2 || got[0] != &requests[0] || got[1] != &requests[1] {
			t.Fatalf("baseline close drain = %v", got)
		}
		if admission.observe().phase != primaryJournalAdmissionClosed {
			t.Fatal("close did not publish terminal phase")
		}
		closedRequest := &primaryJournalAdmissionRequest{ordinal: 3}
		if decision := admission.admitPrepared(
			closedRequest, observation,
		); !decision.closed || decision.queued || decision.role != primaryJournalAdmissionNoRole {
			t.Fatalf("post-close admission = %+v", decision)
		}
		decision, sealed := admission.seal(pilot)
		if !sealed || !decision.closed || admission.activeRole != primaryJournalAdmissionNoRole {
			t.Fatalf("closed active seal = %+v sealed=%v", decision, sealed)
		}
		if again := admission.closeAndDrain(); len(again.detached()) != 0 ||
			again.active.role != primaryJournalAdmissionNoRole {
			t.Fatalf("second close drain = %+v", again)
		}
	})

	t.Run("prepared-baseline", func(t *testing.T) {
		var admission primaryJournalAdmission
		requests := [2]primaryJournalAdmissionRequest{{ordinal: 1}, {ordinal: 2}}
		pilot := admission.admitPrepared(&requests[0], admission.observe())
		if decision := admission.admitPrepared(
			&requests[1], admission.observe(),
		); !decision.queued {
			t.Fatalf("prepared follower = %+v", decision)
		}
		drain := admission.closeAndDrain()
		if drain.active.role != primaryJournalAdmissionBaselineRole ||
			drain.active.ticket != pilot.ticket ||
			drain.active.leader != &requests[0] || !drain.active.prepared ||
			drain.active.batchCount != 0 {
			t.Fatalf("active prepared baseline = %+v", drain.active)
		}
		got := drain.detached()
		if len(got) != 1 || got[0] != &requests[1] {
			t.Fatalf("prepared close drain = %v", got)
		}
		decision, sealed := admission.seal(pilot.ticket)
		if !sealed || !decision.closed || admission.activeLeader != nil ||
			admission.preparedPilot {
			t.Fatalf("prepared closed seal = %+v sealed=%v", decision, sealed)
		}
		requirePrimaryJournalAdmissionCompletion(
			t, &decision.completed, &requests[0],
		)
	})

	t.Run("cohort", func(t *testing.T) {
		var admission primaryJournalAdmission
		pilot, _ := admission.tryStartInitialPilot()
		batch := [3]primaryJournalAdmissionRequest{{ordinal: 1}, {ordinal: 2}, {ordinal: 3}}
		observation := admission.observe()
		for index := range batch {
			admission.admitPrepared(&batch[index], observation)
		}
		cohort, _ := admission.seal(pilot)
		arrivals := [2]primaryJournalAdmissionRequest{{ordinal: 4}, {ordinal: 5}}
		cohortObservation := admission.observe()
		for index := range arrivals {
			admission.admitPrepared(&arrivals[index], cohortObservation)
		}
		drain := admission.closeAndDrain()
		if drain.active.role != primaryJournalAdmissionCohortRole ||
			drain.active.ticket != cohort.ticket ||
			drain.active.leader != &batch[0] || drain.active.batchCount != len(batch) {
			t.Fatalf("active cohort = %+v", drain.active)
		}
		got := drain.detached()
		if len(got) != 2 || got[0] != &arrivals[0] || got[1] != &arrivals[1] {
			t.Fatalf("cohort close drain = %v", got)
		}
		requirePrimaryJournalAdmissionBatch(
			t, &admission, cohort.ticket, &batch[0], &batch[1], &batch[2],
		)
		decision, sealed := admission.seal(cohort.ticket)
		if !sealed || !decision.closed || admission.batchCount != 0 ||
			admission.activeRole != primaryJournalAdmissionNoRole {
			t.Fatalf("closed cohort seal = %+v sealed=%v", decision, sealed)
		}
		requirePrimaryJournalAdmissionCompletion(
			t, &decision.completed, &batch[0], &batch[1], &batch[2],
		)
	})
}

func TestPrimaryJournalAdmissionTerminalDrainsWithoutBaton(t *testing.T) {
	var admission primaryJournalAdmission
	pilot, _ := admission.tryStartInitialPilot()
	batch := [3]primaryJournalAdmissionRequest{{ordinal: 1}, {ordinal: 2}, {ordinal: 3}}
	observation := admission.observe()
	for index := range batch {
		admission.admitPrepared(&batch[index], observation)
	}
	cohort, _ := admission.seal(pilot)
	arrivals := [2]primaryJournalAdmissionRequest{{ordinal: 4}, {ordinal: 5}}
	cohortObservation := admission.observe()
	for index := range arrivals {
		admission.admitPrepared(&arrivals[index], cohortObservation)
	}
	drain, ok := admission.terminateAndDrain(cohort.ticket, 1)
	if !ok || drain.active.role != primaryJournalAdmissionNoRole ||
		drain.activeCount != len(batch) || drain.unresolvedAt != 1 {
		t.Fatalf("terminal drain metadata = %+v ok=%v", drain, ok)
	}
	want := []*primaryJournalAdmissionRequest{
		&batch[0], &batch[1], &batch[2], &arrivals[0], &arrivals[1],
	}
	got := drain.detached()
	if len(got) != len(want) {
		t.Fatalf("terminal drain length = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("terminal drain[%d] = %p, want %p", index, got[index], want[index])
		}
	}
	if admission.observe().phase != primaryJournalAdmissionClosed ||
		admission.count != 0 || admission.batchCount != 0 ||
		admission.activeRole != primaryJournalAdmissionNoRole {
		t.Fatalf(
			"terminal engine state phase=%d queue=%d batch=%d active=%d",
			admission.observe().phase, admission.count,
			admission.batchCount, admission.activeRole,
		)
	}
	if decision, sealed := admission.seal(cohort.ticket); sealed ||
		decision != (primaryJournalAdmissionDecision{}) {
		t.Fatalf("terminal path selected baton %+v sealed=%v", decision, sealed)
	}
}

func TestPrimaryJournalAdmissionTerminalDetachesPreparedPilot(t *testing.T) {
	var admission primaryJournalAdmission
	requests := [3]primaryJournalAdmissionRequest{
		{ordinal: 1}, {ordinal: 2}, {ordinal: 3},
	}
	pilot := admission.admitPrepared(&requests[0], admission.observe())
	if pilot.role != primaryJournalAdmissionBaselineRole || pilot.leader != &requests[0] {
		t.Fatalf("prepared pilot = %+v", pilot)
	}
	observation := admission.observe()
	for index := 1; index < len(requests); index++ {
		if decision := admission.admitPrepared(
			&requests[index], observation,
		); !decision.queued {
			t.Fatalf("queued request %d = %+v", index, decision)
		}
	}
	drain, ok := admission.terminateAndDrain(pilot.ticket, 0)
	if !ok || drain.activeCount != 1 || drain.unresolvedAt != 0 {
		t.Fatalf("prepared terminal drain = %+v ok=%v", drain, ok)
	}
	got := drain.detached()
	if len(got) != len(requests) {
		t.Fatalf("prepared terminal length = %d, want %d", len(got), len(requests))
	}
	for index := range requests {
		if got[index] != &requests[index] {
			t.Fatalf("prepared terminal[%d] = %p, want %p", index, got[index], &requests[index])
		}
	}
	if admission.activeLeader != nil || admission.activeRole != primaryJournalAdmissionNoRole ||
		admission.count != 0 || admission.observe().phase != primaryJournalAdmissionClosed {
		t.Fatalf(
			"prepared terminal retained leader=%p role=%d count=%d phase=%d",
			admission.activeLeader, admission.activeRole,
			admission.count, admission.observe().phase,
		)
	}
}

func TestPrimaryJournalAdmissionCohortAndPressureAllocateZero(t *testing.T) {
	requests := [3]primaryJournalAdmissionRequest{{ordinal: 1}, {ordinal: 2}, {ordinal: 3}}
	var cohortAdmission primaryJournalAdmission
	if allocs := testing.AllocsPerRun(1000, func() {
		pilot, _ := cohortAdmission.tryStartInitialPilot()
		observation := cohortAdmission.observe()
		cohortAdmission.admitPrepared(&requests[0], observation)
		cohortAdmission.admitPrepared(&requests[1], observation)
		cohort, _ := cohortAdmission.seal(pilot)
		completed, _ := cohortAdmission.seal(cohort.ticket)
		if completed.completed.count != 2 {
			panic("cohort completion lost")
		}
	}); allocs != 0 {
		t.Fatalf("cohort admission allocations = %v, want 0", allocs)
	}

	var pressureAdmission primaryJournalAdmission
	if allocs := testing.AllocsPerRun(1000, func() {
		pilot, _ := pressureAdmission.tryStartInitialPilot()
		observation := pressureAdmission.observe()
		for index := range requests {
			pressureAdmission.admitPrepared(&requests[index], observation)
		}
		cohort, _ := pressureAdmission.seal(pilot)
		pressure, _ := pressureAdmission.sealPressure(cohort.ticket, len(requests)-1)
		completed, _ := pressureAdmission.seal(pressure.ticket)
		if pressure.completed.count != len(requests)-1 ||
			completed.completed.count != 1 {
			panic("pressure completion lost")
		}
	}); allocs != 0 {
		t.Fatalf("pressure admission allocations = %v, want 0", allocs)
	}
}
