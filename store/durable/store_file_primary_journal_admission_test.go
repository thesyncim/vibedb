package durable

import "testing"

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
	if _, ok := admission.seal(decision.ticket); !ok {
		t.Fatal("late follower baseline pilot could not seal")
	}
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
	if _, sealed := admission.seal(next.ticket); !sealed {
		t.Fatal("rebound follower seal failed")
	}
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
	requirePrimaryJournalAdmissionBatch(t, &admission, next.ticket, second...)
	if _, ok := admission.seal(next.ticket); !ok {
		t.Fatal("second cohort seal failed")
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
