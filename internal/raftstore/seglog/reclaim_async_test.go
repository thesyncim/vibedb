package seglog

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func waitReclaimSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitReclaimTicket(t *testing.T, ticket *reclaimTicket) error {
	t.Helper()
	select {
	case err := <-ticket.done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reclaim cleaner")
		return nil
	}
}

func TestStartReclaimDeadPrefixReturnsBeforePhysicalCleanup(t *testing.T) {
	oldMin, oldMax, oldBeforeRemove := reclaimMinSegments, reclaimMaxSegments, reclaimBeforeRemove
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimBeforeRemove = oldMin, oldMax, oldBeforeRemove })
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	dir := t.TempDir()
	e, removed, _ := newReclaimableEngine(t, dir)
	defer e.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	target := segmentPath(dir, removed[0].FileID)
	reclaimBeforeRemove = func(path string) {
		if path == target {
			close(entered)
			<-release
		}
	}
	result := make(chan error, 1)
	go func() { result <- e.StartReclaimDeadPrefix() }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("reclaim scheduling waited for physical cleanup")
	}
	waitReclaimSignal(t, entered, "physical cleanup")
	close(release)

	deadline := time.Now().Add(10 * time.Second)
	for {
		e.writeMu.Lock()
		done, cleanupErr := e.cleanupTicket == nil && e.log.metadata.slot.ReclaimPhase == reclaimNone, e.cleanupErr
		e.writeMu.Unlock()
		if cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for scheduled reclaim")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAsyncReclaimDetachesReadersAndAllowsRotation(t *testing.T) {
	oldMin, oldMax, oldBeforeRemove := reclaimMinSegments, reclaimMaxSegments, reclaimBeforeRemove
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimBeforeRemove = oldMin, oldMax, oldBeforeRemove })
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	dir := t.TempDir()
	e, removed, _ := newReclaimableEngine(t, dir)
	defer e.Close()
	if err := e.PersistWave(Wave{ID: waveID(e.sequence + 1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 3, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.ReserveReaders(1); err != nil {
		t.Fatal(err)
	}
	if err := e.PrepareSegment(removed[0].ID); err != nil {
		t.Fatal(err)
	}
	e.readerMu.Lock()
	_, held, ok := e.acquireReaderLocked(removed[0].ID)
	e.readerMu.Unlock()
	if !ok {
		t.Fatal("failed to acquire retired reader lease")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	target := segmentPath(dir, removed[0].FileID)
	reclaimBeforeRemove = func(path string) {
		if path != target {
			return
		}
		close(entered)
		<-release
	}
	ticket, err := e.beginReclaim()
	if err != nil {
		held.Close()
		t.Fatal(err)
	}
	if ticket == nil {
		held.Close()
		t.Fatal("reclaim did not create a cleaner ticket")
	}
	e.writeMu.Lock()
	slot, state := e.log.metadata.slot, e.log.state
	e.writeMu.Unlock()
	if slot.ReclaimPhase != reclaimDurable || state.AnchorID != removed[1].ID {
		held.Close()
		t.Fatalf("logical cut not installed: slot=%+v state=%+v", slot, state)
	}
	select {
	case <-entered:
		t.Fatal("cleaner passed a live reader lease")
	default:
	}
	held.Close()
	waitReclaimSignal(t, entered, "blocked cleaner")

	if err := e.Rotate(nil); err != nil {
		close(release)
		t.Fatalf("rotation blocked by physical cleanup: %v", err)
	}
	if _, _, _, ok, err := e.LookupExact(1, 3); err != nil || !ok {
		close(release)
		t.Fatalf("retained read lookup failed during cleanup: ok=%v err=%v", ok, err)
	}
	close(release)
	if err := waitReclaimTicket(t, ticket); err != nil {
		t.Fatalf("async cleanup: %v", err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if e.log.metadata.slot.ReclaimPhase != reclaimNone {
		t.Fatalf("retirement phase remains after cleanup: %+v", e.log.metadata.slot)
	}
	for _, segment := range removed {
		if _, err := os.Stat(segmentPath(dir, segment.FileID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired segment %d remains: %v", segment.ID, err)
		}
	}
}

func TestAsyncReclaimFailureReopensAndRetriesWithoutReuse(t *testing.T) {
	oldMin, oldMax, oldRemove := reclaimMinSegments, reclaimMaxSegments, reclaimRemove
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimRemove = oldMin, oldMax, oldRemove })
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	dir := t.TempDir()
	e, removed, _ := newReclaimableEngine(t, dir)
	reclaimRemove = func(string) error { return syscall.EIO }
	ticket, err := e.beginReclaim()
	if err != nil {
		e.Close()
		t.Fatal(err)
	}
	if err := waitReclaimTicket(t, ticket); !errors.Is(err, syscall.EIO) {
		e.Close()
		t.Fatalf("cleaner failure=%v", err)
	}
	if e.log.metadata.slot.ReclaimPhase != reclaimDurable || e.log.state.AnchorID != removed[1].ID {
		e.Close()
		t.Fatalf("failed cleanup lost durable intent: slot=%+v state=%+v", e.log.metadata.slot, e.log.state)
	}
	reclaimRemove = oldRemove
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.log.metadata.slot.ReclaimPhase != reclaimNone || reopened.log.state.AnchorID != removed[1].ID {
		t.Fatalf("reopen did not finish authenticated retirement: slot=%+v state=%+v", reopened.log.metadata.slot, reopened.log.state)
	}
	for _, segment := range removed {
		if _, err := os.Stat(segmentPath(dir, segment.FileID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired segment %d remains after reopen: %v", segment.ID, err)
		}
		for _, reserve := range reopened.log.state.Reserves {
			if reserve.FileID == segment.FileID {
				t.Fatalf("retired file %d was reused as reserve", segment.ID)
			}
		}
	}
}

func TestReclaimEntryPointsShareUnifiedCleanupPath(t *testing.T) {
	oldMin, oldMax, oldSync, oldHook := reclaimMinSegments, reclaimMaxSegments, reclaimSyncDir, reclaimPublishHook
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() {
		reclaimMinSegments, reclaimMaxSegments, reclaimSyncDir, reclaimPublishHook = oldMin, oldMax, oldSync, oldHook
	})

	var reference []reclaimPublishPhase
	for _, entryPoint := range []struct {
		name   string
		public bool
	}{
		{name: "public-maintenance-request", public: true},
		{name: "internal-maintenance-event", public: false},
	} {
		t.Run(entryPoint.name, func(t *testing.T) {
			reclaimPublishHook = nil
			reclaimSyncDir = oldSync
			dir := t.TempDir()
			e, removed, _ := newReclaimableEngine(t, dir)
			defer e.Close()

			var phases []reclaimPublishPhase
			reclaimPublishHook = func(phase reclaimPublishPhase) error {
				phases = append(phases, phase)
				return nil
			}
			syncs := 0
			reclaimSyncDir = func(path string) error {
				syncs++
				return oldSync(path)
			}

			if entryPoint.public {
				if err := e.ReclaimDeadPrefix(); err != nil {
					t.Fatal(err)
				}
			} else {
				ticket, err := e.beginReclaim()
				if err != nil {
					t.Fatal(err)
				}
				if ticket == nil {
					t.Fatal("maintenance event did not create a reclaim ticket")
				}
				if err := waitReclaimTicket(t, ticket); err != nil {
					t.Fatal(err)
				}
			}

			if syncs == 0 {
				t.Fatal("unified cleaner did not perform a directory sync")
			}
			wantPhases := []reclaimPublishPhase{
				reclaimCheckpointAPublished,
				reclaimPreparedPublished,
				reclaimCheckpointBPublished,
				reclaimDurablePublished,
				reclaimFileRemoved,
				reclaimFileRemoved,
				reclaimQueueClearFirst,
				reclaimQueueClearSecond,
			}
			if len(phases) != len(wantPhases) {
				t.Fatalf("cleanup phases=%v, want=%v", phases, wantPhases)
			}
			for i := range wantPhases {
				if phases[i] != wantPhases[i] {
					t.Fatalf("cleanup phase %d=%v, want %v (all=%v)", i, phases[i], wantPhases[i], phases)
				}
			}
			if len(reference) == 0 {
				reference = append([]reclaimPublishPhase(nil), phases...)
			} else {
				if len(reference) != len(phases) {
					t.Fatalf("entry point phase count=%d, reference=%d", len(phases), len(reference))
				}
				for i := range reference {
					if phases[i] != reference[i] {
						t.Fatalf("entry point phase %d=%v, reference=%v", i, phases[i], reference[i])
					}
				}
			}
			e.writeMu.Lock()
			slot, state := e.log.metadata.slot, e.log.state
			e.writeMu.Unlock()
			if slot.ReclaimPhase != reclaimNone || slot.RetiredCount != 0 || state.AnchorID != removed[1].ID {
				t.Fatalf("cleanup outcome=%+v state=%+v", slot, state)
			}
			for _, segment := range removed {
				if _, err := os.Stat(segmentPath(dir, segment.FileID)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("retired segment %d remains: %v", segment.ID, err)
				}
			}
		})
	}
}
