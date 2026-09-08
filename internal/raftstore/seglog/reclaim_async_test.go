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
	ticket, err := e.beginReclaim(false)
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
	ticket, err := e.beginReclaim(false)
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
