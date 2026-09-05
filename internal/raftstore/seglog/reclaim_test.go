package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestReclaimCheckpointIODoesNotBlockActiveReadsOrAppends(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook = oldMin, oldMax, nil })
	e, _, _ := newReclaimableEngine(t, t.TempDir())
	defer e.Close()
	if err := e.ReserveGroup(2, 4096); err != nil {
		t.Fatal(err)
	}
	if err := e.PersistWave(Wave{ID: waveID(4), Batches: []ReadyBatch{{GroupID: 2, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	reclaimPublishHook = func(phase reclaimPublishPhase) error {
		if phase == reclaimCheckpointAPublished {
			close(entered)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- e.ReclaimDeadPrefix() }()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reclamation did not enter checkpoint I/O")
	}
	foreground := make(chan error, 1)
	go func() {
		_, term, compacted, found, err := e.LookupExact(2, 1)
		if err == nil && (term != 1 || compacted || !found) {
			err = ErrRaftState
		}
		if err == nil {
			err = e.PersistWave(Wave{ID: waveID(5), Batches: []ReadyBatch{{GroupID: 2, Entries: []Entry{{Index: 2, Term: 1}}}}})
		}
		foreground <- err
	}()
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reclamation I/O blocked active read/append")
	}
}

func newReclaimableEngine(t *testing.T, dir string) (*Engine, []SegmentMeta, Checkpoint) {
	t.Helper()
	engine := newEngineAt(t, dir, 1)
	for index := uint64(1); index <= 2; index++ {
		if err := engine.PersistWave(Wave{ID: waveID(index), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: index, Term: 1}}}}}); err != nil {
			t.Fatal(err)
		}
		if err := engine.Rotate(nil); err != nil {
			t.Fatal(err)
		}
		if err := engine.WaitSeal(); err != nil {
			t.Fatal(err)
		}
	}
	removed := append([]SegmentMeta(nil), engine.log.state.Segments...)
	checkpoint := Checkpoint{ID: [16]byte{9}, Index: 2, Term: 1}
	hard := HardState{Term: 1, Vote: 1, Commit: 2}
	if err := engine.PersistWave(Wave{ID: waveID(3), Batches: []ReadyBatch{{GroupID: 1, Checkpoint: &checkpoint, Hard: &hard}}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	return engine, removed, checkpoint
}

func TestReclaimDeadSealedPrefixTwoCutRoundTrip(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	dir := t.TempDir()
	engine, removed, checkpoint := newReclaimableEngine(t, dir)
	if got := engine.liveSealed[removed[0].ID] + engine.liveSealed[removed[1].ID]; got != 0 {
		t.Fatalf("dead prefix still has %d live runs", got)
	}
	if err := engine.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if engine.log.state.AnchorID != 2 || len(engine.log.state.Segments) != 1 || engine.log.state.Segments[0].ID != 3 || engine.log.metadata.slot.ReclaimPhase != reclaimNone {
		t.Fatalf("state=%+v slot=%+v", engine.log.state, engine.log.metadata.slot)
	}
	for i := range removed {
		if _, err := os.Stat(segmentPath(dir, removed[i].FileID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removed segment %d remains: %v", removed[i].ID, err)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	metadata, ok := engine.Metadata(1)
	if !ok || metadata.Checkpoint != checkpoint || metadata.FirstIndex != 3 || metadata.LastIndex != 2 {
		t.Fatalf("metadata=%+v ok=%v", metadata, ok)
	}
}

func TestReclaimAfterSealPublicationBeforeWaitSeal(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	dir := t.TempDir()
	engine, removed, _ := newReclaimableEngine(t, dir)
	defer engine.Close()
	if err := engine.PersistWave(Wave{ID: waveID(4), Batches: []ReadyBatch{
		{GroupID: 1, Entries: []Entry{{Index: 3, Term: 1, Data: []byte("retained")}}},
	}}); err != nil {
		t.Fatal(err)
	}
	published := make(chan struct{})
	if err := engine.Rotate(func(phase RotationPhase) error {
		if phase == RotationSealedMetadataPublished {
			close(published)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("seal publication did not finish")
	}
	// Ordinary node traffic only consumes this completion at the next rotation.
	// Reclamation must work in the intervening active segment, and must leave
	// the completion available for the explicit control-plane waiter.
	if err := engine.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	for _, segment := range removed {
		if _, err := os.Stat(segmentPath(dir, segment.FileID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("dead segment %d remains: %v", segment.ID, err)
		}
	}
	if err := engine.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if _, term, compacted, found, err := engine.LookupExact(1, 3); err != nil || term != 1 || compacted || !found {
		t.Fatalf("retained entry: term=%d compacted=%v found=%v err=%v", term, compacted, found, err)
	}
}

func TestReclaimAdmissionUsesCountOrTwoSegmentBytes(t *testing.T) {
	oldMin := reclaimMinSegments
	reclaimMinSegments = 8
	t.Cleanup(func() { reclaimMinSegments = oldMin })
	const capacity = uint64(1 << 20)
	if reclaimThresholdReached(1, 32, capacity*2-1, capacity) {
		t.Fatal("sub-threshold bytes admitted")
	}
	if !reclaimThresholdReached(1, 32, capacity*2, capacity) {
		t.Fatal("two-segment byte threshold rejected")
	}
	if !reclaimThresholdReached(8, 32, 1, capacity) || !reclaimThresholdReached(32, 32, 1, capacity) {
		t.Fatal("count/queue-bound threshold rejected")
	}
}

func TestReclaimCrashCutsResumeOnOpen(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook = oldMin, oldMax, nil })
	phases := []reclaimPublishPhase{reclaimCheckpointAPublished, reclaimPreparedPublished, reclaimCheckpointBPublished, reclaimDurablePublished, reclaimFileRemoved, reclaimQueueClearFirst, reclaimQueueClearSecond}
	for _, phase := range phases {
		t.Run(string(rune('A'+phase)), func(t *testing.T) {
			dir := t.TempDir()
			engine, _, checkpoint := newReclaimableEngine(t, dir)
			injected := errors.New("reclaim crash cut")
			reclaimPublishHook = func(got reclaimPublishPhase) error {
				if got == phase {
					return injected
				}
				return nil
			}
			if err := engine.ReclaimDeadPrefix(); !errors.Is(err, injected) {
				t.Fatalf("phase %d: %v", phase, err)
			}
			reclaimPublishHook = nil
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openTestEngine(dir)
			if err != nil {
				t.Fatalf("phase %d reopen: %v", phase, err)
			}
			defer reopened.Close()
			metadata, ok := reopened.Metadata(1)
			if !ok || metadata.Checkpoint != checkpoint || metadata.LastIndex != 2 {
				t.Fatalf("phase %d metadata=%+v", phase, metadata)
			}
			if phase >= reclaimPreparedPublished && reopened.log.metadata.slot.ReclaimPhase != reclaimNone {
				t.Fatalf("phase %d queue not resumed: %+v", phase, reopened.log.metadata.slot)
			}
		})
	}
}

func TestReclaimResumesPreparedAndDurableCutsWithoutReopen(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook = oldMin, oldMax, nil })
	for _, phase := range []reclaimPublishPhase{reclaimPreparedPublished, reclaimCheckpointBPublished, reclaimDurablePublished} {
		t.Run(string(rune('0'+phase)), func(t *testing.T) {
			e, _, _ := newReclaimableEngine(t, t.TempDir())
			defer e.Close()
			injected := errors.New("same-process reclaim cut")
			reclaimPublishHook = func(got reclaimPublishPhase) error {
				if got == phase {
					return injected
				}
				return nil
			}
			if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
				t.Fatalf("phase %d=%v", phase, err)
			}
			reclaimPublishHook = nil
			if err := e.ReclaimDeadPrefix(); err != nil {
				t.Fatalf("phase %d resume=%v", phase, err)
			}
			if e.log.state.AnchorID != 2 || len(e.log.state.Segments) != 1 || e.log.state.Segments[0].ID != 3 || len(e.reclaimAfter) != len(e.log.state.Segments) {
				t.Fatalf("phase %d state=%+v fences=%v", phase, e.log.state, e.reclaimAfter)
			}
			metadata, ok := e.Metadata(1)
			if !ok || metadata.LastIndex != 2 || metadata.FirstIndex != 3 {
				t.Fatalf("phase %d metadata=%+v ok=%v", phase, metadata, ok)
			}
		})
	}
}

func TestReclaimDeleteFailuresRetainDurableQueue(t *testing.T) {
	oldMin, oldMax, oldRemove, oldSync := reclaimMinSegments, reclaimMaxSegments, reclaimRemove, reclaimSyncDir
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() {
		reclaimMinSegments, reclaimMaxSegments, reclaimRemove, reclaimSyncDir = oldMin, oldMax, oldRemove, oldSync
	})
	for _, test := range []struct {
		name string
		set  func(error)
	}{
		{name: "unlink", set: func(injected error) { reclaimRemove = func(string) error { return injected } }},
		{name: "dirsync", set: func(injected error) { reclaimSyncDir = func(string) error { return injected } }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			engine, _, _ := newReclaimableEngine(t, dir)
			injected := syscall.EIO
			test.set(injected)
			if err := engine.ReclaimDeadPrefix(); !errors.Is(err, injected) {
				t.Fatalf("got %v", err)
			}
			if engine.log.metadata.slot.ReclaimPhase != reclaimDurable || engine.log.metadata.slot.RetiredCount == 0 {
				t.Fatalf("retirement intent cleared: %+v", engine.log.metadata.slot)
			}
			if rotateErr := engine.Rotate(nil); !errors.Is(rotateErr, ErrBounds) {
				t.Fatalf("rotation crossed durable reclaim intent: %v", rotateErr)
			}
			before, readErr := os.ReadDir(dir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			for attempt := 0; attempt < 8; attempt++ {
				if retryErr := engine.ReclaimDeadPrefix(); !errors.Is(retryErr, injected) {
					t.Fatalf("retry %d=%v", attempt, retryErr)
				}
			}
			after, readErr := os.ReadDir(dir)
			if readErr != nil || len(after) != len(before) {
				t.Fatalf("queue retries leaked files: before=%d after=%d err=%v", len(before), len(after), readErr)
			}
			reclaimRemove, reclaimSyncDir = oldRemove, oldSync
			engine.runMetadataMaintenance()
			if engine.log.metadata.slot.ReclaimPhase != reclaimNone {
				t.Fatalf("ticker maintenance did not resume queue: %+v", engine.log.metadata.slot)
			}
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openTestEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.log.metadata.slot.ReclaimPhase != reclaimNone {
				t.Fatalf("queue not resumed: %+v", reopened.log.metadata.slot)
			}
		})
	}
}

func TestReclaimClosesOpenPrefixReaderBeforeUnlink(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	engine, removed, _ := newReclaimableEngine(t, t.TempDir())
	defer engine.Close()
	if err := engine.ReserveReaders(1); err != nil {
		t.Fatal(err)
	}
	if err := engine.PrepareSegment(removed[0].ID); err != nil {
		t.Fatal(err)
	}
	opened := engine.readers[0].file
	if err := engine.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if engine.readers[0].file != nil {
		t.Fatal("retired reader remained cached")
	}
	if _, err := opened.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("retired fd still usable: %v", err)
	}
}

func TestReclaimRejectsNamespaceSubstitutionAfterExactFileValidation(t *testing.T) {
	oldMin, oldMax, oldHook := reclaimMinSegments, reclaimMaxSegments, reclaimBeforeRemove
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimBeforeRemove = oldMin, oldMax, oldHook })
	dir := t.TempDir()
	engine, removed, _ := newReclaimableEngine(t, dir)
	defer engine.Close()
	target := segmentPath(dir, removed[0].FileID)
	backup := target + ".validated"
	replaced := false
	reclaimBeforeRemove = func(path string) {
		if replaced || path != target {
			return
		}
		replaced = true
		if err := os.Rename(path, backup); err != nil {
			t.Fatalf("rename validated inode: %v", err)
		}
		if err := os.WriteFile(path, []byte("substitute"), 0o600); err != nil {
			t.Fatalf("install substitute: %v", err)
		}
	}
	if err := engine.ReclaimDeadPrefix(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("namespace substitution accepted: %v", err)
	}
	if engine.log.metadata.slot.ReclaimPhase != reclaimDurable || engine.log.metadata.slot.RetiredCount == 0 {
		t.Fatalf("durable retirement intent cleared: %+v", engine.log.metadata.slot)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "substitute" {
		t.Fatalf("substitute removed or changed: %q %v", got, err)
	}
	reclaimBeforeRemove = nil
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, target); err != nil {
		t.Fatal(err)
	}
	if err := engine.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
}

func dropReserveForRecycle(t *testing.T, engine *Engine, slot int) {
	t.Helper()
	descriptor := engine.log.state.Reserves[slot]
	file := engine.log.reserveFiles[slot]
	if file == nil || !descriptor.Ready {
		t.Fatal("test reserve missing")
	}
	opened, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	engine.log.reserveFiles[slot] = nil
	if err = removeExactPublishedPath(opened, segmentPath(engine.log.dir, descriptor.FileID), engine.log.dir); err != nil {
		t.Fatal(err)
	}
	next := engine.log.metadata.slot
	next.Generation++
	next.Reserves[slot] = reserveDescriptor{}
	if err = engine.log.metadata.publish(next, nil); err != nil {
		t.Fatal(err)
	}
	engine.log.state.Generation = next.Generation
	engine.log.state.Reserves[slot] = reserveDescriptor{}
}

func TestReclaimRecyclesDeadSegmentIntoMissingReserve(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	engine, removed, _ := newReclaimableEngine(t, t.TempDir())
	defer engine.Close()
	dropReserveForRecycle(t, engine, 0)
	if err := engine.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if !engine.log.state.Reserves[0].Ready || engine.log.state.Reserves[0].FileID != removed[0].FileID {
		t.Fatalf("reserve=%+v removed=%+v", engine.log.state.Reserves[0], removed[0])
	}
	if err := verifyPhysicalReserve(engine.log.reserveFiles[0], engine.log.state.Reserves[0], engine.log.state.LogID, engine.authKey); err != nil {
		t.Fatal(err)
	}
}

func TestReclaimRecycleCrashCutsResumeAuthenticatedLifecycle(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	tests := []struct {
		name   string
		inject func(error)
	}{
		{name: "identity-partial", inject: func(injected error) {
			recycleIdentityWrite = func(file *os.File, bytes []byte, offset int64) error {
				_, _ = file.WriteAt(bytes[:len(bytes)/2], offset)
				return injected
			}
		}},
		{name: "truncate", inject: func(injected error) {
			recycleTruncate = func(file *os.File, size int64) error {
				if err := file.Truncate(size); err != nil {
					return err
				}
				return injected
			}
		}},
		{name: "preallocate", inject: func(injected error) {
			physical := reservePhysicalFile
			reservePhysicalFile = func(file *os.File, capacity uint64) error {
				if err := physical(file, capacity); err != nil {
					return err
				}
				return injected
			}
		}},
		{name: "first-sync", inject: func(injected error) {
			calls := 0
			recycleFileSync = func(file *os.File) error {
				calls++
				if err := file.Sync(); err != nil {
					return err
				}
				if calls == 1 {
					return injected
				}
				return nil
			}
		}},
		{name: "final-sync", inject: func(injected error) {
			calls := 0
			recycleFileSync = func(file *os.File) error {
				calls++
				if err := file.Sync(); err != nil {
					return err
				}
				if calls == 2 {
					return injected
				}
				return nil
			}
		}},
		{name: "reserve-publication", inject: func(injected error) {
			reclaimPublishHook = func(phase reclaimPublishPhase) error {
				if phase == reclaimReservePublished {
					return injected
				}
				return nil
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			savedWrite, savedTruncate, savedPhysical, savedSync, savedHook := recycleIdentityWrite, recycleTruncate, reservePhysicalFile, recycleFileSync, reclaimPublishHook
			defer func() {
				recycleIdentityWrite, recycleTruncate, reservePhysicalFile, recycleFileSync, reclaimPublishHook = savedWrite, savedTruncate, savedPhysical, savedSync, savedHook
			}()
			dir := t.TempDir()
			engine, _, _ := newReclaimableEngine(t, dir)
			dropReserveForRecycle(t, engine, 0)
			injected := syscall.EIO
			test.inject(injected)
			if err := engine.ReclaimDeadPrefix(); !errors.Is(err, injected) {
				t.Fatalf("got %v", err)
			}
			recycleIdentityWrite, recycleTruncate, reservePhysicalFile, recycleFileSync, reclaimPublishHook = savedWrite, savedTruncate, savedPhysical, savedSync, savedHook
			if err := engine.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openTestEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.log.metadata.slot.ReclaimPhase != reclaimNone || !reopened.log.state.Reserves[0].Ready {
				t.Fatalf("slot=%+v reserve=%+v", reopened.log.metadata.slot, reopened.log.state.Reserves[0])
			}
		})
	}
}

func TestReclaimStopsAtFirstLiveSegmentDespiteLaterDeadHole(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 1, 32
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	e := newEngineAt(t, t.TempDir(), 1, 2)
	defer e.Close()
	for _, batch := range []ReadyBatch{
		{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}},
		{GroupID: 2, Entries: []Entry{{Index: 1, Term: 1}}},
		{GroupID: 1, Entries: []Entry{{Index: 2, Term: 1}}},
	} {
		if err := e.PersistWave(Wave{ID: waveID(e.sequence + 1), Batches: []ReadyBatch{batch}}); err != nil {
			t.Fatal(err)
		}
		if err := e.Rotate(nil); err != nil {
			t.Fatal(err)
		}
		if err := e.WaitSeal(); err != nil {
			t.Fatal(err)
		}
	}
	cp := Checkpoint{ID: [16]byte{7}, Index: 2, Term: 1}
	hard1 := HardState{Term: 1, Vote: 1, Commit: 2}
	if err := e.PersistWave(Wave{ID: waveID(4), Batches: []ReadyBatch{{GroupID: 1, Checkpoint: &cp, Hard: &hard1}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if e.log.state.AnchorID != 1 || len(e.log.state.Segments) != 3 || e.log.state.Segments[0].ID != 2 {
		t.Fatalf("reclaimed through live middle segment: anchor=%d segments=%+v", e.log.state.AnchorID, e.log.state.Segments)
	}
}

func TestReclaimPreservesControlOnlySummaryAndRetainedOverwrite(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 1, 32
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1, 2)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	hard2 := HardState{Term: 2, Vote: 2}
	if err := e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{GroupID: 2, Hard: &hard2}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.PersistWave(Wave{ID: waveID(3), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 2, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{ID: [16]byte{8}, Index: 1, Term: 1}
	hard1 := HardState{Term: 1, Vote: 1, Commit: 1}
	if err := e.PersistWave(Wave{ID: waveID(4), Batches: []ReadyBatch{{GroupID: 1, Checkpoint: &cp, Hard: &hard1}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	// Segment 1 is dead, segment 2 is control-only and carried into the base,
	// while segment 3 remains live.
	if e.log.state.AnchorID != 2 || len(e.log.state.Segments) != 2 {
		t.Fatalf("anchor=%d segments=%+v", e.log.state.AnchorID, e.log.state.Segments)
	}
	entry := Entry{Index: 2, Term: 2}
	if err := e.PersistWave(Wave{ID: waveID(5), Batches: []ReadyBatch{{GroupID: 1, ReplaceFrom: 2, Entries: []Entry{entry}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	meta2, ok := e.Metadata(2)
	if !ok || meta2.Hard != hard2 {
		t.Fatalf("control-only summary=%+v", meta2)
	}
	location, term, _, ok, err := e.LookupExact(1, 2)
	if err != nil || !ok || term != 2 || location.Term != 2 {
		t.Fatalf("replacement=%+v term=%d ok=%v err=%v", location, term, ok, err)
	}
}

func TestRecycledReserveIsActivatedAndWrittenBeforeSecondReclaim(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 1, 32
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	dir := t.TempDir()
	e, removed, _ := newReclaimableEngine(t, dir)
	dropReserveForRecycle(t, e, 0)
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	recycledID := removed[0].FileID
	if err := e.PersistWave(Wave{ID: waveID(4), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: 3, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if e.log.state.ActiveFileID != recycledID {
		t.Fatalf("active=%x recycled=%x", e.log.state.ActiveFileID, recycledID)
	}
	cp := Checkpoint{ID: [16]byte{12}, Index: 3, Term: 1}
	hard := HardState{Term: 1, Vote: 1, Commit: 3}
	if err := e.PersistWave(Wave{ID: waveID(5), Batches: []ReadyBatch{{GroupID: 1, Checkpoint: &cp, Hard: &hard}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	meta, ok := e.Metadata(1)
	if !ok || meta.Checkpoint != cp || meta.LastIndex != 3 {
		t.Fatalf("metadata=%+v", meta)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	segmentFiles, checkpointFiles := 0, 0
	allocated := uint64(0)
	for _, entry := range entries {
		info, statErr := entry.Info()
		if statErr != nil {
			t.Fatal(statErr)
		}
		if bytes, ok := allocatedFileBytes(info); ok {
			allocated += bytes
		}
		if strings.HasPrefix(entry.Name(), "segment-") {
			segmentFiles++
		}
		if strings.HasPrefix(entry.Name(), "catalog-checkpoint-") {
			checkpointFiles++
		}
	}
	wantSegments := 1 + len(e.log.state.Segments)
	for _, reserve := range e.log.state.Reserves {
		if reserve.Ready {
			wantSegments++
		}
	}
	if segmentFiles != wantSegments || checkpointFiles > 2 || allocated > uint64(segmentFiles)*e.log.state.SegmentCapacity+metadataCatalogEnd+(2<<20) {
		t.Fatalf("segment files=%d want=%d checkpoints=%d allocated=%d", segmentFiles, wantSegments, checkpointFiles, allocated)
	}
}

func durableReclaimCrashImage(t *testing.T) (string, []SegmentMeta) {
	t.Helper()
	dir := t.TempDir()
	e, removed, _ := newReclaimableEngine(t, dir)
	injected := errors.New("durable reclaim cut")
	reclaimPublishHook = func(phase reclaimPublishPhase) error {
		if phase == reclaimDurablePublished {
			return injected
		}
		return nil
	}
	if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	reclaimPublishHook = nil
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, removed
}

func TestReclaimCorruptLifecycleCertificateFailsClosed(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook = oldMin, oldMax, nil })
	dir, removed := durableReclaimCrashImage(t)
	file, err := os.OpenFile(segmentPath(dir, removed[0].FileID), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err = file.ReadAt(one[:], segmentIdentityBytes+56); err == nil {
		one[0] ^= 1
		_, err = file.WriteAt(one[:], segmentIdentityBytes+56)
	}
	err = errors.Join(err, file.Sync(), file.Close())
	if err != nil {
		t.Fatal(err)
	}
	if reopened, openErr := openTestEngine(dir); openErr == nil {
		_ = reopened.Close()
		t.Fatal("corrupt lifecycle certificate opened")
	}
}

func TestReclaimCorruptRetiredPreviousHashFailsClosed(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook = oldMin, oldMax, nil })
	dir, _ := durableReclaimCrashImage(t)
	file, err := os.OpenFile(dir+string(os.PathSeparator)+metadataName, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var raw [metadataSlotBytes]byte
	for _, offset := range []int64{metadataSlot0Offset, metadataSlot1Offset} {
		if err = readFullAt(file, raw[:], offset); err != nil {
			t.Fatal(err)
		}
		slot, decodeErr := unmarshalMetadataSlot(raw[:], testAuthKey)
		if decodeErr != nil || slot.RetiredCount == 0 {
			t.Fatalf("slot decode=%v slot=%+v", decodeErr, slot)
		}
		retired := slot.Retired[0]
		retired.PreviousHash[0] ^= 1
		putRetiredDescriptor(raw[retiredDescriptorsOffset:retiredDescriptorsOffset+retiredDescriptorBytes], retired)
		mac := hmac.New(sha256.New, testAuthKey[:])
		_, _ = mac.Write(raw[:metadataSlotMACOffset])
		copy(raw[metadataSlotMACOffset:metadataSlotCRCOffset], mac.Sum(nil))
		binary.LittleEndian.PutUint32(raw[metadataSlotCRCOffset:], crc32.Checksum(raw[:metadataSlotCRCOffset], crcTable))
		if err = writeFullAt(file, raw[:], offset); err != nil {
			t.Fatal(err)
		}
	}
	if err = errors.Join(file.Sync(), file.Close()); err != nil {
		t.Fatal(err)
	}
	if reopened, openErr := openTestEngine(dir); openErr == nil {
		_ = reopened.Close()
		t.Fatal("fabricated retired predecessor opened")
	}
}

func TestReclaimAfterSealedClipWhileActiveRemainsNonempty(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	dir := t.TempDir()
	e, _, checkpoint := newReclaimableEngine(t, dir)
	if err := e.ReserveGroup(2, 4096); err != nil {
		t.Fatal(err)
	}
	// The clipping checkpoint is sealed, but unrelated submission continues in
	// the new active segment. Its sequence must not starve the dead prefix.
	if err := e.PersistWave(Wave{ID: waveID(4), Batches: []ReadyBatch{{GroupID: 2, Entries: []Entry{{Index: 1, Term: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if e.log.records == 0 || e.log.state.AnchorID != 2 {
		t.Fatalf("active records=%d anchor=%d", e.log.records, e.log.state.AnchorID)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	group1, ok := e.Metadata(1)
	if !ok || group1.Checkpoint != checkpoint {
		t.Fatalf("group1=%+v", group1)
	}
	group2, ok := e.Metadata(2)
	if !ok || group2.LastIndex != 1 {
		t.Fatalf("group2=%+v", group2)
	}
}

func TestReclaimFastOpenReadsOnlyRetainedMetadataAndActiveTail(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments = oldMin, oldMax })
	dir := t.TempDir()
	e, _, _ := newReclaimableEngine(t, dir)
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	retained := len(e.log.state.Segments)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	var counters recoveryIOCounters
	e, err := openEngineAuthenticatedObserved(dir, func(*os.File) error { return nil }, testLogID, testAuthKey, &counters)
	if err != nil {
		t.Fatal(err)
	}
	if counters.sealedMetadataReads != uint64(retained) || counters.sealedMetadataBytes > uint64(retained)*16<<10 || counters.activeScanBytes != 0 || counters.pendingPayloadBytes != 0 || counters.maintenanceReserveAttempts != 0 || counters.maintenanceCheckpointAttempts != 0 {
		t.Fatalf("retained=%d counters=%+v", retained, counters)
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(3, func() {
		opened, openErr := openEngine(dir, func(*os.File) error { return nil }, testLogID, testAuthKey)
		if openErr != nil {
			panic(openErr)
		}
		if closeErr := opened.Close(); closeErr != nil {
			panic(closeErr)
		}
	})
	// Open is control-plane and may allocate, but its allocation count is
	// bounded by retained group-runs rather than reclaimed entry history.
	if allocs > 2_000 {
		t.Fatalf("reclaimed open allocations=%f", allocs)
	}
}

func checkpointFileAccounting(t *testing.T, dir string) (count int, allocated uint64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.Contains(entry.Name(), "catalog-checkpoint-") {
			continue
		}
		count++
		info, infoErr := entry.Info()
		if infoErr != nil {
			t.Fatal(infoErr)
		}
		if bytes, ok := allocatedFileBytes(info); ok {
			allocated += bytes
		}
	}
	return count, allocated
}

func TestReclaimDeterministicCheckpointRetryDoesNotAccumulateOrphans(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() {
		reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook, checkpointPublishHook = oldMin, oldMax, nil, nil
	})
	for _, test := range []struct {
		name   string
		repeat bool
		hook   func(error)
	}{
		{name: "complete-A-before-slot", repeat: true, hook: func(injected error) {
			reclaimPublishHook = func(phase reclaimPublishPhase) error {
				if phase == reclaimCheckpointAPublished {
					return injected
				}
				return nil
			}
		}},
		{name: "complete-temp-before-sync", hook: func(injected error) {
			checkpointPublishHook = func(phase checkpointPublishPhase) error {
				if phase == checkpointTempWritten {
					return injected
				}
				return nil
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			e, _, _ := newReclaimableEngine(t, dir)
			defer e.Close()
			injected := syscall.EIO
			test.hook(injected)
			if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			count, allocated := checkpointFileAccounting(t, dir)
			if test.repeat {
				for retry := 0; retry < 8; retry++ {
					if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
						t.Fatalf("retry %d=%v", retry, err)
					}
					gotCount, gotAllocated := checkpointFileAccounting(t, dir)
					if gotCount != count || gotAllocated != allocated {
						t.Fatalf("retry %d checkpoint files/bytes %d/%d -> %d/%d", retry, count, allocated, gotCount, gotAllocated)
					}
				}
			}
			reclaimPublishHook, checkpointPublishHook = nil, nil
			if err := e.ReclaimDeadPrefix(); err != nil {
				t.Fatal(err)
			}
			if got, _ := checkpointFileAccounting(t, dir); got > 2 {
				t.Fatalf("checkpoint files=%d", got)
			}
		})
	}
}

func TestCheckpointRetireIntentSurvivesUnlinkFailure(t *testing.T) {
	oldMin, oldMax, oldRemove := reclaimMinSegments, reclaimMaxSegments, reclaimRemove
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimRemove = oldMin, oldMax, oldRemove })
	dir := t.TempDir()
	e, _, _ := newReclaimableEngine(t, dir)
	defer e.Close()
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	reclaimMinSegments = 1
	injected := syscall.EIO
	reclaimRemove = func(path string) error {
		if strings.Contains(path, "catalog-checkpoint-") {
			return injected
		}
		return os.Remove(path)
	}
	if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
		t.Fatalf("got %v", err)
	}
	if e.log.metadata.slot.ReclaimPhase != reclaimNone || e.log.metadata.slot.RetiredCheckpointCount == 0 {
		t.Fatalf("checkpoint retirement intent lost: %+v", e.log.metadata.slot)
	}
	count, allocated := checkpointFileAccounting(t, dir)
	for retry := 0; retry < 4; retry++ {
		if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
			t.Fatalf("retry %d=%v", retry, err)
		}
		gotCount, gotAllocated := checkpointFileAccounting(t, dir)
		if gotCount != count || gotAllocated != allocated {
			t.Fatalf("retry %d files/bytes %d/%d -> %d/%d", retry, count, allocated, gotCount, gotAllocated)
		}
	}
	reclaimRemove = oldRemove
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	if e.log.metadata.slot.RetiredCheckpointCount != 0 {
		t.Fatalf("checkpoint GC intent not cleared: %+v", e.log.metadata.slot)
	}
}

func TestCheckpointRetirementRejectsNamespaceSubstitution(t *testing.T) {
	oldMin, oldMax, oldRemove, oldHook := reclaimMinSegments, reclaimMaxSegments, reclaimRemove, reclaimBeforeCheckpointRemove
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() {
		reclaimMinSegments, reclaimMaxSegments, reclaimRemove, reclaimBeforeCheckpointRemove = oldMin, oldMax, oldRemove, oldHook
	})
	dir := t.TempDir()
	e, _, _ := newReclaimableEngine(t, dir)
	defer e.Close()
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	reclaimMinSegments = 1
	injected := syscall.EIO
	reclaimRemove = func(path string) error {
		if strings.Contains(path, "catalog-checkpoint-") {
			return injected
		}
		return os.Remove(path)
	}
	if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
		t.Fatalf("establish checkpoint retirement intent: %v", err)
	}
	if e.log.metadata.slot.RetiredCheckpointCount == 0 {
		t.Fatal("missing checkpoint retirement intent")
	}
	reclaimRemove = oldRemove
	target := filepath.Join(dir, checkpointFileName(e.log.metadata.slot.RetiredCheckpoints[0].ID))
	backup := target + ".validated"
	replaced := false
	reclaimBeforeCheckpointRemove = func(path string) {
		if replaced || path != target {
			return
		}
		replaced = true
		if err := os.Rename(path, backup); err != nil {
			t.Fatalf("rename authenticated checkpoint: %v", err)
		}
		if err := os.WriteFile(path, []byte("substitute"), 0o600); err != nil {
			t.Fatalf("install substitute: %v", err)
		}
	}
	if err := e.ReclaimDeadPrefix(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("checkpoint substitution accepted: %v", err)
	}
	if e.log.metadata.slot.RetiredCheckpointCount == 0 {
		t.Fatal("checkpoint retirement intent cleared")
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "substitute" {
		t.Fatalf("substitute removed or changed: %q %v", got, err)
	}
	reclaimBeforeCheckpointRemove = nil
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, target); err != nil {
		t.Fatal(err)
	}
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableCrashCarriesPreAnchorCheckpointRetirements(t *testing.T) {
	oldMin, oldMax := reclaimMinSegments, reclaimMaxSegments
	reclaimMinSegments, reclaimMaxSegments = 2, 2
	t.Cleanup(func() { reclaimMinSegments, reclaimMaxSegments, reclaimPublishHook = oldMin, oldMax, nil })
	dir := t.TempDir()
	e, _, _ := newReclaimableEngine(t, dir)
	if err := e.ReclaimDeadPrefix(); err != nil {
		t.Fatal(err)
	}
	reclaimMinSegments = 1
	injected := errors.New("crash after durable reclaim")
	reclaimPublishHook = func(phase reclaimPublishPhase) error {
		if phase == reclaimDurablePublished {
			return injected
		}
		return nil
	}
	if err := e.ReclaimDeadPrefix(); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if e.log.metadata.slot.ReclaimPhase != reclaimDurable || e.log.metadata.slot.RetiredCheckpointCount == 0 {
		t.Fatalf("pre-anchor checkpoints not carried: %+v", e.log.metadata.slot)
	}
	before, _ := checkpointFileAccounting(t, dir)
	if before < 4 {
		t.Fatalf("checkpoint fixture count=%d", before)
	}
	reclaimPublishHook = nil
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	after, _ := checkpointFileAccounting(t, dir)
	if e.log.metadata.slot.ReclaimPhase != reclaimNone || e.log.metadata.slot.RetiredCheckpointCount != 0 || after > 2 {
		t.Fatalf("slot=%+v checkpoint files=%d", e.log.metadata.slot, after)
	}
}
