package seglog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func validCatalogCheckpoint(id fileID) catalogCheckpoint {
	hash := [32]byte{9}
	return catalogCheckpoint{
		ID:          id,
		LogID:       testLogID,
		Generation:  2,
		Tail:        metadataCatalogStart + catalogRecordBytes,
		CatalogHash: [32]byte{4},
		Segments: []SegmentMeta{{
			ID: 1, Generation: 1,
			Bytes:       segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes,
			IndexOffset: segmentHeaderBytes,
			IndexBytes:  sealedIndexHeaderBytes,
			Hash:        hash,
			FileID:      fileID{8},
			State:       SegmentSealed,
		}},
	}
}

func TestCatalogCheckpointPublicationCrashPhasesLeaveOnlyUnownedObjects(t *testing.T) {
	injected := errors.New("checkpoint crash")
	for _, phase := range []checkpointPublishPhase{checkpointTempWritten, checkpointFileSynced, checkpointRenamed, checkpointDirectorySynced} {
		t.Run(string(rune('0'+phase)), func(t *testing.T) {
			dir := t.TempDir()
			engine, err := createTestEngine(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = engine.Close(); err != nil {
				t.Fatal(err)
			}
			id := fileID{byte(phase), 7}
			checkpointPublishHook = func(got checkpointPublishPhase) error {
				if got == phase {
					return injected
				}
				return nil
			}
			t.Cleanup(func() { checkpointPublishHook = nil })
			_, err = writeCatalogCheckpoint(dir, validCatalogCheckpoint(id), testAuthKey)
			if !errors.Is(err, injected) {
				t.Fatalf("phase %d error=%v", phase, err)
			}
			tmp := filepath.Join(dir, "."+checkpointFileName(id)+".tmp")
			final := filepath.Join(dir, checkpointFileName(id))
			if phase <= checkpointFileSynced {
				if _, statErr := os.Stat(tmp); statErr != nil {
					t.Fatalf("recognized orphan temp missing: %v", statErr)
				}
			} else if _, statErr := os.Stat(final); statErr != nil {
				t.Fatalf("unreferenced final checkpoint missing: %v", statErr)
			}
			reopened, openErr := openTestEngine(dir)
			if openErr != nil {
				t.Fatalf("Open observed unowned checkpoint: %v", openErr)
			}
			if closeErr := reopened.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		})
	}
}

func TestCatalogCheckpointWriterRejectsImpossibleState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*catalogCheckpoint)
	}{
		{name: "zero generation", mutate: func(c *catalogCheckpoint) { c.Generation = 0 }},
		{name: "invalid geometry", mutate: func(c *catalogCheckpoint) { c.Segments[0].Bytes-- }},
		{name: "overflow-safe geometry", mutate: func(c *catalogCheckpoint) {
			c.Segments[0].Bytes = ^uint64(0)
			c.Segments[0].IndexOffset = ^uint64(0) - segmentFooterBytes + 1
		}},
		{name: "zero segment hash", mutate: func(c *catalogCheckpoint) { c.Segments[0].Hash = [32]byte{} }},
		{name: "final without segment", mutate: func(c *catalogCheckpoint) {
			c.Final = c.Segments[0]
			c.Segments = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checkpoint := validCatalogCheckpoint(fileID{11})
			test.mutate(&checkpoint)
			if _, err := writeCatalogCheckpoint(t.TempDir(), checkpoint, testAuthKey); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestCatalogCheckpointStreamsBaseSummariesRoundTrip(t *testing.T) {
	dir := t.TempDir()
	checkpoint := validCatalogCheckpoint(fileID{14})
	checkpoint.BaseSequence = 91
	groups := []checkpointGroupSummary{
		{GroupID: 3, Summary: sealedRunSummary{LastIndex: 8, LastTerm: 2, Hard: HardState{Term: 2, Commit: 8}, TruncateIndex: 4, TruncateTerm: 1}},
		{GroupID: 100, Summary: sealedRunSummary{LastIndex: 12, LastTerm: 3, Checkpoint: Checkpoint{ID: [16]byte{7}, Index: 10, Term: 2}, TruncateIndex: 10, TruncateTerm: 2}},
	}
	checkpoint.GroupIDs = []uint64{3, 100}
	checkpoint.GroupSummaries = map[uint64]sealedRunSummary{3: groups[0].Summary, 100: groups[1].Summary}
	digest, err := writeCatalogCheckpoint(dir, checkpoint, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	slot := metadataSlot{Generation: checkpoint.Generation, LogID: checkpoint.LogID, CatalogTail: checkpoint.Tail, CatalogHash: checkpoint.CatalogHash, CheckpointID: [16]byte(checkpoint.ID), CheckpointTail: checkpoint.Tail, CheckpointHash: digest, AnchorID: checkpoint.AnchorID, AnchorGeneration: checkpoint.AnchorGeneration, AnchorHash: checkpoint.AnchorHash}
	segments, base, _, _, _, err := readCatalogCheckpoint(dir, slot, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || base.Sequence != checkpoint.BaseSequence || !reflect.DeepEqual(base.Groups, groups) {
		t.Fatalf("segments=%d base=%+v", len(segments), base)
	}
}

func TestCatalogCheckpointRewritesPartialDeterministicTemp(t *testing.T) {
	for _, cut := range []int{1, catalogCheckpointHeaderBytes - 1, catalogCheckpointHeaderBytes + catalogCheckpointEntryBytes/2, catalogCheckpointHeaderBytes + catalogCheckpointEntryBytes + catalogCheckpointTrailerBytes - 1} {
		t.Run(string(rune(cut)), func(t *testing.T) {
			dir := t.TempDir()
			checkpoint := validCatalogCheckpoint(fileID{0x31, byte(cut)})
			tmp := filepath.Join(dir, "."+checkpointFileName(checkpoint.ID)+".tmp")
			if err := os.WriteFile(tmp, make([]byte, cut), 0o640); err != nil {
				t.Fatal(err)
			}
			if _, err := writeCatalogCheckpoint(dir, checkpoint, testAuthKey); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dir, checkpointFileName(checkpoint.ID))); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial temp remains: %v", err)
			}
		})
	}
}

func TestCatalogCheckpointTempRecoveryRejectsNamespaceSubstitution(t *testing.T) {
	dir := t.TempDir()
	checkpoint := validCatalogCheckpoint(fileID{0x41})
	tmp := filepath.Join(dir, "."+checkpointFileName(checkpoint.ID)+".tmp")
	backup := tmp + ".validated"
	if err := os.WriteFile(tmp, []byte{1}, 0o640); err != nil {
		t.Fatal(err)
	}
	checkpointBeforeTempCleanup = func(path string) {
		checkpointBeforeTempCleanup = nil
		if err := os.Rename(path, backup); err != nil {
			t.Fatalf("rename partial temp: %v", err)
		}
		if err := os.WriteFile(path, []byte("substitute"), 0o640); err != nil {
			t.Fatalf("install substitute: %v", err)
		}
	}
	t.Cleanup(func() { checkpointBeforeTempCleanup = nil })
	if _, err := writeCatalogCheckpoint(dir, checkpoint, testAuthKey); !errors.Is(err, os.ErrExist) {
		t.Fatalf("checkpoint temp substitution accepted: %v", err)
	}
	if got, err := os.ReadFile(tmp); err != nil || string(got) != "substitute" {
		t.Fatalf("substitute removed or changed: %q %v", got, err)
	}
}

func TestRepeatedCheckpointENOSPCLeavesDirectoryBounded(t *testing.T) {
	dir := t.TempDir()
	saved := checkpointWriteFullAt
	checkpointWriteFullAt = func(*os.File, []byte, int64) error { return syscall.ENOSPC }
	t.Cleanup(func() { checkpointWriteFullAt = saved })
	for i := 0; i < 32; i++ {
		checkpoint := validCatalogCheckpoint(fileID{byte(i + 1), 12})
		if _, err := writeCatalogCheckpoint(dir, checkpoint, testAuthKey); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("attempt %d error=%v", i, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("attempt %d leaked %d entries", i, len(entries))
		}
	}
}

func TestNilCheckpointHookDoesNotSuppressOrdinaryFailureCleanup(t *testing.T) {
	tests := []struct {
		name   string
		inject func()
	}{
		{name: "sync", inject: func() {
			checkpointFileSync = func(*os.File) error { return syscall.ENOSPC }
		}},
		{name: "rename", inject: func() {
			checkpointRename = func(string, string) error { return syscall.ENOSPC }
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			savedHook, savedSync, savedRename := checkpointPublishHook, checkpointFileSync, checkpointRename
			checkpointPublishHook = func(checkpointPublishPhase) error { return nil }
			test.inject()
			t.Cleanup(func() {
				checkpointPublishHook, checkpointFileSync, checkpointRename = savedHook, savedSync, savedRename
			})
			if _, err := writeCatalogCheckpoint(dir, validCatalogCheckpoint(fileID{17}), testAuthKey); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("error=%v", err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("ordinary failure leaked %d entries", len(entries))
			}
		})
	}
}

func TestOpenRejectsDuplicateCheckpointFileIDAtExactSegmentIdentity(t *testing.T) {
	dir := t.TempDir()
	engine := newEngineAt(t, dir, 1)
	for index := uint64(1); index <= 2; index++ {
		wave := Wave{ID: waveID(index), Batches: []ReadyBatch{{GroupID: 1, Entries: []Entry{{Index: index, Term: 1}}}}}
		if err := engine.PersistWave(wave); err != nil {
			t.Fatal(err)
		}
		if err := engine.Rotate(nil); err != nil {
			t.Fatal(err)
		}
		if err := engine.WaitSeal(); err != nil {
			t.Fatal(err)
		}
	}
	segments := append([]SegmentMeta(nil), engine.log.state.Segments...)
	if len(segments) != 2 {
		t.Fatalf("segments=%d", len(segments))
	}
	segments[1].FileID = segments[0].FileID
	id := fileID{9, 9}
	digest, err := writeCatalogCheckpoint(dir, catalogCheckpoint{ID: id, LogID: testLogID, Generation: engine.log.metadata.slot.Generation, Tail: engine.log.metadata.slot.CatalogTail, CatalogHash: engine.log.metadata.slot.CatalogHash, Segments: segments}, testAuthKey)
	if err != nil {
		t.Fatal(err)
	}
	next := engine.log.metadata.slot
	next.Generation++
	next.CheckpointID, next.CheckpointTail, next.CheckpointHash = [16]byte(id), next.CatalogTail, digest
	if err = engine.log.metadata.publish(next, nil); err != nil {
		t.Fatal(err)
	}
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, openErr := openTestEngine(dir); openErr == nil {
		_ = reopened.Close()
		t.Fatal("duplicate checkpoint FileID survived exact segment-header verification")
	}
}
