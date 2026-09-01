package seglog

import (
	"errors"
	"os"
	"path/filepath"
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

func TestCatalogCheckpointRejectsDuplicateSealedFileOwnership(t *testing.T) {
	id := fileID{8}
	firstHash, secondHash := [32]byte{1}, [32]byte{2}
	segments := []SegmentMeta{
		{ID: 1, Generation: 1, Bytes: segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes, IndexOffset: segmentHeaderBytes, IndexBytes: sealedIndexHeaderBytes, Hash: firstHash, FileID: id, State: SegmentSealed},
		{ID: 2, Generation: 2, Bytes: segmentHeaderBytes + sealedIndexHeaderBytes + segmentFooterBytes, IndexOffset: segmentHeaderBytes, IndexBytes: sealedIndexHeaderBytes, PreviousHash: firstHash, Hash: secondHash, FileID: id, State: SegmentSealed},
	}
	_, err := writeCatalogCheckpoint(t.TempDir(), catalogCheckpoint{ID: fileID{9}, LogID: testLogID, Generation: 2, Tail: metadataCatalogStart + 2*catalogRecordBytes, CatalogHash: [32]byte{3}, Segments: segments}, testAuthKey)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate sealed FileID=%v", err)
	}
}
