package raftmember

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"google.golang.org/protobuf/proto"
)

type runtimeReplayImage struct {
	state       []byte
	publication raftmodel.Publication
	system      []byte
	user        []byte
}

type runtimeReplayHandles struct {
	runtime  *Runtime
	wal      *raftstore.Store
	database *sqldriver.Database
	apply    *sqldriver.ReplicatedApply
}

func TestRuntimeReplaysCommittedWALSuffixFromCheckpointCertificate(t *testing.T) {
	fixture := newRuntimeFixture(t, 247, nil)
	drainRuntime(t, fixture.runtime, nil)
	if err := fixture.runtime.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)

	epoch := openRuntimeTestSession(t, fixture.runtime, fixture.apply, fixture.base)
	key, ok := orderedkey.AppendJSONString(nil, []byte(`"counter"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode replay key")
	}
	prefixCommand := testApplyCommand(
		fixture.base, epoch, 2, key, []byte(`{"id":"counter","value":0}`),
	)
	if err := fixture.runtime.Propose(prefixCommand); err != nil {
		t.Fatal(err)
	}
	drainRuntime(t, fixture.runtime, nil)
	prefixPublication, err := fixture.runtime.Publication()
	if err != nil {
		t.Fatal(err)
	}
	if prefixPublication.Applied <= 1 ||
		fixture.apply.CheckpointAppliedIndex() >= prefixPublication.Applied {
		t.Fatalf(
			"prefix command did not leave a visible uncertified cut: publication %d checkpoint %d",
			prefixPublication.Applied, fixture.apply.CheckpointAppliedIndex(),
		)
	}
	prefixCompletion := captureRuntimeReplayCompletion(
		t, fixture.apply, prefixCommand, replicatedstate.ResultApplied,
	)
	prefixImage := captureRuntimeReplayImage(t, fixture.apply, fixture.base.UserTable)
	if fixture.apply.CheckpointAppliedIndex() != prefixPublication.Applied {
		t.Fatalf(
			"prefix artifact cut checkpoint = %d, want %d",
			fixture.apply.CheckpointAppliedIndex(), prefixPublication.Applied,
		)
	}

	// The artifact cut establishes the exact durable SQL prefix. Admit several
	// user/session transitions into one uncaptured Ready and one WAL record. The
	// crash cut is the first micro-step where the WAL commit covers the suffix,
	// before Runtime can apply any of its committed entries.
	suffixCommands := [][]byte{
		testApplyCommand(fixture.base, epoch, 3, key, []byte(`{"id":"counter","value":1}`)),
		runtimeReplaySessionOpen(fixture.base, 4),
		runtimeReplaySessionOpen(fixture.base, 5),
	}
	suffixResultCodes := []uint32{
		replicatedstate.ResultApplied,
		replicatedstate.ResultSessionOpened,
		replicatedstate.ResultSessionOpened,
	}
	wantFinalApplied := prefixPublication.Applied + uint64(len(suffixCommands))
	crashDir := t.TempDir()
	crashSQLPath := filepath.Join(crashDir, "crash.vdb")
	lastBefore, err := fixture.wal.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	syncsBefore := fixture.wal.SyncCount()
	for ordinal, command := range suffixCommands {
		if err := fixture.runtime.Propose(command); err != nil {
			t.Fatalf("Propose(suffix %d) = %v", ordinal, err)
		}
	}
	// A single-node raft may expose the HardState commit in the same Ready as
	// the new entries or in a later Ready after those entries are durable. Audit
	// the entry-bearing persistence boundary itself so a legitimate later
	// HardState-only record cannot be mistaken for a split proposal batch.
	suffixPersisted := false
	commitPersisted := false
	var crashHardState [3]uint64
	var crashSyncCount uint64
	var crashRemainingBytes int64
	var crashWALLast uint64
	var crashIncarnation uint64
	observedLast := lastBefore
	for step := 0; step < 10_000; step++ {
		result, driveErr := fixture.runtime.DriveReady(
			new(ReadyWorkspace), nil, settleTestApplied,
		)
		if driveErr != nil {
			t.Fatalf("DriveReady(suffix) step %d: %v", step, driveErr)
		}
		if result.Kind == DrivePersisted {
			persistedLast, lastErr := fixture.wal.LastIndex()
			if lastErr != nil {
				t.Fatal(lastErr)
			}
			if persistedLast < observedLast {
				t.Fatalf("suffix WAL last index regressed: %d -> %d", observedLast, persistedLast)
			}
			if persistedLast > observedLast {
				if suffixPersisted {
					t.Fatalf("suffix entries crossed multiple persisted Ready records")
				}
				suffixPersisted = true
				observedLast = persistedLast
				if persistedLast-lastBefore != uint64(len(suffixCommands)) {
					t.Fatalf(
						"entry-bearing suffix WAL range = %d..%d, want %d entries",
						lastBefore, persistedLast, len(suffixCommands),
					)
				}
				if syncs := fixture.wal.SyncCount() - syncsBefore; syncs != 1 {
					t.Fatalf("entry-bearing suffix WAL syncs = %d, want 2", syncs)
				}
			}
			hardState, _, stateErr := fixture.wal.InitialState()
			if stateErr != nil {
				t.Fatal(stateErr)
			}
			durableCommit, commitErr := fixture.wal.DurableCommit()
			if commitErr != nil {
				t.Fatal(commitErr)
			}
			if durableCommit > wantFinalApplied {
				t.Fatalf(
					"suffix WAL commit = %d, want at most %d",
					durableCommit, wantFinalApplied,
				)
			}
			if durableCommit == wantFinalApplied {
				if fixture.apply.Applied() != prefixPublication.Applied ||
					fixture.apply.CheckpointAppliedIndex() != prefixPublication.Applied {
					t.Fatalf(
						"commit-persisted crash cut = applied %d checkpoint %d, want %d",
						fixture.apply.Applied(), fixture.apply.CheckpointAppliedIndex(),
						prefixPublication.Applied,
					)
				}
				copyRuntimeReplayPath(t, fixture.sqlPath, crashSQLPath)
				copyRuntimeReplayPath(t, fixture.sqlPath+".tables", crashSQLPath+".tables")
				crashHardState = [3]uint64{
					hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit(),
				}
				crashSyncCount = fixture.wal.SyncCount()
				crashRemainingBytes = fixture.wal.RemainingBytes()
				crashWALLast = persistedLast
				crashIncarnation = fixture.wal.CurrentIncarnation()
				commitPersisted = true
				break
			}
		}
		if result.Kind == DriveIdle {
			if suffixPersisted {
				// This RawNode schedule exposed the commit only after the entries
				// were durable. Commit-only Ready notifications intentionally stay
				// volatile and are folded by the SQL publication, so there is no
				// distinct WAL crash cut for this test to replay.
				t.Skip("single-node schedule exposed only the publication-backed commit cut")
			}
			t.Fatal("DriveReady(suffix) became idle before persisting the commit")
		}
		if step == 9_999 {
			t.Fatal("DriveReady(suffix) did not become idle")
		}
	}
	if !suffixPersisted {
		t.Fatal("suffix entries were not persisted")
	}
	if !commitPersisted {
		// Commit-only Ready notifications are deliberately folded by the durable
		// SQL publication instead of consuming another WAL record. This fixture
		// needs the distinct entry+commit crash cut; some valid RawNode schedules
		// do not expose it.
		t.Skip("single-node schedule exposed only the publication-backed commit cut")
	}
	lastAfter, err := fixture.wal.LastIndex()
	if err != nil || lastAfter-lastBefore != uint64(len(suffixCommands)) {
		t.Fatalf("batched suffix WAL range = %d..%d, %v", lastBefore, lastAfter, err)
	}
	// Resume the live source only after the old-prefix SQL crash image is
	// detached. Storage geometry may legitimately trigger a pressure checkpoint
	// while applying this suffix; that cannot alter the copied crash cut.
	drainRuntime(t, fixture.runtime, nil)
	finalPublication, err := fixture.runtime.Publication()
	if err != nil {
		t.Fatal(err)
	}
	if finalPublication.Applied != wantFinalApplied {
		t.Fatalf("visible suffix applied = %d, want %d",
			finalPublication.Applied, wantFinalApplied)
	}
	sourceStatus, err := fixture.runtime.Status()
	if err != nil || sourceStatus.Applied != finalPublication.Applied ||
		sourceStatus.Commit != finalPublication.Applied ||
		sourceStatus.CheckpointApplied < prefixPublication.Applied ||
		sourceStatus.CheckpointApplied > finalPublication.Applied {
		t.Fatalf("source runtime cuts = %+v, %v", sourceStatus, err)
	}
	retention, err := fixture.runtime.WALRetentionInput()
	if err != nil || retention != sourceStatus.CheckpointApplied {
		t.Fatalf(
			"source WAL retention cut = %d, want %d: %v",
			retention, sourceStatus.CheckpointApplied, err,
		)
	}
	hardState, _, err := fixture.wal.InitialState()
	lastAfterDrain, lastErr := fixture.wal.LastIndex()
	if err != nil || lastErr != nil ||
		[3]uint64{hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()} != crashHardState ||
		fixture.wal.SyncCount() != crashSyncCount ||
		fixture.wal.RemainingBytes() != crashRemainingBytes ||
		lastAfterDrain != crashWALLast ||
		fixture.wal.CurrentIncarnation() != crashIncarnation {
		t.Fatalf(
			"source WAL changed after crash cut: hard=%d/%d/%d sync=%d remaining=%d last=%d incarnation=%d errors=%v/%v",
			hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit(),
			fixture.wal.SyncCount(), fixture.wal.RemainingBytes(), lastAfterDrain,
			fixture.wal.CurrentIncarnation(), err, lastErr,
		)
	}

	// Capture the exact expected logical result only after the crash image is
	// detached. A full artifact cut may certify the live source as an intentional
	// side effect, but it cannot alter the old-certificate image.
	suffixCompletions := make([][]byte, len(suffixCommands))
	for ordinal, command := range suffixCommands {
		suffixCompletions[ordinal] = captureRuntimeReplayCompletion(
			t, fixture.apply, command, suffixResultCodes[ordinal],
		)
	}
	finalImage := captureRuntimeReplayImage(t, fixture.apply, fixture.base.UserTable)
	if err := fixture.runtime.Close(); err != nil {
		t.Fatal(err)
	}

	recovery := openRuntimeReplayHandles(t, crashSQLPath, fixture)

	// The frozen pre-apply SQL image must open at its exact certified prefix
	// before Runtime construction. The unchanged coexisting WAL commits the suffix.
	if recovery.apply.Applied() != prefixPublication.Applied ||
		recovery.apply.CheckpointAppliedIndex() != prefixPublication.Applied {
		t.Fatalf(
			"crash recovery cuts = applied %d checkpoint %d, want %d",
			recovery.apply.Applied(), recovery.apply.CheckpointAppliedIndex(),
			prefixPublication.Applied,
		)
	}
	assertRuntimeReplayImage(t,
		captureRuntimeReplayImage(t, recovery.apply, fixture.base.UserTable), prefixImage,
	)
	assertRuntimeReplayBytes(
		t, "recovered prefix completion",
		captureRuntimeReplayCompletion(
			t, recovery.apply, prefixCommand, replicatedstate.ResultApplied,
		), prefixCompletion,
	)
	for _, command := range suffixCommands {
		assertRuntimeReplayCompletionMissing(t, recovery.apply, command)
	}
	hardState, _, err = recovery.wal.InitialState()
	if err != nil || hardState.GetCommit() != finalPublication.Applied {
		t.Fatalf("recovery WAL commit = %d, %v", hardState.GetCommit(), err)
	}

	recoveryRuntime := recovery.adopt(t)
	assertRuntimeReplayStatus(t, recoveryRuntime,
		prefixPublication.Applied, finalPublication.Applied, prefixPublication.Applied)
	assertRuntimeReplayPublication(t, recoveryRuntime, prefixPublication)

	drainRuntime(t, recoveryRuntime, nil)
	afterReplay, err := recoveryRuntime.Status()
	if err != nil || afterReplay.Applied != finalPublication.Applied ||
		afterReplay.Commit != finalPublication.Applied ||
		afterReplay.CheckpointApplied < prefixPublication.Applied ||
		afterReplay.CheckpointApplied > finalPublication.Applied {
		t.Fatalf("post-replay runtime cuts = %+v, %v", afterReplay, err)
	}
	assertRuntimeReplayPublication(t, recoveryRuntime, finalPublication)
	retention, err = recoveryRuntime.WALRetentionInput()
	if err != nil || retention != afterReplay.CheckpointApplied {
		t.Fatalf(
			"replayed WAL retention cut = %d, want %d: %v",
			retention, afterReplay.CheckpointApplied, err,
		)
	}

	// A second drain must be a true idle observation. Full system/user image
	// equality also proves the replay did not advance the session ring twice.
	idle, err := recoveryRuntime.DriveReady(
		new(ReadyWorkspace), func(OutboundMessage) error { return nil }, settleTestApplied,
	)
	if err != nil || idle.Progressed() {
		t.Fatalf("post-replay DriveReady = %+v, %v", idle, err)
	}
	afterIdle, err := recoveryRuntime.Status()
	if err != nil || afterIdle != afterReplay {
		t.Fatalf("post-replay status = %+v, %v; want %+v", afterIdle, err, afterReplay)
	}
	assertRuntimeReplayBytes(
		t, "replayed prefix completion",
		captureRuntimeReplayCompletion(
			t, recovery.apply, prefixCommand, replicatedstate.ResultApplied,
		), prefixCompletion,
	)
	for ordinal, command := range suffixCommands {
		assertRuntimeReplayBytes(
			t, "replayed suffix completion",
			captureRuntimeReplayCompletion(
				t, recovery.apply, command, suffixResultCodes[ordinal],
			), suffixCompletions[ordinal],
		)
	}
	// The exact-image audit comes last because it may advance the checkpoint.
	// It must not change apply/publication state, and every user and bounded
	// session byte must equal the pre-crash source image.
	assertRuntimeReplayImage(t,
		captureRuntimeReplayImage(t, recovery.apply, fixture.base.UserTable), finalImage,
	)
	postAudit, err := recoveryRuntime.Status()
	if err != nil || postAudit.Applied != finalPublication.Applied ||
		postAudit.Commit != finalPublication.Applied ||
		postAudit.CheckpointApplied < prefixPublication.Applied ||
		postAudit.CheckpointApplied > finalPublication.Applied {
		t.Fatalf("post-replay exact-image audit status = %+v, %v", postAudit, err)
	}
	assertRuntimeReplayPublication(t, recoveryRuntime, finalPublication)
}

func runtimeReplaySessionOpen(
	identity sqldriver.ReplicatedShardStoreIdentity,
	client byte,
) []byte {
	command := testApplyCommandValue(identity, 0, 1, nil, nil)
	command.Kind = replication.CommandSessionOpen
	command.ClientID = replication.ID128{client}
	command.Fingerprint = replication.Digest{client, 0xa5}
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Batches = nil
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		panic(err)
	}
	return encoded
}

func openRuntimeReplayHandles(
	t testing.TB,
	sqlPath string,
	fixture runtimeFixture,
) *runtimeReplayHandles {
	t.Helper()
	wal, err := raftstore.Open(
		fixture.walPath, fixture.walID, testTopologyRecoveryEpoch,
		fixture.walKey, fixture.options,
	)
	if err != nil {
		t.Fatalf("open replay WAL: %v", err)
	}
	database, apply, err := OpenBoundSQLWithApply(
		sqlPath, wal, testAuthorityProfile(), fixture.base, fixture.applyID,
	)
	if err != nil {
		t.Fatalf("open replay SQL: %v", errors.Join(err, wal.Close()))
	}
	handles := &runtimeReplayHandles{wal: wal, database: database, apply: apply}
	t.Cleanup(func() {
		if closeErr := handles.close(); closeErr != nil {
			t.Errorf("close replay handles: %v", closeErr)
		}
	})
	return handles
}

func (h *runtimeReplayHandles) adopt(t testing.TB) *Runtime {
	t.Helper()
	runtime, err := AdoptRuntime(h.wal, h.database, h.apply)
	if err != nil {
		if runtime != nil {
			h.runtime = runtime
			err = errors.Join(err, h.close())
		}
		t.Fatal(err)
	}
	h.runtime = runtime
	return runtime
}

func (h *runtimeReplayHandles) close() error {
	if h == nil {
		return nil
	}
	if h.runtime != nil {
		if err := h.runtime.Close(); err != nil {
			return err
		}
		h.runtime = nil
		h.apply = nil
		h.database = nil
		h.wal = nil
		return nil
	}
	if h.apply != nil {
		if err := h.apply.Close(); err != nil {
			return err
		}
		h.apply = nil
	}
	if h.database != nil {
		if err := h.database.Close(); err != nil {
			return err
		}
		h.database = nil
	}
	if h.wal != nil {
		if err := h.wal.Close(); err != nil {
			return err
		}
		h.wal = nil
	}
	return nil
}

func captureRuntimeReplayImage(
	t testing.TB,
	apply *sqldriver.ReplicatedApply,
	userTable string,
) runtimeReplayImage {
	t.Helper()
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatalf("capture apply image: %v", err)
	}
	image := runtimeReplayImage{publication: cut.Publication()}
	image.state, err = replicatedstate.AppendState(nil, cut.State())
	if err == nil {
		err = cut.RangeSystem(func(key, value []byte) error {
			image.system = appendRuntimeReplayRow(image.system, key, value)
			return nil
		})
	}
	if err == nil {
		user, ok := cut.Collection(userTable)
		if !ok || user == nil {
			err = replicatedstate.ErrInconsistentSnapshot
		} else {
			err = user.RangeRaw(func(key, value []byte) error {
				image.user = appendRuntimeReplayRow(image.user, key, value)
				return nil
			})
		}
	}
	err = errors.Join(err, cut.Close())
	if err != nil {
		t.Fatalf("read apply image: %v", err)
	}
	return image
}

func appendRuntimeReplayRow(dst, key, value []byte) []byte {
	var lengths [8]byte
	binary.LittleEndian.PutUint32(lengths[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(lengths[4:8], uint32(len(value)))
	dst = append(dst, lengths[:]...)
	dst = append(dst, key...)
	return append(dst, value...)
}

func assertRuntimeReplayImage(
	t testing.TB,
	got, want runtimeReplayImage,
) {
	t.Helper()
	if !bytes.Equal(got.state, want.state) {
		t.Fatal("replayed durable State envelope differs from the original image")
	}
	assertRuntimeReplayPublicationValue(t, got.publication, want.publication)
	if !bytes.Equal(got.system, want.system) || !bytes.Equal(got.user, want.user) {
		t.Fatal("replayed system/user rows differ from the original image")
	}
}

func assertRuntimeReplayPublication(
	t testing.TB,
	runtime *Runtime,
	want raftmodel.Publication,
) {
	t.Helper()
	got, err := runtime.Publication()
	if err != nil {
		t.Fatal(err)
	}
	assertRuntimeReplayPublicationValue(t, got, want)
}

func assertRuntimeReplayStatus(
	t testing.TB,
	runtime *Runtime,
	applied, commit, checkpoint uint64,
) RuntimeStatus {
	t.Helper()
	status, err := runtime.Status()
	if err != nil || status.Applied != applied || status.Commit != commit ||
		status.CheckpointApplied != checkpoint {
		t.Fatalf(
			"runtime cuts = applied %d commit %d checkpoint %d, want %d/%d/%d: %v",
			status.Applied, status.Commit, status.CheckpointApplied,
			applied, commit, checkpoint, err,
		)
	}
	return status
}

func assertRuntimeReplayPublicationValue(
	t testing.TB,
	got, want raftmodel.Publication,
) {
	t.Helper()
	if got.Applied != want.Applied ||
		got.DataChainDigest != want.DataChainDigest ||
		got.ReplicaSetVersion != want.ReplicaSetVersion ||
		!proto.Equal(got.ConfState, want.ConfState) {
		t.Fatalf("publication = %+v, want %+v", got, want)
	}
}

func captureRuntimeReplayCompletion(
	t testing.TB,
	apply *sqldriver.ReplicatedApply,
	command []byte,
	wantResultCode uint32,
) []byte {
	t.Helper()
	lookup, err := apply.LookupCompletion(command)
	if err != nil {
		t.Fatalf("lookup replay completion: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != wantResultCode ||
		completion.AppliedSequence != lookup.AppliedSequence {
		t.Fatalf("replay completion = %+v, lookup %+v, %v", completion, lookup, err)
	}
	return bytes.Clone(lookup.Bytes)
}

func assertRuntimeReplayBytes(
	t testing.TB,
	name string,
	got, want []byte,
) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s = %x, want %x", name, got, want)
	}
}

func assertRuntimeReplayCompletionMissing(
	t testing.TB,
	apply *sqldriver.ReplicatedApply,
	command []byte,
) {
	t.Helper()
	if _, err := apply.LookupCompletion(command); !errors.Is(
		err, replicatedstate.ErrCompletionNotFound,
	) {
		t.Fatalf("pre-apply suffix completion unexpectedly exists: %v", err)
	}
}

func copyRuntimeReplayPath(t testing.TB, source, destination string) {
	t.Helper()
	info, err := os.Lstat(source)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() {
		if err := os.Mkdir(destination, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			copyRuntimeReplayPath(
				t, filepath.Join(source, entry.Name()),
				filepath.Join(destination, entry.Name()),
			)
		}
		return
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("crash fixture %s is not a regular file", source)
	}
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.OpenFile(
		destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm(),
	)
	if err != nil {
		_ = input.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	copyErr = errors.Join(copyErr, output.Sync(), output.Close(), input.Close())
	if copyErr != nil {
		t.Fatal(copyErr)
	}
}
