package raftmember

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

// TestRuntimeReadAuthorityConfiguresBeforeRecoveredReadyReplay exercises the
// restart cut that previously failed: a committed entry is durable while the
// SQL publication still ends at the preceding index, so the new RawNode owns a
// pending Ready as soon as Runtime construction returns. Authority quarantine
// must be installed at that point without consuming or discarding the Ready.
func TestRuntimeReadAuthorityConfiguresBeforeRecoveredReadyReplay(t *testing.T) {
	identity := testWALIdentity(186)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, 186, []uint64{identity.MemberID, peer})
	runtime := fixture.runtime
	drainRuntime(t, runtime, nil)

	status, err := runtime.Status()
	if err != nil {
		t.Fatal(err)
	}
	term := status.Term + 1
	if err := runtime.StepMessage(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &term,
		Commit: runtimeUint64Ptr(status.Commit),
	}); err != nil {
		t.Fatalf("establish follower leader: %v", err)
	}
	drainRuntime(t, runtime, nil)

	status, err = runtime.Status()
	if err != nil {
		t.Fatal(err)
	}
	previousIndex := status.Commit
	previousTerm, err := fixture.wal.Term(previousIndex)
	if err != nil {
		t.Fatalf("previous log term: %v", err)
	}
	sessionIndex := previousIndex + 1
	sessionTerm := status.Term
	sessionOpen := runtimeReplaySessionOpen(fixture.base, 1)
	if err := runtime.StepMessage(&pb.Message{
		Type: pb.MsgApp.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &sessionTerm,
		Index: runtimeUint64Ptr(previousIndex), LogTerm: runtimeUint64Ptr(previousTerm),
		Commit: runtimeUint64Ptr(sessionIndex),
		Entries: []*pb.Entry{{
			Type: pb.EntryNormal.Enum(), Index: runtimeUint64Ptr(sessionIndex),
			Term: runtimeUint64Ptr(sessionTerm), Data: sessionOpen,
		}},
	}); err != nil {
		t.Fatalf("append initialization entry: %v", err)
	}
	drainRuntime(t, runtime, nil)
	status, err = runtime.Status()
	if err != nil || status.Applied != sessionIndex || status.Commit != sessionIndex {
		t.Fatalf("initialized follower cuts = %+v, %v", status, err)
	}
	// Force the source SQL image through its authenticated artifact checkpoint
	// before copying it. This is the durable publication cut that lets the
	// reopened Runtime distinguish the suffix from the initialized prefix.
	_ = captureRuntimeReplayImage(t, fixture.apply, fixture.base.UserTable)

	previousIndex = status.Commit
	previousTerm, err = fixture.wal.Term(previousIndex)
	if err != nil {
		t.Fatalf("suffix predecessor term: %v", err)
	}
	entryIndex := previousIndex + 1
	entryTerm := status.Term
	suffixOpen := runtimeReplaySessionOpen(fixture.base, 2)
	if err := runtime.StepMessage(&pb.Message{
		Type: pb.MsgApp.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &entryTerm,
		Index: runtimeUint64Ptr(previousIndex), LogTerm: runtimeUint64Ptr(previousTerm),
		Commit: runtimeUint64Ptr(entryIndex),
		Entries: []*pb.Entry{{
			Type: pb.EntryNormal.Enum(), Index: runtimeUint64Ptr(entryIndex),
			Term: runtimeUint64Ptr(entryTerm), Data: suffixOpen,
		}},
	}); err != nil {
		t.Fatalf("append committed replay suffix: %v", err)
	}
	if got := fixture.apply.Applied(); got != previousIndex {
		t.Fatalf("source apply advanced before crash cut: %d, want %d", got, previousIndex)
	}

	// Stop immediately after the entry-bearing Ready crosses the WAL boundary.
	// No apply step is allowed to run before the SQL crash image is copied.
	crashSQLPath := filepath.Join(t.TempDir(), "restart.vdb")
	persisted := false
	for step := 0; step < 32; step++ {
		result, driveErr := runtime.DriveReady(new(ReadyWorkspace), nil, settleTestApplied)
		if driveErr != nil {
			t.Fatalf("DriveReady(crash cut) step %d: %v", step, driveErr)
		}
		if result.Kind == DrivePersisted {
			durableCommit, commitErr := fixture.wal.DurableCommit()
			if commitErr != nil {
				t.Fatal(commitErr)
			}
			if durableCommit == entryIndex {
				copyRuntimeReplayPath(t, fixture.sqlPath, crashSQLPath)
				copyRuntimeReplayPath(t, fixture.sqlPath+".tables", crashSQLPath+".tables")
				persisted = true
				break
			}
		}
		if result.Kind == DriveIdle {
			t.Fatalf("DriveReady reached idle before committed suffix persisted: %+v", result)
		}
	}
	if !persisted {
		t.Fatal("committed replay suffix did not cross a durable Ready boundary")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("close source runtime at crash cut: %v", err)
	}

	recovery := openRuntimeReplayHandles(t, crashSQLPath, fixture)
	restarted := recovery.adopt(t)
	hasReady, err := restarted.node.HasReady()
	if err != nil || !hasReady {
		t.Fatalf("restarted HasReady = %t, %v; want committed replay Ready", hasReady, err)
	}
	status, err = restarted.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Commit != entryIndex || status.Applied != previousIndex {
		t.Fatalf("restarted cuts = %+v, want commit=%d applied=%d", status, entryIndex, previousIndex)
	}

	clock := &readAuthorityRuntimeClock{}
	policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID, peer}, time.Second)
	checked := raftauthority.NewCheckedClock(clock)
	if err := restarted.ConfigureReadAuthority(ReadAuthorityOptions{
		Policy: policy, Clock: checked,
	}); err != nil {
		t.Fatalf("ConfigureReadAuthority with recovered Ready: %v", err)
	}
	if hasReady, err := restarted.node.HasReady(); err != nil || !hasReady {
		t.Fatalf("ConfigureReadAuthority consumed recovered Ready: hasReady=%t err=%v", hasReady, err)
	}

	// Ready replay is ordinary storage/application work and remains legal under
	// quarantine. The election gate is exercised only after that replay is
	// fully published and the Runtime has returned to its input window.
	drainRuntime(t, restarted, nil)
	status, err = restarted.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Applied != entryIndex || status.Commit != entryIndex {
		t.Fatalf("replayed cuts = %+v, want applied/commit=%d", status, entryIndex)
	}
	// A RawNode restart does not retain the transient leader hint in its
	// durable HardState. Admit one ordinary heartbeat under quarantine so the
	// election-message checks below use a real current-term leader identity.
	if err := restarted.StepMessage(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: runtimeUint64Ptr(status.Term),
		Commit: runtimeUint64Ptr(status.Commit),
	}); err != nil {
		t.Fatalf("restore current-term leader hint: %v", err)
	}
	drainRuntime(t, restarted, nil)
	status, err = restarted.Status()
	if err != nil || status.LeaderID != peer {
		t.Fatalf("leader hint after replay = %+v, %v; want %d", status, err, peer)
	}

	blocked := []struct {
		name string
		call func() error
	}{
		{name: "tick", call: restarted.Tick},
		{name: "campaign", call: restarted.Campaign},
		{name: "vote", call: func() error {
			return restarted.StepMessage(&pb.Message{
				Type: pb.MsgVote.Enum(), From: runtimeUint64Ptr(peer),
				To: runtimeUint64Ptr(identity.MemberID), Term: runtimeUint64Ptr(status.Term),
			})
		}},
		{name: "timeout-now", call: func() error {
			return restarted.StepMessage(&pb.Message{
				Type: pb.MsgTimeoutNow.Enum(), From: runtimeUint64Ptr(peer),
				To: runtimeUint64Ptr(identity.MemberID), Term: runtimeUint64Ptr(status.Term),
			})
		}},
	}
	for _, test := range blocked {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, ErrAuthorityElectionBlocked) {
				t.Fatalf("%s during restart quarantine = %v, want ErrAuthorityElectionBlocked", test.name, err)
			}
		})
	}
	if after, err := restarted.Status(); err != nil || after.Term != status.Term {
		t.Fatalf("blocked election edge changed term: before=%d after=%+v err=%v", status.Term, after, err)
	}

	quarantine, err := policy.QuarantineDuration()
	if err != nil {
		t.Fatal(err)
	}
	clock.now = quarantine
	if err := restarted.Tick(); err != nil {
		t.Fatalf("tick at restart quarantine deadline: %v", err)
	}
	drainRuntime(t, restarted, nil)
}

func TestRuntimeReadAuthorityLateFirstConfigureRetainsReadyBoundary(t *testing.T) {
	identity := testWALIdentity(187)
	fixture := newRuntimeFixture(t, 187, nil)
	runtime := fixture.runtime
	drainRuntime(t, runtime, nil)
	if err := runtime.Campaign(); err != nil {
		t.Fatal(err)
	}

	clock := &readAuthorityRuntimeClock{}
	policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID}, time.Second)
	if err := runtime.ConfigureReadAuthority(ReadAuthorityOptions{
		Policy: policy, Clock: raftauthority.NewCheckedClock(clock),
	}); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("late first ConfigureReadAuthority = %v, want ErrReadyPending", err)
	}
	if runtime.ReadAuthorityEnabled() {
		t.Fatal("late first ConfigureReadAuthority installed authority despite pending Ready")
	}
	drainRuntime(t, runtime, nil)
	if err := runtime.ConfigureReadAuthority(ReadAuthorityOptions{
		Policy: policy, Clock: raftauthority.NewCheckedClock(clock),
	}); err != nil {
		t.Fatalf("ConfigureReadAuthority after Ready drain: %v", err)
	}
}
