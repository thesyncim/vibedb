package raftmember

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	pb "go.etcd.io/raft/v3/raftpb"
)

// changedRosterRuntime persists a real membership publication and optionally
// closes the original Runtime. The close/reopen boundary is part of the
// startup test: Restore is not a way to install a retained policy on a Node
// that has already run.
func changedRosterRuntime(t *testing.T, seed byte, closeRuntime bool) (runtimeFixture, uint64) {
	t.Helper()
	identity := testWALIdentity(seed)
	fixture := newRuntimeFixture(t, seed, nil)
	runtime := fixture.runtime
	drainRuntime(t, runtime, nil)
	if err := runtime.Campaign(); err != nil {
		t.Fatalf("initial Campaign: %v", err)
	}
	drainRuntime(t, runtime, nil)

	added := identity.MemberID + 1
	digest := MembershipTransitionDigest(runtime.identity.Group,
		[16]byte{seed}, 2, 3, identity.MemberID, added)
	if err := runtime.ProposeConfChange(&pb.ConfChange{
		Type: pb.ConfChangeAddNode.Enum(), NodeId: runtimeUint64Ptr(added),
		Context: append([]byte(nil), digest[:]...),
	}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	drainRuntime(t, runtime, nil)
	publication, err := runtime.Publication()
	if err != nil {
		t.Fatalf("changed publication: %v", err)
	}
	if publication.ReplicaSetVersion == 0 ||
		!slices.Equal(publication.ConfState.GetVoters(), []uint64{identity.MemberID, added}) {
		t.Fatalf("changed publication = %+v", publication)
	}
	if closeRuntime {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close source runtime: %v", err)
		}
	}
	return fixture, added
}

func TestRuntimeRestoreReadAuthorityAfterDurableRosterChange(t *testing.T) {
	identity := testWALIdentity(189)
	fixture, added := changedRosterRuntime(t, 189, true)
	recovery := openRuntimeReplayHandles(t, fixture.sqlPath, fixture)
	restarted := recovery.adopt(t)
	publication, err := restarted.Publication()
	if err != nil {
		t.Fatalf("restarted publication: %v", err)
	}
	if !slices.Equal(publication.ConfState.GetVoters(), []uint64{identity.MemberID, added}) {
		t.Fatalf("restarted publication = %+v", publication)
	}

	policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID}, time.Second)
	clockSource := &readAuthorityRuntimeClock{}
	options := ReadAuthorityOptions{
		Policy: policy, Clock: raftauthority.NewCheckedClock(clockSource),
	}
	if err := restarted.ConfigureReadAuthority(options); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
		t.Fatalf("strict ConfigureReadAuthority = %v, want roster mismatch", err)
	}
	if restarted.ReadAuthorityEnabled() {
		t.Fatal("strict roster mismatch installed authority")
	}
	if err := restarted.RestoreReadAuthority(options); err != nil {
		t.Fatalf("RestoreReadAuthority after durable restart: %v", err)
	}
	if !restarted.ReadAuthorityEnabled() {
		t.Fatal("restored authority is not enabled")
	}

	if err := restarted.Campaign(); !errors.Is(err, ErrAuthorityElectionBlocked) {
		t.Fatalf("Campaign during restore quarantine = %v, want ErrAuthorityElectionBlocked", err)
	}
	if _, err := restarted.ReadAuthorityObservation(); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
		t.Fatalf("restored observation = %v, want roster mismatch", err)
	}
	if _, err := restarted.ReadAuthorityToken(); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
		t.Fatalf("restored token = %v, want roster mismatch", err)
	}
	if err := restarted.StartReadAuthorityRound(); err == nil ||
		(!errors.Is(err, ErrAuthorityConfigurationMismatch) && !errors.Is(err, raftmodel.ErrNotLeader)) {
		t.Fatalf("restored round = %v, want a denied authority request", err)
	}

	quarantine, err := policy.QuarantineDuration()
	if err != nil {
		t.Fatalf("QuarantineDuration: %v", err)
	}
	clockSource.now = quarantine
	if err := restarted.Tick(); err != nil {
		t.Fatalf("ordinary Tick after restore quarantine: %v", err)
	}
	drainRuntime(t, restarted, nil)
	// The changed roster still has no local peer process in this unit fixture,
	// so a post-quarantine campaign may remain an ordinary no-quorum attempt.
	// It must nevertheless reach Raft instead of the authority gate.
	if err := restarted.Campaign(); err != nil {
		t.Fatalf("Campaign after restore quarantine = %v, want ordinary Raft progress", err)
	}
}

func TestRuntimeRestoreReadAuthorityForRemovedOriginalVoter(t *testing.T) {
	identity := testWALIdentity(193)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, 193, []uint64{identity.MemberID, peer})
	runtime := fixture.runtime
	drainRuntime(t, runtime, nil)
	status, err := runtime.Status()
	if err != nil {
		t.Fatalf("initial Status: %v", err)
	}
	previousIndex := status.Commit
	previousTerm, err := fixture.wal.Term(previousIndex)
	if err != nil {
		t.Fatalf("previous log term: %v", err)
	}
	entryIndex := previousIndex + 1
	entryTerm := status.Term
	digest := MembershipTransitionDigest(runtime.identity.Group,
		[16]byte{193}, 2, 3, identity.MemberID, peer)
	_, encoded, err := pb.MarshalConfChange(&pb.ConfChange{
		Type: pb.ConfChangeRemoveNode.Enum(), NodeId: runtimeUint64Ptr(identity.MemberID),
		Context: append([]byte(nil), digest[:]...),
	})
	if err != nil {
		t.Fatalf("encode removal: %v", err)
	}
	if err := runtime.StepMessage(&pb.Message{
		Type: pb.MsgApp.Enum(), From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(identity.MemberID),
		Term: runtimeUint64Ptr(entryTerm), Index: runtimeUint64Ptr(previousIndex),
		LogTerm: runtimeUint64Ptr(previousTerm), Commit: runtimeUint64Ptr(entryIndex),
		Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Index: runtimeUint64Ptr(entryIndex),
			Term: runtimeUint64Ptr(entryTerm), Data: encoded}},
	}); err != nil {
		t.Fatalf("deliver committed removal: %v", err)
	}
	drainRuntime(t, runtime, nil)
	publication, err := runtime.Publication()
	if err != nil || !slices.Equal(publication.ConfState.GetVoters(), []uint64{peer}) {
		t.Fatalf("removed-voter publication = %+v, %v", publication, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("close removed-voter runtime: %v", err)
	}
	recovery := openRuntimeReplayHandles(t, fixture.sqlPath, fixture)
	restarted := recovery.adopt(t)
	policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID, peer}, time.Second)
	if err := restarted.RestoreReadAuthority(ReadAuthorityOptions{
		Policy: policy, Clock: raftauthority.NewCheckedClock(&readAuthorityRuntimeClock{}),
	}); err != nil {
		t.Fatalf("RestoreReadAuthority for removed original voter: %v", err)
	}
	if !restarted.ReadAuthorityEnabled() {
		t.Fatal("removed original voter did not install retained authority")
	}
	if _, err := restarted.ReadAuthorityToken(); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
		t.Fatalf("removed-voter token = %v, want roster mismatch", err)
	}
}

func TestRuntimeRestoreReadAuthorityRejectsUsedNode(t *testing.T) {
	identity := testWALIdentity(190)
	fixture, _ := changedRosterRuntime(t, 190, false)
	runtime := fixture.runtime
	// Drain the ordinary post-campaign Ready so the rejection cannot rely on a
	// transient phase or pending counters. The construction-era proof is still
	// closed and must remain closed for the lifetime of this Node.
	if err := runtime.Tick(); err != nil {
		t.Fatalf("Tick before used-node restore: %v", err)
	}
	drainRuntime(t, runtime, nil)
	policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID}, time.Second)
	if err := runtime.RestoreReadAuthority(ReadAuthorityOptions{
		Policy: policy, Clock: raftauthority.NewCheckedClock(&readAuthorityRuntimeClock{}),
	}); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("used drained Node RestoreReadAuthority = %v, want ErrReadyPending", err)
	}
	if runtime.ReadAuthorityEnabled() {
		t.Fatal("used Node restore installed authority")
	}
}

func TestRuntimeRestoreReadAuthorityRejectsUnenrolledLocalAndExactRoster(t *testing.T) {
	t.Run("exact-roster", func(t *testing.T) {
		identity := testWALIdentity(191)
		fixture := newRuntimeFixture(t, 191, nil)
		drainRuntime(t, fixture.runtime, nil)
		policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID}, time.Second)
		if err := fixture.runtime.RestoreReadAuthority(ReadAuthorityOptions{
			Policy: policy, Clock: raftauthority.NewCheckedClock(&readAuthorityRuntimeClock{}),
		}); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
			t.Fatalf("exact-roster RestoreReadAuthority = %v, want roster mismatch", err)
		}
		if fixture.runtime.ReadAuthorityEnabled() {
			t.Fatal("exact-roster restore installed authority")
		}
	})

	t.Run("local-not-in-original-policy", func(t *testing.T) {
		fixture, added := changedRosterRuntime(t, 192, false)
		policy := readAuthorityRuntimePolicy([]uint64{added}, time.Second)
		if err := fixture.runtime.RestoreReadAuthority(ReadAuthorityOptions{
			Policy: policy, Clock: raftauthority.NewCheckedClock(&readAuthorityRuntimeClock{}),
		}); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
			t.Fatalf("unenrolled local RestoreReadAuthority = %v, want roster mismatch", err)
		}
		if fixture.runtime.ReadAuthorityEnabled() {
			t.Fatal("unenrolled local restore installed authority")
		}
	})
}
