package raftmember

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRuntimeLeaderTransferStaleGuardCannotCancelSuccessor(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 176)
	runtime := fixture.fixture.runtime

	first, err := runtime.PrepareLeaderTransfer(fixture.peer)
	if err != nil {
		t.Fatalf("first PrepareLeaderTransfer: %v", err)
	}
	if err := runtime.CancelLeaderTransfer(first); err != nil {
		t.Fatalf("first CancelLeaderTransfer: %v", err)
	}

	second, err := runtime.PrepareLeaderTransfer(fixture.peer)
	if err != nil {
		t.Fatalf("successor PrepareLeaderTransfer: %v", err)
	}
	if second.sequence == first.sequence {
		t.Fatalf("successor sequence = %d, first = %d", second.sequence, first.sequence)
	}
	if err := runtime.CancelLeaderTransfer(first); !errors.Is(err, ErrLeaderTransferGuard) {
		t.Fatalf("stale CancelLeaderTransfer: %v, want ErrLeaderTransferGuard", err)
	}
	if !runtime.LeaderTransferPending() {
		t.Fatal("stale cancellation released successor guard")
	}
	if err := runtime.CheckLeaderTransferReady(second); err != nil {
		t.Fatalf("successor guard after stale cancellation: %v", err)
	}
	if err := runtime.CancelLeaderTransfer(second); err != nil {
		t.Fatalf("successor CancelLeaderTransfer: %v", err)
	}
}

func TestRuntimeLeaderTransferCancelAllowsFreshAuthorityWithoutRevivingOldToken(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 177)
	runtime := fixture.fixture.runtime
	oldToken := startReadAuthorityRoundWithQuorum(t, fixture)

	guard, err := runtime.PrepareLeaderTransfer(fixture.peer)
	if err != nil {
		t.Fatalf("PrepareLeaderTransfer: %v", err)
	}
	if err := runtime.CancelLeaderTransfer(guard); err != nil {
		t.Fatalf("CancelLeaderTransfer: %v", err)
	}
	if runtime.LeaderTransferPending() {
		t.Fatal("cancelled transfer guard remains pending")
	}
	if err := runtime.ValidateReadAuthorityToken(oldToken); err == nil {
		t.Fatal("cancelled transfer revived the old authority token")
	}

	// PromiseBook permits a newer request only after its request identity moves
	// forward. Advance the deterministic owner clock by one tick so the new
	// request cannot be mistaken for the invalidated round.
	fixture.clock.now = oldToken.StartedAt + time.Nanosecond
	newToken := startReadAuthorityRoundWithQuorum(t, fixture)
	if newToken.Nonce <= oldToken.Nonce {
		t.Fatalf("fresh authority nonce = %d, old = %d", newToken.Nonce, oldToken.Nonce)
	}
	if newToken.Term != oldToken.Term || newToken.Holder != oldToken.Holder {
		t.Fatalf("fresh authority identity = %+v, old = %+v", newToken, oldToken)
	}
	if err := runtime.ValidateReadAuthorityToken(newToken); err != nil {
		t.Fatalf("fresh authority validation: %v", err)
	}
	if err := runtime.ValidateReadAuthorityToken(oldToken); err == nil {
		t.Fatal("old authority token was revived after a fresh round")
	}
}

func TestRuntimeLeaderTransferTimeoutAllowsFreshAuthorityInSameTerm(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 178)
	runtime := fixture.fixture.runtime
	before, err := runtime.Status()
	if err != nil {
		t.Fatal(err)
	}
	guard, err := runtime.PrepareLeaderTransfer(fixture.peer)
	if err != nil {
		t.Fatalf("PrepareLeaderTransfer: %v", err)
	}
	if err := runtime.CheckLeaderTransferReady(guard); err != nil {
		t.Fatalf("CheckLeaderTransferReady: %v", err)
	}
	if err := runtime.TransferLeader(guard); err != nil {
		t.Fatalf("TransferLeader: %v", err)
	}
	transferring, err := runtime.Status()
	if err != nil {
		t.Fatal(err)
	}
	if transferring.Term != before.Term || transferring.LeaderID != fixture.local ||
		transferring.RaftState != raft.StateLeader || transferring.LeadTransferee != fixture.peer {
		t.Fatalf("admitted transfer status = %+v", transferring)
	}

	var timeoutNowSeen bool
	drainRuntime(t, runtime, func(outbound OutboundMessage) error {
		if outbound.Message.GetType() == raftpb.MsgTimeoutNow {
			timeoutNowSeen = true
		}
		return nil
	})
	if !timeoutNowSeen {
		t.Fatal("admitted transfer produced no TimeoutNow")
	}

	var settled RuntimeStatus
	for tick := 0; tick < 2*raftmodel.ElectionTick+2; tick++ {
		if err := runtime.Tick(); err != nil {
			t.Fatalf("transfer timeout tick %d: %v", tick, err)
		}
		drainRuntime(t, runtime, nil)
		settled, err = runtime.Status()
		if err != nil {
			t.Fatal(err)
		}
		if settled.LeadTransferee == raft.None {
			break
		}
	}
	if settled.LeadTransferee != raft.None {
		t.Fatalf("RawNode transfer did not time out: %+v", settled)
	}
	if settled.Term != before.Term || settled.LeaderID != fixture.local ||
		settled.RaftState != raft.StateLeader {
		t.Fatalf("transfer timeout changed local leadership: before=%+v after=%+v", before, settled)
	}
	if runtime.LeaderTransferPending() {
		t.Fatal("transfer guard remained pending after RawNode cleared LeadTransferee")
	}

	fresh := startReadAuthorityRoundWithQuorum(t, fixture)
	if fresh.Term != before.Term || fresh.Holder != fixture.local {
		t.Fatalf("fresh authority crossed transfer timeout boundary: %+v", fresh)
	}
}

func TestRuntimeQuiesceReinstallDoesNotOrphanDrainingLeaderTransfer(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 179)
	runtime := fixture.fixture.runtime
	guard, err := runtime.PrepareLeaderTransfer(fixture.peer)
	if err != nil {
		t.Fatalf("PrepareLeaderTransfer: %v", err)
	}
	if !runtime.LeaderTransferPending() {
		t.Fatal("prepared transfer was not retained before quiesce")
	}
	if err := runtime.QuiesceSQLGeneration(); err != nil {
		t.Fatalf("QuiesceSQLGeneration: %v", err)
	}

	// Quiesce may either retain the exact pre-admission guard for its owner to
	// cancel, or fence it while releasing SQL. Both ordered outcomes are safe;
	// a retained Draining guard must remain cancellable without SQL handles.
	if runtime.LeaderTransferPending() {
		if err := runtime.CancelLeaderTransfer(guard); err != nil {
			t.Fatalf("CancelLeaderTransfer while SQL is quiesced: %v", err)
		}
	} else if err := runtime.CancelLeaderTransfer(guard); !errors.Is(err, ErrLeaderTransferGuard) {
		t.Fatalf("cancelled quiesce guard = %v, want stale guard error", err)
	}
	if runtime.LeaderTransferPending() {
		t.Fatal("quiesced Runtime retained an orphaned transfer guard")
	}

	replacementDatabase, replacementApply, err := OpenBoundSQLWithApply(
		fixture.fixture.sqlPath, fixture.fixture.wal, testAuthorityProfile(),
		fixture.fixture.base, fixture.fixture.applyID,
	)
	skipIfStrictAllocationUnsupported(t, "reopen quiesced SQL generation", err)
	if err != nil {
		t.Fatalf("reopen quiesced SQL generation: %v", err)
	}
	if err := runtime.InstallSQLGeneration(
		replacementDatabase, replacementApply, fixture.fixture.base, fixture.fixture.applyID,
	); err != nil {
		t.Fatalf("InstallSQLGeneration: %v", err)
	}

	fresh := startReadAuthorityRoundWithQuorum(t, fixture)
	if fresh.Holder != fixture.local || fresh.Term == 0 {
		t.Fatalf("fresh authority after SQL reinstall = %+v", fresh)
	}
}
