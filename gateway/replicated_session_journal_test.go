package gateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestNativeSessionJournalRestartsAndRetriesExactPendingCommand(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	binding, err := NativeSessionJournalBinding(route, "orders", "0000-ffff", []byte("controller"), 1,
		serviceauthz.CapabilityDataWrite)
	if err != nil {
		t.Fatal(err)
	}
	client := &nativeSessionClient{state: states["m2"], unknownMutationOnce: true}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	journalOptions := NativeSessionJournalOptions{
		Path: t.TempDir() + "/controller.session", ClientID: replication.ID128{0xa1},
		RetryHome: replication.RetryHome{0xb1}, MaxCommandBytes: 1 << 20, Binding: binding,
	}
	journal, err := OpenNativeSessionJournal(journalOptions)
	if err != nil {
		t.Fatal(err)
	}
	sessionOptions := NativeSessionOptions{
		Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("controller"), ClientID: journalOptions.ClientID,
		RetryHome: journalOptions.RetryHome, Resolver: BaseRelationResolver{Relation: 1},
		ProposalCapability: serviceauthz.CapabilityDataWrite,
		MaxRelationBatches: 1, MaxMutations: 2,
		InitialCommandBytes: 512, MaxCommandBytes: journalOptions.MaxCommandBytes, Journal: journal,
	}
	session, err := NewNativeSession(sessionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	_, err = session.Put(context.Background(), []byte{1}, []byte(`{"id":1}`))
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) || !session.Status().Pending {
		t.Fatalf("unknown mutation=%v status=%+v", err, session.Status())
	}
	wantCommand := session.PendingCommand()

	reopenedJournal, err := OpenNativeSessionJournal(journalOptions)
	if err != nil {
		t.Fatal(err)
	}
	sessionOptions.Journal = reopenedJournal
	restarted, err := NewNativeSession(sessionOptions)
	if err != nil || !restarted.Status().Pending ||
		!bytes.Equal(restarted.PendingCommand(), wantCommand) {
		t.Fatalf("restarted status=%+v exact=%v err=%v",
			restarted.Status(), bytes.Equal(restarted.PendingCommand(), wantCommand), err)
	}
	result, err := restarted.RetryPending(context.Background())
	if err != nil || result.Completion.ResultCode == 0 || restarted.Status().Pending ||
		!bytes.Equal(client.retriedCommand, wantCommand) {
		t.Fatalf("settled=%+v status=%+v exact=%v err=%v",
			result, restarted.Status(), bytes.Equal(client.retriedCommand, wantCommand), err)
	}

	finalJournal, err := OpenNativeSessionJournal(journalOptions)
	if err != nil {
		t.Fatal(err)
	}
	sessionOptions.Journal = finalJournal
	final, err := NewNativeSession(sessionOptions)
	if err != nil || final.Status().Pending || final.Status().NextSequence != 3 ||
		final.Status().AckThrough != 2 {
		t.Fatalf("final status=%+v err=%v", final.Status(), err)
	}
	for slot := 0; slot < 2; slot++ {
		info, statErr := os.Stat(finalJournal.slotPath(slot))
		if statErr != nil || info.Size() > int64(nativeSessionJournalHeader+len(wantCommand)) {
			t.Fatalf("slot %d size=%v err=%v", slot, info, statErr)
		}
	}
}

func TestNativeSessionJournalCodecRejectsEveryTornOrCorruptRecord(t *testing.T) {
	binding := replication.Digest{3}
	state := durableNativeSessionState{
		clientID: replication.ID128{1}, retryHome: replication.RetryHome{2},
		phase: nativeSessionActive, epoch: 7, nextSequence: 9, ackThrough: 8,
		leaseDeadline: 1000,
	}
	raw, err := appendNativeSessionJournalRecord(nil, 3, binding, state, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if generation, openedBinding, opened, openErr := openNativeSessionJournalRecord(raw, 1<<20); openErr != nil ||
		generation != 3 || opened.clientID != state.clientID || opened.nextSequence != 9 {
		t.Fatalf("opened generation=%d binding=%x state=%+v err=%v", generation, openedBinding, opened, openErr)
	}
	for cut := 0; cut < len(raw); cut++ {
		if _, _, _, err = openNativeSessionJournalRecord(raw[:cut], 1<<20); !errors.Is(err, ErrNativeSessionJournal) {
			t.Fatalf("cut %d error=%v", cut, err)
		}
	}
	for _, offset := range []int{0, 4, 6, 8, 40, 112, 116, len(raw) - 1} {
		corrupt := append([]byte(nil), raw...)
		corrupt[offset] ^= 0x80
		if _, _, _, err = openNativeSessionJournalRecord(corrupt, 1<<20); !errors.Is(err, ErrNativeSessionJournal) {
			t.Fatalf("corruption %d error=%v", offset, err)
		}
	}
}

func TestNativeSessionJournalKeepsExactPendingWhenCompletionSyncFails(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	binding, err := NativeSessionJournalBinding(route, "orders", "0000-ffff", []byte("controller"), 1,
		serviceauthz.CapabilityDataWrite)
	if err != nil {
		t.Fatal(err)
	}
	client := &nativeSessionClient{state: states["m2"], unknownMutationOnce: true}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	options := NativeSessionJournalOptions{
		Path: t.TempDir() + "/controller.session", ClientID: replication.ID128{0xc1},
		RetryHome: replication.RetryHome{0xd1}, MaxCommandBytes: 1 << 20, Binding: binding,
	}
	journal, err := OpenNativeSessionJournal(options)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("controller"), ClientID: options.ClientID, RetryHome: options.RetryHome,
		Resolver: BaseRelationResolver{Relation: 1}, MaxRelationBatches: 1, MaxMutations: 2,
		ProposalCapability:  serviceauthz.CapabilityDataWrite,
		InitialCommandBytes: 512, MaxCommandBytes: options.MaxCommandBytes, Journal: journal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	if _, err = session.Put(context.Background(), []byte{1}, []byte(`{"id":1}`)); !errors.Is(err, raftservice.ErrOutcomeUnknown) {
		t.Fatal(err)
	}
	want := session.PendingCommand()
	nextSlot := 0
	if journal.active == 0 {
		nextSlot = 1
	}
	blocked := journal.slotPath(nextSlot)
	if err = os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = session.RetryPending(context.Background()); !errors.Is(err, raftservice.ErrOutcomeUnknown) ||
		!session.Status().Pending || !bytes.Equal(session.PendingCommand(), want) {
		t.Fatalf("failed completion sync err=%v status=%+v exact=%v",
			err, session.Status(), bytes.Equal(session.PendingCommand(), want))
	}
	if err = os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if _, err = session.RetryPending(context.Background()); err != nil || session.Status().Pending {
		t.Fatalf("completion retry err=%v status=%+v", err, session.Status())
	}
}

func TestNativeSessionJournalFallsBackFromTornNewestSlot(t *testing.T) {
	directory := t.TempDir()
	options := NativeSessionJournalOptions{
		Path: directory + "/controller.session", ClientID: replication.ID128{0xe1},
		RetryHome: replication.RetryHome{0xf1}, MaxCommandBytes: 1 << 20,
		Binding: replication.Digest{0xa5},
	}
	journal, err := OpenNativeSessionJournal(options)
	if err != nil {
		t.Fatal(err)
	}
	active := durableNativeSessionState{
		clientID: options.ClientID, retryHome: options.RetryHome,
		phase: nativeSessionActive, epoch: 7, nextSequence: 2, ackThrough: 1,
		leaseDeadline: 1000,
	}
	if err = journal.store(active); err != nil {
		t.Fatal(err)
	}
	newest := journal.slotPath(journal.active)
	raw, err := os.ReadFile(newest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(newest, raw[:len(raw)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenNativeSessionJournal(options)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.load()
	if err != nil || state.phase != nativeSessionNew || state.nextSequence != 1 {
		t.Fatalf("fallback state=%+v err=%v", state, err)
	}
}

func TestNativeSessionJournalRejectsEqualGenerationAndWrongBinding(t *testing.T) {
	directory := t.TempDir()
	options := NativeSessionJournalOptions{
		Path: directory + "/controller.session", ClientID: replication.ID128{0x11},
		RetryHome: replication.RetryHome{0x12}, MaxCommandBytes: 1 << 20,
		Binding: replication.Digest{0x13},
	}
	state := durableNativeSessionState{
		clientID: options.ClientID, retryHome: options.RetryHome,
		phase: nativeSessionNew, nextSequence: 1,
	}
	raw, err := appendNativeSessionJournalRecord(nil, 9, options.Binding, state, options.MaxCommandBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(options.Path+".0", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(options.Path+".1", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenNativeSessionJournal(options); !errors.Is(err, ErrNativeSessionJournal) {
		t.Fatalf("equal generation error=%v", err)
	}
	if err = os.Remove(options.Path + ".1"); err != nil {
		t.Fatal(err)
	}
	options.Binding[0] ^= 0xff
	if _, err = OpenNativeSessionJournal(options); !errors.Is(err, ErrNativeSessionJournal) {
		t.Fatalf("wrong binding error=%v", err)
	}
}

func TestNativeSessionJournalBindingFencesCommandIdentity(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	want, err := NativeSessionJournalBinding(route, "orders", "0000-ffff", []byte("controller"), 1,
		serviceauthz.CapabilityDataWrite)
	if err != nil || want == (replication.Digest{}) {
		t.Fatalf("binding=%x err=%v", want, err)
	}
	changed := route
	changed.Command.SchemaGeneration++
	got, err := NativeSessionJournalBinding(changed, "orders", "0000-ffff", []byte("controller"), 1,
		serviceauthz.CapabilityDataWrite)
	if err != nil || got == want {
		t.Fatalf("changed binding=%x want=%x err=%v", got, want, err)
	}
	got, err = NativeSessionJournalBinding(route, "orders", "0000-ffff", []byte("controller"), 2,
		serviceauthz.CapabilityDataWrite)
	if err != nil || got == want {
		t.Fatalf("changed relation binding=%x want=%x err=%v", got, want, err)
	}
	got, err = NativeSessionJournalBinding(route, "orders", "0000-ffff", []byte("controller"), 1,
		serviceauthz.CapabilityTopology)
	if err != nil || got == want {
		t.Fatalf("changed capability binding=%x want=%x err=%v", got, want, err)
	}
}

func TestNativeSessionJournalRejectsCrossCapabilityReopenBidirectionally(t *testing.T) {
	capabilities := []serviceauthz.Capability{
		serviceauthz.CapabilityDataWrite,
		serviceauthz.CapabilityTopology,
	}
	for _, owner := range capabilities {
		foreign := serviceauthz.CapabilityTopology
		if owner == serviceauthz.CapabilityTopology {
			foreign = serviceauthz.CapabilityDataWrite
		}
		name := "data_owner"
		if owner == serviceauthz.CapabilityTopology {
			name = "topology_owner"
		}
		t.Run(name, func(t *testing.T) {
			route, _, states := testReplicatedRouteCommand(t)
			binding, err := NativeSessionJournalBinding(
				route, "orders", "0000-ffff", []byte("controller"), 1, owner,
			)
			if err != nil {
				t.Fatal(err)
			}
			clientID := replication.ID128{0xe1, byte(owner)}
			retryHome := replication.RetryHome{0xf1, byte(owner)}
			journalOptions := NativeSessionJournalOptions{
				Path: t.TempDir() + "/controller.session", ClientID: clientID,
				RetryHome: retryHome, MaxCommandBytes: 1 << 20, Binding: binding,
			}
			journal, err := OpenNativeSessionJournal(journalOptions)
			if err != nil {
				t.Fatal(err)
			}
			executor, err := NewReplicatedExecutor(
				&nativeSessionClient{state: states["m2"]}, 1, time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			options := NativeSessionOptions{
				Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
				Tenant: []byte("controller"), ClientID: clientID, RetryHome: retryHome,
				Resolver: BaseRelationResolver{Relation: 1}, Journal: journal,
				ProposalCapability: owner, MaxRelationBatches: 1, MaxMutations: 2,
				InitialCommandBytes: 512, MaxCommandBytes: journalOptions.MaxCommandBytes,
			}
			ownerSession, err := NewNativeSession(options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ownerSession.Open(context.Background(), 1000); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenNativeSessionJournal(journalOptions)
			if err != nil {
				t.Fatal(err)
			}
			options.Journal = reopened
			options.ProposalCapability = foreign
			if _, err = NewNativeSession(options); !errors.Is(err, ErrNativeSession) {
				t.Fatalf("cross-capability reopen error = %v", err)
			}
		})
	}
}
