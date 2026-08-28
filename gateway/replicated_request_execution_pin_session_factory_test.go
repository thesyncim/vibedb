package gateway

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestTerminalExecutionPinSessionCleanupResumesRetiredAndReleasedJournals(t *testing.T) {
	for _, phase := range []string{"retired", "released", "release-response-lost"} {
		t.Run(phase, func(t *testing.T) {
			route, machine, reopen := newRouteSessionMachine(t)
			client := &routeSessionDropClient{base: machine}
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			principal := serviceauthz.Authority{Node: [16]byte{7}, Generation: 1}
			factory, err := NewJournaledDurableRequestExecutionPinSessionFactory(executor, t.TempDir(), principal)
			if err != nil {
				t.Fatal(err)
			}
			execution, _ := bindTypedExecutionPin(t, typedExecutionFixture(t), route)
			session, _, unlock, err := factory.OpenExecutionPinSession(t.Context(), execution, route)
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			ctx, err := serviceauthz.WithAuthority(t.Context(), principal)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = session.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			if phase != "retired" {
				if phase == "release-response-lost" {
					client.drop = replication.CommandSessionRelease
				}
				_, err = session.Release(ctx)
				if (err != nil) != (phase == "release-response-lost") {
					t.Fatalf("release: %v", err)
				}
			}
			unlock()
			reopen()
			// A new gateway object must finish the exact retained lifecycle,
			// even after retirement or release was already persisted locally.
			fresh, err := NewJournaledDurableRequestExecutionPinSessionFactory(executor, factory.directory, principal)
			if err != nil {
				t.Fatal(err)
			}
			if phase != "release-response-lost" {
				_, _, release, openErr := fresh.OpenExecutionPinSession(t.Context(), execution, route)
				if release != nil {
					release()
				}
				if !errors.Is(openErr, ErrDurableRequestConflict) {
					t.Fatalf("ordinary open accepted terminal session: %v", openErr)
				}
			}
			results := make(chan error, 8)
			for range cap(results) {
				go func() { results <- fresh.RetireTerminalExecutionPinSession(t.Context(), execution, route) }()
			}
			for range cap(results) {
				if err = <-results; err != nil {
					t.Fatalf("terminal cleanup after %s: %v", phase, err)
				}
			}
			entries, err := os.ReadDir(factory.directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("retained journals=%v err=%v", entries, err)
			}
			applied := machine.state.Applied
			if err = fresh.RetireTerminalExecutionPinSession(t.Context(), execution, route); err != nil || machine.state.Applied != applied {
				t.Fatal(err)
			}
		})
	}
}

func TestDurableExecutionPinSessionIdentityBindsPinAndPrincipalGeneration(t *testing.T) {
	pin := executionpin.PinID{1, 2, 3}
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	want := durableExecutionPinSessionIdentity(pin, principal)
	if want == ([16]byte{}) || durableExecutionPinSessionIdentity(pin, principal) != want {
		t.Fatal("session identity is zero or nondeterministic")
	}
	changedPin := pin
	changedPin[0]++
	changedPrincipal := principal
	changedPrincipal.Generation++
	changedNode := principal
	changedNode.Node[0]++
	if durableExecutionPinSessionIdentity(changedPin, principal) == want ||
		durableExecutionPinSessionIdentity(pin, changedPrincipal) == want ||
		durableExecutionPinSessionIdentity(pin, changedNode) == want {
		t.Fatal("session identity aliases pin or principal authority")
	}
}

func TestReleasedExecutionPinJournalChurnLeavesNoFilesAndActiveSurvivesRestart(t *testing.T) {
	directory := t.TempDir()
	for ordinal := 1; ordinal <= 256; ordinal++ {
		options := NativeSessionJournalOptions{
			Path:            filepath.Join(directory, fmt.Sprintf("pin-%04d", ordinal)),
			ClientID:        replication.ID128{byte(ordinal), byte(ordinal >> 8)},
			RetryHome:       replication.RetryHome{byte(ordinal), 1},
			MaxCommandBytes: replication.MaxCommandBytes,
			Binding:         replication.Digest{byte(ordinal), byte(ordinal >> 8), 1},
		}
		journal, err := OpenNativeSessionJournal(options)
		if err != nil {
			t.Fatal(err)
		}
		if ordinal == 1 {
			if err = journal.destroyReleased(); err == nil {
				t.Fatal("active/recoverable journal was deleted")
			}
			if _, err = OpenNativeSessionJournal(options); err != nil {
				t.Fatalf("recoverable journal did not survive reopen: %v", err)
			}
			continue
		}
		state, err := journal.load()
		if err != nil {
			t.Fatal(err)
		}
		state.phase, state.epoch = nativeSessionReleased, 1
		state.ackThrough, state.nextSequence = 1, 2
		state.terminalSequence = 1
		state.terminalFingerprint = replication.Digest{1}
		if err = journal.store(state); err != nil {
			t.Fatal(err)
		}
		if err = journal.destroyReleased(); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 2 {
		t.Fatalf("journal churn retained %d files, want only one recoverable two-slot journal", len(entries))
	}
}

func TestAcknowledgedExecutionPinJournalCleanupUsesExactFullPinIdentity(t *testing.T) {
	directory := t.TempDir()
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	binding := executionpin.Binding{
		RequestKeyDigest: executionpin.Digest{1}, RequestDigest: executionpin.Digest{2},
		CatalogGeneration: 3, SchemaManifestDigest: executionpin.Digest{4},
		TransactionManifestDigest: executionpin.Digest{5},
		ParticipantAuthorityRoot:  executionpin.Digest{6}, ParticipantCount: 1,
		ExecutionContractDigest: executionpin.Digest{7}, LedgerHomeGroup: executionpin.ID{8},
	}
	bindingDigest, err := executionpin.BindingDigest(binding)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := executionpin.DerivePinIDFromBindingDigest(bindingDigest)
	if err != nil {
		t.Fatal(err)
	}
	identity := durableExecutionPinSessionIdentity(pin, principal)
	base := filepath.Join(directory, fmt.Sprintf("%x", identity))
	journal, err := OpenNativeSessionJournal(NativeSessionJournalOptions{
		Path: base, ClientID: identity, RetryHome: replication.RetryHome{1},
		MaxCommandBytes: replication.MaxCommandBytes, Binding: replication.Digest{9},
	})
	if err != nil || journal == nil {
		t.Fatalf("journal=%v err=%v", journal, err)
	}
	factory := &JournaledDurableRequestExecutionPinSessionFactory{
		directory: directory, principal: principal,
	}
	route := durableFaultParticipants(t)[0].Route
	if err = factory.RetireAcknowledgedExecutionPinSession(
		t.Context(), pin, route, replication.Digest{10},
	); err != nil {
		t.Fatal(err)
	}
	for slot := 0; slot < 2; slot++ {
		if _, statErr := os.Stat(base + "." + string(rune('0'+slot))); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("slot %d survived acknowledged cleanup: %v", slot, statErr)
		}
	}
	if err = factory.RetireAcknowledgedExecutionPinSession(
		t.Context(), pin, route, replication.Digest{10},
	); err != nil {
		t.Fatalf("idempotent cleanup: %v", err)
	}
	changed := pin
	changed[0]++
	if durableExecutionPinSessionIdentity(changed, principal) == identity {
		t.Fatal("changed full pin identity aliased journal path")
	}
}

func TestDurableExecutionPinSessionStripesDistributeAndOnlySameIdentitySerializes(t *testing.T) {
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	seen := make([]bool, durableExecutionPinSessionStripes)
	var distinct int
	var first, second [16]byte
	for ordinal := uint64(1); ordinal <= 8192; ordinal++ {
		var pin executionpin.PinID
		for shift := 0; shift < 8; shift++ {
			pin[shift] = byte(ordinal >> (8 * shift))
		}
		identity := durableExecutionPinSessionIdentity(pin, principal)
		stripe := durableExecutionPinSessionStripe(identity)
		if !seen[stripe] {
			seen[stripe] = true
			distinct++
			if first == ([16]byte{}) {
				first = identity
			} else if second == ([16]byte{}) {
				second = identity
			}
		}
	}
	if distinct < 3000 || durableExecutionPinSessionStripe(first) == durableExecutionPinSessionStripe(second) {
		t.Fatalf("distinct stripes = %d/%d", distinct, durableExecutionPinSessionStripes)
	}
	var stripes [durableExecutionPinSessionStripes]sync.Mutex
	stripes[durableExecutionPinSessionStripe(first)].Lock()
	acquired := make(chan struct{})
	go func() {
		stripes[durableExecutionPinSessionStripe(second)].Lock()
		close(acquired)
		stripes[durableExecutionPinSessionStripe(second)].Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("unrelated execution-pin sessions serialized")
	}
	stripes[durableExecutionPinSessionStripe(first)].Unlock()
}

func BenchmarkDurableExecutionPinSessionStripe(b *testing.B) {
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{4}, Generation: 5}
	pin := executionpin.PinID{1, 2, 3}
	identity := durableExecutionPinSessionIdentity(pin, principal)
	b.ReportAllocs()
	for range b.N {
		_ = durableExecutionPinSessionStripe(identity)
	}
}
