package gateway

import (
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
