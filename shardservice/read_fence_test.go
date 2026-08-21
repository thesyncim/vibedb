package shardservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

func TestReadFenceScopesWriterAdmissionAndFairness(t *testing.T) {
	gate := newReadFenceSet(DefaultMaxReadFences)
	t.Cleanup(gate.close)
	readID := testTransactionID(101)
	readScope := []distributedtxn.IntentScope{{Start: 10, End: 12}}
	if err := gate.acquire(readID, time.Second, 8, readScope); err != nil {
		t.Fatal(err)
	}
	if !gate.validate(readID, 8, readScope) {
		t.Fatal("active fence did not validate its exact scope")
	}
	if gate.validate(readID, 8, []distributedtxn.IntentScope{{Start: 10, End: 11}}) {
		t.Fatal("narrower request reused a fence with a different declared scope")
	}

	disjoint := []distributedtxn.IntentScope{{Start: 30, End: 31}}
	token, err := gate.enterWrite(context.Background(), 8, disjoint)
	if err != nil {
		t.Fatalf("disjoint writer: %v", err)
	}
	gate.leaveWrite(token)

	entered := make(chan uint64, 1)
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		token, err := gate.enterWrite(waitCtx, 8, readScope)
		if err != nil {
			entered <- 0
			return
		}
		entered <- token
	}()
	waitForReadFenceWriters(t, gate, 1)
	if err := gate.acquire(testTransactionID(102), time.Second, 8, readScope); !errors.Is(err, errReadFenceBusy) {
		t.Fatalf("overlapping fence ahead of writer = %v, want busy", err)
	}
	if err := gate.acquire(testTransactionID(103), time.Second, 8, disjoint); err != nil {
		t.Fatalf("disjoint fence while writer waits: %v", err)
	}
	if err := gate.release(readID); err != nil {
		t.Fatal(err)
	}
	var writerToken uint64
	select {
	case writerToken = <-entered:
		if writerToken == 0 {
			t.Fatal("overlapping writer failed")
		}
	case <-time.After(time.Second):
		t.Fatal("released fence did not wake writer")
	}
	if err := gate.acquire(testTransactionID(104), time.Second, 8, readScope); !errors.Is(err, errReadFenceBusy) {
		t.Fatalf("fence crossed active writer = %v, want busy", err)
	}
	gate.leaveWrite(writerToken)
	if err := gate.acquire(testTransactionID(104), time.Second, 8, readScope); err != nil {
		t.Fatalf("fence after writer publication: %v", err)
	}
}

func TestReadFenceLeaseExpiresAndWakesWriter(t *testing.T) {
	gate := newReadFenceSet(DefaultMaxReadFences)
	t.Cleanup(gate.close)
	id := testTransactionID(111)
	scope := []distributedtxn.IntentScope{{Start: 40, End: 41}}
	if err := gate.acquire(id, 10*time.Millisecond, 8, scope); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	token, err := gate.enterWrite(ctx, 8, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.leaveWrite(token)
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("writer crossed a live lease after %v", elapsed)
	}
	if gate.validate(id, 8, scope) {
		t.Fatal("expired fence still validates")
	}
}

func TestReadFenceCapacityIsBoundedAndRecovers(t *testing.T) {
	gate := newReadFenceSet(1)
	t.Cleanup(gate.close)
	first := testTransactionID(115)
	if err := gate.acquire(first, time.Second, 8,
		[]distributedtxn.IntentScope{{Start: 1, End: 2}}); err != nil {
		t.Fatal(err)
	}
	second := testTransactionID(116)
	if err := gate.acquire(second, time.Second, 8,
		[]distributedtxn.IntentScope{{Start: 3, End: 4}}); !errors.Is(err, errReadFenceCapacity) {
		t.Fatalf("second fence = %v, want capacity refusal", err)
	}
	if err := gate.release(first); err != nil {
		t.Fatal(err)
	}
	if err := gate.acquire(second, time.Second, 8,
		[]distributedtxn.IntentScope{{Start: 3, End: 4}}); err != nil {
		t.Fatalf("fence after release: %v", err)
	}
}

func TestParticipantReservationHandsWriteToDurableBarrier(t *testing.T) {
	gate := newReadFenceSet(DefaultMaxReadFences)
	t.Cleanup(gate.close)
	scope := []distributedtxn.IntentScope{{Start: 50, End: 51}}
	first, err := gate.enterWrite(context.Background(), 8, scope)
	if err != nil {
		t.Fatal(err)
	}
	type admission struct {
		token uint64
		err   error
	}
	participant := make(chan admission, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		token, err := gate.enterParticipant(ctx, 8, scope)
		participant <- admission{token: token, err: err}
	}()
	waitForParticipantReservations(t, gate, 1)
	second := make(chan admission, 1)
	go func() {
		token, err := gate.enterWrite(ctx, 8, scope)
		second <- admission{token: token, err: err}
	}()
	select {
	case result := <-second:
		t.Fatalf("new writer crossed participant reservation: %+v", result)
	case <-time.After(10 * time.Millisecond):
	}
	gate.leaveWrite(first)
	var participantToken uint64
	select {
	case result := <-participant:
		if result.err != nil || result.token == 0 {
			t.Fatalf("participant admission = %+v", result)
		}
		participantToken = result.token
	case <-time.After(time.Second):
		t.Fatal("participant did not follow active writer")
	}
	select {
	case result := <-second:
		t.Fatalf("writer crossed held participant reservation: %+v", result)
	case <-time.After(10 * time.Millisecond):
	}
	gate.leaveParticipant(participantToken)
	select {
	case result := <-second:
		if result.err != nil || result.token == 0 {
			t.Fatalf("writer after participant = %+v", result)
		}
		gate.leaveWrite(result.token)
	case <-time.After(time.Second):
		t.Fatal("participant release did not wake writer")
	}
}

func waitForReadFenceWriters(t *testing.T, gate *readFenceSet, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		got := len(gate.writers)
		gate.mu.Unlock()
		if got == count {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
	t.Fatalf("writer count did not reach %d", count)
}

func waitForParticipantReservations(t *testing.T, gate *readFenceSet, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		got := len(gate.barriers)
		gate.mu.Unlock()
		if got == count {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
	t.Fatalf("participant reservation count did not reach %d", count)
}

func BenchmarkReadFenceWriteAdmissionNoFence(b *testing.B) {
	gate := newReadFenceSet(DefaultMaxReadFences)
	scope := []distributedtxn.IntentScope{{Start: 17, End: 18}}
	ctx := context.Background()
	// Retain capacity so the benchmark measures the steady-state write path.
	token, err := gate.enterWrite(ctx, 20, scope)
	if err != nil {
		b.Fatal(err)
	}
	gate.leaveWrite(token)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		token, err = gate.enterWrite(ctx, 20, scope)
		if err != nil {
			b.Fatal(err)
		}
		gate.leaveWrite(token)
	}
}
