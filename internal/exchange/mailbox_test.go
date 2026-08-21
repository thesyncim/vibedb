package exchange

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"
)

func testID(seed byte) ID {
	var id ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testSpec() Spec {
	return Spec{
		Key:       Key{Operation: testID(1), Stage: 2, Partition: 3, Attempt: 4},
		Producers: 2, QueuedBatches: 2, ProducerBatches: 1,
		BufferedRows: 4, BufferedBytes: 64,
		TotalRows: 8, TotalBytes: 128,
	}
}

func TestMailboxBackpressureSequencingAndCompletion(t *testing.T) {
	box := newMailbox(testSpec())
	firstData := []byte("first")
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Sequence: 0, Rows: 1, Data: firstData,
	}); err != nil {
		t.Fatalf("first Push: %v", err)
	}

	started := make(chan struct{})
	pushed := make(chan error, 1)
	secondData := []byte("second")
	go func() {
		close(started)
		pushed <- box.Push(context.Background(), Batch{
			Producer: 0, Sequence: 1, Rows: 1, Data: secondData, Final: true,
		})
	}()
	<-started
	select {
	case err := <-pushed:
		t.Fatalf("second producer batch bypassed its one-batch credit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	first, err := box.Pull(context.Background())
	if err != nil || first.Sequence != 0 || string(first.Data) != "first" {
		t.Fatalf("first Pull = %+v, %v", first, err)
	}
	if repeated, err := box.Pull(context.Background()); err != nil ||
		repeated.Producer != first.Producer || repeated.Sequence != first.Sequence ||
		string(repeated.Data) != "first" {
		t.Fatalf("repeated Pull = %+v, %v", repeated, err)
	}
	select {
	case err := <-pushed:
		t.Fatalf("second Push completed before Ack: %v", err)
	default:
	}
	if err := box.Ack(1, first.Sequence); !errors.Is(err, ErrAck) {
		t.Fatalf("wrong Ack = %v, want ErrAck", err)
	}
	if err := box.Ack(first.Producer, first.Sequence); err != nil {
		t.Fatalf("first Ack: %v", err)
	}
	if err := box.Ack(first.Producer, first.Sequence); err != nil {
		t.Fatalf("idempotent first Ack: %v", err)
	}
	if err := <-pushed; err != nil {
		t.Fatalf("second Push: %v", err)
	}
	second, err := box.Pull(context.Background())
	if err != nil || second.Sequence != 1 || !second.Final || string(second.Data) != "second" {
		t.Fatalf("second Pull = %+v, %v", second, err)
	}
	if err := box.Ack(second.Producer, second.Sequence); err != nil {
		t.Fatalf("second Ack: %v", err)
	}
	if err := box.Push(context.Background(), Batch{Producer: 1, Final: true}); err != nil {
		t.Fatalf("producer 1 terminal Push: %v", err)
	}
	terminal, err := box.Pull(context.Background())
	if err != nil || !terminal.Final || terminal.Producer != 1 {
		t.Fatalf("terminal Pull = %+v, %v", terminal, err)
	}
	if err := box.Ack(terminal.Producer, terminal.Sequence); err != nil {
		t.Fatalf("terminal Ack: %v", err)
	}
	if _, err := box.Pull(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("drained Pull = %v, want EOF", err)
	}
	if err := box.Push(context.Background(), Batch{Producer: 0, Sequence: 2, Final: true}); !errors.Is(err, ErrProducerFinal) {
		t.Fatalf("post-final Push = %v, want ErrProducerFinal", err)
	}
}

func TestMailboxRejectsSequenceLimitsAndConcurrentProducer(t *testing.T) {
	spec := testSpec()
	spec.QueuedBatches = 1
	box := newMailbox(spec)
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Sequence: 1, Rows: 1, Data: []byte("x"),
	}); !errors.Is(err, ErrSequence) {
		t.Fatalf("sequence gap = %v, want ErrSequence", err)
	}
	if err := box.Push(context.Background(), Batch{
		Producer: 3, Rows: 1, Data: []byte("x"),
	}); !errors.Is(err, ErrProducer) {
		t.Fatalf("bad producer = %v, want ErrProducer", err)
	}
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Sequence: ^uint32(0), Rows: 1, Data: []byte("x"),
	}); !errors.Is(err, ErrBatchLimit) {
		t.Fatalf("non-final max sequence = %v, want ErrBatchLimit", err)
	}

	if err := box.Push(context.Background(), Batch{
		Producer: 0, Rows: 1, Data: []byte("held"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Rows: 1, Data: []byte("held"),
	}); err != nil {
		t.Fatalf("idempotent Push: %v", err)
	}
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Rows: 1, Data: []byte("different"),
	}); !errors.Is(err, ErrSequence) {
		t.Fatalf("conflicting retry = %v, want ErrSequence", err)
	}
	blocked := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		blocked <- box.Push(ctx, Batch{
			Producer: 1, Rows: 1, Data: []byte("blocked"),
		})
	}()
	deadline := time.Now().Add(time.Second)
	for {
		box.mu.Lock()
		active := box.producers[1].active
		box.mu.Unlock()
		if active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("producer did not block")
		}
		runtime.Gosched()
	}
	if err := box.Push(context.Background(), Batch{
		Producer: 1, Rows: 1, Data: []byte("duplicate"),
	}); !errors.Is(err, ErrProducerBusy) {
		t.Fatalf("concurrent Push = %v, want ErrProducerBusy", err)
	}
	cancel()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Push = %v", err)
	}
}

func TestMailboxCapsAndCancellationWakeWaiters(t *testing.T) {
	spec := testSpec()
	spec.TotalRows = spec.BufferedRows
	spec.TotalBytes = spec.BufferedBytes
	box := newMailbox(spec)
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Rows: 4, Data: make([]byte, 64),
	}); err != nil {
		t.Fatal(err)
	}
	delivered, err := box.Pull(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := box.Ack(delivered.Producer, delivered.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := box.Push(context.Background(), Batch{
		Producer: 0, Sequence: 1, Rows: 1, Data: []byte("x"), Final: true,
	}); !errors.Is(err, ErrTotalLimit) {
		t.Fatalf("total overflow = %v, want ErrTotalLimit", err)
	}

	waiting := newMailbox(testSpec())
	pulled := make(chan error, 1)
	go func() {
		_, err := waiting.Pull(context.Background())
		pulled <- err
	}()
	boom := errors.New("boom")
	waiting.Cancel(boom)
	if err := <-pulled; !errors.Is(err, boom) {
		t.Fatalf("canceled Pull = %v, want boom", err)
	}
	if err := waiting.Push(context.Background(), Batch{Producer: 0, Final: true}); !errors.Is(err, boom) {
		t.Fatalf("post-cancel Push = %v, want boom", err)
	}

	manySpec := testSpec()
	manySpec.Producers = 3
	manySpec.QueuedBatches = 1
	many := newMailbox(manySpec)
	if err := many.Push(context.Background(), Batch{
		Producer: 0, Rows: 1, Data: []byte("held"),
	}); err != nil {
		t.Fatal(err)
	}
	waiters := make(chan error, 2)
	for producer := uint16(1); producer < 3; producer++ {
		go func() {
			waiters <- many.Push(context.Background(), Batch{
				Producer: producer, Rows: 1, Data: []byte("wait"),
			})
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		many.mu.Lock()
		both := many.producers[1].active && many.producers[2].active
		many.mu.Unlock()
		if both {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("producers did not block")
		}
		runtime.Gosched()
	}
	many.Cancel(boom)
	for range 2 {
		if err := <-waiters; !errors.Is(err, boom) {
			t.Fatalf("canceled producer = %v, want boom", err)
		}
	}
	statistics := many.Statistics()
	if statistics.QueuedBatches != 0 || statistics.QueuedRows != 0 || statistics.QueuedBytes != 0 {
		t.Fatalf("canceled statistics = %+v", statistics)
	}
}

func TestRegistryAdmissionIdempotenceReapAndPartitioning(t *testing.T) {
	spec := testSpec()
	r := NewRegistry(RegistryOptions{
		MaxMailboxes: 1, MaxReservedBufferBytes: spec.BufferedBytes,
	})
	box, err := r.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.Open(spec)
	if err != nil || again != box {
		t.Fatalf("idempotent Open = %p, %v; want %p", again, err, box)
	}
	retried := spec
	retried.DeadlineUnixNano = time.Now().Add(time.Hour).UnixNano()
	again, err = r.Open(retried)
	if err != nil || again != box || again.Spec().DeadlineUnixNano != spec.DeadlineUnixNano {
		t.Fatalf("deadline-shifted Open retry = %p, %v, deadline %d; want original %p deadline %d",
			again, err, again.Spec().DeadlineUnixNano, box, spec.DeadlineUnixNano)
	}
	conflict := spec
	conflict.TotalRows++
	if _, err := r.Open(conflict); !errors.Is(err, ErrSpecConflict) {
		t.Fatalf("conflicting Open = %v, want ErrSpecConflict", err)
	}
	second := spec
	second.Key.Partition++
	if _, err := r.Open(second); !errors.Is(err, ErrRegistryLimit) {
		t.Fatalf("over-cap Open = %v, want ErrRegistryLimit", err)
	}
	if !r.Delete(spec.Key, ErrClosed) {
		t.Fatal("Delete did not find mailbox")
	}
	if _, ok := r.Lookup(spec.Key); ok {
		t.Fatal("deleted mailbox remains visible")
	}

	expiring := spec
	expiring.DeadlineUnixNano = time.Now().Add(time.Millisecond).UnixNano()
	if _, err := r.Open(expiring); err != nil {
		t.Fatal(err)
	}
	if got := r.Reap(time.Now().Add(time.Second)); got != 1 {
		t.Fatalf("Reap = %d, want 1", got)
	}

	for _, test := range []struct {
		hash       uint64
		partitions uint32
		want       uint32
	}{
		{0, 16, 0}, {^uint64(0), 16, 15}, {uint64(1) << 63, 16, 8},
	} {
		got, err := PartitionFor(test.hash, test.partitions)
		if err != nil || got != test.want {
			t.Fatalf("PartitionFor(%x,%d) = %d,%v want %d", test.hash, test.partitions, got, err, test.want)
		}
	}
	if _, err := PartitionFor(1, 0); !errors.Is(err, ErrPartitions) {
		t.Fatalf("zero partitions = %v", err)
	}
}

func TestMailboxDeadlineWakesBlockedOperationWithoutReaper(t *testing.T) {
	spec := testSpec()
	spec.DeadlineUnixNano = time.Now().Add(20 * time.Millisecond).UnixNano()
	box := newMailbox(spec)
	started := time.Now()
	if _, err := box.Pull(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline Pull = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond || elapsed > time.Second {
		t.Fatalf("deadline elapsed = %s", elapsed)
	}
	if !errors.Is(box.Err(), context.DeadlineExceeded) {
		t.Fatalf("mailbox cause = %v, want DeadlineExceeded", box.Err())
	}
}

func BenchmarkMailboxPushPullAck(b *testing.B) {
	spec := testSpec()
	spec.Producers = 1
	spec.QueuedBatches = 1
	spec.BufferedRows = 1
	spec.BufferedBytes = 256
	spec.TotalRows = MaxMailboxRows
	spec.TotalBytes = MaxMailboxBytes
	box := newMailbox(spec)
	data := make([]byte, 256)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	cycle := int(MaxMailboxBytes / uint64(len(data)))
	for i := 0; i < b.N; i++ {
		if i != 0 && i%cycle == 0 {
			b.StopTimer()
			box = newMailbox(spec)
			b.StartTimer()
		}
		if err := box.Push(context.Background(), Batch{
			Sequence: uint32(i % cycle), Rows: 1, Data: data,
		}); err != nil {
			b.Fatal(err)
		}
		delivered, err := box.Pull(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if err := box.Ack(delivered.Producer, delivered.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}
