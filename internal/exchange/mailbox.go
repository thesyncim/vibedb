// Package exchange provides the bounded worker-to-worker rendezvous state used
// by distributed execution. It owns no network listener or query planner; those
// layers must authenticate peers and open generation-fenced mailboxes before
// transferring owned byte batches.
package exchange

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"time"
)

const (
	MaxProducers       uint16 = 1024
	MaxQueuedBatches   uint16 = 4096
	MaxBatchRows       uint32 = 64 << 10
	MaxBatchBytes      uint32 = 4 << 20
	MaxMailboxRows     uint64 = 16 << 20
	MaxMailboxBytes    uint64 = 1 << 30
	MaxProducerBatches uint16 = 64
)

var (
	ErrInvalidSpec   = errors.New("exchange: invalid mailbox specification")
	ErrClosed        = errors.New("exchange: mailbox is closed")
	ErrProducer      = errors.New("exchange: producer is out of range")
	ErrProducerBusy  = errors.New("exchange: producer already has an in-flight push")
	ErrProducerFinal = errors.New("exchange: producer already ended its stream")
	ErrSequence      = errors.New("exchange: producer sequence is not the next sequence")
	ErrAck           = errors.New("exchange: acknowledgment does not name the delivered batch")
	ErrBatchLimit    = errors.New("exchange: batch exceeds its row or byte bound")
	ErrTotalLimit    = errors.New("exchange: mailbox exceeds its total row or byte bound")
)

// ID is a raw operation identity. It is deliberately not a string: exchange
// routing and lookup never format an ID on the data path.
type ID [16]byte

func (id ID) IsZero() bool { return id == ID{} }

// Key identifies one destination partition within one operation/stage attempt.
// Attempt fencing prevents a retried stage from writing into an abandoned
// mailbox with the same logical partition number.
type Key struct {
	Operation ID
	Stage     uint32
	Partition uint32
	Attempt   uint32
}

func (k Key) valid() bool { return !k.Operation.IsZero() }

// Spec is the immutable admission contract for one destination mailbox.
// Buffered limits bound live queued ownership; total limits bound all accepted
// batches even after the consumer releases them. DeadlineUnixNano is optional.
type Spec struct {
	Key Key

	Producers        uint16
	QueuedBatches    uint16
	ProducerBatches  uint16
	BufferedRows     uint64
	BufferedBytes    uint64
	TotalRows        uint64
	TotalBytes       uint64
	DeadlineUnixNano int64
}

// Valid reports whether every mailbox identity and bound is canonical.
func (s Spec) Valid() bool {
	return s.Key.valid() && s.Producers != 0 && s.Producers <= MaxProducers &&
		s.QueuedBatches != 0 && s.QueuedBatches <= MaxQueuedBatches &&
		s.ProducerBatches != 0 && s.ProducerBatches <= MaxProducerBatches &&
		s.ProducerBatches <= s.QueuedBatches &&
		s.BufferedRows != 0 && s.BufferedRows <= MaxMailboxRows &&
		s.BufferedBytes != 0 && s.BufferedBytes <= MaxMailboxBytes &&
		s.TotalRows >= s.BufferedRows && s.TotalRows <= MaxMailboxRows &&
		s.TotalBytes >= s.BufferedBytes && s.TotalBytes <= MaxMailboxBytes &&
		s.DeadlineUnixNano >= 0
}

// Batch transfers ownership of Data to a mailbox on a successful Push. Data is
// opaque canonical row-block bytes; the exchange layer never parses JSON or
// converts it to strings. Final may accompany the producer's last non-empty
// batch or an empty terminal batch.
type Batch struct {
	Producer uint16
	Sequence uint32
	Rows     uint32
	Data     []byte
	Final    bool
}

type producerState struct {
	next         uint32
	lastSequence uint32
	lastRows     uint32
	lastDigest   [sha256.Size]byte
	queued       uint16
	active       bool
	final        bool
	lastFinal    bool
	hasLast      bool
}

// Mailbox is one bounded multi-producer, single-consumer partition queue.
// Push and Pull are safe for concurrent use. Per-producer order is exact;
// cross-producer arrival order is intentionally unspecified and is carried in
// Batch metadata for downstream operators that require deterministic merging.
type Mailbox struct {
	spec Spec

	mu          sync.Mutex
	queue       []Batch
	head        uint16
	tail        uint16
	count       uint16
	rows        uint64
	bytes       uint64
	totalRows   uint64
	totalBytes  uint64
	finals      uint16
	cause       error
	delivered   bool
	hasAck      bool
	ackProducer uint16
	ackSequence uint32
	consumer    bool

	producers  []producerState
	dataReady  chan struct{}
	spaceReady chan struct{}
}

// ProducerProgress is a constant-size retry probe for one output producer.
// Accepted is true after at least one batch; Final proves the exact stream has
// reached its terminal batch.
type ProducerProgress struct {
	Accepted bool
	Final    bool
}

// ProducerProgress reports whether producer has begun or completed its stream.
func (m *Mailbox) ProducerProgress(producer uint16) (ProducerProgress, error) {
	if m == nil {
		return ProducerProgress{}, ErrClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if producer >= uint16(len(m.producers)) {
		return ProducerProgress{}, ErrProducer
	}
	if err := m.stateErrorLocked(); err != nil {
		return ProducerProgress{}, err
	}
	state := &m.producers[producer]
	return ProducerProgress{Accepted: state.hasLast, Final: state.final}, nil
}

// ClaimConsumer admits exactly one reducer or pull loop for the lifetime of a
// mailbox drain. It prevents duplicate reduce requests from concurrently
// acknowledging the same redelivered input stream.
func (m *Mailbox) ClaimConsumer() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	if m.consumer || m.stateErrorLocked() != nil {
		m.mu.Unlock()
		return false
	}
	m.consumer = true
	m.mu.Unlock()
	return true
}

// ReleaseConsumer releases a successful or failed drain claim.
func (m *Mailbox) ReleaseConsumer() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.consumer = false
	m.mu.Unlock()
}

// Statistics is a detached constant-size mailbox accounting snapshot.
type Statistics struct {
	QueuedBatches  uint16
	FinalProducers uint16
	QueuedRows     uint64
	QueuedBytes    uint64
	TotalRows      uint64
	TotalBytes     uint64
}

func newMailbox(spec Spec) *Mailbox {
	return &Mailbox{
		spec:       spec,
		queue:      make([]Batch, spec.QueuedBatches),
		producers:  make([]producerState, spec.Producers),
		dataReady:  make(chan struct{}, 1),
		spaceReady: make(chan struct{}, 1),
	}
}

// Spec returns the immutable mailbox contract by value.
func (m *Mailbox) Spec() Spec {
	if m == nil {
		return Spec{}
	}
	return m.spec
}

// Statistics reports live queue pressure and accepted totals without exposing
// queue storage or producer state.
func (m *Mailbox) Statistics() Statistics {
	if m == nil {
		return Statistics{}
	}
	m.mu.Lock()
	statistics := Statistics{
		QueuedBatches: m.count, FinalProducers: m.finals,
		QueuedRows: m.rows, QueuedBytes: m.bytes,
		TotalRows: m.totalRows, TotalBytes: m.totalBytes,
	}
	m.mu.Unlock()
	return statistics
}

// Push admits one producer batch and transfers ownership of batch.Data only on
// success. A blocked Push holds no queue capacity and wakes on space,
// cancellation, deadline expiry, or mailbox cancellation.
func (m *Mailbox) Push(ctx context.Context, batch Batch) error {
	if m == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch.Producer >= m.spec.Producers {
		return ErrProducer
	}
	if batch.Rows > MaxBatchRows || len(batch.Data) > int(MaxBatchBytes) ||
		uint64(batch.Rows) > m.spec.BufferedRows ||
		uint64(len(batch.Data)) > m.spec.BufferedBytes ||
		(batch.Rows == 0 && len(batch.Data) != 0) ||
		(batch.Rows != 0 && len(batch.Data) == 0) ||
		(!batch.Final && (batch.Rows == 0 || batch.Sequence == ^uint32(0))) {
		return ErrBatchLimit
	}

	digest := sha256.Sum256(batch.Data)
	m.mu.Lock()
	state := &m.producers[batch.Producer]
	if state.active {
		m.mu.Unlock()
		return ErrProducerBusy
	}
	if state.hasLast && batch.Sequence == state.lastSequence {
		if batch.Rows == state.lastRows && batch.Final == state.lastFinal &&
			digest == state.lastDigest {
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
		return ErrSequence
	}
	if state.final {
		m.mu.Unlock()
		return ErrProducerFinal
	}
	if batch.Sequence != state.next {
		m.mu.Unlock()
		return ErrSequence
	}
	state.active = true
	m.mu.Unlock()

	for {
		m.mu.Lock()
		state = &m.producers[batch.Producer]
		if err := m.stateErrorLocked(); err != nil {
			state.active = false
			m.mu.Unlock()
			m.signalAll()
			return err
		}
		batchBytes := uint64(len(batch.Data))
		if batch.Rows > uint32(m.spec.TotalRows-m.totalRows) ||
			batchBytes > m.spec.TotalBytes-m.totalBytes {
			state.active = false
			m.mu.Unlock()
			return ErrTotalLimit
		}
		fits := m.count < m.spec.QueuedBatches &&
			state.queued < m.spec.ProducerBatches &&
			uint64(batch.Rows) <= m.spec.BufferedRows-m.rows &&
			batchBytes <= m.spec.BufferedBytes-m.bytes
		if fits {
			m.queue[m.tail] = batch
			m.tail++
			if m.tail == uint16(len(m.queue)) {
				m.tail = 0
			}
			m.count++
			m.rows += uint64(batch.Rows)
			m.bytes += batchBytes
			m.totalRows += uint64(batch.Rows)
			m.totalBytes += batchBytes
			state.queued++
			state.active = false
			state.lastSequence = batch.Sequence
			state.lastRows = batch.Rows
			state.lastDigest = digest
			state.lastFinal = batch.Final
			state.hasLast = true
			if batch.Final {
				state.final = true
				m.finals++
			} else {
				state.next++
			}
			m.mu.Unlock()
			signal(m.dataReady)
			return nil
		}
		m.mu.Unlock()
		if err := m.wait(ctx, m.spaceReady); err != nil {
			m.releaseProducer(batch.Producer)
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				m.Cancel(context.DeadlineExceeded)
			}
			return err
		}
	}
}

// Pull returns the next arrived batch. It redelivers the same batch until Ack
// names its producer and sequence, so a lost network response cannot lose data.
// io.EOF means every producer ended and the acknowledged queue is drained.
func (m *Mailbox) Pull(ctx context.Context) (Batch, error) {
	if m == nil {
		return Batch{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return Batch{}, err
		}
		m.mu.Lock()
		if err := m.stateErrorLocked(); err != nil {
			m.mu.Unlock()
			m.signalAll()
			return Batch{}, err
		}
		if m.count != 0 {
			batch := m.queue[m.head]
			m.delivered = true
			m.mu.Unlock()
			return batch, nil
		}
		if m.finals == m.spec.Producers {
			m.mu.Unlock()
			return Batch{}, io.EOF
		}
		m.mu.Unlock()
		if err := m.wait(ctx, m.dataReady); err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				m.Cancel(context.DeadlineExceeded)
			}
			return Batch{}, err
		}
	}
}

// Ack releases the currently delivered batch. Repeating the most recently
// accepted acknowledgment is idempotent; every other stale or speculative ack
// fails closed.
func (m *Mailbox) Ack(producer uint16, sequence uint32) error {
	if m == nil {
		return ErrClosed
	}
	m.mu.Lock()
	if err := m.stateErrorLocked(); err != nil {
		m.mu.Unlock()
		m.signalAll()
		return err
	}
	if m.hasAck && producer == m.ackProducer && sequence == m.ackSequence {
		m.mu.Unlock()
		return nil
	}
	if !m.delivered || m.count == 0 {
		m.mu.Unlock()
		return ErrAck
	}
	batch := m.queue[m.head]
	if batch.Producer != producer || batch.Sequence != sequence {
		m.mu.Unlock()
		return ErrAck
	}
	m.queue[m.head] = Batch{}
	m.head++
	if m.head == uint16(len(m.queue)) {
		m.head = 0
	}
	m.count--
	m.rows -= uint64(batch.Rows)
	m.bytes -= uint64(len(batch.Data))
	m.producers[batch.Producer].queued--
	m.delivered = false
	m.hasAck = true
	m.ackProducer = producer
	m.ackSequence = sequence
	m.mu.Unlock()
	signal(m.spaceReady)
	return nil
}

// Cancel fails current and future operations and drops queued ownership.
func (m *Mailbox) Cancel(cause error) {
	if m == nil {
		return
	}
	if cause == nil {
		cause = ErrClosed
	}
	m.mu.Lock()
	if m.cause == nil {
		m.cause = cause
		for i := range m.queue {
			m.queue[i] = Batch{}
		}
		m.head, m.tail, m.count = 0, 0, 0
		m.rows, m.bytes = 0, 0
		m.delivered = false
		m.consumer = false
	}
	m.mu.Unlock()
	m.signalAll()
}

// Err returns the terminal cancellation cause, or nil while the mailbox is
// open or naturally drained.
func (m *Mailbox) Err() error {
	if m == nil {
		return ErrClosed
	}
	m.mu.Lock()
	err := m.stateErrorLocked()
	m.mu.Unlock()
	if err != nil {
		m.signalAll()
	}
	return err
}

func (m *Mailbox) stateErrorLocked() error {
	if m.cause != nil {
		return m.cause
	}
	if m.spec.DeadlineUnixNano != 0 &&
		time.Now().UnixNano() >= m.spec.DeadlineUnixNano {
		m.cause = context.DeadlineExceeded
		return m.cause
	}
	return nil
}

func (m *Mailbox) releaseProducer(producer uint16) {
	m.mu.Lock()
	m.producers[producer].active = false
	m.mu.Unlock()
	signal(m.spaceReady)
}

func (m *Mailbox) signalAll() {
	signal(m.dataReady)
	signal(m.spaceReady)
}

// wait allocates a timer only on the backpressured slow path and only when the
// mailbox has its own deadline. The ordinary non-blocking Push/Pull path stays
// allocation-free and does not read the wall clock when no deadline exists.
func (m *Mailbox) wait(ctx context.Context, ready <-chan struct{}) error {
	if m.spec.DeadlineUnixNano == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready:
			return nil
		}
	}
	delay := time.Until(time.Unix(0, m.spec.DeadlineUnixNano))
	if delay <= 0 {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		return nil
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
