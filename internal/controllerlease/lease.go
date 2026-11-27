// Package controllerlease provides CAS-fenced, monotonic-epoch leader election
// for distributed gateway controllers (split, replica move, and recovery).
//
// Safety model:
//   - Every acquisition or handoff strictly increments Epoch via atomic
//     CompareAndSwap against the backing Store.
//   - A leader's local authority is bounded by ExpiresAtUnixNano: Lease()
//     returns false immediately once the local clock reaches or passes the
//     committed deadline, even if a renewal RPC is stalled or partitioned.
//   - Downstream control-plane mutations carry the leader's Epoch as a fencing
//     token so a paused former leader cannot overwrite work performed under a
//     higher epoch.
//   - Release() clears Holder while preserving Epoch so a standby frontend can
//     acquire Epoch+1 on its very next tick without waiting out LeaseTTL.
package controllerlease

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultLeaseTTL is the default lease duration when Config.LeaseTTL <= 0.
	DefaultLeaseTTL = 10 * time.Second
	// maxHolderBytes bounds Holder length in wire encodings.
	maxHolderBytes = 256
	// binaryMagicVersion identifies version 1 of the canonical binary lease record.
	binaryMagicVersion uint8 = 1
)

var (
	// ErrInvalidConfig is returned when Elector configuration is invalid.
	ErrInvalidConfig = errors.New("controllerlease: invalid configuration")
	// ErrInvalidRecord is returned when decoding or validating a malformed LeaseRecord.
	ErrInvalidRecord = errors.New("controllerlease: invalid lease record")
	// ErrStaleEpoch is returned when a fencing check rejects a stale epoch.
	ErrStaleEpoch = errors.New("controllerlease: stale lease epoch")
)

// LeaseRecord is the durable CAS state for one controller lease key.
//
// An actively held lease has Holder != "" and ExpiresAtUnixNano > 0.
// A gracefully released lease has Holder == "" and ExpiresAtUnixNano == 0 while
// retaining its monotonic Epoch so the next acquirer always advances to Epoch+1.
type LeaseRecord struct {
	Holder            string `json:"holder"`
	Epoch             uint64 `json:"epoch"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano"`
}

// Valid reports whether r is a well-formed lease record.
func (r LeaseRecord) Valid() bool {
	if r.Epoch == 0 {
		return false
	}
	if len(r.Holder) > maxHolderBytes {
		return false
	}
	if r.Holder == "" {
		return r.ExpiresAtUnixNano == 0
	}
	return r.ExpiresAtUnixNano > 0
}

// IsExpired reports whether r has no active holder or has reached/passed now.
func (r LeaseRecord) IsExpired(now time.Time) bool {
	return r.Holder == "" || now.UnixNano() >= r.ExpiresAtUnixNano
}

// MarshalBinary encodes r into a canonical, self-delimiting binary frame:
//
//	[version:1][epoch:8 LE][expiresAtUnixNano:8 LE][holderLen:2 LE][holder:holderLen]
func (r LeaseRecord) MarshalBinary() ([]byte, error) {
	if !r.Valid() {
		return nil, ErrInvalidRecord
	}
	buf := make([]byte, 1+8+8+2+len(r.Holder))
	buf[0] = binaryMagicVersion
	binary.LittleEndian.PutUint64(buf[1:9], r.Epoch)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(r.ExpiresAtUnixNano))
	binary.LittleEndian.PutUint16(buf[17:19], uint16(len(r.Holder)))
	copy(buf[19:], r.Holder)
	return buf, nil
}

// UnmarshalBinary decodes a canonical binary LeaseRecord frame.
func UnmarshalBinary(data []byte) (LeaseRecord, error) {
	if len(data) < 19 {
		return LeaseRecord{}, fmt.Errorf("%w: short frame (%d bytes)", ErrInvalidRecord, len(data))
	}
	if data[0] != binaryMagicVersion {
		return LeaseRecord{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidRecord, data[0])
	}
	holderLen := int(binary.LittleEndian.Uint16(data[17:19]))
	if holderLen > maxHolderBytes || len(data) != 19+holderLen {
		return LeaseRecord{}, fmt.Errorf("%w: length mismatch", ErrInvalidRecord)
	}
	rec := LeaseRecord{
		Epoch:             binary.LittleEndian.Uint64(data[1:9]),
		ExpiresAtUnixNano: int64(binary.LittleEndian.Uint64(data[9:17])),
		Holder:            string(data[19:]),
	}
	if !rec.Valid() {
		return LeaseRecord{}, ErrInvalidRecord
	}
	return rec, nil
}

// Store provides linearizable Read and CompareAndSwap operations for lease
// records (for example backed by the replicated catalog/control Raft group).
type Store interface {
	// Read returns the current LeaseRecord for key, or (zero, false, nil) if
	// no lease record exists yet.
	Read(ctx context.Context, key string) (LeaseRecord, bool, error)

	// CompareAndSwap atomically replaces key with next if and only if:
	//   - expected == nil and key does not currently exist, OR
	//   - expected != nil and key exists and equals *expected exactly.
	//
	// Returns (true, next, nil) on success, or (false, current, nil) when the
	// precondition did not match.
	CompareAndSwap(ctx context.Context, key string, expected *LeaseRecord, next LeaseRecord) (bool, LeaseRecord, error)
}

// MemoryStore is a race-free in-memory Store for deterministic tests and
// single-process multi-frontend topologies.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]LeaseRecord
}

// NewMemoryStore creates an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string]LeaseRecord)}
}

// Read implements Store.
func (s *MemoryStore) Read(ctx context.Context, key string) (LeaseRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return LeaseRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[key]
	return rec, ok, nil
}

// CompareAndSwap implements Store.
func (s *MemoryStore) CompareAndSwap(
	ctx context.Context,
	key string,
	expected *LeaseRecord,
	next LeaseRecord,
) (bool, LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return false, LeaseRecord{}, err
	}
	if !next.Valid() {
		return false, LeaseRecord{}, ErrInvalidRecord
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[key]
	if expected == nil {
		if exists {
			return false, current, nil
		}
		s.records[key] = next
		return true, next, nil
	}
	if !exists || current != *expected {
		return false, current, nil
	}
	// Enforce monotonic epoch invariant at the store boundary as defense-in-depth.
	if next.Epoch < current.Epoch {
		return false, current, ErrStaleEpoch
	}
	s.records[key] = next
	return true, next, nil
}

// Config configures one controller lease Elector.
type Config struct {
	// Key uniquely names the controller lease (e.g. "vibedb/controller/split").
	Key string
	// Holder uniquely identifies this frontend instance (e.g. nodeID/incarnation).
	Holder string
	// LeaseTTL is the validity duration of each lease acquisition or renewal.
	// Defaults to DefaultLeaseTTL when <= 0.
	LeaseTTL time.Duration
	// RenewInterval is how often the leader renews its lease.
	// Defaults to LeaseTTL / 3 when <= 0. Must be strictly less than LeaseTTL.
	RenewInterval time.Duration
	// RetryInterval is how often a standby polls/attempts acquisition.
	// Defaults to LeaseTTL / 5 when <= 0.
	RetryInterval time.Duration
	// Clock supplies the current time. Defaults to time.Now when nil.
	Clock func() time.Time
	// OnAcquire is invoked asynchronously whenever this Elector acquires
	// leadership. leaderCtx is cancelled immediately upon step-down, expiry,
	// Release, or Run cancellation.
	OnAcquire func(leaderCtx context.Context, epoch uint64)
	// OnLose is invoked when this Elector steps down from leadership.
	OnLose func(epoch uint64)
}

func (c *Config) normalize() error {
	if c.Key == "" || c.Holder == "" || len(c.Holder) > maxHolderBytes {
		return fmt.Errorf("%w: non-empty key and holder (<= %d bytes) required", ErrInvalidConfig, maxHolderBytes)
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = max(c.LeaseTTL/3, time.Millisecond)
	}
	if c.RenewInterval >= c.LeaseTTL {
		return fmt.Errorf("%w: renew interval (%s) must be less than lease TTL (%s)", ErrInvalidConfig, c.RenewInterval, c.LeaseTTL)
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = max(c.LeaseTTL/5, time.Millisecond)
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	return nil
}

// Status is a point-in-time snapshot of an Elector's leadership state.
type Status struct {
	Holder            string
	Epoch             uint64
	Leader            bool
	ExpiresAtUnixNano int64
}

// Elector manages CAS-fenced leadership acquisition, renewal, deadline-bounded
// step-down, and graceful Release handoff for one controller frontend.
type Elector struct {
	store Store
	cfg   Config

	mu           sync.Mutex
	holding      bool
	record       LeaseRecord
	leaderCancel context.CancelFunc
	leaderDone   chan struct{}
}

// NewElector validates cfg and constructs an Elector bound to store.
func NewElector(store Store, cfg Config) (*Elector, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidConfig)
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &Elector{
		store: store,
		cfg:   cfg,
	}, nil
}

// Lease reports whether this Elector currently holds a valid, unexpired lease
// and returns its monotonic fencing epoch. It fails closed as soon as the local
// clock reaches or passes the committed lease deadline.
func (e *Elector) Lease() (uint64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.holding || e.record.IsExpired(e.cfg.Clock()) {
		return 0, false
	}
	return e.record.Epoch, true
}

// Status returns a snapshot of the Elector's current leadership state suitable
// for observability (/metrics) and control-plane diagnostics.
func (e *Elector) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.cfg.Clock()
	leader := e.holding && !e.record.IsExpired(now)
	return Status{
		Holder:            e.cfg.Holder,
		Epoch:             e.record.Epoch,
		Leader:            leader,
		ExpiresAtUnixNano: e.record.ExpiresAtUnixNano,
	}
}

// Step performs one acquisition or renewal attempt against the backing Store.
// Exported for deterministic step-by-step testing as well as Run().
func (e *Elector) Step(ctx context.Context) (bool, uint64, error) {
	if err := ctx.Err(); err != nil {
		e.stepDown()
		return false, 0, err
	}

	e.mu.Lock()
	holding := e.holding
	current := e.record
	e.mu.Unlock()

	now := e.cfg.Clock()

	// If we think we are leader but our local deadline already elapsed, step
	// down immediately before touching the store so we never act past TTL.
	if holding && current.IsExpired(now) {
		e.stepDown()
		holding = false
	}

	if holding {
		next := LeaseRecord{
			Holder:            e.cfg.Holder,
			Epoch:             current.Epoch,
			ExpiresAtUnixNano: now.Add(e.cfg.LeaseTTL).UnixNano(),
		}
		swapped, _, err := e.store.CompareAndSwap(ctx, e.cfg.Key, &current, next)
		if err != nil {
			// If our local deadline elapsed while the RPC was in flight, step down.
			if current.IsExpired(e.cfg.Clock()) {
				e.stepDown()
			}
			epoch, ok := e.Lease()
			return ok, epoch, err
		}
		if !swapped {
			// Another node took over or our record changed: step down immediately.
			e.stepDown()
			return false, 0, nil
		}
		// Check deadline again after RPC returns (in case renewal took longer than TTL).
		if next.IsExpired(e.cfg.Clock()) {
			e.stepDown()
			return false, 0, nil
		}
		e.mu.Lock()
		if e.holding && e.record == current {
			e.record = next
			epoch := next.Epoch
			e.mu.Unlock()
			return true, epoch, nil
		}
		e.mu.Unlock()
		return false, 0, nil
	}

	// Standby path: read current lease and attempt CAS if absent or expired/released.
	existing, exists, err := e.store.Read(ctx, e.cfg.Key)
	if err != nil {
		return false, 0, err
	}
	now = e.cfg.Clock()
	if exists && !existing.IsExpired(now) {
		// If we somehow already own an unexpired record (e.g., after a brief restart
		// with the same Holder), do NOT silently reuse its epoch; wait for expiry
		// or let only a fresh epoch advance.
		return false, existing.Epoch, nil
	}

	nextEpoch := uint64(1)
	var expected *LeaseRecord
	if exists {
		nextEpoch = existing.Epoch + 1
		expected = &existing
	}
	next := LeaseRecord{
		Holder:            e.cfg.Holder,
		Epoch:             nextEpoch,
		ExpiresAtUnixNano: now.Add(e.cfg.LeaseTTL).UnixNano(),
	}
	swapped, observed, err := e.store.CompareAndSwap(ctx, e.cfg.Key, expected, next)
	if err != nil {
		return false, 0, err
	}
	if !swapped {
		return false, observed.Epoch, nil
	}
	if next.IsExpired(e.cfg.Clock()) {
		return false, 0, nil
	}
	e.becomeLeader(ctx, next)
	return true, next.Epoch, nil
}

func (e *Elector) becomeLeader(parent context.Context, rec LeaseRecord) {
	e.mu.Lock()
	if e.leaderCancel != nil {
		e.leaderCancel()
	}
	leaderCtx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	e.holding = true
	e.record = rec
	e.leaderCancel = cancel
	e.leaderDone = done
	onAcquire := e.cfg.OnAcquire
	e.mu.Unlock()

	if onAcquire != nil {
		go func() {
			defer close(done)
			onAcquire(leaderCtx, rec.Epoch)
		}()
	} else {
		close(done)
	}
}

func (e *Elector) stepDown() {
	e.mu.Lock()
	if !e.holding {
		e.mu.Unlock()
		return
	}
	lostEpoch := e.record.Epoch
	cancel := e.leaderCancel
	done := e.leaderDone
	e.holding = false
	e.leaderCancel = nil
	e.leaderDone = nil
	onLose := e.cfg.OnLose
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	if onLose != nil {
		onLose(lostEpoch)
	}
}

// Release gracefully steps down from leadership and clears Holder in the Store
// (preserving Epoch) so a standby frontend can acquire Epoch+1 immediately
// without waiting out LeaseTTL. Safe to call when not currently leader.
func (e *Elector) Release(ctx context.Context) error {
	e.mu.Lock()
	holding := e.holding
	current := e.record
	e.mu.Unlock()

	e.stepDown()
	if !holding {
		return nil
	}

	released := LeaseRecord{
		Holder:            "",
		Epoch:             current.Epoch,
		ExpiresAtUnixNano: 0,
	}
	_, _, err := e.store.CompareAndSwap(ctx, e.cfg.Key, &current, released)
	return err
}

// Run drives the election and renewal loop until ctx is cancelled. On exit it
// steps down and attempts a best-effort graceful Release so standby frontends
// fail over immediately.
func (e *Elector) Run(ctx context.Context) error {
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), min(e.cfg.LeaseTTL, 2*time.Second))
		defer cancel()
		_ = e.Release(releaseCtx)
	}()

	for {
		leader, _, _ := e.Step(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		wait := e.cfg.RetryInterval
		if leader {
			wait = e.cfg.RenewInterval
			e.mu.Lock()
			rem := time.Until(time.Unix(0, e.record.ExpiresAtUnixNano))
			if e.cfg.Clock != nil {
				rem = time.Unix(0, e.record.ExpiresAtUnixNano).Sub(e.cfg.Clock())
			}
			e.mu.Unlock()
			if rem > 0 && rem < wait {
				wait = rem
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Guard verifies that candidateEpoch is not behind minEpoch. Downstream
// control-plane handlers can use Guard to reject fenced stale leaders.
func Guard(candidateEpoch, minEpoch uint64) error {
	if candidateEpoch == 0 || candidateEpoch < minEpoch {
		return fmt.Errorf("%w: epoch %d < required %d", ErrStaleEpoch, candidateEpoch, minEpoch)
	}
	return nil
}
