package driver

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// The reuse lane is intentionally small and private to one apply claim. It is
// a bounded optimization for RF3's repeated one-table reads, never a general
// prepared-statement cache. A request that cannot acquire a slot continues on
// the ordinary constructor/prepare path.
const (
	replicatedReadReuseSlotCount = 8
	replicatedReadReuseMaxBytes  = 2 << 20
	replicatedReadReuseMaxSQL    = 64 << 10
	replicatedReadReuseMaxParams = 32
	replicatedReadReuseMaxPath   = 256
	replicatedReadReuseMaxLimit  = 256
)

var (
	// ErrReplicatedReadReuseUnsupported asks a caller to use the existing
	// replicated SQL path. It is not a SQL error and must never escape as an RF3
	// refusal on its own.
	ErrReplicatedReadReuseUnsupported = errors.New(
		"vibedb: replicated read reuse does not support this request",
	)
	ErrReplicatedReadLeaseClosed = errors.New(
		"vibedb: replicated read lease is closed",
	)
)

// ReplicatedReadReuseSource is the optional source interface used by the
// shard SQL adapter. A source may omit it and retain the original read path.
// The lease deliberately exposes query operations rather than its underlying
// Prepared or Session, so a caller cannot retain a cached object after release.
type ReplicatedReadReuseSource interface {
	AcquireReplicatedDataRead(
		context.Context, *replicatedstate.DataReadCut, string, []ParamType,
		bool, query.ExecOptions,
	) (*ReplicatedReadLease, error)
	AcquireReplicatedPointRead(
		context.Context, replication.RelationID, []byte, bool, []byte, []byte,
		string, []ParamType, bool, query.ExecOptions,
	) (*ReplicatedReadLease, error)
}

// ReplicatedReadLease owns one exclusive execution slot. Query methods are
// valid until Release, Abort, or Close. Callers should pass the execution error
// to Finish so a failed or panicking execution retires the slot.
type ReplicatedReadLease struct {
	cache      *replicatedReadReuseCache
	slot       *replicatedReadReuseSlot
	generation uint64
}

// QueryInto runs the prepared read against the current fresh cut.
func (l *ReplicatedReadLease) QueryInto(
	ctx context.Context, values []any, dst *Cursor,
) error {
	if err := l.live(); err != nil {
		return err
	}
	return l.slot.prepared.QueryInto(ctx, values, dst)
}

// QueryCandidateKeysInto runs the prepared read over caller-owned primary
// keys. The key bytes are borrowed only for this synchronous call.
func (l *ReplicatedReadLease) QueryCandidateKeysInto(
	ctx context.Context,
	values []any,
	primaryPath []byte,
	keys [][]byte,
	dst *Cursor,
) error {
	if err := l.live(); err != nil {
		return err
	}
	return l.slot.prepared.QueryCandidateKeysInto(
		ctx, values, primaryPath, keys, dst,
	)
}

// Columns returns borrowed immutable output names. They remain valid until
// Finish returns.
func (l *ReplicatedReadLease) Columns() []string {
	if l == nil || l.live() != nil {
		return nil
	}
	return l.slot.prepared.Columns()
}

// Stats reports the current execution counters without exposing the retained
// session. It is useful to diagnostics and remains valid until Finish.
func (l *ReplicatedReadLease) Stats() query.ExecStats {
	if l == nil || l.live() != nil {
		return query.ExecStats{}
	}
	return l.slot.reader.conn.exec.Stats
}

// Finish releases a successful lease into the fixed cache when err is nil.
// Any non-nil error retires its session and prepared statement. It is
// idempotent and safe to call from a defer.
func (l *ReplicatedReadLease) Finish(err error) error {
	if l == nil || l.cache == nil || l.slot == nil {
		return nil
	}
	return l.cache.finish(l, err)
}

// Release is the successful-execution spelling of Finish(nil).
func (l *ReplicatedReadLease) Release() error { return l.Finish(nil) }

// Abort retires the slot even when the caller has no execution error to pass.
func (l *ReplicatedReadLease) Abort(err error) error {
	if err == nil {
		err = ErrReplicatedReadLeaseClosed
	}
	return l.Finish(err)
}

// Close is a conservative alias for Release. Call Abort from error/panic
// cleanup when a query may have left state bound in the executor.
func (l *ReplicatedReadLease) Close() error { return l.Release() }

func (l *ReplicatedReadLease) live() error {
	if l == nil || l.cache == nil || l.slot == nil {
		return ErrReplicatedReadLeaseClosed
	}
	l.cache.mu.Lock()
	valid := !l.cache.closed && l.slot.active && l.slot.lease == l &&
		l.slot.generation == l.generation
	l.cache.mu.Unlock()
	if !valid {
		return ErrReplicatedReadLeaseClosed
	}
	return nil
}

type replicatedReadReuseKey struct {
	sql              string
	paramTypes       []ParamType
	relation         replication.RelationID
	primaryPath      string
	schemaGeneration uint64
	allocationGen    uint64
	layoutToken      *catalogLayoutIdentity
	manifestDigest   [32]byte
}

func (k replicatedReadReuseKey) equal(other replicatedReadReuseKey) bool {
	if k.sql != other.sql || k.relation != other.relation ||
		k.primaryPath != other.primaryPath || k.schemaGeneration != other.schemaGeneration ||
		k.allocationGen != other.allocationGen ||
		k.layoutToken != other.layoutToken ||
		k.manifestDigest != other.manifestDigest ||
		len(k.paramTypes) != len(other.paramTypes) {
		return false
	}
	for i := range k.paramTypes {
		if k.paramTypes[i] != other.paramTypes[i] {
			return false
		}
	}
	return true
}

type replicatedReadReuseSlot struct {
	active       bool
	generation   uint64
	lease        *ReplicatedReadLease
	key          replicatedReadReuseKey
	reader       *ReplicatedReadSession
	prepared     *Prepared
	cacheable    bool
	retainedByte int64
}

type replicatedReadReuseCache struct {
	mu        sync.Mutex
	closed    bool
	retained  int64
	nextGen   uint64
	hits      uint64
	misses    uint64
	evictions uint64
	slots     [replicatedReadReuseSlotCount]replicatedReadReuseSlot
}

type replicatedReadReuseStats struct {
	Hits, Misses, Evictions uint64
	RetainedSlots           int
	RetainedBytes           int64
}

func (c *replicatedReadReuseCache) stats() replicatedReadReuseStats {
	if c == nil {
		return replicatedReadReuseStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := replicatedReadReuseStats{
		Hits: c.hits, Misses: c.misses, Evictions: c.evictions,
		RetainedBytes: c.retained,
	}
	for i := range c.slots {
		if c.slots[i].reader != nil && !c.slots[i].active {
			stats.RetainedSlots++
		}
	}
	return stats
}

func (a *ReplicatedApply) replicatedReadReuseCache() *replicatedReadReuseCache {
	if a == nil {
		return nil
	}
	a.readReuseMu.Lock()
	defer a.readReuseMu.Unlock()
	if a.readReuse == nil {
		a.readReuse = new(replicatedReadReuseCache)
	}
	return a.readReuse
}

func (a *ReplicatedApply) replicatedReadReuseStats() replicatedReadReuseStats {
	if a == nil {
		return replicatedReadReuseStats{}
	}
	a.readReuseMu.Lock()
	c := a.readReuse
	a.readReuseMu.Unlock()
	return c.stats()
}

// AcquireReplicatedDataRead validates a fresh data cut, then either attaches
// it to an idle prepared slot or builds one ordinary session and preparation.
func (a *ReplicatedApply) AcquireReplicatedDataRead(
	ctx context.Context,
	cut *replicatedstate.DataReadCut,
	text string,
	parameterTypes []ParamType,
	partialAggregate bool,
	options query.ExecOptions,
) (*ReplicatedReadLease, error) {
	if partialAggregate || !replicatedReadReuseTextBound(text) ||
		!replicatedReadReuseParamTypesBound(parameterTypes) {
		return nil, ErrReplicatedReadReuseUnsupported
	}
	key, err := a.readReuseDataKey(text, parameterTypes, cutFenceDigest(cut))
	if err != nil {
		return nil, err
	}
	cache := a.replicatedReadReuseCache()
	if cache == nil {
		return nil, ErrReplicatedApplyClosed
	}
	slot, lease, err := cache.reserve(key)
	if err != nil {
		return nil, err
	}
	var pendingReader *ReplicatedReadSession
	defer func() {
		if cause := recover(); cause != nil {
			reader := cache.retire(slot)
			if pendingReader != reader {
				closeReplicatedReadSlot(pendingReader)
			}
			closeReplicatedReadSlot(reader)
			panic(cause)
		}
	}()
	if lease != nil {
		reader, constructErr := a.newDataReadSessionInto(
			ctx, cut, options, slot.reader,
		)
		if constructErr != nil {
			_ = cache.finish(lease, constructErr)
			return nil, constructErr
		}
		if reader != slot.reader {
			_ = cache.finish(lease, ErrReplicatedReadLeaseClosed)
			return nil, ErrReplicatedReadReuseUnsupported
		}
		if _, bindErr := a.bindReplicatedReadReuseKey(key, slot.prepared); bindErr != nil {
			_ = cache.finish(lease, bindErr)
			return nil, bindErr
		}
		return lease, nil
	}

	reader, err := a.newDataReadSessionInto(ctx, cut, options, nil)
	if err != nil {
		cache.cancelReservation(slot)
		return nil, err
	}
	pendingReader = reader
	prepared, err := prepareReplicatedRead(
		ctx, reader, text, parameterTypes,
	)
	if err != nil {
		_ = reader.Close()
		cache.cancelReservation(slot)
		return nil, err
	}
	if !prepared.statement.replicatedReadReuseEligible() {
		// The bounded acquire already paid for ordinary preparation. Return a
		// one-shot lease so the adapter does not prepare the same unsupported
		// statement a second time on the fallback path; Finish always retires
		// this non-cacheable slot.
		if err := cache.install(slot, key, reader, prepared, false); err != nil {
			_ = reader.Close()
			cache.cancelReservation(slot)
			return nil, err
		}
		return cache.leaseFor(slot), nil
	}
	key, err = a.bindReplicatedReadReuseKey(key, prepared)
	if err != nil {
		if !errors.Is(err, ErrReplicatedReadReuseUnsupported) {
			_ = reader.Close()
			cache.cancelReservation(slot)
			return nil, err
		}
		if installErr := cache.install(slot, key, reader, prepared, false); installErr != nil {
			_ = reader.Close()
			cache.cancelReservation(slot)
			return nil, installErr
		}
		return cache.leaseFor(slot), nil
	}
	if err := cache.install(slot, key, reader, prepared, true); err != nil {
		_ = reader.Close()
		cache.cancelReservation(slot)
		return nil, err
	}
	lease = cache.leaseFor(slot)
	return lease, nil
}

// AcquireReplicatedPointRead is the point-read counterpart. Point bytes are
// copied only into the fresh transaction state and are released on Finish.
func (a *ReplicatedApply) AcquireReplicatedPointRead(
	ctx context.Context,
	relation replication.RelationID,
	keyBytes []byte,
	found bool,
	raw []byte,
	primaryPath []byte,
	text string,
	parameterTypes []ParamType,
	partialAggregate bool,
	options query.ExecOptions,
) (*ReplicatedReadLease, error) {
	if partialAggregate || !replicatedReadReuseTextBound(text) ||
		!replicatedReadReuseParamTypesBound(parameterTypes) ||
		len(primaryPath) > replicatedReadReuseMaxPath {
		return nil, ErrReplicatedReadReuseUnsupported
	}
	key, err := a.readReuseKey(
		relation, string(primaryPath), text, parameterTypes, [32]byte{},
	)
	if err != nil {
		return nil, err
	}
	cache := a.replicatedReadReuseCache()
	if cache == nil {
		return nil, ErrReplicatedApplyClosed
	}
	slot, lease, err := cache.reserve(key)
	if err != nil {
		return nil, err
	}
	var pendingReader *ReplicatedReadSession
	defer func() {
		if cause := recover(); cause != nil {
			reader := cache.retire(slot)
			if pendingReader != reader {
				closeReplicatedReadSlot(pendingReader)
			}
			closeReplicatedReadSlot(reader)
			panic(cause)
		}
	}()
	if lease != nil {
		reader, constructErr := a.newPointReadSessionInto(
			ctx, relation, keyBytes, found, raw, primaryPath, options, slot.reader,
		)
		if constructErr != nil {
			_ = cache.finish(lease, constructErr)
			return nil, constructErr
		}
		if reader != slot.reader {
			_ = cache.finish(lease, ErrReplicatedReadLeaseClosed)
			return nil, ErrReplicatedReadReuseUnsupported
		}
		if _, bindErr := a.bindReplicatedReadReuseKey(key, slot.prepared); bindErr != nil {
			_ = cache.finish(lease, bindErr)
			return nil, bindErr
		}
		return lease, nil
	}

	reader, err := a.newPointReadSessionInto(
		ctx, relation, keyBytes, found, raw, primaryPath, options, nil,
	)
	if err != nil {
		cache.cancelReservation(slot)
		return nil, err
	}
	pendingReader = reader
	prepared, err := prepareReplicatedRead(
		ctx, reader, text, parameterTypes,
	)
	if err != nil {
		_ = reader.Close()
		cache.cancelReservation(slot)
		return nil, err
	}
	if !prepared.statement.replicatedReadReuseEligible() {
		if err := cache.install(slot, key, reader, prepared, false); err != nil {
			_ = reader.Close()
			cache.cancelReservation(slot)
			return nil, err
		}
		return cache.leaseFor(slot), nil
	}
	_, bindErr := a.bindReplicatedReadReuseKey(key, prepared)
	if bindErr != nil && !errors.Is(bindErr, ErrReplicatedReadReuseUnsupported) {
		_ = reader.Close()
		cache.cancelReservation(slot)
		return nil, bindErr
	}
	if err := cache.install(slot, key, reader, prepared, bindErr == nil); err != nil {
		_ = reader.Close()
		cache.cancelReservation(slot)
		return nil, err
	}
	lease = cache.leaseFor(slot)
	return lease, nil
}

func setReadPreparationCancellation(parser *sqlast.Parser, ctx context.Context, cancel *query.CancelFlag) {
	if ctx.Done() == nil && cancel == nil {
		return
	}
	parser.SetCancellationCheck(func() error {
		if err := contextCheckpoint(ctx); err != nil {
			return err
		}
		if cancel != nil && cancel.Canceled() {
			return query.ErrCanceled
		}
		return nil
	})
}

func prepareReplicatedRead(
	ctx context.Context, reader *ReplicatedReadSession, text string,
	parameterTypes []ParamType,
) (*Prepared, error) {
	reader.conn.retainReadPreparation = true
	defer func() { reader.conn.retainReadPreparation = false }()
	if len(parameterTypes) == 0 {
		return reader.Prepare(ctx, text)
	}
	return reader.PrepareWithParameterTypes(ctx, text, parameterTypes)
}

func (c *replicatedReadReuseCache) reserve(
	key replicatedReadReuseKey,
) (*replicatedReadReuseSlot, *ReplicatedReadLease, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, ErrReplicatedApplyClosed
	}
	for i := range c.slots {
		slot := &c.slots[i]
		if !slot.active && slot.reader != nil && slot.key.equal(key) {
			c.hits++
			c.nextGen++
			if c.nextGen == 0 {
				c.nextGen++
			}
			slot.active, slot.generation = true, c.nextGen
			lease := &ReplicatedReadLease{
				cache: c, slot: slot, generation: slot.generation,
			}
			slot.lease = lease
			c.mu.Unlock()
			return slot, lease, nil
		}
	}
	for i := range c.slots {
		slot := &c.slots[i]
		if slot.reader == nil && !slot.active {
			c.misses++
			c.nextGen++
			if c.nextGen == 0 {
				c.nextGen++
			}
			slot.active, slot.generation, slot.key = true, c.nextGen, key
			c.mu.Unlock()
			return slot, nil, nil
		}
	}
	// A schema/layout publication can leave an idle slot with the same SQL
	// text but a stale physical identity. Reclaim one such bounded slot so a
	// full fixed cache recovers without routing the shape forever to the cold
	// path. Close the old session after releasing cache.mu.
	for i := range c.slots {
		slot := &c.slots[i]
		if slot.active || slot.reader == nil ||
			slot.key.sql != key.sql || slot.key.relation != key.relation {
			continue
		}
		oldReader := slot.reader
		c.detachSlotLocked(slot)
		c.misses++
		c.nextGen++
		if c.nextGen == 0 {
			c.nextGen++
		}
		slot.active, slot.generation, slot.key = true, c.nextGen, key
		c.mu.Unlock()
		closeReplicatedReadSlot(oldReader)
		return slot, nil, nil
	}
	c.mu.Unlock()
	return nil, nil, ErrReplicatedReadReuseUnsupported
}

func (c *replicatedReadReuseCache) leaseFor(
	slot *replicatedReadReuseSlot,
) *ReplicatedReadLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextGen++
	if c.nextGen == 0 {
		c.nextGen++
	}
	slot.active, slot.generation = true, c.nextGen
	lease := &ReplicatedReadLease{cache: c, slot: slot, generation: slot.generation}
	slot.lease = lease
	return lease
}

func (c *replicatedReadReuseCache) install(
	slot *replicatedReadReuseSlot,
	key replicatedReadReuseKey,
	reader *ReplicatedReadSession,
	prepared *Prepared,
	cacheable bool,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || slot == nil || !slot.active || slot.reader != nil {
		return ErrReplicatedReadReuseUnsupported
	}
	slot.key = key
	slot.reader, slot.prepared = reader, prepared
	slot.cacheable = cacheable
	return nil
}

func (c *replicatedReadReuseCache) cancelReservation(
	slot *replicatedReadReuseSlot,
) {
	c.mu.Lock()
	if slot != nil && slot.active && slot.reader == nil {
		slot.active, slot.lease, slot.key = false, nil, replicatedReadReuseKey{}
	}
	c.mu.Unlock()
}

func (c *replicatedReadReuseCache) finish(
	lease *ReplicatedReadLease,
	executionErr error,
) (err error) {
	if lease == nil || lease.cache != c || lease.slot == nil {
		return nil
	}
	c.mu.Lock()
	if !lease.slot.active || lease.slot.lease != lease ||
		lease.slot.generation != lease.generation {
		c.mu.Unlock()
		return ErrReplicatedReadLeaseClosed
	}
	slot := lease.slot
	// Invalidate the generation before any cleanup can call back into the
	// cache. The slot remains active until the cleanup decision is installed.
	slot.lease = nil
	lease.slot = nil
	lease.cache = nil
	c.mu.Unlock()
	settled := false
	retire := func() {
		reader := c.retire(slot)
		settled = true
		closeReplicatedReadSlot(reader)
	}
	defer func() {
		if !settled {
			retire()
		}
	}()

	if executionErr == nil {
		if reader := slot.reader; reader != nil && reader.session.current != nil {
			executionErr = reader.session.current.Close()
		}
		if slot.reader != nil && slot.reader.conn.open {
			executionErr = errors.Join(executionErr, ErrCursorOpen)
		}
	}
	if executionErr == nil && !slot.cacheable {
		retire()
		return nil
	}
	if executionErr == nil {
		executionErr = detachReplicatedReadSlot(slot)
		if errors.Is(executionErr, ErrReplicatedReadReuseUnsupported) {
			// A stricter reset helper may decline an otherwise valid SQL read.
			// Retention eligibility must never turn successful SQL into an error.
			retire()
			return nil
		}
	}

	if executionErr != nil {
		retire()
		return executionErr
	}
	retained, ok := retainedReplicatedReadSlotBytes(slot)
	if !ok || retained > replicatedReadReuseMaxBytes {
		retire()
		return nil
	}
	c.mu.Lock()
	if c.closed || c.retained-slot.retainedByte > replicatedReadReuseMaxBytes-retained {
		c.mu.Unlock()
		retire()
		return nil
	}
	c.retained += retained - slot.retainedByte
	slot.retainedByte = retained
	slot.active = false
	settled = true
	c.mu.Unlock()
	return nil
}

// detachReplicatedReadSlot runs without cache locks. It is the ownership
// boundary that drops every request/argument/point/rowset/Exec buffer while
// preserving the parser, typed metadata, and prepared wrapper.
func detachReplicatedReadSlot(slot *replicatedReadReuseSlot) error {
	if slot == nil || slot.reader == nil || slot.prepared == nil ||
		slot.prepared.statement == nil {
		return ErrReplicatedReadReuseUnsupported
	}
	if err := slot.prepared.statement.resetReadForReuse(); err != nil {
		return err
	}
	return slot.reader.resetForReadReuse()
}

func retainedReplicatedReadSlotBytes(
	slot *replicatedReadReuseSlot,
) (int64, bool) {
	if slot == nil || slot.reader == nil || slot.prepared == nil ||
		slot.prepared.statement == nil {
		return 0, false
	}
	retained, ok := slot.prepared.statement.readReuseRetainedBytes()
	if !ok {
		return 0, false
	}
	if len(slot.reader.session.prepared) != 1 {
		return 0, false
	}
	if _, exists := slot.reader.session.prepared[slot.prepared]; !exists {
		return 0, false
	}
	// The private map is created once with exactly one Prepared and never
	// grows. 1 KiB conservatively covers its header and single runtime group.
	// Charge the entire fixed cache per slot too, so even one parked slot
	// includes its control storage. No Exec workspace survives detachment.
	const fixed = int64(unsafe.Sizeof(ReplicatedReadSession{})) +
		int64(unsafe.Sizeof(Prepared{})) + int64(unsafe.Sizeof(replicatedReadReuseCache{})) +
		int64(unsafe.Sizeof(catalogLayoutIdentity{})) + 1024
	if retained > math.MaxInt64-fixed {
		return 0, false
	}
	retained += fixed
	add := func(value int64) bool {
		if value < 0 || retained > math.MaxInt64-value {
			return false
		}
		retained += value
		return true
	}
	if !add(int64(len(slot.key.sql))) || !add(int64(len(slot.key.primaryPath))) {
		return 0, false
	}
	paramBytes, overflow := boundedCapacityBytes(
		cap(slot.key.paramTypes), unsafe.Sizeof(ParamType(0)),
	)
	if overflow || !add(paramBytes) {
		return 0, false
	}
	return retained, true
}

func (c *replicatedReadReuseCache) detachSlotLocked(
	slot *replicatedReadReuseSlot,
) {
	if slot == nil {
		return
	}
	if slot.retainedByte > 0 {
		c.retained -= slot.retainedByte
		if c.retained < 0 {
			c.retained = 0
		}
	}
	c.evictions++
	slot.active = false
	slot.generation = 0
	slot.lease = nil
	slot.key = replicatedReadReuseKey{}
	slot.reader = nil
	slot.prepared = nil
	slot.cacheable = false
	slot.retainedByte = 0
}

func (c *replicatedReadReuseCache) retire(
	slot *replicatedReadReuseSlot,
) *ReplicatedReadSession {
	if c == nil || slot == nil {
		return nil
	}
	c.mu.Lock()
	reader := slot.reader
	c.detachSlotLocked(slot)
	c.mu.Unlock()
	return reader
}

func closeReplicatedReadSlot(reader *ReplicatedReadSession) {
	if reader != nil {
		// A panic can occur after queryRows opens the internal rows but before
		// Prepared publishes a Cursor in Session.current. Close that state too,
		// otherwise conn.Close refuses to release its executor and transaction.
		_ = reader.conn.rowset.Close()
		_ = reader.Close()
	}
}

func (c *replicatedReadReuseCache) shutdown() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	var retire [replicatedReadReuseSlotCount]*ReplicatedReadSession
	count := 0
	for i := range c.slots {
		slot := &c.slots[i]
		if slot.active || slot.reader == nil {
			continue
		}
		retire[count], count = slot.reader, count+1
		c.detachSlotLocked(slot)
	}
	c.mu.Unlock()
	for i := 0; i < count; i++ {
		closeReplicatedReadSlot(retire[i])
	}
}

func (a *ReplicatedApply) readReuseKey(
	relation replication.RelationID,
	primaryPath, text string,
	parameterTypes []ParamType,
	manifest [32]byte,
) (replicatedReadReuseKey, error) {
	if a == nil || a.database == nil {
		return replicatedReadReuseKey{}, ErrReplicatedApplyClosed
	}
	if len(text) == 0 || len(text) > replicatedReadReuseMaxSQL {
		return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
	}
	if len(parameterTypes) > replicatedReadReuseMaxParams {
		return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return replicatedReadReuseKey{}, err
	}
	base := a.database.catalog.ReplicatedShardStore
	if base == nil {
		return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
	}
	if relation == 0 || layoutIdentityToken(a.database.layoutEpoch) == nil {
		return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
	}
	var table *table
	if relation != 0 {
		if int(relation) > len(base.Relations) {
			return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
		}
		if base.Relations[int(relation)-1].Table != base.UserTable {
			return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
		}
		table = a.database.tables[base.Relations[int(relation)-1].Table]
		if table == nil || table.meta == nil || table.meta.PrimaryKey != primaryPath {
			return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
		}
	}
	if manifest == ([32]byte{}) {
		var err error
		manifest, err = a.machine.RelationManifestDigest()
		if err != nil {
			return replicatedReadReuseKey{}, err
		}
	}
	return replicatedReadReuseKey{
		sql:        strings.Clone(text),
		paramTypes: append([]ParamType(nil), parameterTypes...),
		relation:   relation, primaryPath: strings.Clone(primaryPath),
		schemaGeneration: uint64(base.Binding.Authority.SchemaGeneration),
		allocationGen:    uint64(base.Binding.AllocationGeneration),
		layoutToken:      layoutIdentityToken(a.database.layoutEpoch),
		manifestDigest:   manifest,
	}, nil
}

func (a *ReplicatedApply) readReuseDataKey(
	text string, parameterTypes []ParamType, manifest [32]byte,
) (replicatedReadReuseKey, error) {
	if a == nil || a.database == nil {
		return replicatedReadReuseKey{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	base := a.database.catalog.ReplicatedShardStore
	var relation replication.RelationID
	var primaryPath string
	if base != nil {
		for ordinal := range base.Relations {
			if base.Relations[ordinal].Table == base.UserTable {
				relation = replication.RelationID(ordinal + 1)
				if table := a.database.tables[base.UserTable]; table != nil && table.meta != nil {
					primaryPath = table.meta.PrimaryKey
				}
				break
			}
		}
	}
	a.database.mu.RUnlock()
	if relation == 0 || primaryPath == "" {
		return replicatedReadReuseKey{}, ErrReplicatedReadReuseUnsupported
	}
	key, err := a.readReuseKey(relation, primaryPath, text, parameterTypes, manifest)
	if err != nil {
		return key, err
	}
	return key, nil
}

// bindReplicatedReadReuseKey records the physical relation proved by the
// prepared statement. The first lane admits only the apply claim's primary
// user relation, so a data read can carry an exact relation identity in its
// cache key before preparation.
func (a *ReplicatedApply) bindReplicatedReadReuseKey(
	key replicatedReadReuseKey, prepared *Prepared,
) (replicatedReadReuseKey, error) {
	if prepared == nil || prepared.statement == nil || prepared.statement.query == nil ||
		prepared.session == nil || prepared.session.conn == nil {
		return key, ErrReplicatedReadReuseUnsupported
	}
	connection := prepared.session.conn
	if connection.db != a.database || connection.tx == nil ||
		key.layoutToken == nil || layoutIdentityToken(connection.tx.layoutEpoch) != key.layoutToken {
		return key, ErrReplicatedReadReuseUnsupported
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return key, err
	}
	base := a.database.catalog.ReplicatedShardStore
	if base == nil || layoutIdentityToken(a.database.layoutEpoch) != key.layoutToken ||
		uint64(base.Binding.Authority.SchemaGeneration) != key.schemaGeneration ||
		uint64(base.Binding.AllocationGeneration) != key.allocationGen ||
		key.relation == 0 || int(key.relation) > len(base.Relations) ||
		base.Relations[int(key.relation)-1].Table != base.UserTable ||
		prepared.statement.query.Collection() != base.UserTable ||
		preparedReadPrimaryPath(prepared.statement) != key.primaryPath {
		return key, ErrReplicatedReadReuseUnsupported
	}
	// Never relabel an old preparation with a newly published epoch. Both the
	// attached transaction and the currently published layout must match the
	// identity captured before preparation/lookup.
	return key, nil
}

func preparedReadPrimaryPath(statement *stmt) string {
	if statement == nil {
		return ""
	}
	if statement.primaryPoint {
		return statement.pointPath
	}
	if statement.primaryRange != nil {
		return statement.primaryRange.path
	}
	return ""
}

func layoutIdentityToken(epoch *catalogLayoutEpoch) *catalogLayoutIdentity {
	if epoch == nil {
		return nil
	}
	return epoch.identity
}

func cutFenceDigest(cut *replicatedstate.DataReadCut) [32]byte {
	if cut == nil {
		return [32]byte{}
	}
	return cut.Fence().RelationManifestDigest
}

func replicatedReadReuseTextBound(text string) bool {
	return len(text) != 0 && len(text) <= replicatedReadReuseMaxSQL
}

func replicatedReadReuseParamTypesBound(types []ParamType) bool {
	if len(types) > replicatedReadReuseMaxParams {
		return false
	}
	for _, typ := range types {
		if typ >= ParamTypeInvalid {
			return false
		}
	}
	return true
}

func (s *stmt) replicatedReadReuseEligible() bool {
	if s == nil || s.closed || s.parser == nil || s.tree == nil ||
		s.tree.Kind != sqlast.KindSelect || s.tree.Select == nil ||
		s.tree.Explain || s.tree.Analyze || s.query == nil ||
		s.query.RequiresCatalog() || s.query.NumJoins() != 0 ||
		s.views != nil || s.catalogJoin || s.insertSource != nil || s.dependencies != nil ||
		s.mutation != nil || !serializableDirectRelationSelect(
		s.tree.Select, s.query.Collection(),
	) {
		return false
	}
	selectTree := s.tree.Select
	if selectTree.Distinct || len(selectTree.GroupBy) != 0 ||
		selectTree.Having != nil || len(selectTree.Windows) != 0 ||
		selectTree.Offset != nil || len(selectTree.From) != 1 {
		return false
	}
	for i := range selectTree.Columns {
		column := &selectTree.Columns[i]
		if column.Path == nil || column.Scalar != nil || column.Window != nil ||
			column.Agg != sqlast.AggNone || len(column.Path.Segments) == 0 ||
			len(column.Path.AppendPointer(nil)) > replicatedReadReuseMaxPath {
			return false
		}
	}
	for i := range selectTree.OrderBy {
		term := &selectTree.OrderBy[i]
		if term.Scalar != nil || term.Path == nil || term.Output != 0 ||
			len(term.Path.AppendPointer(nil)) > replicatedReadReuseMaxPath {
			return false
		}
	}
	if s.primaryPoint {
		// A point source has at most one row and therefore does not need a
		// literal LIMIT. Parameterized limits remain unsupported below.
	} else if s.primaryRange == nil || !s.primaryRange.coversPredicate {
		return false
	}
	if selectTree.Limit != nil {
		if selectTree.Limit.Kind != sqlast.OperandNumber {
			return false
		}
		limit, err := strconv.ParseInt(selectTree.Limit.Text, 10, 64)
		if err != nil || limit < 0 || limit > replicatedReadReuseMaxLimit {
			return false
		}
	} else if !s.primaryPoint {
		return false
	}
	return true
}

func (s *stmt) resetReadForReuse() error {
	if !s.replicatedReadReuseEligible() {
		return ErrReplicatedReadReuseUnsupported
	}
	if s.query == nil {
		return ErrReplicatedReadReuseUnsupported
	}
	if s.parser != nil {
		// The parser's cancellation hook belongs to the request context. A
		// parked preparation cannot retain it or the context graph it closes
		// over. The parser helper validates that the lexer hook is gone too.
		s.parser.SetCancellationCheck(nil)
	}
	queryBytes, ok := s.query.ResetReadBindingsForReuse()
	if !ok {
		return ErrReplicatedReadReuseUnsupported
	}
	if s.primaryRange != nil {
		// The program originally borrows a catalog primary-path string. Its
		// idle copy must own only these bytes, not a larger catalog source.
		s.primaryRange.path = strings.Clone(s.primaryRange.path)
	}
	s.reuseRetainedBytes = queryBytes
	s.closed = false
	return nil
}

func (s *stmt) readReuseRetainedBytes() (int64, bool) {
	if s == nil || s.parser == nil || s.tree == nil || s.query == nil {
		return 0, false
	}
	parserBytes, ok := s.parser.ReadPreparationRetainedBytes()
	if !ok || parserBytes < 0 {
		return 0, false
	}
	queryBytes := s.reuseRetainedBytes
	if queryBytes < 0 {
		return 0, false
	}
	if parserBytes > math.MaxInt64-queryBytes {
		return 0, false
	}
	total := parserBytes + queryBytes
	add := func(value int64) bool {
		if value < 0 || total > math.MaxInt64-value {
			return false
		}
		total += value
		return true
	}
	addCapacity := func(count int, element uintptr) bool {
		bytes, overflow := boundedCapacityBytes(count, element)
		return !overflow && add(bytes)
	}
	if !add(int64(unsafe.Sizeof(*s))) || !add(int64(unsafe.Sizeof(*s.tree))) ||
		!add(int64(len(s.text))) || !add(int64(len(s.pointPath))) ||
		!addCapacity(cap(s.paramKinds), unsafe.Sizeof(ParamKind(0))) ||
		!addCapacity(cap(s.paramTypes), unsafe.Sizeof(ParamType(0))) ||
		!addCapacity(cap(s.paramPositions), unsafe.Sizeof(int(0))) ||
		!addCapacity(cap(s.paramTypePositions), unsafe.Sizeof(int(0))) ||
		!addCapacity(cap(s.paramTypeTargetDefaults), unsafe.Sizeof(bool(false))) {
		return 0, false
	}
	if s.primaryRange != nil {
		termsBytes, overflow := boundedCapacityBytes(
			cap(s.primaryRange.terms), unsafe.Sizeof(primaryRangeTerm{}),
		)
		if overflow || !add(int64(unsafe.Sizeof(*s.primaryRange))) ||
			!add(termsBytes) || !add(int64(len(s.primaryRange.path))) {
			return 0, false
		}
	}
	return total, true
}

func boundedCapacityBytes(count int, element uintptr) (int64, bool) {
	if count < 0 || element == 0 {
		return 0, count < 0
	}
	if uint64(count) > uint64(math.MaxInt64)/uint64(element) {
		return 0, true
	}
	return int64(uint64(count) * uint64(element)), false
}

// resetForReadReuse drops request and execution storage while retaining the
// database handle and the address used by a prepared statement.
func (reader *ReplicatedReadSession) resetForReadReuse() error {
	if reader == nil || reader.conn.db == nil {
		return ErrReplicatedReadLeaseClosed
	}
	if reader.session.current != nil || reader.conn.open {
		return ErrCursorOpen
	}
	if reader.conn.tx != nil {
		if err := reader.conn.tx.Rollback(); err != nil {
			return err
		}
		reader.conn.tx = nil
	}
	reader.session.state = SessionIdle
	return reader.conn.resetForReadReuse()
}

func (c *conn) resetForReadReuse() error {
	if c == nil {
		return nil
	}
	if c.open {
		return ErrCursorOpen
	}
	var err error
	c.exec.Release()
	c.exec = query.Exec{}
	err = errors.Join(err, c.joinSnapshot.Close(), c.insertSnapshot.Close())
	c.args = nil
	c.pointDocs = store.Segment{}
	c.pointSource = query.ValidatedRawSource{}
	c.pointRaw = nil
	c.pointKeyRaw = nil
	c.pointKeyEnds = nil
	c.pointKeys = nil
	c.fileRange = query.FileRangeSource{}
	c.matchKeys = nil
	c.insertSeeds = nil
	c.insertKeyRaw = nil
	c.insertSeen = nil
	c.insertTape = nil
	c.joinCatalog = nil
	c.joinSnapshot = durable.DatabaseSnapshot{}
	c.insertSnapshot = durable.Snapshot{}
	c.rowset = rows{closed: true}
	c.tx = nil
	c.routing = nil
	c.open = false
	c.closed = false
	return err
}
