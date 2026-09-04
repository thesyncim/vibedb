package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const postgresDirectLanes = 16
const postgresDirectReservation = uint64(65536)

type postgresDirectReservationLane struct {
	Installation    replication.ID128
	ReservedThrough uint64
}
type postgresDirectReservationRecord struct {
	Version   uint32
	Authority serviceauthz.Authority
	Lanes     []postgresDirectReservationLane
}
type postgresDirectPending struct {
	identity durableExecBatchIdentity
	queries  []gateway.Query
	plan     *gateway.DurableSQLDirectPlan
}
type postgresDirectSlot struct {
	index       int
	next, limit uint64
	reference   gateway.ReplicatedIssuerReference
	pending     *postgresDirectPending
}

// Only single-Raft-group autocommit requests use this pool. Direct issuer
// sequences may skip values; coordinated ledger issuers must never use it.
// Before accepting work, reserve a new block durably for every slot. Restart
// abandons the old blocks, including identities whose outcomes were unknown.
// Such PG clients must verify their outcome; reconnect is not an idempotency key.
// A live slot retains the exact command until its terminal result is known.
// No acknowledged data depends on this file: the data quorum commits durably.
type postgresDirectPool struct {
	mu       sync.Mutex
	path     string
	lock     *os.File
	record   postgresDirectReservationRecord
	poison   error
	service  postgresDurableService
	prepared postgresPreparedDirectService
	slots    chan *postgresDirectSlot
}

func openPostgresDirectPool(path string, authority serviceauthz.Authority, service postgresDurableService) (_ *postgresDirectPool, err error) {
	prepared, ok := service.(postgresPreparedDirectService)
	if !ok || !authority.Valid() || path == "" {
		return nil, errInvalidDurableRequestAdapter
	}
	for _, name := range []string{path, path + ".lock"} {
		info, e := os.Lstat(name)
		if e == nil && !info.Mode().IsRegular() {
			return nil, errInvalidDurableRequestAdapter
		}
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		return nil, err
	}
	p := &postgresDirectPool{path: path, lock: lock, service: service, prepared: prepared, slots: make(chan *postgresDirectSlot, postgresDirectLanes)}
	defer func() {
		if err != nil {
			_ = p.Close()
		}
	}()
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		p.record.Version, p.record.Authority = 1, authority
		p.record.Lanes = make([]postgresDirectReservationLane, postgresDirectLanes)
		for i := range p.record.Lanes {
			if _, err = rand.Read(p.record.Lanes[i].Installation[:]); err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	} else {
		raw, readErr := io.ReadAll(io.LimitReader(file, 65537))
		if err = errors.Join(readErr, file.Close()); err != nil {
			return nil, err
		}
		if len(raw) <= sha256.Size || len(raw) > 65536 {
			return nil, errInvalidDurableRequestAdapter
		}
		digest := sha256.Sum256(raw[sha256.Size:])
		if !bytes.Equal(raw[:sha256.Size], digest[:]) {
			return nil, errInvalidDurableRequestAdapter
		}
		if err = vibejson.Unmarshal(raw[sha256.Size:], &p.record); err != nil {
			return nil, err
		}
	}
	if p.record.Version != 1 || p.record.Authority != authority || len(p.record.Lanes) != postgresDirectLanes {
		return nil, errInvalidDurableRequestAdapter
	}
	seen := make(map[replication.ID128]bool, postgresDirectLanes)
	for i := range p.record.Lanes {
		lane := &p.record.Lanes[i]
		if lane.Installation == (replication.ID128{}) || seen[lane.Installation] || lane.ReservedThrough%postgresDirectReservation != 0 || lane.ReservedThrough >= math.MaxUint64-postgresDirectReservation {
			return nil, errInvalidDurableRequestAdapter
		}
		seen[lane.Installation] = true
		slot := &postgresDirectSlot{index: i, next: lane.ReservedThrough + 1}
		lane.ReservedThrough += postgresDirectReservation
		slot.limit = lane.ReservedThrough
		p.slots <- slot
	}
	// Slots are not visible to callers until the rename and directory sync finish.
	if err = p.save(); err != nil {
		return nil, err
	}
	return p, nil
}

// Caller owns mu (or the pool has not yet been published). Any uncertain
// publication poisons allocation until reopen skips the possibly saved block.
func (p *postgresDirectPool) save() (err error) {
	defer func() {
		if err != nil {
			p.poison = err
		}
	}()
	if p.poison != nil {
		return p.poison
	}
	raw, err := vibejson.Marshal(&p.record)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	name := p.path + ".pending"
	if info, e := os.Lstat(name); e == nil {
		if !info.Mode().IsRegular() {
			return errInvalidDurableRequestAdapter
		}
		if err = os.Remove(name); err != nil {
			return err
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if _, err = file.Write(digest[:]); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	if err = os.Rename(name, p.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(p.path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// Close is called only after the owner has drained active requests.
func (p *postgresDirectPool) Close() error {
	if p.lock == nil {
		return nil
	}
	err := errors.Join(storeio.UnlockWriter(p.lock), p.lock.Close())
	p.lock = nil
	return err
}

func (p *postgresDirectPool) identity(ctx context.Context, slot *postgresDirectSlot) (durableExecBatchIdentity, error) {
	p.mu.Lock()
	if p.poison == nil && slot.next > slot.limit {
		lane := &p.record.Lanes[slot.index]
		if lane.ReservedThrough >= math.MaxUint64-postgresDirectReservation {
			p.poison = errInvalidDurableRequestAdapter
		} else {
			lane.ReservedThrough += postgresDirectReservation
			if p.save() == nil {
				slot.limit = lane.ReservedThrough
			}
		}
	}
	err := p.poison
	installation := p.record.Lanes[slot.index].Installation
	p.mu.Unlock()
	if err != nil {
		return durableExecBatchIdentity{}, err
	}
	if slot.reference.GrantDigest == (replication.Digest{}) {
		grant, err := p.service.OpenIssuer(ctx, p.record.Authority, gateway.ReplicatedIssuerOpen{Installation: installation, Epoch: 1})
		if err != nil {
			return durableExecBatchIdentity{}, err
		}
		slot.reference = gateway.ReplicatedIssuerReference{Installation: grant.Installation, Epoch: grant.Epoch, LaneOrdinal: grant.LaneOrdinal, GrantDigest: grant.GrantDigest}
		if slot.reference.Installation != installation || slot.reference.Epoch != 1 || slot.reference.LaneOrdinal != 0 {
			slot.reference = gateway.ReplicatedIssuerReference{}
			return durableExecBatchIdentity{}, errInvalidDurableRequestAdapter
		}
	}
	id := durableExecBatchIdentity{Reference: slot.reference, IssuerSequence: slot.next}
	if _, err = rand.Read(id.RequestID[:]); err != nil {
		return id, err
	}
	if !validDurableExecBatchIdentity(id) {
		return id, errInvalidDurableRequestAdapter
	}
	slot.next++ // Reserved blocks never include MaxUint64, so this cannot wrap.
	return id, nil
}

func postgresDirectUnknown(id replication.ID128, err error) error {
	return fmt.Errorf("PostgreSQL write outcome unknown for request %x; verify database state before resubmitting: %w", id, errors.Join(durable.ErrCommitOutcomeUnknown, err))
}

func (p *postgresDirectPool) resolve(ctx context.Context, slot *postgresDirectSlot) (*gateway.Result, error) {
	pending := slot.pending
	region := trace.StartRegion(ctx, "pg.direct.execute")
	result, err := p.prepared.ExecutePreparedDirectBatch(ctx, p.record.Authority, pending.identity, pending.queries, pending.plan)
	region.End()
	if errors.Is(err, gateway.ErrDurableSQLAborted) {
		slot.pending = nil
		return nil, err
	}
	if err == nil && (!result.Direct || result.Result == nil || result.Ack != (durableExecBatchAckWireRequest{})) {
		err = errInvalidDurableRequestAdapter
	}
	if err != nil {
		return nil, postgresDirectUnknown(pending.identity.RequestID, err)
	}
	slot.pending = nil
	return result.Result, nil
}

// handled=false is possible only before a proposal. Unknown commands are never
// replanned or resubmitted under a new identity. A definitive guard abort can
// safely retry an implicit single-statement transaction using a fresh preimage.
func (p *postgresDirectPool) Write(ctx context.Context, q gateway.Query) (*gateway.Result, bool, error) {
	select {
	case <-ctx.Done():
		return nil, true, ctx.Err()
	default:
	}
	var slot *postgresDirectSlot
	select {
	case <-ctx.Done():
		return nil, true, ctx.Err()
	case slot = <-p.slots:
	}
	defer func() { p.slots <- slot }()
	if slot.pending != nil {
		if _, err := p.resolve(ctx, slot); err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) {
			return nil, true, fmt.Errorf("previous PostgreSQL write is unresolved; this statement was not executed: %w", err)
		}
	}
	raw, err := vibejson.Marshal(&q)
	if err != nil {
		return nil, true, err
	}
	if len(raw) > maxPostgreSQLWriteJournalBytes/2 {
		return nil, true, gateway.ErrTransactionByteLimit
	}
	var owned gateway.Query
	if err = vibejson.Unmarshal(raw, &owned); err != nil {
		return nil, true, err
	}
	if _, err = postgresWriteJournalVersion(&owned); err != nil {
		return nil, true, err
	}
	queries := []gateway.Query{owned}
	for attempt := 0; attempt < 8; attempt++ {
		if err = ctx.Err(); err != nil {
			return nil, true, err
		}
		id, err := p.identity(ctx, slot)
		if err != nil {
			return nil, true, err
		}
		region := trace.StartRegion(ctx, "pg.direct.prepare")
		plan, err := p.prepared.PrepareDirectBatch(ctx, p.record.Authority, id, queries)
		region.End()
		if err != nil {
			return nil, !errors.Is(err, gateway.ErrDurableSQLDirectIneligible), err
		}
		if plan == nil {
			return nil, true, errInvalidDurableRequestAdapter
		}
		slot.pending = &postgresDirectPending{identity: id, queries: queries, plan: plan}
		result, err := p.resolve(ctx, slot)
		if !errors.Is(err, gateway.ErrDurableSQLAborted) {
			return result, true, err
		}
	}
	return nil, true, gateway.ErrDurableSQLAborted
}

// A context-aware, writer-preferring table gate lets direct requests overlap
// while keeping coordinated outbox work exclusive. Backend guards and intents
// remain authoritative across gateways.
type postgresTableGate struct {
	mu               sync.Mutex
	readers, waiting int
	writer           bool
	changed          chan struct{}
}

func (g *postgresTableGate) signal() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}
func (g *postgresTableGate) acquire(ctx context.Context, exclusive bool) (func(), error) {
	g.mu.Lock()
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
	if exclusive {
		g.waiting++
	}
	for {
		if err := ctx.Err(); err != nil {
			if exclusive {
				g.waiting--
				g.signal()
			}
			g.mu.Unlock()
			return nil, err
		}
		if !g.writer && (exclusive && g.readers == 0 || !exclusive && g.waiting == 0) {
			if exclusive {
				g.waiting--
				g.writer = true
			} else {
				g.readers++
			}
			g.mu.Unlock()
			return func() {
				g.mu.Lock()
				if exclusive {
					g.writer = false
				} else {
					g.readers--
				}
				g.signal()
				g.mu.Unlock()
			}, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		g.mu.Lock()
	}
}
