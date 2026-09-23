package gatewayruntime

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
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
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
	unknown  bool
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
	// admissionWindow bounds waiting for catalog convergence after
	// pre-admission refusals; zero uses preAdmissionWriteRecoveryWindow.
	admissionWindow time.Duration
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

// ownPostgresWriteQuery takes ownership of the mutable portions of a PG bind
// request without routing it through JSON. The pgwire decoder reuses its
// parameter buffers after Write returns, while SQL is an immutable Go string.
// Keeping the same validation boundary as the journal writer makes this a
// byte-for-byte semantic replacement for the old marshal/unmarshal copy on
// the direct hot path.
func ownPostgresWriteQuery(q gateway.Query) (gateway.Query, error) {
	owned := q
	if len(q.Params) != 0 {
		owned.Params = slices.Clone(q.Params)
		for index := range owned.Params {
			owned.Params[index].Bytes = bytes.Clone(q.Params[index].Bytes)
		}
	}
	// ParamTypes is json:",omitempty" in the legacy journal. A present but
	// empty slice therefore round-trips as nil; preserve that normalization so
	// the typed fast path has exactly the same version/error behavior.
	if len(q.ParamTypes) != 0 {
		owned.ParamTypes = slices.Clone(q.ParamTypes)
	} else {
		owned.ParamTypes = nil
	}
	if _, err := postgresWriteJournalVersion(&owned); err != nil {
		return gateway.Query{}, err
	}
	return owned, nil
}

// postgresWriteQueryNeedsSizeCheck is a cheap conservative upper bound for
// JSON escaping. Small requests, which dominate the direct workload, skip a
// second serialization entirely. Large or heavily escaped input still takes
// the exact legacy size check before planning, so the journal bound is kept.
func postgresWriteQueryNeedsSizeCheck(q gateway.Query) bool {
	upper := uint64(256) + uint64(len(q.SQL))*6 + uint64(len(q.ParamTypes))*16
	for _, param := range q.Params {
		upper += 64 + uint64(len(param.Bytes))*6
		if upper > maxPostgreSQLWriteJournalBytes/2 {
			return true
		}
	}
	return upper > maxPostgreSQLWriteJournalBytes/2
}

func (p *postgresDirectPool) resolve(ctx context.Context, slot *postgresDirectSlot) (*gateway.Result, error) {
	pending := slot.pending
	region := trace.StartRegion(ctx, "pg.direct.execute")
	result, err := p.prepared.ExecutePreparedDirectBatch(
		ctx, p.record.Authority, pending.identity, pending.queries, pending.plan, pending.unknown,
	)
	region.End()
	if errors.Is(err, gateway.ErrDurableSQLAborted) {
		if os.Getenv("VIBEDB_RF3_DIAGNOSTIC_ABORT_REASON") == "1" {
			queryDigest := sha256.Sum256([]byte(pending.queries[0].SQL))
			fmt.Fprintf(os.Stderr,
				"VIBEDB_RF3_DIRECT_ABORT_ATTEMPT time=%s issuer_sequence=%d request_id=%x installation=%x epoch=%d issuer_lane=%d pool_lane=%d prior_unknown=%t group=%v allocation_generation=%d query_sha256=%x error=%q\n",
				time.Now().UTC().Format(time.RFC3339Nano), pending.identity.IssuerSequence, pending.identity.RequestID,
				pending.identity.Reference.Installation, pending.identity.Reference.Epoch,
				pending.identity.Reference.LaneOrdinal, slot.index, pending.unknown,
				pending.plan.Target.Route.Group, pending.plan.Target.Route.AllocationGeneration,
				queryDigest, err)
		}
		slot.pending = nil
		return nil, err
	}
	if err == nil && (!result.Direct || result.Result == nil || result.Ack != (durableExecBatchAckWireRequest{})) {
		err = errInvalidDurableRequestAdapter
	}
	if err != nil {
		if !pending.unknown && errors.Is(err, gateway.ErrDurableSQLNotAdmitted) {
			slot.pending = nil
			return nil, err
		}
		pending.unknown = true
		return nil, postgresDirectUnknown(pending.identity.RequestID, err)
	}
	slot.pending = nil
	return result.Result, nil
}

// Preparation is read-only. Admission backpressure or a changing leader can
// therefore be retried without creating an ambiguous write. Wait instead of
// spinning through every replica while the leader's response budget is full.
func (p *postgresDirectPool) prepare(ctx context.Context, id durableExecBatchIdentity, queries []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
	return p.prepareWithServingFenceWindow(ctx, id, queries, 10*time.Second)
}

func (p *postgresDirectPool) prepareWithServingFenceWindow(
	ctx context.Context,
	id durableExecBatchIdentity,
	queries []gateway.Query,
	servingFenceWindow time.Duration,
) (*gateway.DurableSQLDirectPlan, error) {
	var servingFenceUntil time.Time
	transientAttempt := 0
	for {
		plan, err := p.prepared.PrepareDirectBatch(ctx, p.record.Authority, id, queries)
		if err == nil {
			return plan, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(err, ctxErr)
		}
		if errors.Is(err, gateway.ErrReplicatedUnauthorized) ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return plan, err
		}
		if postgresDirectServingFenceRetry(err) {
			if servingFenceUntil.IsZero() {
				servingFenceUntil = time.Now().Add(servingFenceWindow)
			}
			remaining := time.Until(servingFenceUntil)
			if remaining <= 0 {
				return plan, err
			}
			delay := min(100*time.Millisecond, remaining)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, errors.Join(err, ctx.Err())
			case <-timer.C:
				if time.Until(servingFenceUntil) <= 0 {
					return plan, err
				}
			}
			continue
		}
		if transientAttempt == 7 ||
			errors.Is(err, gateway.ErrReplicatedRoute) ||
			!(errors.Is(err, raftmodel.ErrAdmissionBound) || errors.Is(err, gateway.ErrReplicatedLeader) || errors.Is(err, gateway.ErrReplicatedReadBehind)) {
			return plan, err
		}
		timer := time.NewTimer(time.Duration(1<<min(transientAttempt, 4)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		transientAttempt++
	}
}

func postgresDirectServingFenceRetry(err error) bool {
	return err != nil && errors.Is(err, gateway.ErrDurableSQLNotAdmitted) &&
		errors.Is(err, raftservice.ErrServingFence) &&
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) &&
		!errors.Is(err, gateway.ErrReplicatedUnauthorized) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
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
	owned, err := ownPostgresWriteQuery(q)
	if err != nil {
		return nil, true, err
	}
	if postgresWriteQueryNeedsSizeCheck(owned) {
		raw, marshalErr := vibejson.Marshal(&owned)
		if marshalErr != nil {
			return nil, true, marshalErr
		}
		if len(raw) > maxPostgreSQLWriteJournalBytes/2 {
			return nil, true, gateway.ErrTransactionByteLimit
		}
	}
	queries := []gateway.Query{owned}
	const maxAbortedWrites = 8
	var abortedWrites, admissionRefusals int
	// Pre-admission refusals persist until the catalog publication for an
	// applied membership or ownership transition reaches this gateway; wait for
	// that convergence within a time budget, not a fixed number of probes.
	var admissionDeadline time.Time
	for {
		if err = ctx.Err(); err != nil {
			return nil, true, err
		}
		id, err := p.identity(ctx, slot)
		if err != nil {
			return nil, true, err
		}
		region := trace.StartRegion(ctx, "pg.direct.prepare")
		plan, err := p.prepare(ctx, id, queries)
		region.End()
		if err != nil {
			return nil, !errors.Is(err, gateway.ErrDurableSQLDirectIneligible), err
		}
		if plan == nil {
			return nil, true, errInvalidDurableRequestAdapter
		}
		slot.pending = &postgresDirectPending{identity: id, queries: queries, plan: plan}
		result, err := p.resolve(ctx, slot)
		aborted := errors.Is(err, gateway.ErrDurableSQLAborted)
		retryAborted := aborted && postgresDirectAbortRetry(err)
		retryAdmission := slot.pending == nil && postgresDirectPreAdmissionRetry(err)
		if aborted && !retryAborted {
			return nil, true, err
		}
		if !retryAborted && !retryAdmission {
			return result, true, err
		}
		if retryAborted {
			abortedWrites++
			if abortedWrites == maxAbortedWrites {
				return nil, true, err
			}
			continue
		}
		if admissionRefusals == 0 {
			window := p.admissionWindow
			if window <= 0 {
				window = preAdmissionWriteRecoveryWindow
			}
			admissionDeadline = time.Now().Add(window)
		} else if !time.Now().Before(admissionDeadline) {
			return nil, true, err
		}
		timer := time.NewTimer(preAdmissionWriteRetryDelay(admissionRefusals))
		admissionRefusals++
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, true, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		// Never mint a new identity for a write the cluster may already hold
		// when the original one can still settle it. While the logical route
		// is unchanged (a replica move or ownership advance, not a split),
		// re-drive this exact identity through direct recovery: the target's
		// durable transaction control returns the retained outcome if the
		// refused-looking attempt was in fact admitted, or applies it once.
		// A new identity on the same route would turn such an attempt into a
		// duplicate write (an IndexConflict on the client's own row).
		if current, prepareErr := p.prepare(ctx, id, queries); prepareErr == nil && current != nil &&
			gateway.DirectMutationRecoverableRoute(plan.Target.Route, current.Target.Route) {
			slot.pending = &postgresDirectPending{identity: id, queries: queries, plan: plan, unknown: true}
			for {
				result, err = p.resolve(ctx, slot)
				if err == nil || !postgresDirectRecoveryRefused(err) || !time.Now().Before(admissionDeadline) {
					break
				}
				// Every retry reuses this identity, so waiting for the route to
				// converge cannot duplicate the write.
				timer := time.NewTimer(preAdmissionWriteRetryDelay(admissionRefusals))
				admissionRefusals++
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, true, errors.Join(err, ctx.Err())
				case <-timer.C:
				}
			}
			if !errors.Is(err, gateway.ErrDurableSQLAborted) {
				return result, true, err
			}
			if !postgresDirectAbortRetry(err) {
				return nil, true, err
			}
			abortedWrites++
			if abortedWrites == maxAbortedWrites {
				return nil, true, err
			}
		}
	}
}

// postgresDirectRecoveryRefused reports that a retained-identity recovery
// attempt was itself refused before admission because the route has not yet
// converged. The earlier attempt's outcome is still unknown, but retrying the
// same identity is safe.
func postgresDirectRecoveryRefused(err error) bool {
	return err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) &&
		(errors.Is(err, raftservice.ErrServingFence) || errors.Is(err, raftserve.ErrProposalRefused)) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func postgresDirectAbortRetry(err error) bool {
	code, ok := gateway.DurableSQLAbortResultCode(err)
	return ok && (code == replicatedstate.ResultIntentBusy ||
		code == replicatedstate.ResultTransactionConflict)
}

func postgresDirectPreAdmissionRetry(err error) bool {
	return err != nil && errors.Is(err, gateway.ErrDurableSQLNotAdmitted) &&
		!errors.Is(err, durable.ErrCommitOutcomeUnknown) &&
		(errors.Is(err, raftservice.ErrServingFence) || errors.Is(err, raftserve.ErrProposalRefused))
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
