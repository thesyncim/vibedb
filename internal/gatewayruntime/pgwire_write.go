package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	maxPostgreSQLWriteJournalBytes = 4 << 20
	maxPostgreSQLWriteParameters   = 1 << 16

	// Version 1 is the deployed journal format. The ParamTypes member is omitted
	// from its Query JSON, preserving its exact bytes. Version 2 is selected only
	// while a typed query is retained, so an older binary rejects the record at
	// its version fence instead of decoding and replaying it without the type
	// metadata.
	postgresWriteJournalVersionUntyped = 1
	postgresWriteJournalVersionTyped   = 2
)

type postgresDurableService interface {
	durableRequestService
	ReplayBatch(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query) (durableExecBatchExecuteResult, bool, error)
}

// Production writers persist two issuer domains in one serialized outbox.
// One outbox preserves table ordering across execution protocols.
type postgresModeDurableService interface {
	ExecBatchMode(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query, gateway.DurableSQLExecutionMode) (durableExecBatchExecuteResult, error)
	ReplayBatchMode(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query, gateway.DurableSQLExecutionMode) (durableExecBatchExecuteResult, bool, error)
}

type postgresPreparedDirectService interface {
	PrepareDirectBatch(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query) (*gateway.DurableSQLDirectPlan, error)
	ExecutePreparedDirectBatch(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query, *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error)
}

type postgresCoordinatedIssuer struct {
	Installation replication.ID128
	Sequence     uint64
	Reference    gateway.ReplicatedIssuerReference
}

// Alias without the public wire decoder: the private checksummed journal stores
// the complete typed ACK, not the public protocol's flattened JSON envelope.
type postgresStoredAck durableExecBatchAckWireRequest
type postgresWriteRecord struct {
	DirectPlan   *gateway.DurableSQLDirectPlan   `json:",omitempty"`
	Mode         gateway.DurableSQLExecutionMode `json:",omitempty"`
	Coordinated  *postgresCoordinatedIssuer      `json:",omitempty"`
	Version      uint32
	Table        string `json:",omitempty"`
	Authority    serviceauthz.Authority
	Installation replication.ID128
	Sequence     uint64
	Reference    gateway.ReplicatedIssuerReference
	Identity     durableExecBatchIdentity
	Query        *gateway.Query
	Ack          *postgresStoredAck
}

// One bounded, serialized lane is sufficient for the opt-in local development
// endpoint. No state or worker is allocated when PostgreSQL is disabled. The
// journal is a native client outbox: PG reconnects do NOT carry an idempotency
// key and must never be advertised as exactly-once application retries.
type postgresDurableWriter struct {
	path    string
	lock    *os.File
	gate    chan struct{}
	service postgresDurableService
	record  postgresWriteRecord
	poison  error
}

// The storage sentinel is also used to classify unresolved distributed work.
// Its "reopen required" text is not a PostgreSQL client recovery instruction:
// a healthy outbox can resolve the original identity without either handle
// being reopened. Keep the complete typed cause, but report the actual owner
// and recovery state at the SQL boundary.
type postgresWriteOutcomeError struct {
	request  replication.ID128
	previous bool
	poisoned bool
	cause    error
}

func (e *postgresWriteOutcomeError) Error() string {
	state := "server retains the request for automatic recovery"
	if e.poisoned {
		state = "server-side storage recovery is required"
	}
	if e.previous {
		return fmt.Sprintf("previous PostgreSQL write %x is awaiting recovery; this statement was not executed; %s; reconnecting does not resolve or cancel the previous write", e.request, state)
	}
	return fmt.Sprintf("PostgreSQL write outcome unknown for request %x; %s; do not resubmit the write without verifying its outcome; reconnecting does not resolve or cancel it", e.request, state)
}

func (e *postgresWriteOutcomeError) Unwrap() error { return e.cause }

func (w *postgresDurableWriter) outcomeError(id replication.ID128, err error, previous bool) error {
	return &postgresWriteOutcomeError{request: id,
		previous: previous, poisoned: w.poison != nil,
		cause: errors.Join(durable.ErrCommitOutcomeUnknown, err)}
}

func openPostgresDurableWriter(path string, authority serviceauthz.Authority, service postgresDurableService, table ...string) (*postgresDurableWriter, error) {
	if len(table) > 1 || len(table) == 1 && table[0] == "" {
		return nil, errInvalidDurableRequestAdapter
	}
	if path == "" || !authority.Valid() || service == nil {
		return nil, errInvalidDurableRequestAdapter
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	for _, name := range []string{path, path + ".lock"} {
		info, err := os.Lstat(name)
		if err == nil && !info.Mode().IsRegular() {
			return nil, fmt.Errorf("gateway: PostgreSQL journal must be a regular file")
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
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
	w := &postgresDurableWriter{path: path, lock: lock, gate: make(chan struct{}, 1), service: service}
	w.gate <- struct{}{}
	fail := func(err error) (*postgresDurableWriter, error) { _ = w.Close(); return nil, err }
	file, err := os.Open(path)
	if err == nil {
		raw, readErr := io.ReadAll(io.LimitReader(file, maxPostgreSQLWriteJournalBytes+1))
		err = errors.Join(readErr, file.Close())
		if err != nil {
			return fail(err)
		}
		if len(raw) <= sha256.Size || len(raw) > maxPostgreSQLWriteJournalBytes {
			return fail(errInvalidDurableRequestAdapter)
		}
		digest := sha256.Sum256(raw[sha256.Size:])
		if !bytes.Equal(digest[:], raw[:sha256.Size]) {
			return fail(errInvalidDurableRequestAdapter)
		}
		if err = vibejson.Unmarshal(raw[sha256.Size:], &w.record); err != nil {
			return fail(err)
		}
		if !validPostgresWriteJournalVersion(&w.record) || w.record.Authority != authority || w.record.Installation == (replication.ID128{}) || w.record.Sequence == 0 {
			return fail(errInvalidDurableRequestAdapter)
		}
		if _, modeAware := service.(postgresModeDurableService); w.record.Mode != gateway.DurableSQLLegacyAuto && !modeAware {
			return fail(errInvalidDurableRequestAdapter)
		}
		if _, prepared := service.(postgresPreparedDirectService); w.record.DirectPlan != nil && !prepared {
			return fail(errInvalidDurableRequestAdapter)
		}
		if len(table) == 1 && w.record.Table != table[0] {
			return fail(errInvalidDurableRequestAdapter)
		}
		if ref := w.record.Reference; ref.GrantDigest != (replication.Digest{}) && (ref.Installation != w.record.Installation || ref.Epoch != 1 || ref.LaneOrdinal != 0) {
			return fail(errInvalidDurableRequestAdapter)
		}
		if w.record.Query != nil {
			if !validDurableExecBatchIdentity(w.record.Identity) || !w.pendingIssuerMatches() {
				return fail(errInvalidDurableRequestAdapter)
			}
		} else if w.record.Ack != nil {
			return fail(errInvalidDurableRequestAdapter)
		}
		if w.record.Ack != nil && (!validDurableExecBatchAckRequest((*durableExecBatchAckWireRequest)(w.record.Ack)) ||
			w.record.Ack.Identity.RequestID != w.record.Identity.RequestID ||
			w.record.Ack.Identity.Reference != w.record.Identity.Reference ||
			w.record.Ack.Identity.IssuerSequence != w.record.Identity.IssuerSequence ||
			w.record.Mode == gateway.DurableSQLDirectOnly) {
			return fail(errInvalidDurableRequestAdapter)
		}
		return w, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	w.record = postgresWriteRecord{Version: postgresWriteJournalVersionUntyped, Authority: authority, Sequence: 1}
	if len(table) == 1 {
		w.record.Table = table[0]
	}
	if _, err = rand.Read(w.record.Installation[:]); err != nil {
		return fail(err)
	}
	if err = w.save(); err != nil {
		return fail(err)
	}
	return w, nil
}

func (w *postgresDurableWriter) save() (err error) {
	region := trace.StartRegion(context.Background(), "pg.outbox.save")
	defer region.End()
	defer func() {
		if err != nil {
			w.poison = err
		}
	}()
	if w.poison != nil {
		return w.poison
	}
	version, versionErr := postgresWriteRecordVersion(&w.record)
	if versionErr != nil {
		return versionErr
	}
	w.record.Version = version
	raw, err := vibejson.Marshal(&w.record)
	if err != nil {
		return err
	}
	if len(raw)+sha256.Size > maxPostgreSQLWriteJournalBytes {
		return gateway.ErrTransactionByteLimit
	}
	digest := sha256.Sum256(raw)
	// The single-writer lock owns one fixed staging slot, including after a
	// crash. Staging is never admission authority; only the synced rename is.
	name := w.path + ".pending"
	if info, statErr := os.Lstat(name); statErr == nil {
		if !info.Mode().IsRegular() {
			return errInvalidDurableRequestAdapter
		}
		if err = os.Remove(name); err != nil {
			return err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		w.poison = err
		return err
	}
	defer os.Remove(name)
	if _, err = file.Write(digest[:]); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err == nil {
		err = os.Rename(name, w.path)
	}
	if err == nil {
		var dir *os.File
		dir, err = os.Open(filepath.Dir(w.path))
		if err == nil {
			err = errors.Join(dir.Sync(), dir.Close())
		}
	}
	if err != nil {
		w.poison = err
	}
	return err
}

func (w *postgresDurableWriter) Close() error {
	<-w.gate
	defer func() { w.gate <- struct{}{} }()
	if w.lock == nil {
		return nil
	}
	err := errors.Join(storeio.UnlockWriter(w.lock), w.lock.Close())
	w.lock = nil
	w.poison = errInvalidDurableRequestAdapter
	return err
}

func (w *postgresDurableWriter) finishAck(ctx context.Context) error {
	if w.record.Ack == nil {
		return nil
	}
	if _, err := w.service.AckExecBatch(ctx, w.record.Authority, durableExecBatchAckWireRequest(*w.record.Ack)); err != nil {
		return err
	}
	sequence := &w.record.Sequence
	if w.record.Mode == gateway.DurableSQLCoordinated {
		if w.record.Coordinated == nil {
			return errInvalidDurableRequestAdapter
		}
		sequence = &w.record.Coordinated.Sequence
	}
	if *sequence == ^uint64(0) {
		return errInvalidDurableRequestAdapter
	}
	(*sequence)++
	w.record.Identity = durableExecBatchIdentity{}
	w.record.Query, w.record.Ack, w.record.DirectPlan = nil, nil, nil
	w.record.Version = postgresWriteJournalVersionUntyped
	return w.save()
}

func (w *postgresDurableWriter) resolve(ctx context.Context, fresh bool) (*gateway.Result, error) {
	ctx, authorityErr := serviceauthz.WithAuthority(ctx, w.record.Authority)
	if authorityErr != nil {
		return nil, authorityErr
	}
	if w.poison != nil {
		return nil, w.poison
	}
	if w.record.Ack != nil {
		return nil, w.finishAck(ctx)
	}
	if w.record.Query == nil {
		return nil, nil
	}
	queries := []gateway.Query{*w.record.Query}
	var result durableExecBatchExecuteResult
	var err error
	found := false
	if w.record.DirectPlan != nil {
		service, ok := w.service.(postgresPreparedDirectService)
		if !ok {
			return nil, errInvalidDurableRequestAdapter
		}
		region := trace.StartRegion(ctx, "pg.direct.execute")
		result, err = service.ExecutePreparedDirectBatch(ctx, w.record.Authority, w.record.Identity, queries, w.record.DirectPlan)
		region.End()
		found = true
	} else if !fresh {
		if service, ok := w.service.(postgresModeDurableService); ok {
			result, found, err = service.ReplayBatchMode(ctx, w.record.Authority, w.record.Identity, queries, w.record.Mode)
		} else {
			result, found, err = w.service.ReplayBatch(ctx, w.record.Authority, w.record.Identity, queries)
		}
		if err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) {
			return nil, err
		}
	}
	if !found {
		if service, ok := w.service.(postgresModeDurableService); ok {
			result, err = service.ExecBatchMode(ctx, w.record.Authority, w.record.Identity, queries, w.record.Mode)
		} else {
			result, err = w.service.ExecBatch(ctx, w.record.Authority, w.record.Identity, queries)
		}
	}
	if err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) {
		if fresh && errors.Is(err, gateway.ErrDurableSQLNotAdmitted) {
			// This invocation had no earlier unknown attempt. Reuse the sequence
			// but never the failed command's nonce for the next independent write.
			w.record.Query = nil
			w.record.DirectPlan = nil
			w.record.Identity = durableExecBatchIdentity{}
			w.record.Version = postgresWriteJournalVersionUntyped
			if saveErr := w.save(); saveErr != nil {
				// The old durable outbox may still authorize recovery. A refusal
				// is final only after its removal is durable too.
				return nil, errors.Join(durable.ErrCommitOutcomeUnknown, err, saveErr)
			}
		}
		return nil, err
	}
	if w.record.Mode == gateway.DurableSQLCoordinated && result.Direct ||
		w.record.Mode == gateway.DurableSQLDirectOnly && !result.Direct ||
		result.Result == nil || !result.Direct && !validDurableExecBatchAckRequest(&result.Ack) ||
		result.Direct && result.Ack != (durableExecBatchAckWireRequest{}) {
		return nil, errInvalidDurableRequestAdapter
	}
	if result.Direct {
		if w.record.Sequence == ^uint64(0) {
			return nil, errInvalidDurableRequestAdapter
		}
		w.record.Sequence++
		w.record.Identity = durableExecBatchIdentity{}
		w.record.Query = nil
		w.record.DirectPlan = nil
		w.record.Version = postgresWriteJournalVersionUntyped
		if saveErr := w.save(); saveErr != nil {
			return nil, saveErr
		}
		return result.Result, err
	}
	ack := postgresStoredAck(result.Ack)
	w.record.Ack = &ack
	if saveErr := w.save(); saveErr != nil {
		return nil, saveErr
	}
	// Terminal outcome is already known and durably retained. ACK cleanup must
	// not turn a committed success into an apparent failed write.
	_ = w.finishAck(ctx)
	return result.Result, err
}

func (w *postgresDurableWriter) resolveLeaderChanges(ctx context.Context, fresh bool) (*gateway.Result, error) {
	result, err := w.resolve(ctx, fresh)
	// Both unresolved execution and terminal ACK cleanup retain the exact
	// durable identity. A new statement cannot overtake either one.
	for retry := 0; retry < 3 && w.poison == nil && w.record.Query != nil && ctx.Err() == nil &&
		!errors.Is(err, gateway.ErrDurableSQLNotAdmitted) && !errors.Is(err, gateway.ErrDurableSQLAborted) &&
		(errors.Is(err, gateway.ErrReplicatedLeader) || errors.Is(err, gateway.ErrReplicatedReadBehind)); retry++ {
		timer := time.NewTimer(time.Duration(retry+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
			result, err = w.resolve(ctx, false)
		}
	}
	return result, err
}

func (w *postgresDurableWriter) Write(ctx context.Context, authority serviceauthz.Authority, q gateway.Query) (*gateway.Result, error) {
	region := trace.StartRegion(ctx, "pg.write")
	defer region.End()
	if authority != w.record.Authority {
		return nil, gateway.ErrReplicatedUnauthorized
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.gate:
	}
	defer func() { w.gate <- struct{}{} }()
	if w.poison != nil {
		return nil, w.poison
	}
	previousID := w.record.Identity.RequestID
	if _, err := w.resolveLeaderChanges(ctx, false); err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) || w.record.Query != nil {
		return nil, w.outcomeError(previousID, err, true)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mode := gateway.DurableSQLLegacyAuto
	if _, ok := w.service.(postgresModeDurableService); ok {
		mode = gateway.DurableSQLDirectOnly
	}
	result, err := w.writeFresh(ctx, authority, q, mode)
	if mode == gateway.DurableSQLDirectOnly && errors.Is(err, gateway.ErrDurableSQLDirectIneligible) &&
		errors.Is(err, gateway.ErrDurableSQLNotAdmitted) && w.record.Query == nil && w.poison == nil {
		return w.writeFresh(ctx, authority, q, gateway.DurableSQLCoordinated)
	}
	return result, err
}

func (w *postgresDurableWriter) writeFresh(ctx context.Context, authority serviceauthz.Authority, q gateway.Query, mode gateway.DurableSQLExecutionMode) (*gateway.Result, error) {
	installation, reference, sequence := &w.record.Installation, &w.record.Reference, &w.record.Sequence
	if mode == gateway.DurableSQLCoordinated {
		if w.record.Coordinated == nil {
			lane := &postgresCoordinatedIssuer{Sequence: 1}
			if _, err := rand.Read(lane.Installation[:]); err != nil {
				return nil, err
			}
			w.record.Coordinated = lane
			// Persist installation before opening its replicated grant, so an
			// uncertain grant response cannot create a new installation on restart.
			if err := w.save(); err != nil {
				return nil, err
			}
		}
		installation, reference, sequence = &w.record.Coordinated.Installation, &w.record.Coordinated.Reference, &w.record.Coordinated.Sequence
	}
	if reference.GrantDigest == (replication.Digest{}) {
		grant, err := w.service.OpenIssuer(ctx, authority, gateway.ReplicatedIssuerOpen{Installation: *installation, Epoch: 1})
		if err != nil {
			return nil, err
		}
		*reference = gateway.ReplicatedIssuerReference{Installation: grant.Installation, Epoch: grant.Epoch, LaneOrdinal: grant.LaneOrdinal, GrantDigest: grant.GrantDigest}
	}
	identity := durableExecBatchIdentity{Reference: *reference, IssuerSequence: *sequence}
	if _, err := rand.Read(identity.RequestID[:]); err != nil {
		return nil, err
	}
	if !validDurableExecBatchIdentity(identity) {
		return nil, errInvalidDurableRequestAdapter
	}
	// Own all caller bytes before publication; PG bind buffers are reused.
	queries, err := vibejson.Marshal(&q)
	if err != nil {
		return nil, err
	}
	if len(queries) > maxPostgreSQLWriteJournalBytes/2 {
		return nil, gateway.ErrTransactionByteLimit
	}
	var owned gateway.Query
	if err = vibejson.Unmarshal(queries, &owned); err != nil {
		return nil, err
	}
	version, err := postgresWriteJournalVersion(&owned)
	if err != nil {
		return nil, err
	}
	var directPlan *gateway.DurableSQLDirectPlan
	if service, ok := w.service.(postgresPreparedDirectService); ok && mode == gateway.DurableSQLDirectOnly {
		// No mutation has been proposed. Keep preparation outside the outbox
		// publication so an interrupted read cannot leave a half-prepared write.
		region := trace.StartRegion(ctx, "pg.direct.prepare")
		directPlan, err = service.PrepareDirectBatch(ctx, authority, identity, []gateway.Query{owned})
		region.End()
		if err != nil {
			return nil, errors.Join(gateway.ErrDurableSQLNotAdmitted, err)
		}
		if directPlan == nil {
			return nil, errInvalidDurableRequestAdapter
		}
	}
	w.record.Version = version
	w.record.Mode = mode
	w.record.DirectPlan = directPlan
	w.record.Query, w.record.Identity = &owned, identity
	if err = w.save(); err != nil {
		return nil, w.outcomeError(identity.RequestID, err, false)
	}
	// A leader transition is not a new application request. Resolve the exact
	// fsynced identity before reporting an unknown outcome to PostgreSQL. This
	// bounded slow path adds no work to successful writes and never mints a new
	// nonce, reorders the table lane, or retries a final refusal/abort.
	result, err := w.resolveLeaderChanges(ctx, true)
	if err != nil && !errors.Is(err, gateway.ErrDurableSQLNotAdmitted) && !errors.Is(err, gateway.ErrDurableSQLAborted) {
		return nil, w.outcomeError(identity.RequestID, err, false)
	}
	return result, err
}

func postgresWriteJournalVersion(query *gateway.Query) (uint32, error) {
	if query == nil || query.ParamTypes == nil {
		return postgresWriteJournalVersionUntyped, nil
	}
	if len(query.ParamTypes) == 0 ||
		len(query.ParamTypes) != len(query.Params) ||
		len(query.ParamTypes) > maxPostgreSQLWriteParameters {
		return 0, errInvalidDurableRequestAdapter
	}
	typed := false
	for index, parameterType := range query.ParamTypes {
		if parameterType >= driver.ParamTypeInvalid {
			return 0, errInvalidDurableRequestAdapter
		}
		if !query.Params[index].Valid() {
			return 0, errInvalidDurableRequestAdapter
		}
		if parameterType != driver.ParamTypeUnspecified &&
			query.Params[index].Kind == shardservice.ParamDocument {
			return 0, errInvalidDurableRequestAdapter
		}
		typed = typed || parameterType != driver.ParamTypeUnspecified
	}
	if !typed {
		return 0, errInvalidDurableRequestAdapter
	}
	return postgresWriteJournalVersionTyped, nil
}

// Versions 3/4 fence two-domain recovery from older binaries. The query
// encoding is unchanged from versions 1/2, respectively. Versions 5/6 also
// retain an exact prepared direct recipe, which must never be replanned.
func postgresWriteRecordVersion(record *postgresWriteRecord) (uint32, error) {
	if record == nil || record.Mode > gateway.DurableSQLCoordinated {
		return 0, errInvalidDurableRequestAdapter
	}
	if lane := record.Coordinated; lane != nil {
		if lane.Installation == (replication.ID128{}) || lane.Installation == record.Installation || lane.Sequence == 0 {
			return 0, errInvalidDurableRequestAdapter
		}
		if ref := lane.Reference; ref.GrantDigest != (replication.Digest{}) &&
			(ref.Installation != lane.Installation || ref.Epoch != 1 || ref.LaneOrdinal != 0) {
			return 0, errInvalidDurableRequestAdapter
		}
	}
	if record.Mode == gateway.DurableSQLCoordinated && record.Coordinated == nil {
		return 0, errInvalidDurableRequestAdapter
	}
	version, err := postgresWriteJournalVersion(record.Query)
	if record.DirectPlan != nil {
		if record.Mode != gateway.DurableSQLDirectOnly || record.Query == nil || record.Ack != nil ||
			record.DirectPlan.Key.Request != requestledger.RequestID(record.Identity.RequestID) ||
			record.DirectPlan.Key.IssuerSequence != record.Identity.IssuerSequence {
			return 0, errInvalidDurableRequestAdapter
		}
		version += 4
	} else if record.Mode != gateway.DurableSQLLegacyAuto || record.Coordinated != nil {
		version += 2
	}
	return version, err
}

func (w *postgresDurableWriter) pendingIssuerMatches() bool {
	installation, reference, sequence := w.record.Installation, w.record.Reference, w.record.Sequence
	if w.record.Mode == gateway.DurableSQLCoordinated {
		if w.record.Coordinated == nil {
			return false
		}
		installation, reference, sequence = w.record.Coordinated.Installation, w.record.Coordinated.Reference, w.record.Coordinated.Sequence
	}
	return w.record.Identity.Reference.Installation == installation &&
		w.record.Identity.Reference == reference && w.record.Identity.IssuerSequence == sequence
}

func validPostgresWriteJournalVersion(record *postgresWriteRecord) bool {
	if record == nil {
		return false
	}
	version, err := postgresWriteRecordVersion(record)
	if err != nil {
		return false
	}
	return record.Version == version
}

func (w *postgresDurableWriter) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		select {
		case <-ctx.Done():
			return
		case <-w.gate:
		default:
			continue
		}
		if w.record.Query != nil {
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, _ = w.resolve(attempt, false)
			cancel()
		}
		w.gate <- struct{}{}
	}
}
