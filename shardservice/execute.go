package shardservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

// One connection's request lifecycle: read a frame, admit it, and on success pin
// a snapshot, prepare the SQL text, bind the typed parameters, execute locally,
// and stream the result back. No serialized plan crosses the wire; the borrowed
// Session parses and plans the statement text itself.
//
// The lifecycle for one request is: admit -> (read) pin snapshot -> prepare ->
// bind -> execute -> stream rows/completion -> release snapshot. Every failure is
// a typed error frame — a not-owner/stale admission refusal, a malformed request,
// a deadline, or a resource limit — never a panic.

// pgOIDJSON is the type OID advertised for every result column. The shard wire
// carries each cell as JSON text, so every column names the JSON type rather
// than guessing a more specific type.
const pgOIDJSON = 114

// shardConn is one served connection: the socket and the single Session it owns.
type shardConn struct {
	server *Server
	nc     net.Conn
	sess   *sqldriver.Session
}

// loop serves requests until the peer disconnects or the framing desynchronizes.
// A body-level malformation keeps the stream aligned, so it is answered with a
// typed error frame and serving continues; a framing-level failure or a vanished
// peer ends the connection.
func (c *shardConn) loop() error {
	for {
		setDeadline(c.nc.SetReadDeadline, c.server.opts.IdleTimeout)
		req, err := DecodeRequest(c.nc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if isFramingError(err) {
				return err
			}
			if werr := c.writeResponse(
				NewErrorResponse(ErrorMalformedRequest, err.Error())); werr != nil {
				return werr
			}
			continue
		}
		if werr := c.writeResponse(c.handle(req)); werr != nil {
			return werr
		}
	}
}

// isFramingError reports whether a decode failure left the byte stream in an
// unknown position, which is unrecoverable and must end the connection. Every
// other decode failure consumed exactly one framed body and left the stream
// aligned on the next frame.
func isFramingError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, errBadTag) ||
		errors.Is(err, errBadLength) ||
		errors.Is(err, errFrameTooLarge) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

// writeResponse frames resp to the connection. A response the codec refuses for
// size — a result too large for one frame — is downgraded to a typed
// resource-limit frame rather than dropped, because that refusal happens before
// any bytes are written.
func (c *shardConn) writeResponse(resp *ShardResponse) error {
	setDeadline(c.nc.SetWriteDeadline, c.server.opts.WriteTimeout)
	err := EncodeResponse(c.nc, resp)
	if err == nil {
		return nil
	}
	if isEncodeRejection(err) {
		setDeadline(c.nc.SetWriteDeadline, c.server.opts.WriteTimeout)
		return EncodeResponse(c.nc, NewErrorResponse(ErrorResourceLimit, err.Error()))
	}
	return err
}

// isEncodeRejection reports whether EncodeResponse rejected a response before
// writing any bytes, which means a substitute frame can be sent safely.
func isEncodeRejection(err error) bool {
	return errors.Is(err, errFrameTooLarge) ||
		errors.Is(err, errFieldTooLarge) ||
		errors.Is(err, errRowArity) ||
		errors.Is(err, errBadEnum)
}

// handle admits req and, on success, executes it, always returning a response
// frame to send.
func (c *shardConn) handle(req *ShardRequest) *ShardResponse {
	if err := c.server.ownership.Admit(req); err != nil {
		var ae *AdmissionError
		if errors.As(err, &ae) {
			return ae.Response()
		}
		return NewErrorResponse(ErrorMalformedRequest, err.Error())
	}
	if req.Transaction.Operation != TransactionNone {
		return c.transaction(req)
	}
	barrierCtx, barrierCancel := c.server.requestContext(req)
	err := c.server.journal.WaitNoParticipantBarrier(
		barrierCtx, req.BucketBits, req.AccessScopes,
	)
	barrierCancel()
	if err != nil {
		return classifyError(err)
	}
	return c.execute(req)
}

// transaction executes the durable subset of the transaction protocol. Stage
// and lookup are safe independently of SQL publication. State transitions stay
// closed until commit proof and participant apply are joined to the local SQL
// transaction boundary; accepting them earlier would permit partial commit.
func (c *shardConn) transaction(req *ShardRequest) *ShardResponse {
	tx := req.Transaction
	if req.SQL != "" || len(req.Params) != 0 || len(req.AccessScopes) != 0 || req.BucketBits != 0 {
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: a transaction command cannot also carry SQL or parameters")
	}
	if tx.Operation != TransactionLookupCoordinator &&
		tx.Operation != TransactionLookupParticipant &&
		tx.Operation != TransactionReadParticipant &&
		tx.Operation != TransactionScanCoordinator &&
		req.ExecutionMode != ExecutionReadWrite {
		return NewErrorResponse(ErrorReadOnly,
			"shardservice: a transaction mutation requires read-write execution authority")
	}
	var (
		status distributedtxn.Status
		err    error
	)
	switch tx.Operation {
	case TransactionStageCoordinator:
		var participants [distributedtxn.MaxParticipants]distributedtxn.ParticipantRef
		record, openErr := distributedtxn.OpenCoordinatorInto(tx.Record, participants[:])
		if openErr != nil {
			return transactionError(openErr)
		}
		// The fixed-set protocol designates the first sorted participant as
		// coordinator, avoiding another identity field in every record.
		first := record.Participants[0]
		if !vibejson.BytesEqualString(first.Distribution, string(req.Distribution)) ||
			!vibejson.BytesEqualString(first.Shard, string(req.Shard)) ||
			first.RoutingVersion != uint64(req.RoutingVersion) ||
			first.AllocationGeneration != uint64(req.AllocationGeneration) ||
			first.OwnershipEpoch != uint64(req.OwnershipEpoch) {
			return transactionError(distributedtxn.ErrJournalConflict)
		}
		status, err = c.server.journal.StageCoordinator(tx.Record)
	case TransactionStageParticipant:
		var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
		record, openErr := distributedtxn.OpenParticipantInto(tx.Record, scopes[:])
		if openErr != nil {
			return transactionError(openErr)
		}
		if record.RoutingVersion != uint64(req.RoutingVersion) ||
			record.AllocationGeneration != uint64(req.AllocationGeneration) ||
			record.OwnershipEpoch != uint64(req.OwnershipEpoch) {
			return transactionError(distributedtxn.ErrJournalConflict)
		}
		status, err = c.server.journal.StageParticipant(tx.Record)
	case TransactionLookupCoordinator:
		return c.lookupCoordinator(tx.ID)
	case TransactionScanCoordinator:
		return c.scanCoordinator(tx.ID)
	case TransactionLookupParticipant:
		return c.lookupParticipant(tx.ID)
	case TransactionReadParticipant:
		return c.readParticipant(tx.ID)
	case TransactionPrepareParticipant:
		return c.prepareParticipant(req)
	case TransactionApplyParticipant:
		return c.applyParticipant(req)
	case TransactionCommitCoordinator:
		status, err = c.server.journal.TransitionCoordinator(
			tx.ID, tx.Revision, distributedtxn.CoordinatorCommitted,
		)
	case TransactionAbortCoordinator:
		status, err = c.server.journal.TransitionCoordinator(
			tx.ID, tx.Revision, distributedtxn.CoordinatorAborted,
		)
	case TransactionAbortParticipant:
		status, err = c.server.journal.AbortParticipant(tx.ID, tx.Revision)
	case TransactionReleaseParticipant:
		status, err = c.server.journal.TransitionParticipant(
			tx.ID, tx.Revision, distributedtxn.ParticipantReleased,
		)
	case TransactionRetireCoordinator:
		status, err = c.server.journal.TransitionCoordinator(
			tx.ID, tx.Revision, distributedtxn.CoordinatorRetired,
		)
	default:
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: transaction operation is not recognized")
	}
	if err != nil {
		return transactionError(err)
	}
	return transactionStatusResponse(status)
}

func (c *shardConn) lookupCoordinator(id distributedtxn.ID) *ShardResponse {
	status, ok := c.server.journal.CoordinatorStatus(id)
	if !ok {
		return transactionError(distributedtxn.ErrJournalNotFound)
	}
	record, err := c.server.journal.CoordinatorStage(id)
	if err != nil {
		return transactionError(err)
	}
	response := transactionStatusResponse(status)
	response.Transaction.Record = record
	return response
}

func (c *shardConn) scanCoordinator(after distributedtxn.ID) *ShardResponse {
	status, record, ok := c.server.journal.NextCoordinator(after)
	if !ok {
		return CompletionResponse(0)
	}
	response := transactionStatusResponse(status)
	response.Transaction.Record = record
	return response
}

func (c *shardConn) lookupParticipant(id distributedtxn.ID) *ShardResponse {
	status, ok := c.server.journal.ParticipantStatus(id)
	if !ok {
		return transactionError(distributedtxn.ErrJournalNotFound)
	}
	revision, rowsAffected, applied, err := c.server.db.DistributedParticipantStatus(id)
	if err != nil {
		return transactionError(err)
	}
	if applied {
		if status.ParticipantState == distributedtxn.ParticipantPrepared && revision == status.Revision+1 {
			if repaired, transitionErr := c.server.journal.TransitionParticipant(
				id, status.Revision, distributedtxn.ParticipantApplied,
			); transitionErr == nil {
				status = repaired
			}
		}
		validApplied := status.ParticipantState == distributedtxn.ParticipantApplied &&
			status.Revision == revision
		validReleased := status.ParticipantState == distributedtxn.ParticipantReleased &&
			status.Revision == revision+1
		if !validApplied && !validReleased {
			return transactionError(sqldriver.ErrDistributedTransactionConflict)
		}
		resp := transactionStatusResponse(status)
		resp.RowsAffected = rowsAffected
		return resp
	}
	return transactionStatusResponse(status)
}

func (c *shardConn) readParticipant(id distributedtxn.ID) *ShardResponse {
	response := c.lookupParticipant(id)
	if response.Kind == ResponseError {
		return response
	}
	record, err := c.server.journal.ParticipantStage(id)
	if err != nil {
		return transactionError(err)
	}
	response.Transaction.Record = record
	return response
}

func (c *shardConn) applyParticipant(req *ShardRequest) *ShardResponse {
	tx := req.Transaction
	status, ok := c.server.journal.ParticipantStatus(tx.ID)
	if !ok {
		return transactionError(distributedtxn.ErrJournalNotFound)
	}
	if (status.ParticipantState == distributedtxn.ParticipantApplied && status.Revision == tx.Revision+1) ||
		(status.ParticipantState == distributedtxn.ParticipantReleased && status.Revision == tx.Revision+2) {
		return c.lookupParticipant(tx.ID)
	}
	if status.ParticipantState != distributedtxn.ParticipantPrepared || status.Revision != tx.Revision {
		return transactionError(distributedtxn.ErrJournalConflict)
	}
	if revision, rowsAffected, found, err := c.server.db.DistributedParticipantStatus(tx.ID); err != nil {
		return transactionError(err)
	} else if found {
		if revision != tx.Revision+1 {
			return transactionError(sqldriver.ErrDistributedTransactionConflict)
		}
		_, _ = c.server.journal.TransitionParticipant(
			tx.ID, tx.Revision, distributedtxn.ParticipantApplied,
		)
		status.Revision = revision
		status.ParticipantState = distributedtxn.ParticipantApplied
		resp := transactionStatusResponse(status)
		resp.RowsAffected = rowsAffected
		return resp
	}
	batch, err := c.participantBatch(tx.ID)
	if err != nil {
		return transactionError(err)
	}
	return c.executeParticipantMutation(req, batch)
}

func (c *shardConn) prepareParticipant(req *ShardRequest) *ShardResponse {
	tx := req.Transaction
	status, ok := c.server.journal.ParticipantStatus(tx.ID)
	if !ok {
		return transactionError(distributedtxn.ErrJournalNotFound)
	}
	if status.ParticipantState == distributedtxn.ParticipantPrepared && status.Revision == tx.Revision+1 {
		return transactionStatusResponse(status)
	}
	if status.ParticipantState != distributedtxn.ParticipantStaged || status.Revision != tx.Revision {
		return transactionError(distributedtxn.ErrJournalConflict)
	}
	batch, err := c.participantBatch(tx.ID)
	if err != nil {
		return transactionError(err)
	}
	rows, resultBytes := c.server.resultLimits(req)
	if err := c.sess.SetResultLimits(rows, resultBytes); err != nil {
		return classifyError(err)
	}
	if err := c.sess.SetIntermediateLimit(c.server.opts.MaxIntermediateBytes); err != nil {
		return classifyError(err)
	}
	ctx, cancel := c.server.requestContext(req)
	defer cancel()
	if err := c.sess.Begin(ctx, sqldriver.TxOptions{Isolation: sqldriver.IsolationSerializable}); err != nil {
		return classifyError(err)
	}
	defer c.sess.Rollback(context.Background())
	if _, response := c.executeParticipantStatements(ctx, batch); response != nil {
		return response
	}
	if err := c.sess.Rollback(ctx); err != nil {
		return classifyError(err)
	}
	status, err = c.server.journal.TransitionParticipant(
		tx.ID, tx.Revision, distributedtxn.ParticipantPrepared,
	)
	if err != nil {
		return transactionError(err)
	}
	return transactionStatusResponse(status)
}

func (c *shardConn) participantBatch(id distributedtxn.ID) (MutationBatch, error) {
	raw, err := c.server.journal.ParticipantStage(id)
	if err != nil {
		return MutationBatch{}, err
	}
	var scopes [distributedtxn.MaxIntentScopes]distributedtxn.IntentScope
	record, err := distributedtxn.OpenParticipantInto(raw, scopes[:])
	if err != nil {
		return MutationBatch{}, err
	}
	if distributedtxn.ParticipantDigest(
		record.BucketBits, record.IntentScopes, record.Mutation,
	) != record.MutationDigest {
		return MutationBatch{}, distributedtxn.ErrCorrupt
	}
	return OpenMutationBatch(record.Mutation)
}

func (c *shardConn) executeParticipantMutation(
	outer *ShardRequest,
	batch MutationBatch,
) *ShardResponse {
	rows, resultBytes := c.server.resultLimits(outer)
	if err := c.sess.SetResultLimits(rows, resultBytes); err != nil {
		return classifyError(err)
	}
	if err := c.sess.SetIntermediateLimit(c.server.opts.MaxIntermediateBytes); err != nil {
		return classifyError(err)
	}
	ctx, cancel := c.server.requestContext(outer)
	defer cancel()
	if err := c.sess.Begin(ctx, sqldriver.TxOptions{Isolation: sqldriver.IsolationSerializable}); err != nil {
		return classifyError(err)
	}
	defer c.sess.Rollback(context.Background())
	rowsAffected, response := c.executeParticipantStatements(ctx, batch)
	if response != nil {
		return response
	}
	affected, err := c.sess.CommitDistributedParticipant(
		ctx, outer.Transaction.ID, outer.Transaction.Revision, rowsAffected,
	)
	if err != nil {
		return classifyError(err)
	}
	status, err := c.server.journal.TransitionParticipant(
		outer.Transaction.ID, outer.Transaction.Revision, distributedtxn.ParticipantApplied,
	)
	if err != nil {
		// SQL participant state is the atomic authority. A journal delta is a
		// compact recovery index and lookup repairs it after reopen.
		status = distributedtxn.Status{
			Role: distributedtxn.RoleParticipant, ID: outer.Transaction.ID,
			Revision:         outer.Transaction.Revision + 1,
			ParticipantState: distributedtxn.ParticipantApplied,
		}
	}
	resp := transactionStatusResponse(status)
	resp.RowsAffected = affected
	return resp
}

func (c *shardConn) executeParticipantStatements(
	ctx context.Context,
	batch MutationBatch,
) (int64, *ShardResponse) {
	var rowsAffected int64
	for {
		mutation, ok, err := batch.Next()
		if err != nil {
			return 0, transactionError(err)
		}
		if !ok {
			break
		}
		prep, err := c.sess.Prepare(ctx, mutation.SQL)
		if err != nil {
			return 0, classifyError(err)
		}
		if prep.Kind() == sqlast.KindSelect || prep.ReturnsRows() {
			_ = prep.Close()
			return 0, NewErrorResponse(ErrorMalformedRequest,
				"shardservice: a distributed participant mutation must be non-row-returning DML")
		}
		result, execErr := prep.Exec(ctx, runtimeArgs(mutation.Params))
		closeErr := prep.Close()
		if execErr != nil || closeErr != nil {
			return 0, classifyError(errors.Join(execErr, closeErr))
		}
		if result.RowsAffected < 0 || result.RowsAffected > math.MaxInt64-rowsAffected {
			return 0, NewErrorResponse(ErrorResourceLimit,
				"shardservice: participant affected-row count overflow")
		}
		rowsAffected += result.RowsAffected
	}
	return rowsAffected, nil
}

func transactionStatusResponse(status distributedtxn.Status) *ShardResponse {
	resp := CompletionResponse(0)
	resp.Transaction.ID = status.ID
	resp.Transaction.Revision = status.Revision
	switch status.Role {
	case distributedtxn.RoleCoordinator:
		resp.Transaction.Role = TransactionRoleCoordinator
		resp.Transaction.CoordinatorState = status.CoordinatorState
	case distributedtxn.RoleParticipant:
		resp.Transaction.Role = TransactionRoleParticipant
		resp.Transaction.ParticipantState = status.ParticipantState
	}
	return resp
}

func transactionError(err error) *ShardResponse {
	if errors.Is(err, distributedtxn.ErrOutcomeUnknown) {
		return NewErrorResponse(ErrorCommitOutcomeUnknown, err.Error())
	}
	if errors.Is(err, distributedtxn.ErrJournalNotFound) {
		return NewErrorResponse(ErrorTransactionNotFound, err.Error())
	}
	if errors.Is(err, distributedtxn.ErrJournalConflict) ||
		errors.Is(err, distributedtxn.ErrJournalBusy) ||
		errors.Is(err, distributedtxn.ErrInvalidState) ||
		errors.Is(err, sqldriver.ErrDistributedTransactionConflict) {
		return NewErrorResponse(ErrorTransactionConflict, err.Error())
	}
	return NewErrorResponse(ErrorMalformedRequest, err.Error())
}

// execute runs one admitted request against the borrowed Session and returns its
// typed response. Resource limits and the execution deadline are applied here;
// reads pin a snapshot and writes autocommit.
func (c *shardConn) execute(req *ShardRequest) *ShardResponse {
	rows, resultBytes := c.server.resultLimits(req)
	if err := c.sess.SetResultLimits(rows, resultBytes); err != nil {
		return classifyError(err)
	}
	if err := c.sess.SetIntermediateLimit(c.server.opts.MaxIntermediateBytes); err != nil {
		return classifyError(err)
	}

	ctx, cancel := c.server.requestContext(req)
	defer cancel()

	prep, err := c.sess.Prepare(ctx, req.SQL)
	if err != nil {
		return classifyError(err)
	}
	defer prep.Close()
	if req.ExecutionMode == ExecutionReadOnly && prep.Kind() != sqlast.KindSelect {
		return NewErrorResponse(
			ErrorReadOnly,
			fmt.Sprintf("shardservice: %s is not permitted by a read-only request", prep.Kind()),
		)
	}

	args := runtimeArgs(req.Params)
	if prep.ReturnsRows() {
		return c.executeQuery(ctx, prep, args)
	}
	return c.executeExec(ctx, prep, args)
}

// executeQuery pins a read-only snapshot for the request, runs the SELECT against
// it, materializes the rows into owned wire cells, and releases the snapshot. A
// leader-only strong read is served from the shard's current snapshot.
func (c *shardConn) executeQuery(
	ctx context.Context,
	prep *sqldriver.Prepared,
	args []any,
) *ShardResponse {
	// A SELECT is one statement-level snapshot. The SQL runtime defaults to
	// Read Committed, which refreshes the committed base before each statement;
	// pin this read-only transaction explicitly so a concurrent write cannot
	// leak into a result whose execution has already begun. A mutation with a
	// RETURNING projection must instead commit as a writable statement: pinning
	// it read-only would reject the DML, and the RETURNING rows are exactly the
	// rows the statement publishes, so autocommit is the correct cut.
	if prep.Kind() == sqlast.KindSelect {
		if err := c.sess.Begin(ctx, sqldriver.TxOptions{
			ReadOnly:  true,
			Isolation: sqldriver.IsolationRepeatableRead,
		}); err != nil {
			return classifyError(err)
		}
		// Release the snapshot unconditionally; a canceled request context must
		// not leave the pinned lease behind, so rollback runs on a background
		// context.
		defer c.sess.Rollback(context.Background())
	}

	cur, err := prep.Query(ctx, args)
	if err != nil {
		return classifyError(err)
	}
	defer cur.Close()

	cols := responseColumns(prep)
	rows := collectRows(cur, len(cols))
	return RowsResponse(cols, rows)
}

// executeExec runs a DML or DDL statement and reports its affected-row count.
// Writes autocommit as one durable statement; DDL cannot run inside the
// read-only snapshot transaction reads use.
func (c *shardConn) executeExec(
	ctx context.Context,
	prep *sqldriver.Prepared,
	args []any,
) *ShardResponse {
	result, err := prep.Exec(ctx, args)
	if err != nil {
		return classifyError(err)
	}
	return CompletionResponse(result.RowsAffected)
}

// responseColumns copies the prepared statement's output names into owned wire
// columns. The names are cloned because they borrow compiler storage the
// deferred Prepared.Close releases before the response is encoded.
func responseColumns(prep *sqldriver.Prepared) []Column {
	names := prep.Columns()
	cols := make([]Column, len(names))
	for i, name := range names {
		cols[i] = Column{Name: strings.Clone(name), TypeOID: pgOIDJSON}
	}
	return cols
}

// collectRows materializes every cursor row into owned wire cells. Each cell's
// bytes are copied because the cursor's storage is valid only until it closes;
// the result is already bounded by the session's result limits.
func collectRows(cur *sqldriver.Cursor, ncols int) [][]Cell {
	var rows [][]Cell
	for cur.Next() {
		row := make([]Cell, ncols)
		for i := 0; i < ncols; i++ {
			cell := cur.Cell(i)
			if cell.IsNull() {
				row[i] = Cell{Null: true}
				continue
			}
			row[i] = Cell{Bytes: cell.AppendJSON(nil)}
		}
		rows = append(rows, row)
	}
	return rows
}

// runtimeArgs materializes each typed parameter into the standard-library value
// the local Session binds. It allocates nothing for a parameterless statement.
func runtimeArgs(params []Param) []any {
	if len(params) == 0 {
		return nil
	}
	args := make([]any, len(params))
	for i := range params {
		args[i] = params[i].RuntimeValue()
	}
	return args
}

// resultLimits selects the row and byte caps for one request: a non-zero
// per-request cap overrides the server default; each is clamped to its signed
// domain before crossing into the Session.
func (s *Server) resultLimits(req *ShardRequest) (rows int, bytes int64) {
	rows = s.opts.MaxResultRows
	if req.MaxRows != 0 {
		requested := req.MaxRows
		if req.MaxRows > uint64(math.MaxInt32) {
			requested = math.MaxInt32
		}
		if s.opts.MaxResultRows != UnlimitedResults &&
			requested > uint64(s.opts.MaxResultRows) {
			requested = uint64(s.opts.MaxResultRows)
		}
		rows = int(requested)
	}
	bytes = s.opts.MaxResultBytes
	if req.MaxResultBytes != 0 {
		requested := req.MaxResultBytes
		if req.MaxResultBytes > uint64(math.MaxInt64) {
			requested = math.MaxInt64
		}
		if s.opts.MaxResultBytes != UnlimitedResults &&
			requested > uint64(s.opts.MaxResultBytes) {
			requested = uint64(s.opts.MaxResultBytes)
		}
		bytes = int64(requested)
	}
	return rows, bytes
}

// requestContext derives the execution context for one request from the server's
// base context, applying the request's deadline budget or the configured default.
// Deriving from the base context lets Close cancel an in-flight execution.
func (s *Server) requestContext(req *ShardRequest) (context.Context, context.CancelFunc) {
	budget := req.Deadline
	if budget <= 0 {
		budget = s.opts.DefaultRequestDeadline
	}
	if budget <= 0 {
		return context.WithCancel(s.baseCtx)
	}
	return context.WithTimeout(s.baseCtx, budget)
}

// classifyError maps a preparation or execution failure to its typed error
// frame. A deadline or cancellation, a resource-budget overflow, and every other
// failure (a syntax or execution error the shard could not accept) map to the
// deadline, resource-limit, and malformed-request kinds respectively.
func classifyError(err error) *ShardResponse {
	switch {
	case errors.Is(err, durable.ErrCommitOutcomeUnknown):
		return NewErrorResponse(ErrorCommitOutcomeUnknown, err.Error())
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled),
		errors.Is(err, query.ErrCanceled):
		return NewErrorResponse(ErrorDeadlineExceeded, err.Error())
	case errors.Is(err, query.ErrResultBudget),
		errors.Is(err, query.ErrIntermediateBudget),
		errors.Is(err, query.ErrWorkBudget),
		errors.Is(err, query.ErrSpillBudget),
		errors.Is(err, query.ErrJoinPairBudget),
		errors.Is(err, query.ErrAggregateBudget):
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	default:
		return NewErrorResponse(ErrorMalformedRequest, err.Error())
	}
}
