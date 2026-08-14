package shardservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
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
	return c.execute(req)
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
	// A request is one statement-level snapshot. The SQL runtime defaults to
	// Read Committed, which refreshes the committed base before each statement;
	// pin this read-only transaction explicitly so a concurrent write cannot
	// leak into a result whose execution has already begun.
	if err := c.sess.Begin(ctx, sqldriver.TxOptions{
		ReadOnly:  true,
		Isolation: sqldriver.IsolationRepeatableRead,
	}); err != nil {
		return classifyError(err)
	}
	// Release the snapshot unconditionally; a canceled request context must not
	// leave the pinned lease behind, so rollback runs on a background context.
	defer c.sess.Rollback(context.Background())

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
