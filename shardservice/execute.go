package shardservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
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

type shardSQLPreparer interface {
	Prepare(context.Context, string) (*sqldriver.Prepared, error)
	PrepareWithParameterTypes(
		context.Context, string, []sqldriver.ParamType,
	) (*sqldriver.Prepared, error)
	PreparePartialAggregate(context.Context, string) (*sqldriver.Prepared, error)
	PreparePartialAggregateWithParameterTypes(
		context.Context, string, []sqldriver.ParamType,
	) (*sqldriver.Prepared, error)
}

func prepareShardSQL(
	ctx context.Context,
	preparer shardSQLPreparer,
	sqlText string,
	parameterTypes []sqldriver.ParamType,
	partialAggregate bool,
) (*sqldriver.Prepared, error) {
	if partialAggregate {
		if len(parameterTypes) != 0 {
			return preparer.PreparePartialAggregateWithParameterTypes(
				ctx, sqlText, parameterTypes,
			)
		}
		return preparer.PreparePartialAggregate(ctx, sqlText)
	}
	if len(parameterTypes) != 0 {
		return preparer.PrepareWithParameterTypes(ctx, sqlText, parameterTypes)
	}
	return preparer.Prepare(ctx, sqlText)
}

// pgOIDJSON is the type OID advertised for every result column. The shard wire
// carries each cell as JSON text, so every column names the JSON type rather
// than guessing a more specific type.
const pgOIDJSON = 114

// shardConn is one served connection: the socket and the single Session it owns.
type shardConn struct {
	server          *Server
	nc              net.Conn
	sess            *sqldriver.Session
	writeCancel     context.CancelFunc
	writeToken      uint64
	responseBatched bool
	writeActive     bool
	peer            rafttransport.NodeID
	authorization   *serviceauthz.Gate
	audit           serviceauthz.AuditSink
}

func terminalResponse(resp *ShardResponse) bool {
	return resp == nil || resp.Kind != ResponseRowBatch || resp.RowBatch.Final
}

func (c *shardConn) releaseWriteAdmission() {
	if c == nil || !c.writeActive {
		return
	}
	token := c.writeToken
	cancel := c.writeCancel
	c.writeToken = 0
	c.writeCancel = nil
	c.writeActive = false
	c.server.readFences.leaveWrite(token)
	cancel()
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
		if c.authorization != nil && !c.authorize(req) {
			if werr := c.writeResponse(NewErrorResponse(ErrorUnauthorized, "authorization denied")); werr != nil {
				return werr
			}
			continue
		}
		c.responseBatched = req.RowBatch.present()
		werr := c.handle(req, c.writeRequestResponse)
		c.responseBatched = false
		if werr != nil {
			if errors.Is(werr, errRowBatchTerminated) {
				continue
			}
			return werr
		}
	}
}

func (c *shardConn) authorize(request *ShardRequest) bool {
	if c == nil || c.authorization == nil || request == nil || c.peer == (rafttransport.NodeID{}) ||
		!request.Authority.Valid() {
		return false
	}
	generation := request.Authority.Generation
	if serviceauthz.CheckAndAudit(c.authorization, c.audit, c.peer, generation,
		serviceauthz.CapabilityDelegate) != serviceauthz.DecisionAllow {
		return false
	}
	capability, ok := sealedRequestCapability(request)
	if !ok {
		return false
	}
	return serviceauthz.CheckAndAudit(c.authorization, c.audit, request.Authority.Node,
		generation, capability) == serviceauthz.DecisionAllow
}

// sealedRequestCapability derives authority exclusively from the decoded
// operation union. ExecutionMode is an execution fence, never an authorization
// assertion controlled by a trusted principal.
func sealedRequestCapability(request *ShardRequest) (serviceauthz.Capability, bool) {
	if request == nil {
		return 0, false
	}
	if operation := request.Transaction.Operation; operation != TransactionNone {
		switch operation {
		case TransactionLookupCoordinator, TransactionLookupParticipant,
			TransactionScanCoordinator, TransactionReadParticipant,
			TransactionReadManifestSegment:
			return serviceauthz.CapabilityDataRead, true
		case TransactionStageCoordinator, TransactionStageParticipant,
			TransactionStageManifestCoordinator, TransactionStageManifestSegment,
			TransactionCommitCoordinator, TransactionApplyParticipant,
			TransactionAbortCoordinator, TransactionAbortParticipant,
			TransactionRetireCoordinator, TransactionReleaseParticipant,
			TransactionPrepareParticipant, TransactionAcquireReadFence,
			TransactionReleaseReadFence, TransactionPulseCoordinator:
			return serviceauthz.CapabilityDataWrite, true
		default:
			return 0, false
		}
	}
	if request.mutationCapturePresent() {
		return serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite, true
	}
	if request.Exchange.present() || request.GlobalIndexLookup.present() || request.DocumentScan.present() {
		return serviceauthz.CapabilityDataRead, true
	}
	if request.SQL != "" {
		capability := serviceauthz.SQLCapability(request.SQL)
		if request.Repartition.present() || request.PrimaryKeyRead.present() ||
			request.PartialAggregate || request.RowBatch.present() {
			capability |= serviceauthz.CapabilityDataRead
		}
		return capability, true
	}
	return 0, false
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
	return c.writeResponseFrame(resp, false)
}

func (c *shardConn) writeRequestResponse(resp *ShardResponse) error {
	return c.writeResponseFrame(resp, c.responseBatched)
}

var errRowBatchTerminated = errors.New("shardservice: row batch stream terminated by a typed error")

// write publishes one response frame. A terminal frame releases any ordinary
// write admission first, so a completed write cannot remain transiently busy
// after its result is observable. A rejected batch is replaced with a terminal
// error frame and the private sentinel stops the stream without closing the
// healthy connection.
func (c *shardConn) writeResponseFrame(resp *ShardResponse, batched bool) error {
	if c == nil {
		return errors.New("shardservice: nil response writer")
	}
	if terminalResponse(resp) {
		c.releaseWriteAdmission()
	}
	setDeadline(c.nc.SetWriteDeadline, c.server.opts.WriteTimeout)
	err := EncodeResponse(c.nc, resp)
	if err == nil {
		return nil
	}
	if !isEncodeRejection(err) {
		return err
	}
	// A codec rejection is converted into a terminal error frame. Release an
	// admitted writer before that substitute response can become observable,
	// including when the rejected row batch itself was nonterminal.
	c.releaseWriteAdmission()
	setDeadline(c.nc.SetWriteDeadline, c.server.opts.WriteTimeout)
	if writeErr := EncodeResponse(c.nc, NewErrorResponse(ErrorResourceLimit, err.Error())); writeErr != nil {
		return writeErr
	}
	if batched {
		return errRowBatchTerminated
	}
	return nil
}

// isEncodeRejection reports whether EncodeResponse rejected a response before
// writing any bytes, which means a substitute frame can be sent safely.
func isEncodeRejection(err error) bool {
	return errors.Is(err, errFrameTooLarge) ||
		errors.Is(err, errFieldTooLarge) ||
		errors.Is(err, errRowArity) ||
		errors.Is(err, errBadRowBatch) ||
		errors.Is(err, errBadEnum)
}

// handle admits req and writes its single response or bounded row stream.
func (c *shardConn) handle(req *ShardRequest, write func(*ShardResponse) error) error {
	if err := c.server.ownership.Admit(req); err != nil {
		var ae *AdmissionError
		if errors.As(err, &ae) {
			return write(ae.Response())
		}
		return write(NewErrorResponse(ErrorMalformedRequest, err.Error()))
	}
	if req.Exchange.present() {
		return write(c.executeExchange(req))
	}
	if c.server.hotRecorder == nil {
		return c.handleAdmitted(req, write)
	}
	started := time.Now()
	var payloadBytes uint64
	observed := false
	observe := func() {
		if observed {
			return
		}
		observed = true
		c.server.observeHotRequest(req, time.Since(started), payloadBytes)
	}
	observedWrite := func(resp *ShardResponse) error {
		payloadBytes = saturatingPayloadAdd(payloadBytes, responsePayloadBytes(resp))
		// Publish telemetry before the terminal frame becomes visible to the
		// caller. A controller can therefore rotate after observing completion
		// without moving this request into the following evidence window.
		if resp == nil || resp.Kind != ResponseRowBatch || resp.RowBatch.Final {
			observe()
		}
		return write(resp)
	}
	defer observe()
	return c.handleAdmitted(req, observedWrite)
}

// handleAdmitted executes a non-exchange request after the static ownership
// fence has accepted it. Keeping telemetry outside this function preserves the
// original path for servers that do not opt into a recorder.
func (c *shardConn) handleAdmitted(req *ShardRequest, write func(*ShardResponse) error) error {
	if err := c.ensureSession(); err != nil {
		return write(classifyError(err))
	}
	if req.Transaction.Operation != TransactionNone {
		if req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() || req.mutationCapturePresent() || req.DocumentScan.present() {
			return write(NewErrorResponse(ErrorMalformedRequest,
				"shardservice: a transaction command cannot carry an index read envelope"))
		}
		return write(c.transaction(req))
	}
	if req.ExecutionMode == ExecutionReadWrite {
		writeCtx, writeCancel := c.server.requestContext(req)
		for {
			if err := c.server.journal.WaitNoParticipantBarrier(
				writeCtx, req.BucketBits, req.AccessScopes,
			); err != nil {
				writeCancel()
				return write(classifyError(err))
			}
			writeToken, err := c.server.readFences.enterWrite(
				writeCtx, req.BucketBits, req.AccessScopes,
			)
			if err != nil {
				writeCancel()
				return write(classifyError(err))
			}
			conflict, barrierErr := c.server.journal.ParticipantBarrierConflict(
				req.BucketBits, req.AccessScopes,
			)
			if barrierErr != nil {
				c.server.readFences.leaveWrite(writeToken)
				writeCancel()
				return write(classifyError(barrierErr))
			}
			if conflict {
				c.server.readFences.leaveWrite(writeToken)
				continue
			}
			c.writeCancel = writeCancel
			c.writeToken = writeToken
			c.writeActive = true
			defer c.releaseWriteAdmission()
			break
		}
	} else {
		barrierCtx, barrierCancel := c.server.requestContext(req)
		err := c.server.journal.WaitNoParticipantBarrier(
			barrierCtx, req.BucketBits, req.AccessScopes,
		)
		barrierCancel()
		if err != nil {
			return write(classifyError(err))
		}
	}
	if !req.ReadFenceID.IsZero() {
		if req.ExecutionMode != ExecutionReadOnly || !c.server.readFences.validate(
			req.ReadFenceID, req.BucketBits, req.AccessScopes,
		) {
			return write(transactionError(distributedtxn.ErrJournalConflict))
		}
	}
	if req.Repartition.present() {
		return write(c.executeRepartition(req))
	}
	if req.GlobalIndexLookup.present() {
		return write(c.executeGlobalIndexLookup(req))
	}
	if req.mutationCapturePresent() {
		return write(c.executeMutationCapture(req))
	}
	if req.DocumentScan.present() {
		return write(c.executeDocumentScan(req))
	}
	return c.execute(req, write)
}

// observeHotRequest attributes exact one-bucket work directly and distributes
// work with unknown point locality across the fixed sketch. Resource totals
// remain exact either way; bounded scopes independently retain split fan-out
// evidence. The path allocates no maps, strings, or JSON values.
func (s *Server) observeHotRequest(req *ShardRequest, elapsed time.Duration, payloadBytes uint64) {
	recorder := s.hotRecorder
	if recorder == nil || req == nil {
		return
	}
	latency := clippedUint32(elapsed.Microseconds())
	if latency == 0 {
		latency = 1
	}
	load := autosplit.LoadVector{
		autosplit.ResourceIO:          clippedUint32FromUint64(payloadBytes),
		autosplit.ResourceRequests:    1,
		autosplit.ResourceLatencyDebt: latency,
	}
	source := recorder.Source()
	exact := len(req.AccessScopes) == 1 && req.BucketBits == source.BucketBits &&
		req.AccessScopes[0].End == req.AccessScopes[0].Start+1
	if exact {
		bucketRange, ok := distribution.VirtualBucketRange(
			distribution.VirtualBucket(req.AccessScopes[0].Start), req.BucketBits,
		)
		if ok && recorder.ObserveBucket(bucketRange.Start, load, latency, 1) {
			return
		}
	}
	if req.BucketBits != source.BucketBits || len(req.AccessScopes) == 0 {
		recorder.ObserveUnknown(load, 1)
		return
	}
	recorder.ObserveUniform(load)
	for i := range req.AccessScopes {
		start, end, ok := virtualBucketSpan(req.AccessScopes[i], req.BucketBits)
		if !ok || !recorder.ObserveSpan(start, end, 1) {
			recorder.ObserveUnbounded(1)
		}
	}
}

func virtualBucketSpan(
	scope distributedtxn.IntentScope,
	bits uint8,
) (distribution.KeyspacePoint, distribution.KeyspaceEnd, bool) {
	count := distribution.VirtualBucketCount(bits)
	if scope.Start >= scope.End || scope.End > count {
		return distribution.KeyspacePoint{}, distribution.KeyspaceEnd{}, false
	}
	first, ok := distribution.VirtualBucketRange(distribution.VirtualBucket(scope.Start), bits)
	if !ok {
		return distribution.KeyspacePoint{}, distribution.KeyspaceEnd{}, false
	}
	if scope.End == count {
		return first.Start, distribution.KeyspaceEnd{Max: true}, true
	}
	after, ok := distribution.VirtualBucketRange(distribution.VirtualBucket(scope.End), bits)
	if !ok {
		return distribution.KeyspacePoint{}, distribution.KeyspaceEnd{}, false
	}
	return first.Start, distribution.KeyspaceEnd{Point: after.Start}, true
}

func responsePayloadBytes(resp *ShardResponse) uint64 {
	if resp == nil {
		return 0
	}
	var total uint64
	for i := range resp.Columns {
		total = saturatingPayloadAdd(total, uint64(len(resp.Columns[i].Name)))
	}
	for i := range resp.Rows {
		for j := range resp.Rows[i] {
			total = saturatingPayloadAdd(total, uint64(len(resp.Rows[i][j].Bytes)))
		}
	}
	total = saturatingPayloadAdd(total, uint64(len(resp.ErrorMessage)))
	total = saturatingPayloadAdd(total, uint64(len(resp.DocumentScan.Next)))
	total = saturatingPayloadAdd(total, uint64(len(resp.Transaction.Record)))
	return total
}

func saturatingPayloadAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func clippedUint32(value int64) uint32 {
	if value <= 0 {
		return 0
	}
	if value > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}

func clippedUint32FromUint64(value uint64) uint32 {
	if value > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}

// executeRepartition runs a read-only shard fragment and pushes its rows
// directly into destination worker mailboxes. The gateway coordinates mailbox
// lifecycle but is not on the row data path.
func (c *shardConn) executeRepartition(req *ShardRequest) *ShardResponse {
	rows, resultBytes := c.server.resultLimits(req)
	if err := c.sess.SetResultLimits(rows, resultBytes); err != nil {
		return classifyError(err)
	}
	if err := c.sess.SetIntermediateLimit(c.server.opts.MaxIntermediateBytes); err != nil {
		return classifyError(err)
	}

	ctx, cancel := c.server.requestContext(req)
	defer cancel()
	prep, err := prepareShardSQL(
		ctx, c.sess, req.SQL, req.ParamTypes, req.PartialAggregate,
	)
	if err != nil {
		return classifyError(err)
	}
	defer prep.Close()
	if prep.Kind() != sqlast.KindSelect || !prep.ReturnsRows() {
		return NewErrorResponse(ErrorReadOnly,
			"shardservice: repartition producer requires a row-returning SELECT")
	}
	if err := c.sess.Begin(ctx, sqldriver.TxOptions{
		ReadOnly: true, Isolation: sqldriver.IsolationRepeatableRead,
	}); err != nil {
		return classifyError(err)
	}
	defer c.sess.Rollback(context.Background())

	var cursor sqldriver.Cursor
	if err := prep.QueryInto(ctx, runtimeArgs(req.Params), &cursor); err != nil {
		return classifyError(err)
	}
	defer cursor.Close()
	columns := len(prep.Columns())
	if columns == 0 || columns > int(exchange.MaxBlockColumns) ||
		!exchange.ValidBlockLimits(uint32(columns), req.Repartition.BlockRows, req.Repartition.BlockBytes) {
		return NewErrorResponse(ErrorResourceLimit, errBadRepartition.Error())
	}
	for _, ordinal := range req.Repartition.KeyColumns {
		if int(ordinal) >= columns {
			return NewErrorResponse(ErrorMalformedRequest, errBadRepartition.Error())
		}
	}

	sink, err := newRepartitionPeerSink(c.server, req.Repartition)
	if err != nil {
		return repartitionError(err)
	}
	defer sink.Close()
	partitioner := &repartitionPartitioner{
		columns: req.Repartition.KeyColumns, partitions: uint32(len(req.Repartition.Targets)),
	}
	producer, err := exchange.NewProducer(
		partitioner, uint32(len(req.Repartition.Targets)), uint32(columns),
		req.Repartition.BlockRows, req.Repartition.BlockBytes, req.Repartition.MaxMemory,
		req.Repartition.Producer, sink,
	)
	if err != nil {
		return repartitionError(err)
	}
	row := make([]Cell, columns)
	arena := make([]byte, 0, min(int(req.Repartition.BlockBytes), 64<<10))
	var produced int64
	for cursor.Next() {
		arena = arena[:0]
		for column := range row {
			cell := cursor.Cell(column)
			if cell.IsNull() {
				row[column] = Cell{Null: true}
				continue
			}
			start := len(arena)
			arena = cell.AppendJSON(arena)
			row[column] = Cell{Bytes: arena[start:len(arena):len(arena)]}
		}
		if err := producer.Add(ctx, row); err != nil {
			return repartitionError(err)
		}
		produced++
	}
	if err := producer.Finish(ctx); err != nil {
		return repartitionError(err)
	}
	return CompletionResponse(produced)
}

func repartitionError(err error) *ShardResponse {
	var peer *peerExchangeError
	if errors.As(err, &peer) && peer.kind.valid() {
		return NewErrorResponse(peer.kind, peer.Error())
	}
	if errors.Is(err, exchange.ErrBlockLimit) || errors.Is(err, exchange.ErrTotalLimit) ||
		errors.Is(err, exchange.ErrRegistryLimit) {
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	}
	return classifyError(err)
}

// ensureSession keeps exchange-only peer connections out of the SQL/session
// allocator entirely. A direct repartition can therefore hold one backpressured
// connection per destination without also pinning a database session there.
func (c *shardConn) ensureSession() error {
	if c.sess != nil {
		return nil
	}
	sess, err := c.server.db.NewSession(c.server.baseCtx)
	if err != nil {
		return err
	}
	c.sess = sess
	return nil
}

// executeExchange runs the ephemeral mailbox protocol after ordinary ownership
// admission but before SQL transaction barriers. Its keys and payloads remain
// raw bytes; no SQL parser, JSON decoder, or formatted identifier is involved.
func (c *shardConn) executeExchange(req *ShardRequest) *ShardResponse {
	command := req.Exchange
	if command.Operation == ExchangeOpen {
		budget := req.Deadline
		if budget <= 0 {
			budget = c.server.opts.DefaultRequestDeadline
		}
		var deadline int64
		if budget > 0 {
			deadline = time.Now().Add(budget).UnixNano()
		}
		_, err := c.server.exchanges.Open(exchange.Spec{
			Key: command.Key, Producers: command.Producers,
			QueuedBatches: command.QueuedBatches, ProducerBatches: command.ProducerBatches,
			BufferedRows: command.BufferedRows, BufferedBytes: command.BufferedBytes,
			TotalRows: command.TotalRows, TotalBytes: command.TotalBytes,
			DeadlineUnixNano: deadline,
		})
		if err != nil {
			return exchangeError(err)
		}
		return exchangeCompletion(ExchangeOpen)
	}

	box, ok := c.server.exchanges.Lookup(command.Key)
	if command.Operation == ExchangeCancel {
		if ok {
			c.server.exchanges.Delete(command.Key, exchange.ErrClosed)
		}
		return exchangeCompletion(ExchangeCancel)
	}
	if !ok {
		return exchangeError(exchange.ErrNotFound)
	}

	ctx, cancel := c.server.requestContext(req)
	defer cancel()
	switch command.Operation {
	case ExchangePush:
		if err := box.Push(ctx, command.Batch); err != nil {
			return exchangeError(err)
		}
		return exchangeCompletion(ExchangePush)
	case ExchangePull:
		if command.HasAck {
			if err := box.Ack(command.AckProducer, command.AckSequence); err != nil {
				return exchangeError(err)
			}
		}
		batch, err := box.Pull(ctx)
		if errors.Is(err, io.EOF) {
			resp := exchangeCompletion(ExchangePull)
			resp.Exchange.EOF = true
			return resp
		}
		if err != nil {
			return exchangeError(err)
		}
		resp := exchangeCompletion(ExchangePull)
		resp.Exchange.Batch = batch
		return resp
	case ExchangeReduce:
		return c.executeExchangeReduce(ctx, box, command)
	default:
		return exchangeError(exchange.ErrInvalidSpec)
	}
}

func exchangeCompletion(operation ExchangeOperation) *ShardResponse {
	resp := CompletionResponse(0)
	resp.Exchange.Operation = operation
	return resp
}

func exchangeError(err error) *ShardResponse {
	switch {
	case errors.Is(err, exchange.ErrNotFound):
		return NewErrorResponse(ErrorExchangeNotFound, err.Error())
	case errors.Is(err, exchange.ErrSpecConflict):
		return NewErrorResponse(ErrorExchangeConflict, err.Error())
	case errors.Is(err, exchange.ErrSequence),
		errors.Is(err, exchange.ErrAck),
		errors.Is(err, exchange.ErrProducer),
		errors.Is(err, exchange.ErrProducerBusy),
		errors.Is(err, exchange.ErrProducerFinal):
		return NewErrorResponse(ErrorExchangeSequence, err.Error())
	case errors.Is(err, exchange.ErrClosed):
		return NewErrorResponse(ErrorExchangeClosed, err.Error())
	case errors.Is(err, exchange.ErrRegistryLimit),
		errors.Is(err, exchange.ErrBatchLimit),
		errors.Is(err, exchange.ErrTotalLimit):
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return NewErrorResponse(ErrorDeadlineExceeded, err.Error())
	default:
		return NewErrorResponse(ErrorMalformedRequest, err.Error())
	}
}

// transaction executes the durable subset of the transaction protocol. Stage
// and lookup are safe independently of SQL publication. State transitions stay
// closed until commit proof and participant apply are joined to the local SQL
// transaction boundary; accepting them earlier would permit partial commit.
func (c *shardConn) transaction(req *ShardRequest) *ShardResponse {
	tx := req.Transaction
	allowAccess := tx.Operation == TransactionAcquireReadFence
	if req.SQL != "" || len(req.Params) != 0 || req.GlobalIndexLookup.present() ||
		req.PrimaryKeyRead.present() || req.mutationCapturePresent() || req.DocumentScan.present() ||
		(!allowAccess && len(req.AccessScopes) != 0) ||
		(!allowAccess && req.BucketBits != 0) {
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: a transaction command cannot also carry SQL or parameters")
	}
	if tx.Operation != TransactionLookupCoordinator &&
		tx.Operation != TransactionLookupParticipant &&
		tx.Operation != TransactionReadParticipant &&
		tx.Operation != TransactionReadManifestSegment &&
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
	case TransactionAcquireReadFence:
		if err := c.server.readFences.acquire(
			tx.ID, req.Deadline, req.BucketBits, req.AccessScopes,
		); err != nil {
			return transactionError(err)
		}
		conflict, err := c.server.journal.ParticipantBarrierConflict(
			req.BucketBits, req.AccessScopes,
		)
		if err != nil || conflict {
			_ = c.server.readFences.release(tx.ID)
			if err != nil {
				return transactionError(err)
			}
			return transactionError(errReadFenceBusy)
		}
		return CompletionResponse(0)
	case TransactionReleaseReadFence:
		if err := c.server.readFences.release(tx.ID); err != nil {
			return transactionError(err)
		}
		return CompletionResponse(0)
	case TransactionStageCoordinator:
		var participants [distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef
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
		writeCtx, writeCancel := c.server.requestContext(req)
		writeToken, waitErr := c.server.readFences.enterParticipant(
			writeCtx, record.BucketBits, record.IntentScopes,
		)
		if waitErr != nil {
			writeCancel()
			return classifyError(waitErr)
		}
		defer writeCancel()
		defer c.server.readFences.leaveParticipant(writeToken)
		status, err = c.server.journal.StageParticipant(tx.Record)
	case TransactionStageManifestCoordinator:
		return c.stageManifestCoordinator(req)
	case TransactionStageManifestSegment:
		return c.stageManifestSegment(req)
	case TransactionReadManifestSegment:
		return c.readManifestSegment(tx.ID, tx.SegmentIndex)
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
		status, err = c.transitionCoordinator(
			tx.ID, tx.Revision, distributedtxn.CoordinatorCommitted)
	case TransactionAbortCoordinator:
		status, err = c.transitionCoordinator(
			tx.ID, tx.Revision, distributedtxn.CoordinatorAborted)
	case TransactionPulseCoordinator:
		status, err = c.server.journal.PulseCoordinator(
			tx.ID, tx.Revision, tx.RecoveryPulse,
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
	if status.CoordinatorState == distributedtxn.CoordinatorRetired ||
		status.ParticipantState == distributedtxn.ParticipantReleased {
		c.server.scheduleJournalCompaction()
	}
	return transactionStatusResponse(status)
}

// executeGlobalIndexLookup serves the raw index lane after ordinary ownership,
// participant-barrier, scoped-intent, and optional coherent-read-fence
// admission. The driver pins exactly one relation snapshot and performs one
// point lookup or prefix-seek scan per ordered finite-domain key; no SQL
// parsing or JSON re-encoding occurs on this path.
func (c *shardConn) executeGlobalIndexLookup(req *ShardRequest) *ShardResponse {
	lookup := req.GlobalIndexLookup
	if req.ExecutionMode != ExecutionReadOnly || req.SQL != "" ||
		len(req.Params) != 0 || req.PrimaryKeyRead.present() || req.mutationCapturePresent() ||
		req.DocumentScan.present() || !lookup.canonical() {
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: invalid global-index lookup request")
	}
	ctx, cancel := c.server.requestContext(req)
	defer cancel()
	maxRows, maxBytes := c.server.resultLimits(req)
	capacity := maxRows
	if capacity < 0 || capacity > 256 {
		capacity = 16
	}
	rows := make([][]Cell, 0, capacity)
	err := c.sess.LookupGlobalIndexKeys(
		ctx, byteview.String(lookup.Relation), lookup.IndexID,
		lookup.Incarnation, lookup.KeyTuples, lookup.LocatorCount,
		lookup.Unique, maxRows, maxBytes,
		func(locator []byte) error {
			owned := append([]byte(nil), locator...)
			rows = append(rows, []Cell{{Bytes: owned}})
			return nil
		},
	)
	if err != nil {
		return classifyError(err)
	}
	return RowsResponse(
		[]Column{{Name: "locator", TypeOID: pgOIDJSON}}, rows,
	)
}

var errMutationCaptureLimit = errors.New("shardservice: mutation capture exceeds result bound")

// executeMutationCapture runs one of the two read-only capture contracts. The
// legacy marker returns key/before rows; the additive image marker returns
// key/before/after rows, with a NULL after image for DELETE. Neither publishes.
func (c *shardConn) executeMutationCapture(req *ShardRequest) *ShardResponse {
	if req.ExecutionMode != ExecutionReadOnly || req.SQL == "" ||
		req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() ||
		req.DocumentScan.present() || !req.ReadFenceID.IsZero() ||
		req.MutationCapture == req.MutationImageCapture {
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: invalid mutation capture request")
	}
	maxRows, maxBytes := c.server.resultLimits(req)
	if err := c.sess.SetResultLimits(maxRows, maxBytes); err != nil {
		return classifyError(err)
	}
	if err := c.sess.SetIntermediateLimit(c.server.opts.MaxIntermediateBytes); err != nil {
		return classifyError(err)
	}
	ctx, cancel := c.server.requestContext(req)
	defer cancel()
	prep, err := prepareShardSQL(ctx, c.sess, req.SQL, req.ParamTypes, false)
	if err != nil {
		return classifyError(err)
	}
	defer prep.Close()
	if prep.Kind() != sqlast.KindUpdate && prep.Kind() != sqlast.KindDelete {
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: mutation capture requires UPDATE or DELETE")
	}
	capacity := maxRows
	if capacity < 0 || capacity > 256 {
		capacity = 16
	}
	rows := make([][]Cell, 0, capacity)
	var retained int64
	columns := []Column{
		{Name: "primary_key", TypeOID: pgOIDJSON},
		{Name: "document", TypeOID: pgOIDJSON},
	}
	if req.MutationImageCapture {
		columns = []Column{
			{Name: "primary_key", TypeOID: pgOIDJSON},
			{Name: "before_document", TypeOID: pgOIDJSON},
			{Name: "after_document", TypeOID: pgOIDJSON},
		}
		err = prep.CaptureMutationImagesInto(ctx, runtimeArgs(req.Params), func(key, before, after []byte) error {
			if maxRows >= 0 && len(rows) >= maxRows {
				return errMutationCaptureLimit
			}
			if maxBytes >= 0 {
				remaining := maxBytes - retained
				if int64(len(key)) > remaining {
					return errMutationCaptureLimit
				}
				remaining -= int64(len(key))
				if int64(len(before)) > remaining {
					return errMutationCaptureLimit
				}
				remaining -= int64(len(before))
				if int64(len(after)) > remaining {
					return errMutationCaptureLimit
				}
			}
			afterCell := Cell{Null: true}
			if after != nil {
				afterCell = Cell{Bytes: append([]byte(nil), after...)}
			}
			rows = append(rows, []Cell{
				{Bytes: append([]byte(nil), key...)},
				{Bytes: append([]byte(nil), before...)},
				afterCell,
			})
			retained += int64(len(key))
			retained += int64(len(before))
			retained += int64(len(after))
			return nil
		})
	} else {
		err = prep.CaptureMutationInto(ctx, runtimeArgs(req.Params), func(key, before []byte) error {
			if maxRows >= 0 && len(rows) >= maxRows {
				return errMutationCaptureLimit
			}
			if maxBytes >= 0 && (int64(len(key)) > maxBytes-retained ||
				int64(len(before)) > maxBytes-retained-int64(len(key))) {
				return errMutationCaptureLimit
			}
			rows = append(rows, []Cell{
				{Bytes: append([]byte(nil), key...)},
				{Bytes: append([]byte(nil), before...)},
			})
			retained += int64(len(key))
			retained += int64(len(before))
			return nil
		})
	}
	if errors.Is(err, errMutationCaptureLimit) {
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	}
	if err != nil {
		return classifyError(err)
	}
	return RowsResponse(columns, rows)
}

func (c *shardConn) executeDocumentScan(req *ShardRequest) *ShardResponse {
	if req.ExecutionMode != ExecutionReadOnly || req.SQL != "" || len(req.Params) != 0 ||
		req.GlobalIndexLookup.present() || req.PrimaryKeyRead.present() || req.mutationCapturePresent() ||
		!req.ReadFenceID.IsZero() || !req.DocumentScan.canonical() ||
		req.MaxRows == 0 || req.MaxResultBytes == 0 {
		return NewErrorResponse(ErrorMalformedRequest,
			"shardservice: invalid bounded document scan request")
	}
	maxRows, maxBytes := c.server.resultLimits(req)
	ctx, cancel := c.server.requestContext(req)
	defer cancel()
	capacity := maxRows
	if capacity > 256 {
		capacity = 256
	}
	rows := make([][]Cell, 0, capacity)
	next, complete, err := c.sess.ScanDocumentsAfter(
		ctx, byteview.String(req.DocumentScan.Relation), req.DocumentScan.After,
		maxRows, maxBytes, func(key, document []byte) error {
			rows = append(rows, []Cell{
				{Bytes: append([]byte(nil), key...)},
				{Bytes: append([]byte(nil), document...)},
			})
			return nil
		},
	)
	if err != nil {
		if errors.Is(err, sqldriver.ErrDocumentScanPageTooSmall) {
			return NewErrorResponse(ErrorResourceLimit, err.Error())
		}
		return classifyError(err)
	}
	resp := RowsResponse([]Column{
		{Name: "primary_key", TypeOID: pgOIDJSON},
		{Name: "document", TypeOID: pgOIDJSON},
	}, rows)
	resp.DocumentScan = DocumentScanReply{
		Present: true, Complete: complete, Next: next,
	}
	return resp
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
	if _, manifestErr := c.server.journal.ManifestCoordinator(id); manifestErr == nil {
		response.Transaction.RecordKind = TransactionRecordManifestCoordinator
	} else if errors.Is(manifestErr, distributedtxn.ErrJournalNotFound) {
		response.Transaction.RecordKind = TransactionRecordInlineCoordinator
	} else {
		return transactionError(manifestErr)
	}
	return response
}

func (c *shardConn) scanCoordinator(after distributedtxn.ID) *ShardResponse {
	status, record, ok := c.server.journal.NextCoordinator(after)
	if !ok {
		return CompletionResponse(0)
	}
	response := transactionStatusResponse(status)
	response.Transaction.Record = record
	if _, manifestErr := c.server.journal.ManifestCoordinator(status.ID); manifestErr == nil {
		response.Transaction.RecordKind = TransactionRecordManifestCoordinator
	} else if errors.Is(manifestErr, distributedtxn.ErrJournalNotFound) {
		response.Transaction.RecordKind = TransactionRecordInlineCoordinator
	} else {
		return transactionError(manifestErr)
	}
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
	response.Transaction.RecordKind = TransactionRecordParticipant
	return response
}

func (c *shardConn) stageManifestSegment(req *ShardRequest) *ShardResponse {
	tx := req.Transaction
	if tx.manifestMeta == nil || !tx.manifestMeta.valid {
		meta, err := inspectTransactionManifestSegment(tx.ManifestSegment)
		if err != nil {
			return transactionError(err)
		}
		tx.manifestMeta = &meta
	}
	if tx.manifestMeta.index == 0 {
		return transactionError(distributedtxn.ErrJournalConflict)
	}
	scratch := borrowTransactionManifestScratch()
	defer releaseTransactionManifestScratch(scratch)
	if _, err := c.server.journal.StageManifestSegment(
		tx.ID, tx.ManifestSegment, scratch.participants[:], scratch.identities[:],
	); err != nil {
		return transactionError(err)
	}
	status, ok := c.server.journal.CoordinatorStatus(tx.ID)
	if !ok {
		return transactionError(distributedtxn.ErrJournalNotFound)
	}
	return transactionStatusResponse(status)
}

func (c *shardConn) stageManifestCoordinator(req *ShardRequest) *ShardResponse {
	tx := req.Transaction
	record, err := distributedtxn.OpenManifestCoordinator(tx.Record)
	if err != nil {
		return transactionError(err)
	}
	if tx.manifestMeta == nil || !tx.manifestMeta.valid {
		meta, inspectErr := inspectTransactionManifestFirstSegment(
			tx.ManifestSegment, record.Manifest)
		if inspectErr != nil {
			return transactionError(inspectErr)
		}
		tx.manifestMeta = &meta
	}
	meta := *tx.manifestMeta
	if !vibejson.BytesEqualString(meta.distribution[:meta.distributionLen], string(req.Distribution)) ||
		!vibejson.BytesEqualString(meta.shard[:meta.shardLen], string(req.Shard)) ||
		meta.routingVersion != uint64(req.RoutingVersion) ||
		meta.allocation != uint64(req.AllocationGeneration) ||
		meta.ownershipEpoch != uint64(req.OwnershipEpoch) {
		return transactionError(distributedtxn.ErrJournalConflict)
	}
	// Both records were fully authenticated above. The two fsyncs intentionally
	// leave a restart-safe incomplete staging coordinator if page-zero durability
	// has an outcome-unknown failure; exact retries converge through the journal.
	status, err := c.server.journal.StageManifestCoordinator(tx.Record)
	if err != nil {
		return transactionError(err)
	}
	scratch := borrowTransactionManifestScratch()
	defer releaseTransactionManifestScratch(scratch)
	if status.CoordinatorState != distributedtxn.CoordinatorStaging {
		page, pageErr := c.server.journal.ManifestPage(
			record.ID, 0, scratch.participants[:], scratch.identities[:])
		if pageErr == nil {
			if !bytes.Equal(page.Segment.Raw, tx.ManifestSegment) {
				return transactionError(distributedtxn.ErrJournalConflict)
			}
			return transactionStatusResponse(status)
		}
		// An incomplete segmented coordinator may be aborted before page zero is
		// durable. That terminal exact-stage retry converges to the abort; a
		// committed coordinator can never be missing its descriptor-bound page.
		if errors.Is(pageErr, distributedtxn.ErrJournalNotFound) &&
			status.CoordinatorState != distributedtxn.CoordinatorCommitted {
			return transactionStatusResponse(status)
		}
		return transactionError(pageErr)
	}
	if _, err = c.server.journal.StageManifestSegment(
		record.ID, tx.ManifestSegment, scratch.participants[:], scratch.identities[:],
	); err != nil {
		return transactionError(err)
	}
	return transactionStatusResponse(status)
}

func (c *shardConn) readManifestSegment(id distributedtxn.ID, index uint32) *ShardResponse {
	status, ok := c.server.journal.CoordinatorStatus(id)
	if !ok {
		return transactionError(distributedtxn.ErrJournalNotFound)
	}
	scratch := borrowTransactionManifestScratch()
	defer releaseTransactionManifestScratch(scratch)
	page, err := c.server.journal.ManifestPage(
		id, index, scratch.participants[:], scratch.identities[:])
	if err != nil {
		return transactionError(err)
	}
	response := transactionStatusResponse(status)
	response.Transaction.RecordKind = TransactionRecordManifestSegment
	response.Transaction.SegmentIndex = page.Segment.Index
	response.Transaction.Record = page.Segment.Raw
	return response
}

func (c *shardConn) transitionCoordinator(
	id distributedtxn.ID,
	expected uint64,
	next distributedtxn.CoordinatorState,
) (distributedtxn.Status, error) {
	if _, err := c.server.journal.ManifestCoordinator(id); err == nil {
		return c.server.journal.SealManifestCoordinator(id, expected, next)
	} else if !errors.Is(err, distributedtxn.ErrJournalNotFound) {
		return distributedtxn.Status{}, err
	}
	return c.server.journal.TransitionCoordinator(id, expected, next)
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
	guardPending := false
	for {
		mutation, ok, err := batch.Next()
		if err != nil {
			return 0, transactionError(err)
		}
		if !ok {
			break
		}
		if mutation.Kind == MutationPrimaryPrecondition {
			if guardPending {
				return 0, transactionError(ErrMutationBatch)
			}
			if err := c.sess.ValidatePrimaryDocumentDigests(
				ctx, mutation.Relation, mutation.PrimaryPath,
				mutation.ExpectedKeys, mutation.ExpectedDigests,
			); err != nil {
				return 0, transactionError(err)
			}
			guardPending = true
			continue
		}
		if mutation.Kind == MutationPrimaryCheck {
			if guardPending {
				return 0, transactionError(ErrMutationBatch)
			}
			if err := c.sess.CheckPrimaryDocumentDigests(
				ctx, mutation.Relation, mutation.PrimaryPath,
				mutation.ExpectedKeys, mutation.ExpectedDigests,
			); err != nil {
				return 0, transactionError(err)
			}
			continue
		}
		if mutation.Kind != MutationSQL {
			if guardPending {
				return 0, transactionError(ErrMutationBatch)
			}
			_, mutationErr := c.sess.ApplyGlobalIndexMutation(
				ctx, mutation.Relation, mutation.IndexID, mutation.Incarnation,
				mutation.EntryKey, mutation.Value, mutation.LocatorCount,
				mutation.Unique, mutation.Kind == MutationGlobalIndexDelete,
			)
			if mutationErr != nil {
				return 0, transactionError(mutationErr)
			}
			continue
		}
		prep, err := prepareShardSQL(
			ctx, c.sess, mutation.SQL, mutation.ParamTypes, false,
		)
		if err != nil {
			return 0, classifyError(err)
		}
		if prep.Kind() == sqlast.KindSelect || prep.ReturnsRows() ||
			(guardPending && prep.Kind() != sqlast.KindUpdate && prep.Kind() != sqlast.KindDelete) {
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
		guardPending = false
	}
	if guardPending {
		return 0, transactionError(ErrMutationBatch)
	}
	return rowsAffected, nil
}

func transactionStatusResponse(status distributedtxn.Status) *ShardResponse {
	resp := CompletionResponse(0)
	resp.Transaction.ID = status.ID
	resp.Transaction.Revision = status.Revision
	resp.Transaction.RecoveryPulse = status.RecoveryPulse
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
	if errors.Is(err, errReadFenceBusy) {
		return NewErrorResponse(ErrorReadFenceBusy, err.Error())
	}
	if errors.Is(err, errReadFenceCapacity) {
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	}
	if errors.Is(err, durable.ErrBatchTooLarge) ||
		errors.Is(err, durable.ErrKeyTooLarge) ||
		errors.Is(err, durable.ErrDocumentTooLarge) ||
		errors.Is(err, sqldriver.ErrTransactionTooLarge) {
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	}
	if errors.Is(err, distributedtxn.ErrJournalConflict) ||
		errors.Is(err, distributedtxn.ErrJournalBusy) ||
		errors.Is(err, distributedtxn.ErrInvalidState) ||
		errors.Is(err, sqldriver.ErrTransactionConflict) ||
		errors.Is(err, sqldriver.ErrDistributedTransactionConflict) {
		return NewErrorResponse(ErrorTransactionConflict, err.Error())
	}
	return NewErrorResponse(ErrorMalformedRequest, err.Error())
}

// execute runs one admitted request against the borrowed Session and writes its
// typed response. Resource limits and the execution deadline are applied here;
// reads pin a snapshot and writes autocommit.
func (c *shardConn) execute(
	req *ShardRequest,
	write func(*ShardResponse) error,
) error {
	rows, resultBytes := c.server.resultLimits(req)
	if err := c.sess.SetResultLimits(rows, resultBytes); err != nil {
		return write(classifyError(err))
	}
	if err := c.sess.SetIntermediateLimit(c.server.opts.MaxIntermediateBytes); err != nil {
		return write(classifyError(err))
	}

	ctx, cancel := c.server.requestContext(req)
	defer cancel()

	prep, err := prepareShardSQL(
		ctx, c.sess, req.SQL, req.ParamTypes, req.PartialAggregate,
	)
	if err != nil {
		return write(classifyError(err))
	}
	defer prep.Close()
	if req.ExecutionMode == ExecutionReadOnly && prep.Kind() != sqlast.KindSelect {
		return write(NewErrorResponse(
			ErrorReadOnly,
			fmt.Sprintf("shardservice: %s is not permitted by a read-only request", prep.Kind()),
		))
	}

	args := runtimeArgs(req.Params)
	if prep.ReturnsRows() {
		if req.RowBatch.present() {
			return c.executeQueryBatches(ctx, prep, args, req.PrimaryKeyRead, req.RowBatch, write)
		}
		return write(c.executeQuery(ctx, prep, args, req.PrimaryKeyRead))
	}
	return write(c.executeExec(ctx, prep, args))
}

// executeQuery pins a read-only snapshot for the request, runs the SELECT against
// it, materializes the rows into owned wire cells, and releases the snapshot. A
// leader-only strong read is served from the shard's current snapshot.
func (c *shardConn) executeQuery(
	ctx context.Context,
	prep *sqldriver.Prepared,
	args []any,
	primaryRead PrimaryKeyReadRequest,
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

	var (
		cur sqldriver.Cursor
		err error
	)
	if primaryRead.present() {
		err = prep.QueryCandidateKeysInto(
			ctx, args, primaryRead.PrimaryPath, primaryRead.Keys, &cur,
		)
	} else {
		err = prep.QueryInto(ctx, args, &cur)
	}
	if err != nil {
		return classifyError(err)
	}
	defer cur.Close()

	cols := responseColumns(prep)
	rows := collectRows(&cur, len(cols))
	return RowsResponse(cols, rows)
}

// executeQueryBatches consumes a materialized SQL cursor into independently
// bounded wire batches. Synchronous writes provide transport backpressure and
// the batch arenas are reused after each frame, avoiding a second whole-result
// copy. The SQL executor's own total row/byte budget remains authoritative.
func (c *shardConn) executeQueryBatches(
	ctx context.Context,
	prep *sqldriver.Prepared,
	args []any,
	primaryRead PrimaryKeyReadRequest,
	limits RowBatchRequest,
	write func(*ShardResponse) error,
) error {
	if prep.Kind() == sqlast.KindSelect {
		if err := c.sess.Begin(ctx, sqldriver.TxOptions{
			ReadOnly: true, Isolation: sqldriver.IsolationRepeatableRead,
		}); err != nil {
			return write(classifyError(err))
		}
		defer c.sess.Rollback(context.Background())
	}

	var (
		cur sqldriver.Cursor
		err error
	)
	if primaryRead.present() {
		err = prep.QueryCandidateKeysInto(
			ctx, args, primaryRead.PrimaryPath, primaryRead.Keys, &cur,
		)
	} else {
		err = prep.QueryInto(ctx, args, &cur)
	}
	if err != nil {
		return write(classifyError(err))
	}
	defer cur.Close()

	cols := responseColumns(prep)
	if len(cols) == 0 {
		return write(NewErrorResponse(ErrorMalformedRequest,
			"shardservice: a row batch requires at least one result column"))
	}
	return streamCursorBatches(&cur, cols, limits, write)
}

// streamCursorBatches packs cell JSON into one pre-sized arena per frame. The
// negotiated capacity prevents growth, so Cell.Bytes can point directly into
// it without a per-cell allocation or a staging copy.
func streamCursorBatches(
	cur *sqldriver.Cursor,
	cols []Column,
	limits RowBatchRequest,
	write func(*ShardResponse) error,
) error {
	ncols := len(cols)
	batchRows := int(limits.BatchRows)
	if byBytes := int(limits.BatchBytes) / ncols; byBytes < batchRows {
		batchRows = byBytes
	}
	if byCells := int(MaxRowBatchCells) / ncols; byCells < batchRows {
		batchRows = byCells
	}
	if batchRows < 1 {
		batchRows = 1
	}

	rows := make([][]Cell, 0, batchRows)
	cells := make([]Cell, 0, batchRows*ncols)
	arena := make([]byte, 0, int(limits.BatchBytes))
	rowDataBytes := 0
	sequence := uint32(0)

	flush := func(final bool) error {
		rows = rows[:0]
		for start := 0; start < len(cells); start += ncols {
			rows = append(rows, cells[start:start+ncols:start+ncols])
		}
		batchCols := []Column(nil)
		if sequence == 0 {
			batchCols = cols
		}
		resp := &ShardResponse{
			Kind: ResponseRowBatch, Columns: batchCols, Rows: rows,
			RowBatch: RowBatchReply{
				Sequence: sequence, ColumnCount: uint32(ncols), Final: final,
			},
		}
		if err := write(resp); err != nil {
			return err
		}
		sequence++
		cells = cells[:0]
		arena = arena[:0]
		rowDataBytes = 0
		return nil
	}

	for cur.Next() {
		pendingBytes := 0
		for i := 0; i < ncols; i++ {
			cell := cur.Cell(i)
			if cell.IsNull() {
				pendingBytes++
				continue
			}
			payload := cell.Payload()
			if payload != nil {
				pendingBytes += 1 + 4 + len(payload)
				continue
			}
			var scratch [32]byte
			pendingBytes += 1 + 4 + len(cell.AppendJSON(scratch[:0]))
		}
		if pendingBytes > int(limits.BatchBytes) {
			return write(NewErrorResponse(ErrorResourceLimit,
				"shardservice: one encoded row exceeds the row-batch byte limit"))
		}
		if len(cells) != 0 && (len(cells)/ncols >= int(limits.BatchRows) ||
			len(cells)+ncols > int(MaxRowBatchCells) ||
			rowDataBytes+pendingBytes > int(limits.BatchBytes)) {
			if err := flush(false); err != nil {
				return err
			}
		}
		for i := 0; i < ncols; i++ {
			cell := cur.Cell(i)
			if cell.IsNull() {
				cells = append(cells, Cell{Null: true})
				continue
			}
			start := len(arena)
			arena = cell.AppendJSON(arena)
			cells = append(cells, Cell{Bytes: arena[start:len(arena):len(arena)]})
		}
		rowDataBytes += pendingBytes
	}
	return flush(true)
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
		errors.Is(err, sqldriver.ErrGlobalIndexLookupTooLarge),
		errors.Is(err, query.ErrIntermediateBudget),
		errors.Is(err, query.ErrWorkBudget),
		errors.Is(err, query.ErrSpillBudget),
		errors.Is(err, query.ErrJoinPairBudget),
		errors.Is(err, query.ErrAggregateBudget):
		return NewErrorResponse(ErrorResourceLimit, err.Error())
	default:
		if diagnostic, ok := classifySQLDiagnostic(err); ok {
			if response, encoded := newSQLDiagnosticResponse(diagnostic); encoded {
				return response
			}
		}
		return NewErrorResponse(ErrorMalformedRequest, err.Error())
	}
}
