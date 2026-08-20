package shardservice

import (
	"time"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

// The shard-service wire vocabulary: the request a gateway sends one leader,
// the response a shard returns, and the closed enums that discriminate their
// typed fields.
//
// The contract carries SQL text plus typed bound parameters, never a serialized
// execution plan or ConstraintProgram. Each shard parses and plans that SQL
// locally with the ordinary vibedb parser and planner, so no second frozen plan
// format is introduced anywhere. This mirrors the pgwire prepared-statement and
// typed-parameter model rather than inventing a distributed plan encoding.

// wireVersion is the first byte of every request and response body. A decoder
// rejects an unknown version rather than guessing an older or newer layout.
const wireVersion = 1

// TransactionOperation is the closed transaction command set. Ordinary
// requests leave it at TransactionNone and retain their existing wire shape.
// Stage commands carry the checksummed durable record itself; later
// commands address it by its raw 128-bit identity and expected revision.
type TransactionOperation uint8

const (
	TransactionNone TransactionOperation = iota
	TransactionStageCoordinator
	TransactionStageParticipant
	TransactionLookupCoordinator
	TransactionLookupParticipant
	TransactionCommitCoordinator
	TransactionApplyParticipant
	TransactionAbortCoordinator
	TransactionAbortParticipant
	TransactionRetireCoordinator
	TransactionReleaseParticipant
)

func (op TransactionOperation) valid() bool {
	return op >= TransactionStageCoordinator && op <= TransactionReleaseParticipant
}

func (op TransactionOperation) stages() bool {
	return op == TransactionStageCoordinator || op == TransactionStageParticipant
}

// TransactionRequest is the optional transaction envelope. Record aliases the
// request frame and is populated only by a stage operation. Every other
// operation uses ID and Revision; lookup permits Revision zero, while a state
// transition requires an exact nonzero revision for compare-and-swap replay.
type TransactionRequest struct {
	Operation TransactionOperation
	ID        distributedtxn.ID
	Revision  uint64
	Record    []byte
}

// TransactionRole discriminates the state returned by a transaction reply.
type TransactionRole uint8

const (
	TransactionRoleNone TransactionRole = iota
	TransactionRoleCoordinator
	TransactionRoleParticipant
)

// TransactionReply reports the durable state observed after a transaction
// command. Exactly one typed state is populated according to Role.
type TransactionReply struct {
	Role             TransactionRole
	ID               distributedtxn.ID
	Revision         uint64
	CoordinatorState distributedtxn.CoordinatorState
	ParticipantState distributedtxn.ParticipantState
}

// ReadPolicy selects the consistency contract a read is served under. The enum
// ordering is a stable public safety contract and must not be reordered: the
// zero value is the strongest, leader-only policy.
type ReadPolicy uint8

const (
	// ReadStrong serves from the shard leader under its ownership/replication
	// authority. It is the only policy the current service honors; the zero value is safe.
	ReadStrong ReadPolicy = iota
	// ReadSession is reserved and refused; no session-read serving path exists.
	ReadSession
	// ReadStale is reserved and refused; no stale-read serving path exists.
	ReadStale
)

// String renders the policy name for diagnostics.
func (p ReadPolicy) String() string {
	switch p {
	case ReadStrong:
		return "Strong"
	case ReadSession:
		return "Session"
	case ReadStale:
		return "Stale"
	default:
		return "Invalid"
	}
}

// valid reports whether p is a known policy value the codec may carry.
func (p ReadPolicy) valid() bool { return p <= ReadStale }

// ExecutionMode declares whether a request is permitted to mutate shard state.
// The zero value is deliberately read-only: a newly constructed or partially
// decoded request cannot acquire write authority by omission.
type ExecutionMode uint8

const (
	// ExecutionReadOnly permits SELECT, including WITH, set operations, and
	// EXPLAIN SELECT. The shard verifies the parsed statement kind before it
	// executes anything.
	ExecutionReadOnly ExecutionMode = iota
	// ExecutionReadWrite permits the direct shard protocol's existing DML and
	// DDL execution. Distributed gateways never select this mode.
	ExecutionReadWrite
)

// String renders the mode name for diagnostics.
func (m ExecutionMode) String() string {
	switch m {
	case ExecutionReadOnly:
		return "ReadOnly"
	case ExecutionReadWrite:
		return "ReadWrite"
	default:
		return "Invalid"
	}
}

func (m ExecutionMode) valid() bool { return m <= ExecutionReadWrite }

// ParamKind is the wire type of one bound parameter. It refines sql/driver's
// scalar/document split: Null, Bool, Number, and String are the scalar members
// (sql/driver ParamScalar), and Document is a complete JSON value (sql/driver
// ParamDocument).
type ParamKind uint8

const (
	// ParamInvalid is the zero value; no constructor produces it and the codec
	// refuses to encode or decode it.
	ParamInvalid ParamKind = iota
	// ParamNull binds SQL NULL.
	ParamNull
	// ParamBool binds a boolean scalar.
	ParamBool
	// ParamNumber binds an exact decimal scalar carried as its canonical JSON
	// number spelling, so numeric equality survives the wire without float
	// rounding.
	ParamNumber
	// ParamString binds a UTF-8 string scalar.
	ParamString
	// ParamDocument binds a complete JSON document (object, array, or scalar).
	ParamDocument
)

// String renders the kind name for diagnostics.
func (k ParamKind) String() string {
	switch k {
	case ParamNull:
		return "Null"
	case ParamBool:
		return "Bool"
	case ParamNumber:
		return "Number"
	case ParamString:
		return "String"
	case ParamDocument:
		return "Document"
	default:
		return "Invalid"
	}
}

// valid reports whether k names a real parameter member.
func (k ParamKind) valid() bool { return k >= ParamNull && k <= ParamDocument }

// Param is one typed bound parameter. Bool is meaningful only for ParamBool;
// Bytes carries the number spelling, UTF-8 string bytes, or document JSON for
// the other non-null kinds. Bytes is borrowed and must remain immutable until
// the request finishes; a decoded request owns the frame backing its slices.
type Param struct {
	Kind  ParamKind
	Bool  bool
	Bytes []byte
}

// Valid reports whether p names a real wire parameter member and carries a
// valid byte payload. This keeps malformed JSON and number spellings out of the
// routing and shard execution paths instead of deferring validation until SQL
// binding.
func (p Param) Valid() bool {
	switch p.Kind {
	case ParamNull, ParamBool:
		return len(p.Bytes) == 0
	case ParamString:
		return utf8.Valid(p.Bytes)
	case ParamNumber:
		_, ok := (vibejson.RawValue{Src: p.Bytes}).NumberBytes()
		return ok
	case ParamDocument:
		return vibejson.Valid(p.Bytes)
	default:
		return false
	}
}

// NullParam returns a SQL NULL parameter.
func NullParam() Param { return Param{Kind: ParamNull} }

// BoolParam returns a boolean scalar parameter.
func BoolParam(v bool) Param { return Param{Kind: ParamBool, Bool: v} }

// NumberParam returns an exact-decimal scalar parameter over spelling, a JSON
// number literal. The codec carries the spelling verbatim; the receiving shard
// binds it through the same exact-number path used by local equality.
func NumberParam(spelling string) Param { return NumberBytesParam(byteview.Bytes(spelling)) }

// NumberBytesParam returns an exact-decimal scalar parameter borrowing
// spelling. The caller must keep spelling immutable until the request finishes.
func NumberBytesParam(spelling []byte) Param { return Param{Kind: ParamNumber, Bytes: spelling} }

// StringParam returns a UTF-8 string scalar parameter.
func StringParam(s string) Param { return StringBytesParam(byteview.Bytes(s)) }

// StringBytesParam returns a UTF-8 scalar parameter borrowing value.
func StringBytesParam(value []byte) Param { return Param{Kind: ParamString, Bytes: value} }

// DocumentParam returns a complete-JSON-document parameter.
func DocumentParam(jsonText string) Param { return DocumentBytesParam(byteview.Bytes(jsonText)) }

// DocumentBytesParam returns a complete JSON parameter borrowing document.
func DocumentBytesParam(document []byte) Param {
	return Param{Kind: ParamDocument, Bytes: document}
}

// RuntimeValue materializes p as a byte-native value the local sql/driver
// Session accepts. Exact numbers use vibejson.RawValue so they remain distinct
// from unquoted UTF-8 strings without encoding/json.Number or a string copy;
// strings and documents remain borrowed byte slices.
func (p Param) RuntimeValue() any {
	switch p.Kind {
	case ParamNull:
		return nil
	case ParamBool:
		return p.Bool
	case ParamNumber:
		return vibejson.RawValue{Src: p.Bytes}
	case ParamString, ParamDocument:
		return p.Bytes
	default:
		return nil
	}
}

// ShardRequest is one SQL statement dispatched to a shard leader. It carries the
// statement text and its typed parameters, the ownership coordinates the shard
// admits against, the read policy, and the deadline and resource limits that
// bound its execution. It never carries a serialized plan.
type ShardRequest struct {
	// SQL is the statement text the shard parses and plans locally.
	SQL string
	// Params are the typed bound parameters, in placeholder order.
	Params []Param

	// Distribution and Shard name the target the shard admits ownership of.
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	// AllocationGeneration identifies the topology-created physical shard
	// allocation. It is checked before routing and per-allocation ownership
	// epochs, so a reused logical label cannot admit a stale request.
	AllocationGeneration distribution.ShardAllocationGeneration
	// RoutingVersion pins the manifest generation the caller routed against.
	RoutingVersion distribution.RoutingVersion
	// OwnershipEpoch is the caller's view of the shard's fencing epoch.
	OwnershipEpoch distribution.OwnershipEpoch
	// HasMinPosition selects MinPosition. The current service carries and validates
	// the field but rejects every present value because it has no serving replicated
	// apply log; it
	// never silently weakens the request to an ordinary strong read. When false,
	// MinPosition must be zero so the optional has one canonical representation.
	HasMinPosition bool
	MinPosition    Position

	// ReadPolicy selects the consistency contract; the current service honors only ReadStrong.
	ReadPolicy ReadPolicy
	// ExecutionMode fences mutation authority. Its zero value is read-only;
	// direct shard writers must opt into ExecutionReadWrite explicitly.
	ExecutionMode ExecutionMode

	// Deadline is the execution budget measured from the shard's receipt of the
	// request. Zero means the shard applies its configured default.
	Deadline time.Duration
	// MaxResultBytes caps the total encoded result bytes; zero means the shard's
	// configured default.
	MaxResultBytes uint64
	// MaxRows caps the returned row count; zero means the shard's configured
	// default.
	MaxRows uint64

	// Transaction selects the transaction path. Its zero value is absent and
	// preserves the ordinary autocommit encoding and execution path.
	Transaction TransactionRequest
}

// ResponseKind discriminates the three shapes a ShardResponse can take.
type ResponseKind uint8

const (
	// ResponseInvalid is the zero value; the codec never encodes or decodes it.
	ResponseInvalid ResponseKind = iota
	// ResponseRows carries column metadata and materialized row cells.
	ResponseRows
	// ResponseCompletion carries a DDL/DML affected-row count.
	ResponseCompletion
	// ResponseError carries a typed error frame.
	ResponseError
)

// String renders the kind name for diagnostics.
func (k ResponseKind) String() string {
	switch k {
	case ResponseRows:
		return "Rows"
	case ResponseCompletion:
		return "Completion"
	case ResponseError:
		return "Error"
	default:
		return "Invalid"
	}
}

// valid reports whether k names a real response member.
func (k ResponseKind) valid() bool { return k >= ResponseRows && k <= ResponseError }

// ErrorKind is the closed set of typed failures a shard reports in an error
// frame. Each maps to exactly one admission or execution refusal.
type ErrorKind uint8

const (
	// ErrorInvalid is the zero value; the codec never encodes or decodes it.
	ErrorInvalid ErrorKind = iota
	// ErrorNotOwner reports a request for a distribution or shard this process
	// does not own.
	ErrorNotOwner
	// ErrorOwnershipEpoch reports a request whose fencing epoch does not match
	// the configured ownership epoch.
	ErrorOwnershipEpoch
	// ErrorRoutingVersion reports a request routed against a stale manifest
	// generation.
	ErrorRoutingVersion
	// ErrorDeadlineExceeded reports a request whose deadline elapsed before it
	// completed.
	ErrorDeadlineExceeded
	// ErrorResourceLimit reports a result that exceeded a byte or row limit.
	ErrorResourceLimit
	// ErrorMalformedRequest reports a request the shard could not accept as
	// well-formed.
	ErrorMalformedRequest
	// ErrorReadOnly reports a mutating statement carried by a read-only request.
	ErrorReadOnly
	// ErrorUnsupportedReadPolicy reports a consistency policy the leader-only
	// shard cannot prove yet.
	ErrorUnsupportedReadPolicy
	// ErrorCommitOutcomeUnknown reports a mutation whose durable completion
	// cannot be determined. Callers must not retry it without command identity.
	ErrorCommitOutcomeUnknown
	// ErrorPositionUnsupported reports that this shard has no replicated apply
	// position against which it can prove a session minimum.
	ErrorPositionUnsupported
	// ErrorPositionIdentity reports a minimum for a different distribution or
	// shard. A numerically larger index from another identity cannot satisfy it.
	ErrorPositionIdentity
	// ErrorPositionNotReached reports a valid, matching minimum above the
	// serving replica's applied index. The current service reserves this refusal
	// for a replicated apply path; it is never guessed from local storage state.
	ErrorPositionNotReached
	// ErrorShardAllocation reports a stale physical allocation generation for
	// an otherwise matching distribution and shard id.
	ErrorShardAllocation
)

// String renders the kind name for diagnostics.
func (k ErrorKind) String() string {
	switch k {
	case ErrorNotOwner:
		return "NotOwner"
	case ErrorOwnershipEpoch:
		return "OwnershipEpoch"
	case ErrorRoutingVersion:
		return "RoutingVersion"
	case ErrorDeadlineExceeded:
		return "DeadlineExceeded"
	case ErrorResourceLimit:
		return "ResourceLimit"
	case ErrorMalformedRequest:
		return "MalformedRequest"
	case ErrorReadOnly:
		return "ReadOnly"
	case ErrorUnsupportedReadPolicy:
		return "UnsupportedReadPolicy"
	case ErrorCommitOutcomeUnknown:
		return "CommitOutcomeUnknown"
	case ErrorPositionUnsupported:
		return "PositionUnsupported"
	case ErrorPositionIdentity:
		return "PositionIdentity"
	case ErrorPositionNotReached:
		return "PositionNotReached"
	case ErrorShardAllocation:
		return "ShardAllocation"
	default:
		return "Invalid"
	}
}

// valid reports whether k names a real error member.
func (k ErrorKind) valid() bool { return k >= ErrorNotOwner && k <= ErrorShardAllocation }

// Column is one result column's metadata: its name and a PostgreSQL-style type
// OID the codec treats as opaque.
type Column struct {
	Name    string
	TypeOID int32
}

// Cell is one result value. When Null is true, Bytes is empty; otherwise Bytes
// holds the column's already-encoded value (for example JSON text for a
// document column), mirroring the pgwire DataRow model.
type Cell struct {
	Null  bool
	Bytes []byte
}

// ShardResponse is a shard's reply to one request: a materialized row set, a
// completion count, or a typed error frame, selected by Kind.
type ShardResponse struct {
	Kind ResponseKind

	// Columns and Rows are set only for ResponseRows. Every row holds exactly
	// len(Columns) cells.
	Columns []Column
	Rows    [][]Cell

	// RowsAffected is set only for ResponseCompletion.
	RowsAffected int64

	// ErrorKind and ErrorMessage are set only for ResponseError.
	ErrorKind    ErrorKind
	ErrorMessage string

	// HasReadPosition selects ReadPosition on a row response and proves the
	// applied log cut from which the rows were read. The current leader-only
	// implementation has no replicated apply log and therefore leaves it false
	// on every successful strong read. When false, ReadPosition must be zero.
	HasReadPosition bool
	ReadPosition    Position

	// Transaction is present only on a successful transaction command. Its zero
	// value preserves the ordinary response encoding.
	Transaction TransactionReply
}

// RowsResponse builds a ResponseRows reply over columns and rows.
func RowsResponse(columns []Column, rows [][]Cell) *ShardResponse {
	return &ShardResponse{Kind: ResponseRows, Columns: columns, Rows: rows}
}

// CompletionResponse builds a ResponseCompletion reply with the affected-row
// count.
func CompletionResponse(rowsAffected int64) *ShardResponse {
	return &ShardResponse{Kind: ResponseCompletion, RowsAffected: rowsAffected}
}

// NewErrorResponse builds a typed error reply.
func NewErrorResponse(kind ErrorKind, message string) *ShardResponse {
	return &ShardResponse{Kind: ResponseError, ErrorKind: kind, ErrorMessage: message}
}
