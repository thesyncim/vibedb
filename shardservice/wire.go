package shardservice

import (
	"encoding/json"
	"time"

	"github.com/thesyncim/vibedb/distribution"
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
const wireVersion1 = 1

// ReadPolicy selects the consistency contract a read is served under. The enum
// ordering is a stable public safety contract and must not be reordered: the
// zero value is the strongest, leader-only policy.
type ReadPolicy uint8

const (
	// ReadStrong serves from the shard leader under its ownership/replication
	// authority. It is the only policy PR 4a honors; the zero value is safe.
	ReadStrong ReadPolicy = iota
	// ReadSession is reserved for read-your-writes replica reads; not served yet.
	ReadSession
	// ReadStale is reserved for lagging replica reads; not served yet.
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
// Text carries the number spelling, string bytes, or document JSON for the
// other non-null kinds.
type Param struct {
	Kind ParamKind
	Bool bool
	Text string
}

// NullParam returns a SQL NULL parameter.
func NullParam() Param { return Param{Kind: ParamNull} }

// BoolParam returns a boolean scalar parameter.
func BoolParam(v bool) Param { return Param{Kind: ParamBool, Bool: v} }

// NumberParam returns an exact-decimal scalar parameter over spelling, a JSON
// number literal. The codec carries the spelling verbatim; the receiving shard
// binds it through the same exact-number path used by local equality.
func NumberParam(spelling string) Param { return Param{Kind: ParamNumber, Text: spelling} }

// StringParam returns a UTF-8 string scalar parameter.
func StringParam(s string) Param { return Param{Kind: ParamString, Text: s} }

// DocumentParam returns a complete-JSON-document parameter.
func DocumentParam(jsonText string) Param { return Param{Kind: ParamDocument, Text: jsonText} }

// RuntimeValue materializes p as a value the local sql/driver Session accepts in
// its Query/Exec argument slice. It returns only standard-library types —
// nil, bool, json.Number, and string — every one of which sql/driver's runtime
// value normalization already binds. It performs no execution; the execution
// stage calls it to bridge a decoded request into the local planner.
func (p Param) RuntimeValue() any {
	switch p.Kind {
	case ParamNull:
		return nil
	case ParamBool:
		return p.Bool
	case ParamNumber:
		return json.Number(p.Text)
	case ParamString, ParamDocument:
		return p.Text
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
	// RoutingVersion pins the manifest generation the caller routed against.
	RoutingVersion distribution.RoutingVersion
	// OwnershipEpoch is the caller's view of the shard's fencing epoch.
	OwnershipEpoch distribution.OwnershipEpoch

	// ReadPolicy selects the consistency contract; PR 4a honors only ReadStrong.
	ReadPolicy ReadPolicy

	// Deadline is the execution budget measured from the shard's receipt of the
	// request. Zero means the shard applies its configured default.
	Deadline time.Duration
	// MaxResultBytes caps the total encoded result bytes; zero means the shard's
	// configured default.
	MaxResultBytes uint64
	// MaxRows caps the returned row count; zero means the shard's configured
	// default.
	MaxRows uint64
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
	default:
		return "Invalid"
	}
}

// valid reports whether k names a real error member.
func (k ErrorKind) valid() bool { return k >= ErrorNotOwner && k <= ErrorMalformedRequest }

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
