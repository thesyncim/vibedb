package shardservice

import (
	"bytes"
	"time"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
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
	TransactionPrepareParticipant
	TransactionScanCoordinator
	TransactionReadParticipant
	TransactionAcquireReadFence
	TransactionReleaseReadFence
	// TransactionStageManifestCoordinator begins a descriptor-bound segmented
	// coordinator from one fixed-size VTCM record. TransactionStageManifestSegment
	// then appends canonical VTM1 pages in order. TransactionReadManifestSegment
	// returns one page without materializing the aggregate participant set.
	TransactionStageManifestCoordinator
	TransactionStageManifestSegment
	TransactionReadManifestSegment
	TransactionPulseCoordinator
)

func (op TransactionOperation) valid() bool {
	return op >= TransactionStageCoordinator && op <= TransactionPulseCoordinator
}

func (op TransactionOperation) stages() bool {
	return op == TransactionStageCoordinator || op == TransactionStageParticipant
}

func (op TransactionOperation) stagesManifestCoordinator() bool {
	return op == TransactionStageManifestCoordinator
}

func (op TransactionOperation) stagesManifestSegment() bool {
	return op == TransactionStageManifestSegment
}

func (op TransactionOperation) readsManifestSegment() bool {
	return op == TransactionReadManifestSegment
}

// TransactionRequest is the optional transaction envelope. Record aliases the
// request frame and is populated only by a stage operation. Every other
// operation uses ID and Revision; lookup permits Revision zero, while a state
// transition requires an exact nonzero revision for compare-and-swap replay.
type TransactionRequest struct {
	Operation     TransactionOperation
	ID            distributedtxn.ID
	Revision      uint64
	RecoveryPulse uint8
	// SegmentIndex is populated only by TransactionReadManifestSegment. Stage
	// requests obtain the canonical index from the checksummed VTM1 page.
	SegmentIndex uint32
	Record       []byte
	// ManifestSegment carries the canonical VTM1 page for segmented stage
	// operations. Coordinator begin carries Record=VTCM and page zero together,
	// so route identity is proven before the first journal write.
	ManifestSegment []byte
	// manifestMeta is a cold decoded-request sidecar. Keeping only a pointer in
	// TransactionRequest avoids adding two maximum-width shard identities to
	// every ordinary SQL, point-read, and transaction request value.
	manifestMeta *transactionManifestSegmentMeta
}

type transactionManifestSegmentMeta struct {
	valid            bool
	index            uint32
	firstParticipant uint64
	participantCount uint32
	distribution     [distributedtxn.MaxShardIdentityBytes]byte
	shard            [distributedtxn.MaxShardIdentityBytes]byte
	distributionLen  uint8
	shardLen         uint8
	routingVersion   uint64
	allocation       uint64
	ownershipEpoch   uint64
}

// GlobalIndexLookupRequest is the optional byte-native lookup envelope for a
// gateway-maintained global index relation. Relation and KeyTuples borrow the
// request frame. Tuples are strictly ordered and deduplicated. Unique selects
// exact point lookups; non-unique lookup seeks directly to each tuple and
// visits its contiguous locator-key prefix. One request pins one relation
// snapshot and shares result bounds across every tuple.
//
// The zero value is absent. An admitted lookup is read-only and mutually
// exclusive with SQL, parameters, and transaction commands. IndexID plus
// Incarnation fence delayed requests against physical relation reuse.
type GlobalIndexLookupRequest struct {
	Relation     []byte
	IndexID      uint64
	Incarnation  uint64
	KeyTuples    [][]byte
	LocatorCount uint8
	Unique       bool
}

// PrimaryKeyReadRequest replaces an ordinary SQL table scan with an exact,
// strictly ordered native-primary candidate set. Relation and
// MaxDocumentBytes bind the optional live point lane to one dense physical
// relation and its catalog-frozen value bound. PrimaryPath fences the gateway
// descriptor against the shard's live SQL catalog before any row is read. The
// zero value is absent; all slices borrow the request frame.
type PrimaryKeyReadRequest struct {
	Relation         replication.RelationID
	MaxDocumentBytes uint32
	PrimaryPath      []byte
	Keys             [][]byte
}

// DocumentScanRequest is a resumable raw base-table scan for online index
// build/export workers. After is an exclusive native-primary cursor. The zero
// value is absent; slices borrow the request frame.
type DocumentScanRequest struct {
	Relation []byte
	After    []byte
}

func (r DocumentScanRequest) present() bool { return len(r.Relation) != 0 }

func (r DocumentScanRequest) canonical() bool {
	if !r.present() {
		return len(r.After) == 0
	}
	return len(r.Relation) <= 1<<16-1 && utf8.Valid(r.Relation) &&
		bytes.IndexByte(r.Relation, 0) < 0 && len(r.After) <= maxFrameBody
}

func (r PrimaryKeyReadRequest) present() bool {
	return len(r.PrimaryPath) != 0 || len(r.Keys) != 0
}

func (r PrimaryKeyReadRequest) canonical() bool {
	if !r.present() {
		return r.Relation == 0 && r.MaxDocumentBytes == 0 &&
			len(r.PrimaryPath) == 0 && len(r.Keys) == 0
	}
	if len(r.PrimaryPath) == 0 || len(r.PrimaryPath) > 1<<16-1 ||
		!utf8.Valid(r.PrimaryPath) || len(r.Keys) == 0 {
		return false
	}
	// Legacy candidate envelopes carry no relation bound and continue through
	// the ordinary snapshot path. The live point lane requires both bounds so
	// a peer cannot widen either the physical relation or detached value.
	if (r.Relation == 0) != (r.MaxDocumentBytes == 0) ||
		r.Relation > replication.MaxRelationID ||
		r.MaxDocumentBytes > replication.MaxMutationValueBytes {
		return false
	}
	for i := range r.Keys {
		if len(r.Keys[i]) == 0 || len(r.Keys[i]) > maxFrameBody ||
			(i != 0 && bytes.Compare(r.Keys[i-1], r.Keys[i]) >= 0) {
			return false
		}
	}
	return true
}

func (r PrimaryKeyReadRequest) livePointEligible() bool {
	return r.present() && len(r.Keys) == 1 && r.Relation != 0 &&
		r.Relation <= replication.MaxRelationID && r.MaxDocumentBytes != 0 &&
		r.MaxDocumentBytes <= replication.MaxMutationValueBytes
}

func (r GlobalIndexLookupRequest) present() bool {
	return r.IndexID != 0
}

func (r GlobalIndexLookupRequest) canonical() bool {
	if !r.present() {
		return len(r.Relation) == 0 && r.Incarnation == 0 &&
			len(r.KeyTuples) == 0 && r.LocatorCount == 0 && !r.Unique
	}
	if len(r.Relation) == 0 || len(r.Relation) > 1<<16-1 ||
		len(r.KeyTuples) == 0 || len(r.KeyTuples) > maxParams ||
		!utf8.Valid(r.Relation) || bytes.IndexByte(r.Relation, 0) >= 0 ||
		r.Incarnation == 0 || r.LocatorCount == 0 || r.LocatorCount > 8 {
		return false
	}
	for i := range r.KeyTuples {
		if len(r.KeyTuples[i]) == 0 || len(r.KeyTuples[i]) > maxFrameBody ||
			r.KeyTuples[i][0] == 0 ||
			i != 0 && bytes.Compare(r.KeyTuples[i-1], r.KeyTuples[i]) >= 0 {
			return false
		}
	}
	return true
}

// TransactionRole discriminates the state returned by a transaction reply.
type TransactionRole uint8

const (
	TransactionRoleNone TransactionRole = iota
	TransactionRoleCoordinator
	TransactionRoleParticipant
)

// TransactionRecordKind makes recovery payloads self-describing without
// guessing from magic bytes. The enum is part of the typed wire contract.
type TransactionRecordKind uint8

const (
	TransactionRecordNone TransactionRecordKind = iota
	TransactionRecordInlineCoordinator
	TransactionRecordParticipant
	TransactionRecordManifestCoordinator
	TransactionRecordManifestSegment
)

func (kind TransactionRecordKind) valid() bool {
	return kind <= TransactionRecordManifestSegment
}

// TransactionReply reports the durable state observed after a transaction
// command. Exactly one typed state is populated according to Role.
type TransactionReply struct {
	Role             TransactionRole
	ID               distributedtxn.ID
	Revision         uint64
	RecoveryPulse    uint8
	CoordinatorState distributedtxn.CoordinatorState
	ParticipantState distributedtxn.ParticipantState
	RecordKind       TransactionRecordKind
	// SegmentIndex is meaningful only for TransactionRecordManifestSegment and
	// repeats the authenticated page index for constant-time dispatch.
	SegmentIndex uint32
	// Record optionally carries the immutable coordinator stage record on lookup
	// and scan replies so recovery can reconstruct the fixed participant set.
	Record []byte
}

// Equal compares the fixed state and optional byte-native recovery record.
func (r TransactionReply) Equal(other TransactionReply) bool {
	return r.Role == other.Role && r.ID == other.ID && r.Revision == other.Revision &&
		r.RecoveryPulse == other.RecoveryPulse &&
		r.CoordinatorState == other.CoordinatorState &&
		r.ParticipantState == other.ParticipantState && r.RecordKind == other.RecordKind &&
		r.SegmentIndex == other.SegmentIndex && bytes.Equal(r.Record, other.Record)
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

// validSQLParameterTypes recognizes the canonical optional analysis metadata
// shared by direct shard requests and durable mutation batches. A present
// vector covers every bound parameter and contains at least one concrete type;
// an all-unspecified vector is represented by absence. Document parameters
// cannot simultaneously claim a scalar SQL input domain.
func validSQLParameterTypes(params []Param, parameterTypes []sqldriver.ParamType) bool {
	if len(parameterTypes) == 0 {
		return true
	}
	if len(parameterTypes) != len(params) || len(parameterTypes) > maxParams {
		return false
	}
	hasType := false
	for index, parameterType := range parameterTypes {
		if parameterType >= sqldriver.ParamTypeInvalid {
			return false
		}
		if parameterType == sqldriver.ParamTypeUnspecified {
			continue
		}
		if params[index].Kind == ParamDocument {
			return false
		}
		hasType = true
	}
	return hasType
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
	// Authority is the exact end-user principal forwarded by an authenticated
	// gateway. It is absent only on the explicit loopback development path.
	Authority serviceauthz.Authority
	// SQL is the statement text the shard parses and plans locally.
	SQL string
	// Params are the typed bound parameters, in placeholder order.
	Params []Param
	// ParamTypes is absent on the ordinary schemaless path. When present it is a
	// full placeholder vector containing at least one analysis-time SQL input
	// type, so the shard's independent prepare preserves gateway semantics.
	ParamTypes []sqldriver.ParamType
	// PartialAggregate asks the shard to lower a grouped SELECT without its
	// final ORDER BY or LIMIT. The coordinator applies those stages after exact
	// partial-state combination, so groups spanning shards cannot be truncated.
	PartialAggregate bool
	// RowBatch opts a read-only SQL request into bounded multi-frame delivery.
	// The ordinary routed lane leaves it zero and receives exactly one response.
	// BatchRows bounds rows per frame; BatchBytes bounds the encoded row-data
	// bytes (cell markers, lengths, and payloads), excluding frame and schema
	// metadata. Both bounds and the request's total MaxRows/MaxResultBytes must
	// be present together.
	RowBatch RowBatchRequest

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

	// BucketBits and AccessScopes optionally declare the canonical virtual
	// buckets touched by ordinary gateway traffic. The absent zero form means
	// whole shard and remains fail-safe for direct clients.
	BucketBits   uint8
	AccessScopes []distributedtxn.IntentScope
	// ReadFenceID authorizes a read against an active short-lived coherent-cut
	// fence. It is absent on ordinary single-shard reads and every write.
	ReadFenceID distributedtxn.ID
	// GlobalIndexLookup selects the raw global-index access path. It may carry a
	// read fence and scoped bucket admission, but never SQL or a transaction.
	GlobalIndexLookup GlobalIndexLookupRequest
	// PrimaryKeyRead narrows an ordinary read-only SQL request to exact native
	// primary storage keys. The shard still evaluates the original predicate,
	// projection, aggregation, order, and limit over those rows; the keys only
	// replace its physical scan source.
	PrimaryKeyRead PrimaryKeyReadRequest
	// MutationCapture executes UPDATE/DELETE target selection without mutation
	// and returns native primary keys plus canonical old documents. It is the
	// legacy two-column, read-only precursor to optimistic indexed maintenance.
	MutationCapture bool
	// MutationImageCapture materializes UPDATE/DELETE images without mutation
	// and returns native primary keys, canonical before documents, and exact
	// after documents (NULL for DELETE). It is mutually exclusive with the
	// legacy MutationCapture mode.
	MutationImageCapture bool
	// DocumentScan selects the bounded raw scan lane. MaxRows and
	// MaxResultBytes are its mandatory page bounds.
	DocumentScan DocumentScanRequest
	// Repartition turns a read-only SQL cursor into a direct worker producer.
	// It carries execution coordinates and peer ownership fences, never a
	// serialized relational plan.
	Repartition RepartitionRequest
	// Exchange selects the ephemeral worker mailbox lane. It is mutually
	// exclusive with SQL, storage reads, fences, and durable transactions.
	Exchange ExchangeRequest

	// Transaction selects the transaction path. Its zero value is absent and
	// preserves the ordinary autocommit encoding and execution path.
	Transaction TransactionRequest
}

func (r *ShardRequest) mutationCapturePresent() bool {
	return r != nil && (r.MutationCapture || r.MutationImageCapture)
}

const (
	MaxRepartitionTargets     = 256
	MaxRepartitionKeyColumns  = 64
	MaxExchangeAddressBytes   = 512
	MaxExchangeReducerColumns = 1024
)

// RepartitionTarget is one destination worker/partition. Address is retained
// as bytes on the wire and converted only at the cold socket-open boundary.
type RepartitionTarget struct {
	Address              []byte
	Distribution         distribution.DistributionName
	Shard                distribution.ShardID
	AllocationGeneration distribution.ShardAllocationGeneration
	RoutingVersion       distribution.RoutingVersion
	OwnershipEpoch       distribution.OwnershipEpoch
}

func (t RepartitionTarget) canonical() bool {
	return len(t.Address) != 0 && len(t.Address) <= MaxExchangeAddressBytes &&
		utf8.Valid(t.Address) && bytes.IndexByte(t.Address, 0) < 0 &&
		len(t.Distribution) != 0 && len(t.Distribution) <= MaxPositionIdentityBytes &&
		utf8.ValidString(string(t.Distribution)) && bytes.IndexByte(byteview.Bytes(string(t.Distribution)), 0) < 0 &&
		len(t.Shard) != 0 && len(t.Shard) <= MaxPositionIdentityBytes &&
		utf8.ValidString(string(t.Shard)) && bytes.IndexByte(byteview.Bytes(string(t.Shard)), 0) < 0 &&
		t.AllocationGeneration != 0 && t.RoutingVersion != 0 && t.OwnershipEpoch != 0
}

// RepartitionRequest selects exact hash partitioning of a shard-local result.
// KeyColumns are result ordinals and use the query engine's exact GROUP BY
// identity. Operation/stage/attempt fence every destination mailbox retry.
type RepartitionRequest struct {
	Operation exchange.ID
	Stage     uint32
	Attempt   uint32
	Producer  uint16

	KeyColumns []uint16
	Targets    []RepartitionTarget

	BlockRows  uint32
	BlockBytes uint32
	MaxMemory  uint64
}

func (r RepartitionRequest) present() bool { return !r.Operation.IsZero() }

func (r RepartitionRequest) canonical() bool {
	fields := r.Stage != 0 || r.Attempt != 0 || r.Producer != 0 ||
		len(r.KeyColumns) != 0 || len(r.Targets) != 0 ||
		r.BlockRows != 0 || r.BlockBytes != 0 || r.MaxMemory != 0
	if !r.present() {
		return !fields
	}
	if r.Stage == 0 || r.Attempt == 0 ||
		len(r.KeyColumns) == 0 || len(r.KeyColumns) > MaxRepartitionKeyColumns ||
		len(r.Targets) == 0 || len(r.Targets) > MaxRepartitionTargets ||
		r.Producer >= exchange.MaxProducers ||
		!exchange.ValidBlockLimits(1, r.BlockRows, r.BlockBytes) ||
		r.MaxMemory == 0 || r.MaxMemory > exchange.MaxMailboxBytes ||
		uint64(len(r.Targets))*uint64(r.BlockBytes) > r.MaxMemory {
		return false
	}
	for i, column := range r.KeyColumns {
		for previous := range i {
			if r.KeyColumns[previous] == column {
				return false
			}
		}
	}
	for i := range r.Targets {
		if !r.Targets[i].canonical() {
			return false
		}
	}
	return true
}

// ExchangeOperation is one mailbox lifecycle or data command.
type ExchangeOperation uint8

const (
	ExchangeNone ExchangeOperation = iota
	ExchangeOpen
	ExchangePush
	ExchangePull
	ExchangeCancel
	ExchangeReduce
)

func (o ExchangeOperation) valid() bool { return o >= ExchangeOpen && o <= ExchangeReduce }

// ExchangeRequest carries raw attempt-fenced mailbox commands without SQL,
// JSON parsing, formatted identities, or a serialized query plan.
type ExchangeRequest struct {
	Operation ExchangeOperation
	Key       exchange.Key

	Producers       uint16
	QueuedBatches   uint16
	ProducerBatches uint16
	BufferedRows    uint64
	BufferedBytes   uint64
	TotalRows       uint64
	TotalBytes      uint64

	Batch exchange.Batch

	HasAck      bool
	AckProducer uint16
	AckSequence uint32

	// Output and the aggregate program are present only for ExchangeReduce.
	// The reducer drains Key, finalizes partition-local groups, and transfers
	// canonical result blocks into Output as producer zero.
	Output        exchange.Key
	Kinds         []distributedagg.Kind
	GroupKeys     []uint16
	MaxStateBytes uint64
	BlockRows     uint32
	BlockBytes    uint32
}

func (r ExchangeRequest) present() bool { return r.Operation != ExchangeNone }

func (r ExchangeRequest) canonical() bool {
	openFields := r.Producers != 0 || r.QueuedBatches != 0 || r.ProducerBatches != 0 ||
		r.BufferedRows != 0 || r.BufferedBytes != 0 || r.TotalRows != 0 || r.TotalBytes != 0
	batchFields := r.Batch.Producer != 0 || r.Batch.Sequence != 0 || r.Batch.Rows != 0 ||
		len(r.Batch.Data) != 0 || r.Batch.Final
	ackFields := r.HasAck || r.AckProducer != 0 || r.AckSequence != 0
	reduceFields := r.Output != (exchange.Key{}) || len(r.Kinds) != 0 || len(r.GroupKeys) != 0 ||
		r.MaxStateBytes != 0 || r.BlockRows != 0 || r.BlockBytes != 0
	if !r.present() {
		return r.Key == (exchange.Key{}) && !openFields && !batchFields && !ackFields && !reduceFields
	}
	if !r.Operation.valid() || r.Key.Operation.IsZero() {
		return false
	}
	switch r.Operation {
	case ExchangeOpen:
		return !batchFields && !ackFields && !reduceFields && (exchange.Spec{
			Key: r.Key, Producers: r.Producers, QueuedBatches: r.QueuedBatches,
			ProducerBatches: r.ProducerBatches, BufferedRows: r.BufferedRows,
			BufferedBytes: r.BufferedBytes, TotalRows: r.TotalRows, TotalBytes: r.TotalBytes,
		}).Valid()
	case ExchangePush:
		return !openFields && !ackFields && !reduceFields && canonicalExchangeBatch(r.Batch)
	case ExchangePull:
		return !openFields && !batchFields && !reduceFields &&
			((r.HasAck && r.AckProducer < exchange.MaxProducers) ||
				(!r.HasAck && r.AckProducer == 0 && r.AckSequence == 0))
	case ExchangeCancel:
		return !openFields && !batchFields && !ackFields && !reduceFields
	case ExchangeReduce:
		if openFields || batchFields || ackFields || r.Output.Operation.IsZero() ||
			r.Output.Operation != r.Key.Operation || r.Output.Attempt != r.Key.Attempt ||
			r.Output.Partition != r.Key.Partition || r.Output.Stage == r.Key.Stage ||
			len(r.Kinds) == 0 || len(r.Kinds) > MaxExchangeReducerColumns ||
			len(r.GroupKeys) == 0 || len(r.GroupKeys) > MaxRepartitionKeyColumns ||
			r.MaxStateBytes == 0 || r.MaxStateBytes > exchange.MaxMailboxBytes ||
			!exchange.ValidBlockLimits(uint32(len(r.Kinds)), r.BlockRows, r.BlockBytes) {
			return false
		}
		for i, column := range r.GroupKeys {
			if int(column) >= len(r.Kinds) || r.Kinds[column] != distributedagg.None {
				return false
			}
			for previous := range i {
				if r.GroupKeys[previous] == column {
					return false
				}
			}
		}
		for column, kind := range r.Kinds {
			if !kind.Valid() {
				return false
			}
			if kind == distributedagg.None {
				found := false
				for _, key := range r.GroupKeys {
					found = found || int(key) == column
				}
				if !found {
					return false
				}
			}
		}
		return true
	default:
		return false
	}
}

func canonicalExchangeBatch(batch exchange.Batch) bool {
	return batch.Producer < exchange.MaxProducers && batch.Rows <= exchange.MaxBatchRows &&
		len(batch.Data) <= int(exchange.MaxBatchBytes) &&
		((batch.Rows == 0 && len(batch.Data) == 0 && batch.Final) ||
			(batch.Rows != 0 && len(batch.Data) != 0)) &&
		(batch.Final || batch.Sequence != ^uint32(0))
}

// RowBatchRequest bounds one streamed row frame. It is an opt-in transport
// envelope, not another wire version: its absent zero value preserves the
// original request and response bytes.
type RowBatchRequest struct {
	BatchRows  uint32
	BatchBytes uint32
}

// Hard row-batch bounds keep one peer-selected frame small enough for pooled
// exchange workers while still matching a columnar execution block. The cell
// bound protects wide or all-null results whose wire bytes understate their
// decoded metadata footprint.
const (
	MaxRowBatchRows  uint32 = 64 << 10
	MaxRowBatchBytes uint32 = 4 << 20
	MaxRowBatchCells uint32 = 1 << 18
)

func (r RowBatchRequest) present() bool {
	return r.BatchRows != 0 || r.BatchBytes != 0
}

func (r RowBatchRequest) canonical() bool {
	if !r.present() {
		return true
	}
	return r.BatchRows != 0 && r.BatchRows <= MaxRowBatchRows &&
		r.BatchBytes != 0 && r.BatchBytes <= MaxRowBatchBytes
}

// ResponseKind discriminates the shapes a ShardResponse can take.
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
	// ResponseRowBatch carries one sequence-checked fragment of an opt-in row
	// stream. Sequence zero owns the schema; later batches carry only its count.
	ResponseRowBatch
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
	case ResponseRowBatch:
		return "RowBatch"
	default:
		return "Invalid"
	}
}

// valid reports whether k names a real response member.
func (k ResponseKind) valid() bool { return k >= ResponseRows && k <= ResponseRowBatch }

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
	// ErrorTransactionConflict reports a durable transaction identity, revision,
	// digest, role, or state that conflicts with the requested transition.
	ErrorTransactionConflict
	// ErrorTransactionNotFound reports that the requested durable coordinator or
	// participant role does not exist on this shard.
	ErrorTransactionNotFound
	// ErrorReadFenceBusy asks a coherent multi-shard reader to release any
	// partial cut and retry after an intersecting writer or participant.
	ErrorReadFenceBusy
	// ErrorExchangeNotFound reports a mailbox key absent on this worker.
	ErrorExchangeNotFound
	// ErrorExchangeConflict reports an incompatible retry of an open command.
	ErrorExchangeConflict
	// ErrorExchangeSequence reports a producer sequence, digest, or consumer ack
	// that does not match the mailbox state.
	ErrorExchangeSequence
	// ErrorExchangeClosed reports a canceled or retired mailbox.
	ErrorExchangeClosed
	// ErrorUnauthorized is a definite pre-execution policy refusal.
	ErrorUnauthorized
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
	case ErrorTransactionConflict:
		return "TransactionConflict"
	case ErrorTransactionNotFound:
		return "TransactionNotFound"
	case ErrorReadFenceBusy:
		return "ReadFenceBusy"
	case ErrorExchangeNotFound:
		return "ExchangeNotFound"
	case ErrorExchangeConflict:
		return "ExchangeConflict"
	case ErrorExchangeSequence:
		return "ExchangeSequence"
	case ErrorExchangeClosed:
		return "ExchangeClosed"
	case ErrorUnauthorized:
		return "Unauthorized"
	default:
		return "Invalid"
	}
}

// valid reports whether k names a real error member.
func (k ErrorKind) valid() bool { return k >= ErrorNotOwner && k <= ErrorUnauthorized }

// Column is one result column's metadata: its name and a PostgreSQL-style type
// OID the codec treats as opaque.
type Column struct {
	Name    string
	TypeOID int32
}

// Cell is one result value. It aliases the exchange block cell so a shard row
// can move into a byte-native intermediate block without an adapter object.
// When Null is true, Bytes is empty; otherwise Bytes holds the already-encoded
// value (for example canonical vibejson bytes for a document column).
type Cell = exchange.Cell

// ShardResponse is a shard's reply to one request: a materialized row set, a
// completion count, or a typed error frame, selected by Kind.
type ShardResponse struct {
	Kind ResponseKind

	// Columns and Rows are set only for ResponseRows. Every row holds exactly
	// len(Columns) cells.
	Columns []Column
	Rows    [][]Cell
	// RowBatch is present only for ResponseRowBatch. Sequence starts at zero and
	// Final terminates the request stream. ColumnCount gives continuation frames
	// an independently decodable row arity without repeating column names.
	RowBatch RowBatchReply

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

	// DocumentScan is present only on a successful raw scan page.
	DocumentScan DocumentScanReply

	// Transaction is present only on a successful transaction command. Its zero
	// value preserves the ordinary response encoding.
	Transaction TransactionReply
	// Exchange is present only on a successful exchange completion response.
	Exchange ExchangeReply
}

// ExchangeReply acknowledges a mailbox command or carries one pull result.
// EOF is valid only for ExchangePull and carries no batch.
type ExchangeReply struct {
	Operation ExchangeOperation
	Batch     exchange.Batch
	EOF       bool
}

func (r ExchangeReply) present() bool { return r.Operation != ExchangeNone }

func (r ExchangeReply) canonical() bool {
	batchFields := r.Batch.Producer != 0 || r.Batch.Sequence != 0 || r.Batch.Rows != 0 ||
		len(r.Batch.Data) != 0 || r.Batch.Final
	if !r.present() {
		return !batchFields && !r.EOF
	}
	if !r.Operation.valid() {
		return false
	}
	if r.Operation != ExchangePull {
		return !r.EOF && !batchFields
	}
	if r.EOF {
		return !batchFields
	}
	return canonicalExchangeBatch(r.Batch)
}

// RowBatchReply is the framing metadata for one bounded row fragment.
type RowBatchReply struct {
	Sequence    uint32
	ColumnCount uint32
	Final       bool
}

// DocumentScanReply carries an owned exclusive resume cursor. Complete is true
// only when the shard snapshot had no later row. Present distinguishes an
// empty completed relation from the absent zero value.
type DocumentScanReply struct {
	Present  bool
	Complete bool
	Next     []byte
}

func (r DocumentScanReply) canonical() bool {
	if !r.Present {
		return !r.Complete && len(r.Next) == 0
	}
	return len(r.Next) <= maxFrameBody && (r.Complete || len(r.Next) != 0)
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
