// Package replicatedstate provides the bounded, unserved replicated apply
// adapter. It deliberately owns no SQL planning, Raft transport, or serving
// authority.
package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/resultformat"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	// ResultFormatMutation is the fixed affected-row result grammar used by this
	// low-level mutation adapter. ResultApplied carries one canonical eight-byte
	// nonnegative count; every refusal and session-lifecycle result is empty.
	ResultFormatMutation uint16 = resultformat.Mutation
	// ResultFormatRouteGate carries exactly one canonical fixed routegate
	// Outcome in the completion envelope.
	ResultFormatRouteGate uint16 = resultformat.RouteGate
	// ResultFormatExecutionPin is the fixed transferable logical-pin proof
	// grammar. Values 3 and 4 are frozen for route-gate and request-ledger.
	ResultFormatExecutionPin uint16 = resultformat.ExecutionPin

	// Zero and unknown result codes are invalid.
	ResultApplied         uint32 = 1
	ResultStaleFence      uint32 = 2
	ResultUnknownRelation uint32 = 3
	ResultInvalidDocument uint32 = 4
	ResultTargetBound     uint32 = 5
	ResultWrongShard      uint32 = 6
	ResultSessionRetired  uint32 = 7
	ResultSessionOpened   uint32 = 8
	ResultSessionRenewed  uint32 = 9
	ResultSessionRevoked  uint32 = 10
	ResultIndexConflict   uint32 = 11
	// ResultIntentBusy is the deterministic ordinary-mutation refusal emitted
	// while an active distributed transaction owns the exact relation key.
	// Result codes are local to their ResultFormat: transaction format code 12
	// independently denotes a transaction-control CAS loss.
	ResultIntentBusy uint32 = 12
	// ResultRequestLedgerConflict is a deterministic request identity,
	// revision, or byte-CAS conflict. The retained state remains authoritative.
	ResultRequestLedgerConflict uint32 = 13
	// ResultRequestLedgerCapacity is a deterministic upfront reservation
	// refusal; no partial builder state is published.
	ResultRequestLedgerCapacity uint32 = 14
	// ResultRequestLedgerNotFound is a stateless non-creation CAS refusal.
	ResultRequestLedgerNotFound uint32 = 15
	// ResultRequestLedgerWrongRange is a stateless stale-route/range-authority
	// refusal. No ledger row was mutated on that group.
	ResultRequestLedgerWrongRange uint32 = 16
	// ResultRouteGate identifies the fixed route-gate outcome grammar.
	ResultRouteGate uint32 = 13

	MaxRouteGateCompletionEnvelopeBytes = replication.MaxEmptyResultCompletionEnvelopeBytes + routegate.OutcomeBytes

	// MaxStateEnvelopeBytes bounds the fixed publication record. Its compact
	// 376-byte header (416 bytes when transaction accounting is present), two
	// 255-byte identities, checksum, and a deterministic protobuf
	// with at most 64 ten-byte member IDs fit below 1.6 KiB; 2 KiB retains a
	// format margin without inflating every hidden collection. Session metadata
	// and completion slots are independently bounded by their compact codecs.
	// MaxRetainedSessions independently caps both live sessions and durable
	// stable-identity authority bindings. The retry ring is additionally bounded
	// by RetryWindow; neither structure grows with operation count.
	MaxStateEnvelopeBytes           = 2 << 10
	MaxRetainedSessions             = 1 << 20
	MaxRetainedTransactions         = 1 << 20
	MaxRetainedExecutionPins        = 1 << 20
	MaxSessionRetryWindow           = 256
	MaxStaticBootstrapBytes         = 1 << 20
	MaxStaticBootstrapEnvelopeBytes = MaxStaticBootstrapBytes + MaxStateEnvelopeBytes
	MaxDistinctMutations            = 64
	MaxStaticBootstrapMembers       = 64
)

var (
	ErrInvalidBinding              = errors.New("replicatedstate: invalid shard binding")
	ErrInvalidOptions              = errors.New("replicatedstate: invalid options")
	ErrInvalidCollection           = errors.New("replicatedstate: invalid collection profile")
	ErrStateCorrupt                = errors.New("replicatedstate: corrupt state record")
	ErrCompletionCorrupt           = errors.New("replicatedstate: corrupt completion record")
	ErrCompletionNotFound          = errors.New("replicatedstate: completion not found")
	ErrCompletionBufferSmall       = errors.New("replicatedstate: completion destination is too small")
	ErrCompletionPublication       = errors.New("replicatedstate: completion snapshot does not match publication")
	ErrCompletionWorkspaceBusy     = errors.New("replicatedstate: completion lookup workspace is busy")
	ErrRetryRetired                = errors.New("replicatedstate: retry is outside the retained session window")
	ErrRequestConflict             = errors.New("replicatedstate: client sequence conflicts with retained command")
	ErrSessionEpoch                = errors.New("replicatedstate: invalid client session epoch")
	ErrSessionSequence             = errors.New("replicatedstate: client sequence is not the next session sequence")
	ErrSessionAck                  = errors.New("replicatedstate: client acknowledgement regressed")
	ErrSessionActive               = errors.New("replicatedstate: current client session is still active")
	ErrSessionRetired              = errors.New("replicatedstate: client session is retired")
	ErrSessionReleased             = errors.New("replicatedstate: client session release is complete")
	ErrSessionLeaseDeadline        = errors.New("replicatedstate: client session lease deadline does not match")
	ErrStaleCommand                = errors.New("replicatedstate: command has a stale mutable fence")
	ErrWrongBinding                = errors.New("replicatedstate: command belongs to another shard binding")
	ErrApplySequence               = errors.New("replicatedstate: invalid apply sequence")
	ErrApplyPoisoned               = errors.New("replicatedstate: machine is poisoned; reopen required")
	ErrStaticSnapshotOnly          = errors.New("replicatedstate: only the exact static bootstrap snapshot is supported")
	ErrAdmissionBound              = errors.New("replicatedstate: command exceeds the frozen apply profile")
	ErrSchemaProfile               = errors.New("replicatedstate: requires an exact authenticated collection validation profile")
	ErrInconsistentSnapshot        = errors.New("replicatedstate: coherent snapshot disagrees with publication")
	ErrCodecAlias                  = errors.New("replicatedstate: codec input aliases destination append region")
	ErrSnapshotArtifact            = errors.New("replicatedstate: corrupt snapshot artifact")
	ErrSnapshotArtifactBound       = errors.New("replicatedstate: snapshot artifact exceeds its bounded format")
	ErrSnapshotStage               = errors.New("replicatedstate: invalid snapshot staging state")
	ErrSnapshotStageIncomplete     = errors.New("replicatedstate: snapshot staging is incomplete")
	ErrSnapshotStageOutcomeUnknown = errors.New("replicatedstate: snapshot staging durability outcome is unknown")
	ErrSnapshotBase                = errors.New("replicatedstate: invalid snapshot base certificate")
	ErrStagedSnapshot              = errors.New("replicatedstate: invalid staged snapshot initialization")
	ErrOwnershipTransition         = errors.New("replicatedstate: invalid ownership transition")
	ErrSchemaTransition            = errors.New("replicatedstate: invalid schema transition")
	ErrSchemaTransitionPending     = errors.New("replicatedstate: schema transition requires target activation")
)

// Binding is the exact shard and recovery lineage owned by one Machine.
// ReplicaSetVersion is intentionally absent: it is applied publication state.
type Binding struct {
	ClusterID             replication.ID128
	ClusterIncarnation    replication.ID128
	TopologyRecoveryEpoch uint64
	Distribution          string
	Shard                 string
	AllocationGeneration  uint64
	ShardIncarnation      replication.ID128
	GroupID               replication.ID128

	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	RoutingVersion         uint64
	RouteGeneration        uint64
	// OwnedRange is the exact currently published half-open keyspace range.
	// Unlike the immutable physical validation profile, it advances with an
	// ownership transition and fences new reads and mutations immediately even
	// while transferred rows remain available for later certified pruning.
	OwnedRange distribution.KeyRange
}

func (b Binding) validate() error {
	if b.ClusterID == (replication.ID128{}) ||
		b.ClusterIncarnation == (replication.ID128{}) ||
		b.ShardIncarnation == (replication.ID128{}) ||
		b.GroupID == (replication.ID128{}) ||
		b.TopologyRecoveryEpoch == 0 || b.AllocationGeneration == 0 ||
		b.ActivePolicyGeneration == 0 || b.ProtectionEpoch == 0 ||
		b.OwnershipEpoch == 0 || b.SchemaGeneration == 0 ||
		b.RoutingVersion == 0 || b.RouteGeneration == 0 || !b.OwnedRange.Valid() ||
		(b.OwnedRange.End.Max && b.OwnedRange.End.Point != (distribution.KeyspacePoint{})) {
		return ErrInvalidBinding
	}
	if b.Distribution == "" || len(b.Distribution) > replication.MaxIdentityBytes ||
		!utf8.ValidString(b.Distribution) || strings.IndexByte(b.Distribution, 0) >= 0 ||
		b.Shard == "" || len(b.Shard) > replication.MaxIdentityBytes ||
		!utf8.ValidString(b.Shard) || strings.IndexByte(b.Shard, 0) >= 0 {
		return ErrInvalidBinding
	}
	return nil
}

// ValidationProfile is a construction-time assertion about the exact
// collection validation contract.
type ValidationProfile uint8

const (
	// ValidationOpaqueBinary is reserved for the hidden system collection. It
	// requires durable opaque-value mode, a zero ValidationDigest, and no
	// MutationValidator.
	ValidationOpaqueBinary ValidationProfile = 1

	// ValidationDeterministicMutation adds a caller-supplied deterministic
	// mutation contract, including explicit wrong-shard refusals. It requires a
	// nonzero ValidationDigest and a non-nil MutationValidator.
	ValidationDeterministicMutation ValidationProfile = 2
)

// MutationValidation is the closed result grammar returned by a
// MutationValidator. Zero and unknown values fail closed.
type MutationValidation uint8

const (
	MutationValidationAccept MutationValidation = iota + 1
	MutationValidationInvalid
	MutationValidationTargetBound
	MutationValidationWrongShard
)

// MutationValidator supplies the deterministic application-specific portion
// of ValidationDeterministicMutation. Implementations must be pure for a fixed
// ValidationDigest and must not retain any input slices.
//
// ValidatePut is also used to validate every existing row during Open.
// ValidateDelete receives the current snapshot value when found is true and a
// nil current value otherwise. In particular, an absent delete remains
// available for deterministic validation before no-op elimination.
type MutationValidator interface {
	ValidatePut(key, value []byte) MutationValidation
	ValidateDelete(key, current []byte, found bool) MutationValidation
}

// ConflictMutationValidator owns a closed deterministic conflict program under
// the relation's authenticated validation/apply contract. It validates the
// candidate and all referenced columns on both branches, and evaluates the
// condition only when found, then assignments only when it is true. A false or
// unknown condition returns current and matched=false. The canonical result may
// borrow candidate or current; no input or snapshot slice is retained.
// The machine independently validates the result's schema, key and ownership.
type ConflictMutationValidator interface {
	MaterializeConflict(key, candidate, program, current []byte, found bool) (value []byte, matched bool, validation MutationValidation)
}

// DeclaredSchemaValidator is a cold construction-time contract. A typed
// collection is admissible only when its deterministic validator authenticates
// the exact immutable durable schema and commits it in ValidationDigest.
// ValidatePut must enforce that same declaration before any durable mutation.
type DeclaredSchemaValidator interface {
	MutationValidator
	ValidateCollectionSchema(*store.Schema) bool
}

func (t CollectionTarget) schemaMatches() bool {
	if !t.Collection.HasSchema() {
		return true
	}
	validator, ok := t.Validator.(DeclaredSchemaValidator)
	return t.Validation == ValidationDeterministicMutation && ok && validator.ValidateCollectionSchema(t.Collection.Schema())
}

// OwnershipMutationValidator proves the actual placement point encoded by a
// mutation key against the current durable ownership range. It is deliberately
// separate from MutationValidator: reopen and snapshot auditing must continue
// to validate retained transferred rows against the immutable physical image.
type OwnershipMutationValidator interface {
	ValidatePutOwnership(key, value []byte, owned distribution.KeyRange) MutationValidation
	ValidateDeleteOwnership(key, current []byte, found bool, owned distribution.KeyRange) MutationValidation
}

// OwnershipPointValidator proves the actual placement point encoded by a
// point-read key. Relations whose key grammar cannot prove placement do not
// implement it and are not silently treated as range-addressable.
type OwnershipPointValidator interface {
	ValidatePointOwnership(key []byte, owned distribution.KeyRange) MutationValidation
}

// GlobalIndexPlacementValidator exposes the canonical point parser and
// immutable physical range authenticated by a relation's ValidationDigest.
// Relations without it remain usable but cannot issue O(1) split proofs.
type GlobalIndexPlacementValidator interface {
	GlobalIndexPlacementPoint(key []byte) (distribution.KeyspacePoint, bool)
	GlobalIndexPlacementRange() distribution.KeyRange
}

// AttemptedMutationKeys is an opaque view of the exact distinct user keys in
// one planned durable transition. Key bytes are borrowed and remain valid only
// for the synchronous observer call.
type AttemptedMutationKeys struct {
	changes []finalMutation
}

// Len reports the number of exact distinct changed keys.
func (keys AttemptedMutationKeys) Len() int { return len(keys.changes) }

// Key returns the key at index. It panics when index is out of range. The
// returned bytes are borrowed and must be cloned before retention.
func (keys AttemptedMutationKeys) Key(index int) []byte { return keys.changes[index].key }

// MutationAttemptObserver is invoked synchronously before ApplyNormal or
// ApplyNormalBatch returns after the configured durable update has been
// attempted with nonempty user changes. A batch supplies the exact sorted union
// of changed keys across its selected logical prefix, including keys whose
// final net value equals the initial cut. This deliberately includes definite
// and outcome-unknown failures. updateErr is the exact durable-update result. A
// caller that needs to publish conflict clocks only for actual or uncertain
// storage publication must compare the collection Generation captured before
// Apply, inspect PersistenceError, and conservatively accept an updateErr
// classified ErrCommitOutcomeUnknown. Implementations must not retain key
// slices or reenter the Machine.
type MutationAttemptObserver func(AttemptedMutationKeys, error)

// CollectionLimits repeats the collection's frozen durable bounds at the
// integration boundary. Open compares every value to the live handle, so a
// catalog/profile mismatch fails before any command can be proposed.
type CollectionLimits struct {
	MaxKeyBytes          int
	MaxDocumentBytes     int
	MaxDistinctMutations int
	MaxBatchBytes        int
}

// CollectionTarget binds a durable handle to its deterministic validation and
// admission profile.
type CollectionTarget struct {
	Collection             *durable.Collection
	Validation             ValidationProfile
	ValidationDigest       [32]byte
	Validator              MutationValidator
	ObserveMutationAttempt MutationAttemptObserver
	Limits                 CollectionLimits
}

// UserCollection is the single logical collection owned by a Machine.
type UserCollection struct {
	Name         string
	Target       CollectionTarget
	LocalIndexes []store.IndexDefinition
}

func (t CollectionTarget) validate() error {
	if t.Collection == nil || !t.schemaMatches() ||
		!t.Collection.HasSynchronousDurability() || !t.Collection.SupportsUpdate() {
		return ErrSchemaProfile
	}
	zeroDigest := t.ValidationDigest == ([32]byte{})
	switch t.Validation {
	case ValidationOpaqueBinary:
		if !t.Collection.HasOpaqueValues() || !zeroDigest || t.Validator != nil {
			return fmt.Errorf("%w: opaque validation requires opaque storage, zero digest, and no validator", ErrInvalidCollection)
		}
	case ValidationDeterministicMutation:
		if t.Collection.HasOpaqueValues() || zeroDigest || t.Validator == nil {
			return fmt.Errorf("%w: deterministic validation requires digest and validator", ErrInvalidCollection)
		}
	default:
		return fmt.Errorf("%w: unknown validation profile", ErrInvalidCollection)
	}
	l := t.Limits
	if l.MaxKeyBytes <= 0 || l.MaxDocumentBytes <= 0 ||
		l.MaxDistinctMutations <= 0 || l.MaxBatchBytes <= 0 {
		return ErrInvalidCollection
	}
	if l.MaxKeyBytes != t.Collection.MaxKeyBytes() ||
		l.MaxDocumentBytes != t.Collection.MaxDocumentBytes() ||
		l.MaxDistinctMutations != t.Collection.MaxBatchDocuments() ||
		l.MaxBatchBytes != t.Collection.MaxBatchBytes() {
		return fmt.Errorf("%w: explicit limits differ from durable handle", ErrInvalidCollection)
	}
	return nil
}

// Options fixes the cross-collection and bounded-session admission profile.
// Zero values fail closed.
type Options struct {
	TxnLimits   durable.TxnLimits
	MaxSessions uint64
	RetryWindow uint16
	// RequestLedgerCapacityBytes is the exact replicated resident+future-byte
	// budget for this dedicated ledger group. Zero, together with a zero cleanup
	// reserve, disables request-ledger commands on ordinary data groups.
	RequestLedgerCapacityBytes uint64
	// RequestLedgerCleanupReserveBytes is carved out from capacity so Create
	// cannot starve ACK, recovery, or bounded tombstone-authorized GC.
	RequestLedgerCleanupReserveBytes uint64
	// RequestLedgerRange is immutable authority for one dedicated ledger Raft
	// group. End is exclusive; an all-zero End denotes the 2^256 upper bound so
	// the last range can cover the complete digest space without a sentinel key.
	// Identity is carried by every ledger command and prevents a stale router
	// from treating a different group/range generation as authoritative.
	RequestLedgerRange RequestLedgerRange
	TransitionCapture  TransitionCapture
	// TransitionCaptureFactory deterministically reconstructs a capture from a
	// Raft-applied activation witness before Open permits subsequent apply.
	TransitionCaptureFactory func(SplitCaptureActivation) (TransitionCapture, error)
	// TransitionCaptureTarget reserves an authenticated target in the
	// fixed checkpoint membership before capture begins. A non-nil capture must
	// name this exact target. It may be installed later under the Machine lock.
	TransitionCaptureTarget TransitionCaptureTarget
	// CheckpointGroup selects the replay-backed replicated apply lane. The
	// group must exclusively own the exact system and user collections before
	// Open. Ordinary callers leave it nil and retain per-transition synchronous
	// UpdateCollections semantics.
	CheckpointGroup *durable.CheckpointGroup
	// SchemaTransition is required only when the durable state's last record is
	// RecordSchema. It lets first target-generation reopen authenticate the full
	// Raft entry commitment against the independently selected checkpoint
	// membership and catalog CAS. Later ordinary entries no longer need it.
	SchemaTransition          []byte
	SchemaMembershipWitness   durable.CheckpointMembershipWitness
	SchemaAuthorizationDigest [sha256.Size]byte
	SchemaCatalogCASDigest    [sha256.Size]byte
	// SchemaSourceRecovery permits only authenticated recovery of a retired
	// source bundle. It never grants serving authority or selects target files.
	SchemaSourceRecovery *SchemaSourceRecoveryProof
	// SchemaImageAudit optionally reuses an opaque preflight row audit for the
	// first target open at RecordSchema. System/session validation and all
	// transition, checkpoint and catalog proofs remain mandatory. A mismatch
	// fails closed instead of performing a surprise scan under the write fence.
	SchemaImageAudit *SchemaImageAudit
}

// SchemaSourceRecoveryProof binds the original catalog preparation to its
// exact durable Raft entry and source checkpoint membership.
type SchemaSourceRecoveryProof struct {
	Command             []byte
	Membership          durable.CheckpointMembershipWitness
	AuthorizationDigest [sha256.Size]byte
	CatalogCASDigest    [sha256.Size]byte
	SourceApplied       uint64
	// PreCommandApplied is nonzero only when the caller independently proved
	// that (SourceApplied, PreCommandApplied] is an all-empty Raft-normal suffix.
	PreCommandApplied uint64
	// CommittedCommand is the exact WAL command when a replica-local catalog
	// CAS makes it byte-distinct from Command. All other semantics must match.
	CommittedCommand []byte
}

// RequestLedgerRange is the immutable, apply-contract-bound authority interval
// for a dedicated request-ledger group. It is deliberately not topology state:
// this safe point refuses online range changes and requires a fresh certified
// group for a different interval.
type RequestLedgerRange struct {
	Start    requestledger.LedgerHome
	End      requestledger.LedgerHome
	Identity requestledger.Digest
}

func (r RequestLedgerRange) enabled() bool {
	return r.Start != (requestledger.LedgerHome{}) ||
		r.End != (requestledger.LedgerHome{}) || r.Identity != (requestledger.Digest{})
}

func (r RequestLedgerRange) valid() bool {
	if r.Identity == (requestledger.Digest{}) {
		return false
	}
	// Zero End is the canonical unbounded upper endpoint. A bounded interval
	// must be nonempty under bytewise digest order.
	return r.End == (requestledger.LedgerHome{}) || bytes.Compare(r.Start[:], r.End[:]) < 0
}

func (r RequestLedgerRange) contains(home requestledger.LedgerHome) bool {
	if !r.valid() || bytes.Compare(home[:], r.Start[:]) < 0 {
		return false
	}
	return r.End == (requestledger.LedgerHome{}) || bytes.Compare(home[:], r.End[:]) < 0
}

func (o Options) validate() error {
	if o.SchemaImageAudit != nil && (len(o.SchemaTransition) == 0 || o.SchemaSourceRecovery != nil) {
		return ErrSchemaTransition
	}
	if o.MaxSessions == 0 || o.MaxSessions > MaxRetainedSessions ||
		o.RetryWindow == 0 || o.RetryWindow > MaxSessionRetryWindow ||
		o.TxnLimits.MaxCollections < 2 ||
		o.TxnLimits.MaxDocuments < 4 || o.TxnLimits.MaxBytes <= 0 {
		return ErrInvalidOptions
	}
	ledgerEnabled := o.RequestLedgerCapacityBytes != 0 ||
		o.RequestLedgerCleanupReserveBytes != 0 || o.RequestLedgerRange.enabled()
	if ledgerEnabled && (o.RequestLedgerCapacityBytes == 0 ||
		o.RequestLedgerCleanupReserveBytes == 0 ||
		o.RequestLedgerCleanupReserveBytes >= o.RequestLedgerCapacityBytes ||
		o.RequestLedgerCapacityBytes > uint64(^uint64(0)>>1) ||
		!o.RequestLedgerRange.valid()) {
		return ErrInvalidOptions
	}
	return nil
}

// RequestConflictError identifies the retained session key involved in a
// sequence reuse. The original result remains authoritative and is not replaced.
type RequestConflictError struct {
	Key [32]byte
}

func (e *RequestConflictError) Error() string {
	return fmt.Sprintf("%v: %x", ErrRequestConflict, e.Key)
}

func (e *RequestConflictError) Is(target error) bool { return target == ErrRequestConflict }

// CompletionLookup owns an exact completion envelope detached from storage.
type CompletionLookup struct {
	Key             [32]byte
	Bytes           []byte
	AppliedSequence uint64
}

// RequestLedgerUsage is the constant-size, replicated admission/accounting
// witness for one dedicated ledger group. Resident and Reserved are disjoint;
// their checked sum is the exact capacity consumption. ACK tombstones are a
// permanent subset of Rows/ResidentBytes and are never hidden by compaction.
type RequestLedgerUsage struct {
	Enabled             bool
	Rows                uint64
	ResidentBytes       uint64
	ReservedBytes       uint64
	AckRows             uint64
	AckBytes            uint64
	CapacityBytes       uint64
	CleanupReserveBytes uint64
	Range               RequestLedgerRange
}

// SessionLeaseLookup is the exact retained lease state for one issued client
// epoch. TerminalResult is zero for an active session and is the retained
// retirement or revocation result for a retired session.
type SessionLeaseLookup struct {
	ClientEpoch           uint64
	HighSequence          uint64
	AckThrough            uint64
	LeaseDeadlineUnixNano int64
	Status                SessionStatus
	TerminalResult        uint32
}

func isSessionTerminalResult(code uint32) bool {
	return code == ResultSessionRetired || code == ResultSessionRevoked
}

func isSessionResultCode(code uint32) bool {
	return code >= ResultApplied && code <= ResultRouteGate
}

func isMutationResultCode(code uint32) bool {
	return code >= ResultApplied && code <= ResultIntentBusy
}
