// Package replicatedstate provides the bounded, unserved replicated apply
// adapter. It deliberately owns no SQL planning, Raft transport, or serving
// authority.
package replicatedstate

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	// ResultFormatMutation is the fixed, empty-payload result grammar used by
	// this low-level unconditional mutation adapter. It includes every closed
	// result below, including the deterministic wrong-shard refusal.
	ResultFormatMutation uint16 = 1

	// Zero and unknown result codes are invalid.
	ResultApplied           uint32 = 1
	ResultStaleFence        uint32 = 2
	ResultUnknownCollection uint32 = 3
	ResultInvalidDocument   uint32 = 4
	ResultTargetBound       uint32 = 5
	ResultWrongShard        uint32 = 6

	// MaxStateEnvelopeBytes and MaxCompletionRecordBytes bound the two binary
	// records before their canonical hexadecimal JSON wrapping.
	MaxStateEnvelopeBytes           = 64 << 10
	MaxCompletionRecordBytes        = 256 << 10
	MaxRetainedCompletions          = 1 << 20
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
	ErrRequestConflict             = errors.New("replicatedstate: client sequence conflicts with retained command")
	ErrStaleCommand                = errors.New("replicatedstate: command has a stale mutable fence")
	ErrWrongBinding                = errors.New("replicatedstate: command belongs to another shard binding")
	ErrApplySequence               = errors.New("replicatedstate: invalid apply sequence")
	ErrApplyPoisoned               = errors.New("replicatedstate: machine is poisoned; reopen required")
	ErrStaticSnapshotOnly          = errors.New("replicatedstate: only the exact static bootstrap snapshot is supported")
	ErrAdmissionBound              = errors.New("replicatedstate: command exceeds the frozen apply profile")
	ErrSchemaProfile               = errors.New("replicatedstate: requires a schema-free JSON collection")
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
}

func (b Binding) validate() error {
	if b.ClusterID == (replication.ID128{}) ||
		b.ClusterIncarnation == (replication.ID128{}) ||
		b.ShardIncarnation == (replication.ID128{}) ||
		b.GroupID == (replication.ID128{}) ||
		b.TopologyRecoveryEpoch == 0 || b.AllocationGeneration == 0 ||
		b.ActivePolicyGeneration == 0 || b.ProtectionEpoch == 0 ||
		b.OwnershipEpoch == 0 || b.SchemaGeneration == 0 ||
		b.RoutingVersion == 0 || b.RouteGeneration == 0 {
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
	// ValidationSchemaFreeJSON is reserved for the hidden system collection. It
	// requires a zero ValidationDigest and no MutationValidator.
	ValidationSchemaFreeJSON ValidationProfile = 1

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

// MutationAttemptObserver is invoked synchronously before ApplyNormal returns
// after UpdateCollections has been attempted with nonempty user changes. This
// deliberately includes definite and outcome-unknown failures. A caller that
// needs to publish conflict clocks only for actual or uncertain storage
// publication must compare the collection Generation captured before Apply to
// the value observed in this callback and also inspect PersistenceError.
// Implementations must not retain key slices or reenter the Machine.
type MutationAttemptObserver func(AttemptedMutationKeys)

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
	Name   string
	Target CollectionTarget
}

func (t CollectionTarget) validate() error {
	if t.Collection == nil || t.Collection.HasSchema() || t.Collection.HasIndexes() ||
		!t.Collection.HasSynchronousDurability() || !t.Collection.SupportsUpdate() {
		return ErrSchemaProfile
	}
	zeroDigest := t.ValidationDigest == ([32]byte{})
	switch t.Validation {
	case ValidationSchemaFreeJSON:
		if !zeroDigest || t.Validator != nil {
			return fmt.Errorf("%w: schema-free validation requires zero digest and no validator", ErrInvalidCollection)
		}
	case ValidationDeterministicMutation:
		if zeroDigest || t.Validator == nil {
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

// Options fixes the cross-collection and completion-retention admission
// profile. Zero values fail closed.
type Options struct {
	TxnLimits         durable.TxnLimits
	MaxCompletions    uint64
	TransitionCapture TransitionCapture
}

func (o Options) validate() error {
	if o.MaxCompletions == 0 || o.MaxCompletions > MaxRetainedCompletions ||
		o.TxnLimits.MaxCollections < 2 ||
		o.TxnLimits.MaxDocuments < 3 || o.TxnLimits.MaxBytes <= 0 {
		return ErrInvalidOptions
	}
	return nil
}

// RequestConflictError identifies the retained completion key involved in a
// tuple reuse. The original record remains authoritative and is not replaced.
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
