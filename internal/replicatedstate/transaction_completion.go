package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	// ResultFormatTransaction is the sole fixed transaction result grammar. It
	// is a grammar selector, not a version ladder.
	ResultFormatTransaction uint16 = 2
	// ResultTransactionConflict is a transaction-control CAS loss. It is
	// intentionally distinct from ResultIndexConflict, which is a user-data
	// mutation precondition failure.
	ResultTransactionConflict uint32 = 13

	transactionCompletionResultBytes      = 24
	MaxTransactionCompletionEnvelopeBytes = replication.MaxEmptyResultCompletionEnvelopeBytes + transactionCompletionResultBytes
)

const (
	transactionCompletionAffectedRows    = byte(1)
	transactionCompletionControlRevision = byte(2)
)

// TransactionCompletionResult is the strict fixed-width result carried by a
// transaction completion. Revision is the durable control cut which settled
// the retry. AffectedRows is meaningful only when AffectedRowsValid is true.
type TransactionCompletionResult struct {
	Role              distributedtxn.ReplicatedRole
	Operation         distributedtxn.ReplicatedOperation
	Revision          uint64
	RevisionValid     bool
	AffectedRows      int64
	AffectedRowsValid bool
}

// OpenTransactionCompletionResult opens the sole fixed transaction-result
// grammar. It rejects unknown flags, reserved bytes, invalid roles/operations,
// and affected-row claims for operations other than participant Apply.
func OpenTransactionCompletionResult(
	resultCode uint32,
	raw []byte,
) (TransactionCompletionResult, error) {
	if len(raw) != transactionCompletionResultBytes || raw[3] != 0 ||
		!allZero(raw[4:8]) ||
		raw[2]&^(transactionCompletionAffectedRows|transactionCompletionControlRevision) != 0 {
		return TransactionCompletionResult{}, ErrCompletionCorrupt
	}
	result := TransactionCompletionResult{
		Role:              distributedtxn.ReplicatedRole(raw[0]),
		Operation:         distributedtxn.ReplicatedOperation(raw[1]),
		AffectedRowsValid: raw[2]&transactionCompletionAffectedRows != 0,
		RevisionValid:     raw[2]&transactionCompletionControlRevision != 0,
		Revision:          binary.LittleEndian.Uint64(raw[8:16]),
		AffectedRows:      int64(binary.LittleEndian.Uint64(raw[16:24])),
	}
	if !transactionOperationRole(result.Operation, result.Role) ||
		result.AffectedRows < 0 ||
		result.AffectedRowsValid && result.Operation != distributedtxn.ReplicatedApplyParticipant ||
		!result.AffectedRowsValid && result.AffectedRows != 0 ||
		result.RevisionValid != (result.Revision != 0) {
		return TransactionCompletionResult{}, ErrCompletionCorrupt
	}
	if resultCode == ResultStaleFence {
		if result.AffectedRowsValid {
			return TransactionCompletionResult{}, ErrCompletionCorrupt
		}
	} else if (resultCode != ResultApplied && resultCode != ResultIndexConflict &&
		resultCode != ResultTransactionConflict) ||
		!result.RevisionValid {
		return TransactionCompletionResult{}, ErrCompletionCorrupt
	}
	if resultCode == ResultIndexConflict &&
		(result.Role != distributedtxn.ReplicatedRoleParticipant ||
			result.Operation != distributedtxn.ReplicatedPrepareParticipant) {
		return TransactionCompletionResult{}, ErrCompletionCorrupt
	}
	if resultCode == ResultApplied &&
		(result.Operation == distributedtxn.ReplicatedApplyParticipant) != result.AffectedRowsValid ||
		(resultCode == ResultIndexConflict || resultCode == ResultTransactionConflict) &&
			result.AffectedRowsValid {
		return TransactionCompletionResult{}, ErrCompletionCorrupt
	}
	return result, nil
}

type transactionRetryDisposition uint8

const (
	transactionRetryUnknown transactionRetryDisposition = iota
	transactionRetryExact
	transactionRetryConflict
)

func (m *Machine) lookupTransactionCompletionAtSnapshot(
	command replication.CommandView,
	completionScratch []byte,
	workspace *CompletionLookupWorkspace,
) (CompletionLookup, error) {
	transaction, err := command.OpenTransactionInto(workspace.transactionCommandScopes[:])
	if err != nil {
		return CompletionLookup{}, err
	}
	storageKey, err := TransactionControlStorageKey(transaction.Role, transaction.ID)
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	snapshot := pointSnapshot{value: workspace.snapshot}
	raw, found, err := snapshot.appendRaw(workspace.transactionRead[:0], storageKey[:])
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	if !found {
		if m.mutableBindingMatchesState(command, m.state) {
			return CompletionLookup{}, ErrCompletionNotFound
		}
		control := TransactionControl{Revision: 0, LastAppliedIndex: m.state.Applied}
		completion, appendErr := m.appendTransactionCompletion(
			completionScratch[:0], command, transaction, control,
			ResultStaleFence, false,
		)
		if appendErr != nil {
			return CompletionLookup{}, m.fail(appendErr)
		}
		key := sha256.Sum256(storageKey[:])
		return CompletionLookup{
			Key: key, Bytes: completion, AppliedSequence: m.state.Applied,
		}, nil
	}
	control, err := OpenTransactionControlInto(raw, workspace.transactionControlScopes[:])
	if err != nil || control.ID != transaction.ID || control.Role != transaction.Role {
		return CompletionLookup{}, m.fail(ErrTransactionStateCorrupt)
	}
	disposition, resultCode, err := transactionCompletionDisposition(
		transaction, LogicalCommandDigest(command), control.TransactionControl, snapshot,
	)
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	settlementControl := control.TransactionControl
	if disposition == transactionRetryUnknown {
		if m.mutableBindingMatchesState(command, m.state) {
			return CompletionLookup{}, ErrCompletionNotFound
		}
		resultCode = ResultStaleFence
		settlementControl.LastAppliedIndex = m.state.Applied
	}
	if disposition == transactionRetryConflict {
		resultCode = ResultTransactionConflict
	}
	completion, err := m.appendTransactionCompletion(
		completionScratch[:0], command, transaction, settlementControl,
		resultCode, disposition == transactionRetryExact,
	)
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	key := sha256.Sum256(storageKey[:])
	return CompletionLookup{
		Key: key, Bytes: completion, AppliedSequence: settlementControl.LastAppliedIndex,
	}, nil
}

func transactionCompletionDisposition(
	command distributedtxn.ReplicatedCommandView,
	commandDigest replication.Digest,
	control TransactionControl,
	snapshot pointSnapshot,
) (transactionRetryDisposition, uint32, error) {
	if command.ID != control.ID || command.Role != control.Role {
		return transactionRetryUnknown, 0, ErrTransactionStateCorrupt
	}
	lastIdentity := control.LastOperation == command.Operation &&
		control.LastExpectedRevision == command.ExpectedRevision
	if lastIdentity && control.LastCommandDigest == commandDigest {
		switch control.LastResultCode {
		case ResultApplied:
			return transactionRetryExact, ResultApplied, nil
		case ResultIndexConflict:
			if command.Role != distributedtxn.ReplicatedRoleParticipant ||
				command.Operation != distributedtxn.ReplicatedPrepareParticipant {
				return transactionRetryUnknown, 0, ErrTransactionStateCorrupt
			}
			return transactionRetryExact, ResultIndexConflict, nil
		default:
			return transactionRetryUnknown, 0, ErrTransactionStateCorrupt
		}
	}
	exact, err := transactionHistoricalRetryExact(command, control, snapshot)
	if err != nil {
		return transactionRetryUnknown, 0, err
	}
	if exact {
		return transactionRetryExact, ResultApplied, nil
	}
	if lastIdentity {
		return transactionRetryConflict, ResultTransactionConflict, nil
	}
	// Creation loses as soon as the exact ID already exists. A transition loses
	// only after its expected revision is durably behind the current control.
	// Equal/future revisions have no durable execution witness and remain
	// unknown so lookup never manufactures a completion for an eligible command.
	if command.ExpectedRevision == 0 || command.ExpectedRevision < control.Revision {
		return transactionRetryConflict, ResultTransactionConflict, nil
	}
	return transactionRetryUnknown, ResultApplied, nil
}

func transactionHistoricalRetryExact(
	command distributedtxn.ReplicatedCommandView,
	control TransactionControl,
	snapshot pointSnapshot,
) (bool, error) {
	switch command.Operation {
	case distributedtxn.ReplicatedStageCoordinator:
		return control.PayloadKind == distributedtxn.ReplicatedPayloadCoordinator &&
			control.PayloadDigest == distributedtxn.Digest(sha256.Sum256(command.Payload)), nil
	case distributedtxn.ReplicatedStageManifestCoordinator:
		return control.PayloadKind == distributedtxn.ReplicatedPayloadManifestCoordinator &&
			control.PayloadDigest == distributedtxn.Digest(sha256.Sum256(command.Payload)), nil
	case distributedtxn.ReplicatedStageManifestSegment:
		return transactionManifestPageRetryExact(
			snapshot, control.ID, command.Payload, command.ExpectedRevision,
			distributedtxn.CoordinatorState(control.State) != distributedtxn.CoordinatorRetired,
		)
	case distributedtxn.ReplicatedStageParticipant:
		return transactionParticipantStageRetryExact(command, control), nil
	case distributedtxn.ReplicatedCommitCoordinator:
		return control.CoordinatorDecision == distributedtxn.CoordinatorCommitted &&
			transactionCoordinatorDecisionExpected(control) == command.ExpectedRevision, nil
	case distributedtxn.ReplicatedAbortCoordinator:
		return control.CoordinatorDecision == distributedtxn.CoordinatorAborted &&
			transactionCoordinatorDecisionExpected(control) == command.ExpectedRevision, nil
	case distributedtxn.ReplicatedRetireCoordinator:
		return distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorRetired &&
			control.Revision == command.ExpectedRevision+1, nil
	case distributedtxn.ReplicatedPrepareParticipant:
		return command.ExpectedRevision == 1 && transactionParticipantWasPrepared(control), nil
	case distributedtxn.ReplicatedApplyParticipant:
		return command.ExpectedRevision == 2 && control.AffectedRowsValid &&
			(distributedtxn.ParticipantState(control.State) == distributedtxn.ParticipantApplied ||
				distributedtxn.ParticipantState(control.State) == distributedtxn.ParticipantReleased), nil
	case distributedtxn.ReplicatedAbortParticipant:
		state := distributedtxn.ParticipantState(control.State)
		return !control.AffectedRowsValid &&
			(state == distributedtxn.ParticipantAborted && control.Revision == command.ExpectedRevision+1 ||
				state == distributedtxn.ParticipantReleased && control.Revision == command.ExpectedRevision+2), nil
	case distributedtxn.ReplicatedReleaseParticipant:
		return distributedtxn.ParticipantState(control.State) == distributedtxn.ParticipantReleased &&
			control.Revision == command.ExpectedRevision+1, nil
	default:
		return false, ErrTransactionStateCorrupt
	}
}

func transactionCoordinatorDecisionExpected(control TransactionControl) uint64 {
	if control.Revision == 0 {
		return 0
	}
	if distributedtxn.CoordinatorState(control.State) == distributedtxn.CoordinatorRetired {
		if control.Revision < 2 {
			return 0
		}
		return control.Revision - 2
	}
	return control.Revision - 1
}

func transactionParticipantWasPrepared(control TransactionControl) bool {
	switch distributedtxn.ParticipantState(control.State) {
	case distributedtxn.ParticipantPrepared, distributedtxn.ParticipantApplied:
		return true
	case distributedtxn.ParticipantAborted:
		return control.Revision == 3
	case distributedtxn.ParticipantReleased:
		return control.AffectedRowsValid || control.Revision == 4
	default:
		return false
	}
}

func transactionParticipantStageRetryExact(
	command distributedtxn.ReplicatedCommandView,
	control TransactionControl,
) bool {
	stage := command.Participant
	return control.PayloadKind == distributedtxn.ReplicatedPayloadParticipantStage &&
		control.MutationDigest == stage.MutationDigest &&
		control.CoordinatorGroup == replication.ID128(stage.CoordinatorGroup) &&
		control.CoordinatorShardIncarnation == replication.ID128(stage.CoordinatorShardIncarnation) &&
		control.CoordinatorAllocation == stage.CoordinatorAllocation &&
		control.BucketBits == stage.BucketBits && slices.Equal(control.IntentScopes, stage.IntentScopes)
}

func transactionManifestPageRetryExact(
	snapshot pointSnapshot,
	id distributedtxn.ID,
	rawPage []byte,
	expectedRevision uint64,
	requireRetained bool,
) (bool, error) {
	meta, ok := openTransactionManifestSegmentMeta(rawPage)
	if !ok || uint64(meta.Index) != expectedRevision {
		return false, nil
	}
	key, err := TransactionManifestPageStorageKey(id, meta.Index)
	if err != nil {
		return false, err
	}
	var pageRead [MaxTransactionManifestPageRecordBytes]byte
	stored, found, err := snapshot.appendRaw(pageRead[:0], key[:])
	if err != nil || !found {
		if err != nil {
			return false, err
		}
		if requireRetained {
			return false, ErrTransactionStateCorrupt
		}
		return false, nil
	}
	storedID, storedMeta, _, err := openTransactionManifestPageWitness(stored)
	if err != nil {
		return false, err
	}
	return storedID == id && storedMeta.Index == meta.Index &&
		storedMeta.Digest == meta.Digest, nil
}

func openTransactionManifestPageWitness(
	src []byte,
) (distributedtxn.ID, transactionManifestSegmentMeta, []byte, error) {
	if len(src) < transactionManifestHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxTransactionManifestPageRecordBytes ||
		!bytes.Equal(src[0:8], transactionManifestMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel ||
		binary.LittleEndian.Uint16(src[10:12]) != transactionManifestHeaderBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		binary.LittleEndian.Uint32(src[36:40]) != 0 ||
		!allZero(src[88:transactionManifestHeaderBytes]) ||
		!verifyRecord(src, transactionManifestChecksumDomain) {
		return distributedtxn.ID{}, transactionManifestSegmentMeta{}, nil, ErrTransactionStateCorrupt
	}
	rawBytes := uint64(binary.LittleEndian.Uint32(src[16:20]))
	if rawBytes > distributedtxn.ManifestSegmentBytes ||
		uint64(transactionManifestHeaderBytes+recordChecksumLen)+rawBytes != uint64(len(src)) {
		return distributedtxn.ID{}, transactionManifestSegmentMeta{}, nil, ErrTransactionStateCorrupt
	}
	var id distributedtxn.ID
	copy(id[:], src[40:56])
	end := transactionManifestHeaderBytes + int(rawBytes)
	meta, ok := openTransactionManifestSegmentMeta(src[transactionManifestHeaderBytes:end:end])
	var digest distributedtxn.Digest
	copy(digest[:], src[56:88])
	if !ok || id.IsZero() || meta.Index != binary.LittleEndian.Uint32(src[20:24]) ||
		meta.FirstParticipant != binary.LittleEndian.Uint64(src[24:32]) ||
		meta.ParticipantCount != binary.LittleEndian.Uint32(src[32:36]) ||
		meta.Digest != digest {
		return distributedtxn.ID{}, transactionManifestSegmentMeta{}, nil, ErrTransactionStateCorrupt
	}
	return id, meta, src[transactionManifestHeaderBytes:end:end], nil
}

func (m *Machine) appendTransactionCompletion(
	dst []byte,
	command replication.CommandView,
	transaction distributedtxn.ReplicatedCommandView,
	control TransactionControl,
	resultCode uint32,
	exact bool,
) ([]byte, error) {
	if resultCode != ResultApplied && resultCode != ResultIndexConflict &&
		resultCode != ResultTransactionConflict &&
		resultCode != ResultStaleFence {
		return dst, ErrCompletionCorrupt
	}
	var result [transactionCompletionResultBytes]byte
	result[0] = byte(transaction.Role)
	result[1] = byte(transaction.Operation)
	if control.Revision != 0 {
		result[2] |= transactionCompletionControlRevision
	}
	binary.LittleEndian.PutUint64(result[8:16], control.Revision)
	if exact && resultCode == ResultApplied &&
		transaction.Operation == distributedtxn.ReplicatedApplyParticipant {
		if !control.AffectedRowsValid || control.AffectedRows < 0 {
			return dst, ErrTransactionStateCorrupt
		}
		result[2] |= transactionCompletionAffectedRows
		binary.LittleEndian.PutUint64(result[16:24], uint64(control.AffectedRows))
	}
	resultDigest := replication.CompletionResultDigest(
		resultCode, ResultFormatTransaction, result[:],
	)
	return replication.AppendCompletionBytes(dst, replication.CompletionBytes{
		ClusterID:              m.binding.ClusterID,
		ClusterIncarnation:     m.binding.ClusterIncarnation,
		TopologyRecoveryEpoch:  m.binding.TopologyRecoveryEpoch,
		Distribution:           m.distribution,
		Shard:                  m.shard,
		AllocationGeneration:   m.binding.AllocationGeneration,
		ShardIncarnation:       m.binding.ShardIncarnation,
		GroupID:                m.binding.GroupID,
		ReplicaSetVersion:      m.state.ReplicaSetVersion,
		ActivePolicyGeneration: m.state.Binding.ActivePolicyGeneration,
		ProtectionEpoch:        m.state.Binding.ProtectionEpoch,
		RoutingVersion:         m.state.Binding.RoutingVersion,
		RouteGeneration:        m.state.Binding.RouteGeneration,
		Tenant:                 command.Tenant,
		ClientID:               command.ClientID,
		ClientEpoch:            command.ClientEpoch,
		ClientSequence:         command.ClientSequence,
		Fingerprint:            command.Fingerprint,
		RetryHome:              command.RetryHome,
		AppliedSequence:        control.LastAppliedIndex,
		ResultCode:             resultCode,
		ResultFormat:           ResultFormatTransaction,
		Storage:                replication.CompletionInline,
		ResultLength:           transactionCompletionResultBytes,
		ResultDigest:           resultDigest,
		InlineResult:           result[:],
	})
}
