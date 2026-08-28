package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	durableRequestManifestDomain    = "vibedb/durable-request/transaction-manifest\x00"
	durableRequestRetryHomeDomain   = "vibedb/durable-request/retry-home-contract\x00"
	durableRequestClockDomain       = "vibedb/durable-request/clock-contract\x00"
	durableRequestCoordinatorDomain = "vibedb/durable-request/coordinator-identity\x00"
	durableRequestSchemaDomain      = "vibedb/durable-request/schema-manifest\x00"
	durableRequestLineageDomain     = "vibedb/durable-request/lineage-forwarding\x00"
	durableRequestProtocolDomain    = "vibedb/durable-request/protocol-program\x00"
	durableRequestTerminalDomain    = "vibedb/durable-request/terminal-contract\x00"
	durableRequestTransactionDomain = "vibedb/durable-request/transaction-id\x00"
	durableRequestPinDomain         = "vibedb/durable-request/schema-pin-id\x00"
	durableRequestPlanBuildDomain   = "vibedb/durable-request/plan-build-id\x00"
)

// SealDurableRequestLogicalProgram computes every aggregate contract digest
// from one already-canonically ordered logical program. It never sorts or
// samples time: caller-selected order, transaction identity, deadline, pins,
// and state/result grammars are semantic inputs.
func SealDurableRequestLogicalProgram(
	program DurableRequestLogicalProgram,
) (DurableRequestLogicalProgram, error) {
	stableMembership := durableRequestMembershipStableProgram(program.Contract)
	expectedTransaction := durableRequestTransactionID(program.KeyDigest, program.RequestDigest)
	if program.Identity.ID != (distributedtxn.ID{}) && program.Identity.ID != expectedTransaction {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	program.Identity.ID = expectedTransaction
	expectedPinID := durableRequestPinID(program.KeyDigest, program.RequestDigest)
	if program.Contract.PinID != (requestledger.PinID{}) && program.Contract.PinID != expectedPinID {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	program.Contract.PinID = expectedPinID
	expectedPlanBuildID := durableRequestPlanBuildID(program.KeyDigest, program.RequestDigest)
	if program.Contract.PlanBuildID != (replication.Digest{}) &&
		program.Contract.PlanBuildID != expectedPlanBuildID {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	program.Contract.PlanBuildID = expectedPlanBuildID
	expectedRetryHome := durableRequestRetryHome(program.KeyDigest, program.Identity.ID)
	if expectedRetryHome == (replication.RetryHome{}) ||
		(program.Identity.RetryHome != (replication.RetryHome{}) &&
			program.Identity.RetryHome != expectedRetryHome) {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	program.Identity.RetryHome = expectedRetryHome
	expectedResultGrammar := durableRequestResultGrammarDigest()
	if program.Contract.ResultGrammarDigest != (replication.Digest{}) &&
		program.Contract.ResultGrammarDigest != expectedResultGrammar {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	program.Contract.ResultGrammarDigest = expectedResultGrammar
	if !durableRequestLogicalProgramBaseValid(program) {
		return DurableRequestLogicalProgram{}, ErrDurableRequest
	}
	contract := &program.Contract
	contract.CatalogGeneration = program.Identity.CatalogGeneration
	contract.KeyDigest = program.KeyDigest
	contract.RequestDigest = program.RequestDigest
	contract.ParticipantCount = uint64(len(program.Participants))
	contract.KernelSemanticsDigest = replication.Digest(requestledger.SemanticsDigest())
	var mutationDigester replication.TransactionMutationDigester
	for index := range program.Participants {
		digest, err := mutationDigester.Digest(program.Participants[index].Batches)
		if err != nil {
			return DurableRequestLogicalProgram{}, err
		}
		program.Participants[index].MutationDigest = digest
	}
	contract.TransactionManifestDigest = durableRequestTransactionManifestDigest(program)
	contract.RetryHomeDerivationDigest = durableRequestRetryHomeContractDigest(program)
	contract.ClockContractDigest = durableRequestClockContractDigest(program)
	contract.CoordinatorIdentityDigest = durableRequestCoordinatorIdentityDigest(program)
	contract.SchemaManifestDigest = durableRequestSchemaManifestDigest(program.Participants)
	contract.LineageForwardingDigest = durableRequestLineageForwardingDigest(program.Participants)
	contract.ProtocolProgramDigest = durableRequestProtocolProgramDigest(*contract)
	if stableMembership {
		contract.ProtocolProgramDigest = durableRequestMembershipStableProgramDigest(*contract)
	}
	contract.TerminalContractDigest = durableRequestTerminalContractDigest(*contract)
	if !validDurableRequestLogicalProgram(program) {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	return program, nil
}

func durableRequestTransactionID(
	key, request replication.Digest,
) distributedtxn.ID {
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(durableRequestTransactionDomain))
	_, _ = hash.Write(key[:])
	_, _ = hash.Write(request[:])
	sum := hash.Sum(nil)
	var transaction distributedtxn.ID
	copy(transaction[:], sum[:len(transaction)])
	return transaction
}

func durableRequestPinID(
	key, request replication.Digest,
) requestledger.PinID {
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(durableRequestPinDomain))
	_, _ = hash.Write(key[:])
	_, _ = hash.Write(request[:])
	sum := hash.Sum(nil)
	var pin requestledger.PinID
	copy(pin[:], sum[:len(pin)])
	return pin
}

func durableRequestPlanBuildID(key, request replication.Digest) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(durableRequestPlanBuildDomain))
	_, _ = hash.Write(key[:])
	_, _ = hash.Write(request[:])
	return sumDurableRequestDigest(hash)
}

func validDurableRequestLogicalProgram(program DurableRequestLogicalProgram) bool {
	if !durableRequestLogicalProgramBaseValid(program) {
		return false
	}
	contract := program.Contract
	if contract.CatalogGeneration != program.Identity.CatalogGeneration ||
		contract.KeyDigest != program.KeyDigest || contract.RequestDigest != program.RequestDigest ||
		contract.ParticipantCount != uint64(len(program.Participants)) ||
		contract.KernelSemanticsDigest != replication.Digest(requestledger.SemanticsDigest()) ||
		contract.TransactionManifestDigest != durableRequestTransactionManifestDigest(program) ||
		contract.RetryHomeDerivationDigest != durableRequestRetryHomeContractDigest(program) ||
		contract.ClockContractDigest != durableRequestClockContractDigest(program) ||
		contract.CoordinatorIdentityDigest != durableRequestCoordinatorIdentityDigest(program) ||
		contract.SchemaManifestDigest != durableRequestSchemaManifestDigest(program.Participants) ||
		contract.LineageForwardingDigest != durableRequestLineageForwardingDigest(program.Participants) ||
		contract.ResultGrammarDigest != durableRequestResultGrammarDigest() ||
		!validDurableRequestProtocolProgram(contract) ||
		contract.TerminalContractDigest != durableRequestTerminalContractDigest(contract) {
		return false
	}
	var mutationDigester replication.TransactionMutationDigester
	for index := range program.Participants {
		digest, err := mutationDigester.Digest(program.Participants[index].Batches)
		if err != nil || digest != program.Participants[index].MutationDigest {
			return false
		}
	}
	return true
}

func durableRequestLogicalProgramBaseValid(program DurableRequestLogicalProgram) bool {
	identity, contract := program.Identity, program.Contract
	if identity.ID == (distributedtxn.ID{}) ||
		identity.ID != durableRequestTransactionID(program.KeyDigest, program.RequestDigest) ||
		identity.RetryHome == (replication.RetryHome{}) ||
		identity.RetryHome != durableRequestRetryHome(program.KeyDigest, identity.ID) ||
		identity.CatalogGeneration == 0 || identity.RecoveryDeadline <= 0 ||
		len(program.Tenant) == 0 || len(program.Tenant) > replication.MaxIdentityBytes ||
		program.KeyDigest == (replication.Digest{}) || program.RequestID == (replication.ID128{}) ||
		program.RequestDigest == (replication.Digest{}) || len(program.Participants) == 0 ||
		uint64(identity.CoordinatorOrdinal) >= uint64(len(program.Participants)) ||
		contract.PinID != durableRequestPinID(program.KeyDigest, program.RequestDigest) || contract.PinEpoch == 0 ||
		contract.PinDigest == (replication.Digest{}) ||
		contract.RouteSchemaCertificateDigest == (replication.Digest{}) ||
		contract.ApplyContractDigest == (replication.Digest{}) ||
		contract.ResultGrammarDigest == (replication.Digest{}) ||
		contract.RetirementWitnessDigest == (replication.Digest{}) ||
		contract.InitialStateDigest == (replication.Digest{}) ||
		contract.CommitTerminalStateDigest == (replication.Digest{}) ||
		contract.AbortTerminalStateDigest == (replication.Digest{}) ||
		contract.TerminalSummaryDigest == (replication.Digest{}) ||
		contract.CommitTerminalStateDigest == contract.AbortTerminalStateDigest ||
		contract.CommitTransitionTag == 0 || contract.AbortTransitionTag == 0 ||
		contract.CommitTransitionTag == contract.AbortTransitionTag ||
		contract.CommitFinalWaveCount == 0 || contract.AbortFinalWaveCount == 0 ||
		contract.MaxPendingWaveBytes == 0 ||
		contract.MaxPendingWaveBytes > requestledger.MaxPendingWaveRecordBytes ||
		contract.MaxContinuationBytes == 0 ||
		contract.MaxContinuationBytes > requestledger.MaxContinuationRecordBytes ||
		contract.MaxTerminalBytes == 0 ||
		contract.MaxTerminalBytes > requestledger.MaxLifecyclePayloadBytes ||
		(contract.MaxActivePayloadBytes == 0) != (contract.MaxActivePayloadChunks == 0) ||
		contract.MaxActivePayloadBytes > requestledger.MaxDynamicWavePayloadBytes ||
		contract.MaxActivePayloadChunks > requestledger.MaxDynamicWavePayloadChunks ||
		contract.PlanBuildID != durableRequestPlanBuildID(program.KeyDigest, program.RequestDigest) ||
		contract.PlanningLeaseSpan == 0 || contract.PlanningLeaseSpan > requestledger.MaxPlanningLeaseSpan ||
		contract.PlanningLeaseGeneration == 0 {
		return false
	}
	for index := range program.Participants {
		participant := &program.Participants[index]
		if len(participant.Distribution) == 0 || len(participant.Distribution) > replication.MaxIdentityBytes ||
			len(participant.Shard) == 0 || len(participant.Shard) > replication.MaxIdentityBytes ||
			participant.RangeIdentity == (replication.Digest{}) ||
			participant.SchemaGeneration == 0 ||
			participant.RelationManifestDigest == (replication.Digest{}) ||
			participant.LineageDigest == (replication.Digest{}) ||
			participant.ForwardingRuleDigest == (replication.Digest{}) ||
			!validDurableRequestLogicalGroup(participant.Group) ||
			!distributedtxn.ValidateIntentScopes(participant.IntentScopes, participant.BucketBits) {
			return false
		}
		if index != 0 && compareDurableRequestLogicalParticipant(
			program.Participants[index-1], *participant,
		) >= 0 {
			return false
		}
	}
	return true
}

func compareDurableRequestLogicalParticipant(left, right DurableRequestLogicalParticipant) int {
	if order := bytes.Compare(byteview.Bytes(string(left.Distribution)), byteview.Bytes(string(right.Distribution))); order != 0 {
		return order
	}
	if order := bytes.Compare(byteview.Bytes(string(left.Shard)), byteview.Bytes(string(right.Shard))); order != 0 {
		return order
	}
	return bytes.Compare(left.RangeIdentity[:], right.RangeIdentity[:])
}

func validDurableRequestLogicalGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func durableRequestTransactionManifestDigest(program DurableRequestLogicalProgram) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestManifestDomain))
	writeDurableRequestIdentity(hash, &scratch, program.Identity)
	_, _ = hash.Write(program.KeyDigest[:])
	_, _ = hash.Write(program.RequestDigest[:])
	for index := range program.Participants {
		writeDurableRequestParticipantIdentity(hash, &scratch, &program.Participants[index])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestRetryHomeContractDigest(program DurableRequestLogicalProgram) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(durableRequestRetryHomeDomain))
	_, _ = hash.Write(program.KeyDigest[:])
	_, _ = hash.Write(program.Identity.ID[:])
	_, _ = hash.Write(program.Identity.RetryHome[:])
	return sumDurableRequestDigest(hash)
}

func durableRequestRetryHome(
	key replication.Digest,
	transaction distributedtxn.ID,
) replication.RetryHome {
	hash := sha256.New()
	_, _ = hash.Write(byteview.Bytes(durableRequestRetryHomeDomain))
	_, _ = hash.Write(key[:])
	_, _ = hash.Write(transaction[:])
	sum := hash.Sum(nil)
	var home replication.RetryHome
	copy(home[:], sum[:len(home)])
	return home
}

func durableRequestClockContractDigest(program DurableRequestLogicalProgram) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestClockDomain))
	_, _ = hash.Write(program.KeyDigest[:])
	_, _ = hash.Write(program.Identity.ID[:])
	writeDurableRequestU64(hash, &scratch, uint64(program.Identity.RecoveryDeadline))
	return sumDurableRequestDigest(hash)
}

func durableRequestCoordinatorIdentityDigest(program DurableRequestLogicalProgram) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestCoordinatorDomain))
	writeDurableRequestU64(hash, &scratch, uint64(program.Identity.CoordinatorOrdinal))
	writeDurableRequestParticipantIdentity(hash, &scratch, &program.Participants[program.Identity.CoordinatorOrdinal])
	return sumDurableRequestDigest(hash)
}

func durableRequestSchemaManifestDigest(participants []DurableRequestLogicalParticipant) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestSchemaDomain))
	writeDurableRequestU64(hash, &scratch, uint64(len(participants)))
	for index := range participants {
		writeDurableRequestU64(hash, &scratch, participants[index].SchemaGeneration)
		_, _ = hash.Write(participants[index].RelationManifestDigest[:])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestLineageForwardingDigest(participants []DurableRequestLogicalParticipant) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestLineageDomain))
	writeDurableRequestU64(hash, &scratch, uint64(len(participants)))
	for index := range participants {
		_, _ = hash.Write(participants[index].RangeIdentity[:])
		_, _ = hash.Write(participants[index].LineageDigest[:])
		_, _ = hash.Write(participants[index].ForwardingRuleDigest[:])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestProtocolProgramDigest(contract DurableRequestExecutionContract) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestProtocolDomain))
	for _, digest := range [...]replication.Digest{
		contract.KernelSemanticsDigest, contract.ApplyContractDigest,
		contract.TransactionManifestDigest, contract.RetryHomeDerivationDigest,
		contract.ClockContractDigest, contract.CoordinatorIdentityDigest,
		contract.SchemaManifestDigest, contract.LineageForwardingDigest,
		contract.InitialStateDigest, contract.CommitTerminalStateDigest,
		contract.AbortTerminalStateDigest, contract.ResultGrammarDigest,
		contract.RetirementWitnessDigest, contract.TerminalSummaryDigest,
		contract.PlanBuildID,
	} {
		_, _ = hash.Write(digest[:])
	}
	writeDurableRequestU64(hash, &scratch, uint64(contract.CommitTransitionTag))
	writeDurableRequestU64(hash, &scratch, uint64(contract.AbortTransitionTag))
	writeDurableRequestU64(hash, &scratch, contract.ParticipantCount)
	writeDurableRequestU64(hash, &scratch, contract.CommitFinalWaveCount)
	writeDurableRequestU64(hash, &scratch, contract.AbortFinalWaveCount)
	writeDurableRequestU64(hash, &scratch, contract.MaxActivePayloadBytes)
	writeDurableRequestU64(hash, &scratch, contract.MaxActivePayloadChunks)
	writeDurableRequestU64(hash, &scratch, contract.PlanningLeaseSpan)
	writeDurableRequestU64(hash, &scratch, contract.PlanningLeaseGeneration)
	return sumDurableRequestDigest(hash)
}

// The command mode is sealed in the existing program digest: no new plan
// fields or larger per-request records are needed. Legacy digests remain
// valid and continue to select legacy command bytes during recovery.
func durableRequestMembershipStableProgramDigest(contract DurableRequestExecutionContract) replication.Digest {
	const domain = "vibedb/durable-request/membership-stable-program/1\x00"
	base := durableRequestProtocolProgramDigest(contract)
	var raw [len(domain) + sha256.Size]byte
	copy(raw[:], domain)
	copy(raw[len(domain):], base[:])
	return replication.Digest(sha256.Sum256(raw[:]))
}

func durableRequestMembershipStableProgram(contract DurableRequestExecutionContract) bool {
	return contract.ProtocolProgramDigest == durableRequestMembershipStableProgramDigest(contract)
}

func validDurableRequestProtocolProgram(contract DurableRequestExecutionContract) bool {
	return contract.ProtocolProgramDigest == durableRequestProtocolProgramDigest(contract) ||
		durableRequestMembershipStableProgram(contract)
}

func durableRequestTerminalContractDigest(contract DurableRequestExecutionContract) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestTerminalDomain))
	writeDurableRequestU64(hash, &scratch, contract.CatalogGeneration)
	for _, digest := range [...]replication.Digest{
		contract.KeyDigest, contract.RequestDigest, contract.ProtocolProgramDigest,
		contract.PinDigest, contract.RouteSchemaCertificateDigest,
		contract.CommitTerminalStateDigest, contract.AbortTerminalStateDigest,
		contract.ResultGrammarDigest, contract.RetirementWitnessDigest,
		contract.TerminalSummaryDigest, contract.PlanBuildID,
	} {
		_, _ = hash.Write(digest[:])
	}
	_, _ = hash.Write(contract.PinID[:])
	for _, value := range [...]uint64{
		contract.PinEpoch, uint64(contract.CommitTransitionTag), uint64(contract.AbortTransitionTag),
		contract.ParticipantCount, contract.MaxPendingWaveBytes,
		contract.MaxContinuationBytes, contract.MaxTerminalBytes,
		contract.CommitFinalWaveCount, contract.AbortFinalWaveCount,
		contract.MaxActivePayloadBytes, contract.MaxActivePayloadChunks,
		contract.PlanningLeaseSpan, contract.PlanningLeaseGeneration,
	} {
		writeDurableRequestU64(hash, &scratch, value)
	}
	return sumDurableRequestDigest(hash)
}

type durableRequestHashWriter interface{ Write([]byte) (int, error) }

func writeDurableRequestIdentity(
	hash durableRequestHashWriter,
	scratch *[8]byte,
	identity ReplicatedTransactionIdentity,
) {
	_, _ = hash.Write(identity.ID[:])
	_, _ = hash.Write(identity.RetryHome[:])
	writeDurableRequestU64(hash, scratch, identity.CatalogGeneration)
	writeDurableRequestU64(hash, scratch, uint64(identity.RecoveryDeadline))
	writeDurableRequestU64(hash, scratch, uint64(identity.CoordinatorOrdinal))
}

func writeDurableRequestParticipantIdentity(
	hash durableRequestHashWriter,
	scratch *[8]byte,
	participant *DurableRequestLogicalParticipant,
) {
	writeDurableRequestBytes(hash, scratch, byteview.Bytes(string(participant.Distribution)))
	writeDurableRequestBytes(hash, scratch, byteview.Bytes(string(participant.Shard)))
	_, _ = hash.Write(participant.RangeIdentity[:])
	_, _ = hash.Write(participant.Group.ClusterID[:])
	_, _ = hash.Write(participant.Group.ClusterIncarnation[:])
	writeDurableRequestU64(hash, scratch, participant.Group.TopologyRecoveryEpoch)
	_, _ = hash.Write(participant.Group.ShardIncarnation[:])
	_, _ = hash.Write(participant.Group.GroupID[:])
	writeDurableRequestU64(hash, scratch, participant.SchemaGeneration)
	_, _ = hash.Write(participant.RelationManifestDigest[:])
	_, _ = hash.Write(participant.LineageDigest[:])
	_, _ = hash.Write(participant.ForwardingRuleDigest[:])
	_, _ = hash.Write(participant.MutationDigest[:])
	scratch[0] = participant.BucketBits
	_, _ = hash.Write(scratch[:1])
	writeDurableRequestU64(hash, scratch, uint64(len(participant.IntentScopes)))
	for index := range participant.IntentScopes {
		writeDurableRequestU64(hash, scratch, uint64(participant.IntentScopes[index].Start))
		writeDurableRequestU64(hash, scratch, uint64(participant.IntentScopes[index].End))
	}
}

func writeDurableRequestBytes(hash durableRequestHashWriter, scratch *[8]byte, value []byte) {
	writeDurableRequestU64(hash, scratch, uint64(len(value)))
	_, _ = hash.Write(value)
}

func writeDurableRequestU64(hash durableRequestHashWriter, scratch *[8]byte, value uint64) {
	binary.LittleEndian.PutUint64(scratch[:], value)
	_, _ = hash.Write(scratch[:])
}

func sumDurableRequestDigest(hash interface{ Sum([]byte) []byte }) replication.Digest {
	var digest replication.Digest
	_ = hash.Sum(digest[:0])
	return digest
}
