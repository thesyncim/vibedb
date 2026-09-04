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
	contract.TargetCount = uint64(len(program.Targets))
	contract.KernelSemanticsDigest = replication.Digest(requestledger.SemanticsDigest())
	var mutationDigester replication.TransactionMutationDigester
	for index := range program.Targets {
		digest, err := mutationDigester.Digest(program.Targets[index].Batches)
		if err != nil {
			return DurableRequestLogicalProgram{}, err
		}
		program.Targets[index].MutationDigest = digest
	}
	contract.TransactionManifestDigest = durableRequestTransactionManifestDigest(program)
	contract.RetryHomeDerivationDigest = durableRequestRetryHomeContractDigest(program)
	contract.ClockContractDigest = durableRequestClockContractDigest(program)
	contract.CoordinatorIdentityDigest = durableRequestCoordinatorIdentityDigest(program)
	contract.SchemaManifestDigest = durableRequestSchemaManifestDigest(program.Targets)
	contract.LineageForwardingDigest = durableRequestLineageForwardingDigest(program.Targets)
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
		contract.TargetCount != uint64(len(program.Targets)) ||
		contract.KernelSemanticsDigest != replication.Digest(requestledger.SemanticsDigest()) ||
		contract.TransactionManifestDigest != durableRequestTransactionManifestDigest(program) ||
		contract.RetryHomeDerivationDigest != durableRequestRetryHomeContractDigest(program) ||
		contract.ClockContractDigest != durableRequestClockContractDigest(program) ||
		contract.CoordinatorIdentityDigest != durableRequestCoordinatorIdentityDigest(program) ||
		contract.SchemaManifestDigest != durableRequestSchemaManifestDigest(program.Targets) ||
		contract.LineageForwardingDigest != durableRequestLineageForwardingDigest(program.Targets) ||
		contract.ResultGrammarDigest != durableRequestResultGrammarDigest() ||
		!validDurableRequestProtocolProgram(contract) ||
		contract.TerminalContractDigest != durableRequestTerminalContractDigest(contract) {
		return false
	}
	var mutationDigester replication.TransactionMutationDigester
	for index := range program.Targets {
		digest, err := mutationDigester.Digest(program.Targets[index].Batches)
		if err != nil || digest != program.Targets[index].MutationDigest {
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
		program.RequestDigest == (replication.Digest{}) || len(program.Targets) == 0 ||
		uint64(identity.CoordinatorOrdinal) >= uint64(len(program.Targets)) ||
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
	for index := range program.Targets {
		target := &program.Targets[index]
		if len(target.Distribution) == 0 || len(target.Distribution) > replication.MaxIdentityBytes ||
			len(target.Shard) == 0 || len(target.Shard) > replication.MaxIdentityBytes ||
			target.RangeIdentity == (replication.Digest{}) ||
			target.SchemaGeneration == 0 ||
			target.RelationManifestDigest == (replication.Digest{}) ||
			target.LineageDigest == (replication.Digest{}) ||
			target.ForwardingRuleDigest == (replication.Digest{}) ||
			!validDurableRequestLogicalGroup(target.Group) ||
			!distributedtxn.ValidateIntentScopes(target.IntentScopes, target.BucketBits) {
			return false
		}
		if index != 0 && compareDurableRequestLogicalTarget(
			program.Targets[index-1], *target,
		) >= 0 {
			return false
		}
	}
	return true
}

func compareDurableRequestLogicalTarget(left, right DurableRequestLogicalTarget) int {
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
	for index := range program.Targets {
		writeDurableRequestTargetIdentity(hash, &scratch, &program.Targets[index])
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
	var framed [len(durableRequestRetryHomeDomain) + sha256.Size + len(distributedtxn.ID{})]byte
	at := copy(framed[:], durableRequestRetryHomeDomain)
	at += copy(framed[at:], key[:])
	copy(framed[at:], transaction[:])
	sum := sha256.Sum256(framed[:])
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
	writeDurableRequestTargetIdentity(hash, &scratch, &program.Targets[program.Identity.CoordinatorOrdinal])
	return sumDurableRequestDigest(hash)
}

func durableRequestSchemaManifestDigest(targets []DurableRequestLogicalTarget) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestSchemaDomain))
	writeDurableRequestU64(hash, &scratch, uint64(len(targets)))
	for index := range targets {
		writeDurableRequestU64(hash, &scratch, targets[index].SchemaGeneration)
		_, _ = hash.Write(targets[index].RelationManifestDigest[:])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestLineageForwardingDigest(targets []DurableRequestLogicalTarget) replication.Digest {
	hash := sha256.New()
	var scratch [8]byte
	_, _ = hash.Write(byteview.Bytes(durableRequestLineageDomain))
	writeDurableRequestU64(hash, &scratch, uint64(len(targets)))
	for index := range targets {
		_, _ = hash.Write(targets[index].RangeIdentity[:])
		_, _ = hash.Write(targets[index].LineageDigest[:])
		_, _ = hash.Write(targets[index].ForwardingRuleDigest[:])
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
	writeDurableRequestU64(hash, &scratch, contract.TargetCount)
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
		contract.TargetCount, contract.MaxPendingWaveBytes,
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

func writeDurableRequestTargetIdentity(
	hash durableRequestHashWriter,
	scratch *[8]byte,
	target *DurableRequestLogicalTarget,
) {
	writeDurableRequestBytes(hash, scratch, byteview.Bytes(string(target.Distribution)))
	writeDurableRequestBytes(hash, scratch, byteview.Bytes(string(target.Shard)))
	_, _ = hash.Write(target.RangeIdentity[:])
	_, _ = hash.Write(target.Group.ClusterID[:])
	_, _ = hash.Write(target.Group.ClusterIncarnation[:])
	writeDurableRequestU64(hash, scratch, target.Group.TopologyRecoveryEpoch)
	_, _ = hash.Write(target.Group.ShardIncarnation[:])
	_, _ = hash.Write(target.Group.GroupID[:])
	writeDurableRequestU64(hash, scratch, target.SchemaGeneration)
	_, _ = hash.Write(target.RelationManifestDigest[:])
	_, _ = hash.Write(target.LineageDigest[:])
	_, _ = hash.Write(target.ForwardingRuleDigest[:])
	_, _ = hash.Write(target.MutationDigest[:])
	scratch[0] = target.BucketBits
	_, _ = hash.Write(scratch[:1])
	writeDurableRequestU64(hash, scratch, uint64(len(target.IntentScopes)))
	for index := range target.IntentScopes {
		writeDurableRequestU64(hash, scratch, uint64(target.IntentScopes[index].Start))
		writeDurableRequestU64(hash, scratch, uint64(target.IntentScopes[index].End))
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
