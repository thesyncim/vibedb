package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	durableRequestCommitTransitionTag uint32 = 1
	durableRequestAbortTransitionTag  uint32 = 2
)

var (
	durableRequestApplyContractDomain    = []byte("vibedb/durable-request/apply-contract/1\x00")
	durableRequestRouteCertificateDomain = []byte("vibedb/durable-request/route-schema-certificate/1\x00")
	durableRequestInitialStateDomain     = []byte("vibedb/durable-request/initial-state/1\x00")
	durableRequestRetirementDomain       = []byte("vibedb/durable-request/retirement-witness/1\x00")
	durableRequestTerminalSummaryDomain  = []byte("vibedb/durable-request/terminal-summary/1\x00")
)

// DurableRequestLogicalProgramBuild contains only authenticated caller,
// catalog, and logical-clock inputs. Protocol digests, terminal cursors, wave
// counts, transaction identity, retry home, and the aggregate execution pin
// are derived by BuildDurableRequestLogicalProgram.
type DurableRequestLogicalProgramBuild struct {
	Home                    DurableRequestLedgerHome
	Key                     DurableRequestLedgerKey
	Tenant                  []byte
	CatalogGeneration       uint64
	RecoveryDeadline        int64
	PlanningLeaseSpan       uint64
	PlanningLeaseGeneration uint64
	PinEpoch                uint64
	Participants            []ReplicatedTransactionParticipant
}

// BuildDurableRequestLogicalProgram lowers physical catalog routes to one
// immutable ordered logical recipe. It performs two aggregate passes: the
// first seals all participant/protocol witnesses; the second binds those
// witnesses and the ledger-home Raft group into the execution-pin digest.
func BuildDurableRequestLogicalProgram(
	build DurableRequestLogicalProgramBuild,
) (DurableRequestLogicalProgram, error) {
	if !validDurableRequestLedgerKey(build.Key) ||
		build.Home.Point == (requestledger.LedgerHome{}) ||
		build.Home.Identity == (replication.Digest{}) ||
		!validReplicatedRoute(build.Home.borrowedRoute()) ||
		len(build.Tenant) == 0 || len(build.Tenant) > replication.MaxIdentityBytes ||
		requestledger.Digest(sha256.Sum256(build.Tenant)) != build.Key.RequestKey.TenantDigest ||
		build.CatalogGeneration == 0 || build.RecoveryDeadline <= 0 ||
		build.PlanningLeaseSpan == 0 || build.PlanningLeaseSpan > requestledger.MaxPlanningLeaseSpan ||
		build.PlanningLeaseGeneration == 0 ||
		build.PinEpoch == 0 || len(build.Participants) == 0 {
		return DurableRequestLogicalProgram{}, ErrDurableRequest
	}
	home, err := requestledger.Home(build.Key.RequestKey)
	if err != nil || home != build.Home.Point {
		return DurableRequestLogicalProgram{}, errors.Join(err, ErrDurableRequestConflict)
	}
	keyDigest, err := requestledger.KeyDigest(build.Key.RequestKey)
	if err != nil {
		return DurableRequestLogicalProgram{}, errors.Join(err, ErrDurableRequest)
	}

	logical := make([]DurableRequestLogicalParticipant, len(build.Participants))
	physical := make([]ReplicatedTransactionParticipant, len(build.Participants))
	for index := range build.Participants {
		participant := build.Participants[index]
		if !validReplicatedRoute(participant.Route) ||
			!distributedtxn.ValidateIntentScopes(participant.IntentScopes, participant.BucketBits) ||
			len(participant.Batches) == 0 {
			return DurableRequestLogicalProgram{}, ErrDurableRequest
		}
		physical[index] = participant
		logical[index] = DurableRequestLogicalParticipant{
			Distribution: participant.Route.Distribution, Shard: participant.Route.Shard,
			RangeIdentity: participant.Route.RangeIdentity, Group: participant.Route.Group,
			SchemaGeneration:       participant.Route.Command.SchemaGeneration,
			RelationManifestDigest: participant.Route.Command.RelationManifestDigest,
			LineageDigest:          participant.Route.LineageDigest,
			ForwardingRuleDigest:   participant.Route.ForwardingRuleDigest,
			BucketBits:             participant.BucketBits,
			IntentScopes:           slices.Clone(participant.IntentScopes),
			Batches:                cloneRelationMutationBatches(participant.Batches),
		}
	}
	order := make([]int, len(logical))
	for index := range order {
		order[index] = index
	}
	slices.SortFunc(order, func(left, right int) int {
		return compareDurableRequestLogicalParticipant(logical[left], logical[right])
	})
	orderedLogical := make([]DurableRequestLogicalParticipant, len(logical))
	orderedPhysical := make([]ReplicatedTransactionParticipant, len(physical))
	for index, source := range order {
		orderedLogical[index], orderedPhysical[index] = logical[source], physical[source]
		if index != 0 && compareDurableRequestLogicalParticipant(orderedLogical[index-1], orderedLogical[index]) >= 0 {
			return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
		}
	}

	program := DurableRequestLogicalProgram{
		Identity: ReplicatedTransactionIdentity{
			CatalogGeneration:  build.CatalogGeneration,
			RecoveryDeadline:   build.RecoveryDeadline,
			CoordinatorOrdinal: 0,
		},
		Tenant: bytes.Clone(build.Tenant), KeyDigest: replication.Digest(keyDigest),
		RequestID:     replication.ID128(build.Key.RequestKey.Request),
		RequestDigest: build.Key.Digest, Participants: orderedLogical,
	}
	program.Identity.ID = durableRequestTransactionID(program.KeyDigest, program.RequestDigest)
	program.Identity.RetryHome = durableRequestRetryHome(program.KeyDigest, program.Identity.ID)
	contract := &program.Contract
	contract.CatalogGeneration = build.CatalogGeneration
	contract.KeyDigest, contract.RequestDigest = program.KeyDigest, program.RequestDigest
	contract.KernelSemanticsDigest = replication.Digest(requestledger.SemanticsDigest())
	contract.PinID = durableRequestPinID(program.KeyDigest, program.RequestDigest)
	contract.PinEpoch = build.PinEpoch
	contract.PlanBuildID = durableRequestPlanBuildID(program.KeyDigest, program.RequestDigest)
	contract.PlanningLeaseSpan = build.PlanningLeaseSpan
	contract.PlanningLeaseGeneration = build.PlanningLeaseGeneration
	contract.ParticipantCount = uint64(len(program.Participants))
	contract.CommitTransitionTag = durableRequestCommitTransitionTag
	contract.AbortTransitionTag = durableRequestAbortTransitionTag
	contract.MaxPendingWaveBytes = requestledger.MaxPendingWaveRecordBytes
	contract.MaxContinuationBytes = requestledger.MaxContinuationRecordBytes
	contract.MaxTerminalBytes = requestledger.MaxLifecyclePayloadBytes
	contract.MaxActivePayloadBytes = requestledger.MaxDynamicWavePayloadBytes
	contract.MaxActivePayloadChunks = requestledger.MaxDynamicWavePayloadChunks
	contract.ResultGrammarDigest = durableRequestResultGrammarDigest()

	var mutationDigester replication.TransactionMutationDigester
	for index := range program.Participants {
		digest, digestErr := mutationDigester.Digest(program.Participants[index].Batches)
		if digestErr != nil {
			return DurableRequestLogicalProgram{}, digestErr
		}
		program.Participants[index].MutationDigest = digest
	}
	contract.ApplyContractDigest = durableRequestApplyContractDigest(program.Participants)
	contract.TransactionManifestDigest = durableRequestTransactionManifestDigest(program)
	contract.RetryHomeDerivationDigest = durableRequestRetryHomeContractDigest(program)
	contract.ClockContractDigest = durableRequestClockContractDigest(program)
	contract.CoordinatorIdentityDigest = durableRequestCoordinatorIdentityDigest(program)
	contract.SchemaManifestDigest = durableRequestSchemaManifestDigest(program.Participants)
	contract.LineageForwardingDigest = durableRequestLineageForwardingDigest(program.Participants)
	contract.RouteSchemaCertificateDigest = durableRequestRouteCertificateDigest(program.Participants)
	contract.InitialStateDigest = durableRequestInitialStateDigest(program)
	commitCursor := appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedCommitted})
	abortCursor := appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedAborted})
	contract.CommitTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(contract.CommitTransitionTag, commitCursor))
	contract.AbortTerminalStateDigest = replication.Digest(requestledger.NextStateDigest(contract.AbortTransitionTag, abortCursor))
	contract.RetirementWitnessDigest = durableRequestRetirementWitnessDigest(program)
	contract.TerminalSummaryDigest = durableRequestTerminalSummaryDigest(program)
	segments, err := durableRequestManifestSegmentCount(orderedPhysical)
	if err != nil {
		return DurableRequestLogicalProgram{}, err
	}
	manifestCommands := uint64(1)
	if segments > distributedtxn.MaxManifestSegmentsPerCommand {
		manifestCommands += uint64(segments-distributedtxn.MaxManifestSegmentsPerCommand+
			distributedtxn.MaxManifestSegmentsPerCommand-1) / uint64(distributedtxn.MaxManifestSegmentsPerCommand)
	}
	contract.CommitFinalWaveCount = manifestCommands + 2*contract.ParticipantCount + 1
	contract.AbortFinalWaveCount = manifestCommands + 3*contract.ParticipantCount + 1
	contract.ProtocolProgramDigest = durableRequestProtocolProgramDigest(*contract)

	binding := executionpin.Binding{
		RequestKeyDigest:          executionpin.Digest(program.KeyDigest),
		RequestDigest:             executionpin.Digest(program.RequestDigest),
		CatalogGeneration:         program.Identity.CatalogGeneration,
		SchemaManifestDigest:      executionpin.Digest(contract.SchemaManifestDigest),
		TransactionManifestDigest: executionpin.Digest(contract.TransactionManifestDigest),
		ParticipantAuthorityRoot:  executionpin.Digest(contract.LineageForwardingDigest),
		ParticipantCount:          contract.ParticipantCount,
		ExecutionContractDigest:   executionpin.Digest(contract.ProtocolProgramDigest),
		LedgerHomeGroup:           executionpin.ID(build.Home.borrowedRoute().Group.GroupID),
	}
	pinDigest, err := executionpin.BindingDigest(binding)
	if err != nil {
		return DurableRequestLogicalProgram{}, errors.Join(err, ErrDurableRequestConflict)
	}
	contract.PinDigest = replication.Digest(pinDigest)
	contract.TerminalContractDigest = durableRequestTerminalContractDigest(*contract)
	if !validDurableRequestLogicalProgram(program) {
		return DurableRequestLogicalProgram{}, ErrDurableRequestConflict
	}
	return program, nil
}

func cloneRelationMutationBatches(source []replication.RelationMutationBatch) []replication.RelationMutationBatch {
	result := make([]replication.RelationMutationBatch, len(source))
	for batchIndex := range source {
		result[batchIndex].Relation = source[batchIndex].Relation
		result[batchIndex].Mutations = make([]replication.Mutation, len(source[batchIndex].Mutations))
		for mutationIndex := range source[batchIndex].Mutations {
			mutation := source[batchIndex].Mutations[mutationIndex]
			mutation.Key, mutation.Value = bytes.Clone(mutation.Key), bytes.Clone(mutation.Value)
			result[batchIndex].Mutations[mutationIndex] = mutation
		}
	}
	return result
}

func durableRequestManifestSegmentCount(participants []ReplicatedTransactionParticipant) (uint32, error) {
	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	builder, err := distributedtxn.NewManifestBuilder(scratch, func(distributedtxn.ManifestSegment) error { return nil })
	if err != nil {
		return 0, err
	}
	var digester replication.TransactionMutationDigester
	for index := range participants {
		participant := &participants[index]
		digest, digestErr := digester.Digest(participant.Batches)
		if digestErr != nil {
			return 0, digestErr
		}
		if err = builder.Append(distributedtxn.ParticipantRef{
			Distribution:         byteview.Bytes(string(participant.Route.Distribution)),
			Shard:                byteview.Bytes(string(participant.Route.Shard)),
			RoutingVersion:       participant.Route.Command.RoutingVersion,
			AllocationGeneration: participant.Route.AllocationGeneration,
			OwnershipEpoch:       participant.Route.Command.OwnershipEpoch,
			AuthorityWitness:     replicatedRouteAuthorityWitness(participant.Route),
			MutationDigest:       digest, State: distributedtxn.ParticipantStaged,
		}); err != nil {
			return 0, err
		}
	}
	descriptor, err := builder.Seal()
	return descriptor.SegmentCount, err
}

func durableRequestDomainDigest(domain []byte, program DurableRequestLogicalProgram, extras ...replication.Digest) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(domain)
	_, _ = hash.Write(program.KeyDigest[:])
	_, _ = hash.Write(program.RequestDigest[:])
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], uint64(len(program.Participants)))
	_, _ = hash.Write(fixed[:])
	for index := range program.Participants {
		_, _ = hash.Write(program.Participants[index].MutationDigest[:])
	}
	for index := range extras {
		_, _ = hash.Write(extras[index][:])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestApplyContractDigest(participants []DurableRequestLogicalParticipant) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(durableRequestApplyContractDomain)
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], uint64(len(participants)))
	_, _ = hash.Write(fixed[:])
	for index := range participants {
		_, _ = hash.Write(participants[index].MutationDigest[:])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestRouteCertificateDigest(participants []DurableRequestLogicalParticipant) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write(durableRequestRouteCertificateDomain)
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], uint64(len(participants)))
	_, _ = hash.Write(fixed[:])
	for index := range participants {
		participant := &participants[index]
		_, _ = hash.Write(participant.Group.ClusterID[:])
		_, _ = hash.Write(participant.Group.ClusterIncarnation[:])
		binary.LittleEndian.PutUint64(fixed[:], participant.Group.TopologyRecoveryEpoch)
		_, _ = hash.Write(fixed[:])
		_, _ = hash.Write(participant.Group.ShardIncarnation[:])
		_, _ = hash.Write(participant.Group.GroupID[:])
		_, _ = hash.Write(participant.RangeIdentity[:])
		binary.LittleEndian.PutUint64(fixed[:], participant.SchemaGeneration)
		_, _ = hash.Write(fixed[:])
		_, _ = hash.Write(participant.RelationManifestDigest[:])
	}
	return sumDurableRequestDigest(hash)
}

func durableRequestInitialStateDigest(program DurableRequestLogicalProgram) replication.Digest {
	return durableRequestDomainDigest(durableRequestInitialStateDomain, program)
}

func durableRequestRetirementWitnessDigest(program DurableRequestLogicalProgram) replication.Digest {
	return durableRequestDomainDigest(durableRequestRetirementDomain, program,
		program.Contract.ApplyContractDigest, program.Contract.RouteSchemaCertificateDigest)
}

func durableRequestTerminalSummaryDigest(program DurableRequestLogicalProgram) replication.Digest {
	return durableRequestDomainDigest(durableRequestTerminalSummaryDomain, program,
		program.Contract.CommitTerminalStateDigest, program.Contract.AbortTerminalStateDigest,
		program.Contract.RetirementWitnessDigest)
}
