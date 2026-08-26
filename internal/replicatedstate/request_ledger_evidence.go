package replicatedstate

import (
	"bytes"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

func openRequestLedgerRouteGateCommand(
	commandBytes []byte,
) (replication.CommandView, routegate.Command, bool) {
	outer, err := replication.OpenCommand(commandBytes)
	if err != nil || outer.Kind() != replication.CommandRouteGate {
		return replication.CommandView{}, routegate.Command{}, false
	}
	command, err := outer.OpenRouteGate()
	if err != nil {
		return replication.CommandView{}, routegate.Command{}, false
	}
	return outer, command, true
}

func openRequestLedgerRouteGatePair(
	commandBytes []byte,
	completionBytes []byte,
) (replication.CommandView, routegate.Command, routegate.Outcome, bool) {
	outer, command, ok := openRequestLedgerRouteGateCommand(commandBytes)
	if !ok {
		return replication.CommandView{}, routegate.Command{}, routegate.Outcome{}, false
	}
	completion, err := replication.OpenCompletion(completionBytes)
	if err != nil || !requestLedgerCompletionMatchesCommand(outer, completion) ||
		completion.ResultCode != ResultRouteGate || completion.ResultFormat != ResultFormatRouteGate ||
		completion.Storage != replication.CompletionInline ||
		completion.ResultLength != routegate.OutcomeBytes ||
		len(completion.InlineResult) != routegate.OutcomeBytes {
		return replication.CommandView{}, routegate.Command{}, routegate.Outcome{}, false
	}
	outcome, err := routegate.OpenOutcome(completion.InlineResult)
	if err != nil {
		return replication.CommandView{}, routegate.Command{}, routegate.Outcome{}, false
	}
	return outer, command, outcome, true
}

func requestLedgerRouteCommandMatches(
	record requestledger.RoutePinRecord,
	outer replication.CommandView,
	command routegate.Command,
	operation routegate.Operation,
) bool {
	physicalWitness, ok := replication.RouteGatePhysicalWitness(outer)
	if !ok || requestledger.Digest(physicalWitness) != record.PhysicalWitnessDigest {
		return false
	}
	identity, err := requestledger.DeriveRouteGateIdentity(
		record.KeyDigest, record.RequestDigest, record.PlanRoot,
		record.PriorContinuationDigest, record.PinID, record.WaveOrdinal,
	)
	if err != nil {
		return false
	}
	binding, err := requestledger.DeriveRouteGateBinding(
		identity, record.BindingDigest, record.PhysicalWitnessDigest, command.Epoch,
	)
	return err == nil && command.Operation == operation &&
		command.Identity == routegate.Identity(identity) &&
		command.Binding == routegate.Binding(binding)
}

func requestLedgerRouteCommandEvidenceAvailable(
	prior requestledger.RoutePinRecord,
	record requestledger.RoutePinRecord,
) bool {
	outer, command, ok := openRequestLedgerRouteGateCommand(record.Command)
	if !ok {
		return false
	}
	switch record.Phase {
	case requestledger.RoutePinAcquiring:
		return prior.Phase == requestledger.RoutePinInvalid &&
			requestLedgerRouteCommandMatches(record, outer, command, routegate.OperationAcquireShared)
	case requestledger.RoutePinReleasing:
		if prior.Phase != requestledger.RoutePinAcquired ||
			!requestLedgerRouteCommandMatches(record, outer, command, routegate.OperationReleaseShared) {
			return false
		}
		_, acquire, acquireOK := openRequestLedgerRouteGateCommand(prior.Command)
		return acquireOK && acquire.Operation == routegate.OperationAcquireShared &&
			acquire.Epoch == command.Epoch && acquire.Identity == command.Identity &&
			acquire.Binding == command.Binding
	default:
		return false
	}
}

func requestLedgerRouteCompletionEvidenceAvailable(
	prior requestledger.RoutePinRecord,
	record requestledger.RoutePinRecord,
) bool {
	outer, command, outcome, ok := openRequestLedgerRouteGatePair(record.Command, record.Completion)
	if !ok {
		return false
	}
	switch record.Phase {
	case requestledger.RoutePinAcquired:
		return prior.Phase == requestledger.RoutePinAcquiring &&
			requestLedgerRouteCommandMatches(record, outer, command, routegate.OperationAcquireShared) &&
			requestLedgerRouteOutcomeProves(command, outcome, routegate.OperationAcquireShared)
	case requestledger.RoutePinReleased:
		return prior.Phase == requestledger.RoutePinReleasing &&
			requestLedgerRouteCommandMatches(record, outer, command, routegate.OperationReleaseShared) &&
			requestLedgerRouteOutcomeProves(command, outcome, routegate.OperationReleaseShared)
	default:
		return false
	}
}

func requestLedgerRouteOutcomeProves(
	command routegate.Command,
	outcome routegate.Outcome,
	operation routegate.Operation,
) bool {
	if command.Operation != operation {
		return false
	}
	switch operation {
	case routegate.OperationAcquireShared:
		return (outcome.Reason == routegate.ReasonAcquired ||
			outcome.Reason == routegate.ReasonIdempotent) &&
			outcome.Status.ActivePins != 0
	case routegate.OperationReleaseShared:
		return (outcome.Reason == routegate.ReasonReleased ||
			outcome.Reason == routegate.ReasonAlreadyReleased) &&
			outcome.Status.ReleasedPins != 0
	default:
		return false
	}
}

func requestLedgerSchemaReleaseEvidenceAvailable(record requestledger.SchemaPinReleaseRecord) bool {
	outer, err := replication.OpenCommand(record.Command)
	if err != nil || outer.Kind() != replication.CommandExecutionPin {
		return false
	}
	command, err := outer.OpenExecutionPin()
	if err != nil || !requestLedgerSchemaReleaseCommandMatches(record, command) {
		return false
	}
	completion, err := replication.OpenCompletion(record.Completion)
	if err != nil || !requestLedgerCompletionMatchesCommand(outer, completion) ||
		completion.ResultCode != ResultApplied ||
		completion.ResultFormat != ResultFormatExecutionPin ||
		completion.Storage != replication.CompletionInline ||
		completion.ResultLength != executionpin.CompletionBytes ||
		len(completion.InlineResult) != executionpin.CompletionBytes {
		return false
	}
	proof, err := executionpin.OpenCompletion(completion.InlineResult)
	if err != nil {
		return false
	}
	authority, ok := replication.ExecutionPinAuthorityDigest(outer)
	if !ok || executionpin.ValidateReleasePair(
		command, proof, executionpin.Digest(authority),
	) != nil {
		return false
	}
	return true
}

func requestLedgerSchemaReleaseCommandAvailable(record requestledger.SchemaPinReleaseRecord) bool {
	outer, err := replication.OpenCommand(record.Command)
	if err != nil || outer.Kind() != replication.CommandExecutionPin {
		return false
	}
	command, err := outer.OpenExecutionPin()
	return err == nil && requestLedgerSchemaReleaseCommandMatches(record, command)
}

func requestLedgerSchemaReleaseCommandMatches(
	record requestledger.SchemaPinReleaseRecord,
	command executionpin.Command,
) bool {
	bindingDigest, err := executionpin.BindingDigest(command.Binding)
	return err == nil && command.Operation == executionpin.OperationRelease &&
		command.Binding.RequestKeyDigest == executionpin.Digest(record.KeyDigest) &&
		command.Binding.RequestDigest == executionpin.Digest(record.RequestDigest) &&
		command.Binding.CatalogGeneration == record.CatalogGeneration &&
		command.Binding.SchemaCertificateDigest == executionpin.Digest(record.RouteSchemaCertificateDigest) &&
		bindingDigest == executionpin.Digest(record.PinDigest) &&
		command.PrepareTerminalDigest == executionpin.Digest(record.PreparedTerminalDigest)
}

func requestLedgerCompletionMatchesCommand(
	command replication.CommandView,
	completion replication.CompletionView,
) bool {
	return completion.AppliedSequence != 0 &&
		completion.ClusterID == command.ClusterID &&
		completion.ClusterIncarnation == command.ClusterIncarnation &&
		completion.TopologyRecoveryEpoch == command.TopologyRecoveryEpoch &&
		bytes.Equal(completion.Distribution, command.Distribution) &&
		bytes.Equal(completion.Shard, command.Shard) &&
		completion.AllocationGeneration == command.AllocationGeneration &&
		completion.ShardIncarnation == command.ShardIncarnation &&
		completion.GroupID == command.GroupID &&
		completion.ReplicaSetVersion == command.ReplicaSetVersion &&
		completion.ActivePolicyGeneration == command.ActivePolicyGeneration &&
		completion.ProtectionEpoch == command.ProtectionEpoch &&
		completion.RoutingVersion == command.RoutingVersion &&
		completion.RouteGeneration == command.RouteGeneration &&
		bytes.Equal(completion.Tenant, command.Tenant) &&
		completion.ClientID == command.ClientID &&
		completion.ClientEpoch == command.ClientEpoch &&
		completion.ClientSequence == command.ClientSequence &&
		completion.Fingerprint == command.Fingerprint &&
		completion.RetryHome == command.RetryHome
}
