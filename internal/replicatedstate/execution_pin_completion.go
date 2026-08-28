package replicatedstate

import (
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
)

// MaxExecutionPinCompletionEnvelopeBytes is the exact upper bound for the
// fixed transferable execution-pin proof carried inline.
const MaxExecutionPinCompletionEnvelopeBytes = replication.MaxEmptyResultCompletionEnvelopeBytes + executionpin.CompletionBytes

func (m *Machine) appendExecutionPinCompletion(
	dst []byte,
	command replication.CommandView,
	slot SessionSlotView,
	snapshot pointSnapshot,
) ([]byte, error) {
	nested, err := command.OpenExecutionPin()
	if err != nil {
		return dst, err
	}
	var proof executionpin.Completion
	if slot.ResultCode == ResultApplied {
		record, found, readErr := executionPinRecordAt(snapshot, nested.PinID)
		if readErr != nil || !found {
			return dst, ErrExecutionPinStateCorrupt
		}
		proof, err = executionpin.CompletionFromApplied(
			nested, record, executionPinAuthorityDigest(command), slot.AppliedSequence,
		)
	} else {
		switch slot.ResultCode {
		case ResultIndexConflict, ResultIntentBusy, ResultTargetBound, ResultStaleFence:
			proof, err = executionpin.RefusalCompletion(nested.Operation)
		default:
			return dst, ErrCompletionCorrupt
		}
	}
	if err != nil {
		return dst, ErrExecutionPinStateCorrupt
	}
	var result [executionpin.CompletionBytes]byte
	resultBytes, err := executionpin.AppendCompletion(result[:0], proof)
	if err != nil {
		return dst, ErrExecutionPinStateCorrupt
	}
	resultDigest := replication.CompletionResultDigest(
		slot.ResultCode, ResultFormatExecutionPin, resultBytes,
	)
	return replication.AppendCompletionBytes(dst, replication.CompletionBytes{
		ClusterID: m.binding.ClusterID, ClusterIncarnation: m.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: m.binding.TopologyRecoveryEpoch,
		Distribution:          m.distribution, Shard: m.shard,
		AllocationGeneration: m.binding.AllocationGeneration,
		ShardIncarnation:     m.binding.ShardIncarnation, GroupID: m.binding.GroupID,
		ReplicaSetVersion:      slot.ReplicaSetVersion,
		ActivePolicyGeneration: slot.ActivePolicyGeneration,
		ProtectionEpoch:        slot.ProtectionEpoch,
		RoutingVersion:         slot.RoutingVersion, RouteGeneration: slot.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID,
		ClientEpoch: command.ClientEpoch, ClientSequence: command.ClientSequence,
		Fingerprint: command.Fingerprint, RetryHome: command.RetryHome,
		AppliedSequence: slot.AppliedSequence,
		ResultCode:      slot.ResultCode, ResultFormat: ResultFormatExecutionPin,
		Storage: replication.CompletionInline, ResultLength: executionpin.CompletionBytes,
		ResultDigest: resultDigest, InlineResult: resultBytes,
	})
}
