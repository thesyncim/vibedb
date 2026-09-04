package replicatedstate

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
)

func (m *Machine) lookupRequestLedgerCompletionAtSnapshot(
	command replication.CommandView,
	dst []byte,
	workspace *CompletionLookupWorkspace,
) (CompletionLookup, error) {
	if workspace == nil || workspace.snapshot.live == nil {
		return CompletionLookup{}, ErrCompletionWorkspaceBusy
	}
	plan, err := m.planRequestLedgerCommand(
		command, m.state, workspace.snapshot,
	)
	if err != nil {
		if errors.Is(err, ErrAdmissionBound) {
			return CompletionLookup{}, ErrCompletionNotFound
		}
		return CompletionLookup{}, m.fail(err)
	}
	lastEntry := m.state.LastKind == RecordNormal &&
		m.state.LastEntryType == 0 &&
		normalEntryDigest(raftmodel.ApplyMeta{
			Index: m.state.Applied, Term: m.state.LastTerm, Type: m.state.LastEntryType,
		}, command.Bytes()) == m.state.LastEntryDigest
	if len(plan.rows) != 0 {
		// A command which would still mutate the coherent post-apply cut has no
		// retained completion. The caller must repropose its inner revision CAS.
		return CompletionLookup{}, ErrCompletionNotFound
	}
	if plan.completion.ResultCode == ResultRequestLedgerCapacity ||
		plan.completion.ResultCode == ResultRequestLedgerNotFound ||
		plan.completion.ResultCode == ResultRequestLedgerWrongRange {
		// Stateless refusals settle directly only while this exact command is
		// the authenticated last applied entry. Once another entry advances the
		// publication there is intentionally no per-request refusal tombstone;
		// retrying the inner CAS is safe because no data was dispatched.
		if !lastEntry {
			return CompletionLookup{}, ErrCompletionNotFound
		}
	}
	var result [RequestLedgerCompletionResultBytes]byte
	resultBytes, err := AppendRequestLedgerCompletionResult(result[:0], plan.completion)
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	completion, err := m.appendRequestLedgerCompletion(dst[:0], command, plan.completion, resultBytes)
	if err != nil {
		return CompletionLookup{}, m.fail(err)
	}
	return CompletionLookup{
		// CompletionLookup.Key remains the outer proposal identity used by the
		// generic raft settlement path. The independently authenticated inner
		// ledger key is carried in RequestLedgerCompletionResult.KeyDigest.
		Key: SessionKey(command.AuthorityClass, command.Tenant, command.ClientID), Bytes: completion,
		AppliedSequence: m.state.Applied,
	}, nil
}

func (m *Machine) appendRequestLedgerCompletion(
	dst []byte,
	command replication.CommandView,
	result RequestLedgerCompletionResult,
	resultBytes []byte,
) ([]byte, error) {
	if len(resultBytes) != RequestLedgerCompletionResultBytes {
		return dst, ErrCompletionCorrupt
	}
	resultDigest := replication.CompletionResultDigest(
		result.ResultCode, ResultFormatRequestLedger, resultBytes,
	)
	return replication.AppendCompletionBytes(dst, replication.CompletionBytes{
		ClusterID: m.binding.ClusterID, ClusterIncarnation: m.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: m.binding.TopologyRecoveryEpoch,
		Distribution:          m.distribution, Shard: m.shard,
		AllocationGeneration: m.binding.AllocationGeneration,
		ShardIncarnation:     m.binding.ShardIncarnation, GroupID: m.binding.GroupID,
		ReplicaSetVersion:      m.state.ReplicaSetVersion,
		ActivePolicyGeneration: m.state.Binding.ActivePolicyGeneration,
		ProtectionEpoch:        m.state.Binding.ProtectionEpoch,
		RoutingVersion:         m.state.Binding.RoutingVersion, RouteGeneration: m.state.Binding.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID,
		ClientEpoch: command.ClientEpoch, ClientSequence: command.ClientSequence,
		Fingerprint: command.Fingerprint, RetryHome: command.RetryHome,
		AppliedSequence: m.state.Applied,
		ResultCode:      result.ResultCode, ResultFormat: ResultFormatRequestLedger,
		Storage: replication.CompletionInline, ResultLength: uint64(len(resultBytes)),
		ResultDigest: resultDigest, InlineResult: resultBytes,
	})
}
