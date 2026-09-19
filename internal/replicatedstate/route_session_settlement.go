package replicatedstate

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
)

// routeSessionRetireCanSettleStale recognizes the one route-session cleanup
// cut that may follow a mutable catalog fence change. The release operation is
// already committed: its fixed seq3 session slot and authenticated route-gate
// outcome are the replicated proof. No caller flag or gateway-local state is
// consulted, so every replica and replay makes the same admission decision.
func (m *Machine) routeSessionRetireCanSettleStale(
	command replication.CommandView,
	state State,
	snapshot pointSnapshot,
	session SessionView,
	scratch *commandPlanScratch,
) (bool, error) {
	if m == nil || !replication.IsRouteSessionAuthority(command.AuthorityClass) ||
		command.Kind() != replication.CommandSessionRetire ||
		command.ClientSequence != 4 || command.AckThrough != 3 ||
		!m.immutableBindingMatches(command) ||
		!replication.CommandMembershipMatches(
			command.AuthorityClass, command.ReplicaSetVersion, state.ReplicaSetVersion,
		) ||
		command.ActivePolicyGeneration != state.Binding.ActivePolicyGeneration ||
		command.ProtectionEpoch != state.Binding.ProtectionEpoch ||
		command.OwnershipEpoch > state.Binding.OwnershipEpoch ||
		command.SchemaGeneration > state.Binding.SchemaGeneration ||
		command.RoutingVersion > state.Binding.RoutingVersion ||
		command.RouteGeneration > state.Binding.RouteGeneration {
		return false, nil
	}
	if session.Status != SessionActive || session.AuthorityClass != command.AuthorityClass ||
		session.ClientEpoch != command.ClientEpoch || session.HighSequence != 3 ||
		session.AckThrough != 2 || session.PhysicalSlotCount != 3 ||
		session.Digest != SessionKey(command.AuthorityClass, command.Tenant, command.ClientID) ||
		!bytes.Equal(session.Tenant, command.Tenant) || session.ClientID != command.ClientID {
		return false, nil
	}

	// The route-session protocol is Open(seq1), Acquire(seq2), Release(seq3),
	// Retire(seq4). Require the retained seq3 slot to carry the route-gate
	// completion grammar and the exact mutable fence used by this cleanup
	// command. A stale ordinary session slot can never satisfy this cut.
	slotKey, err := SessionSlotStorageKey(session.Digest, 2)
	if err != nil {
		return false, err
	}
	slot, found, err := sessionSlotAt(snapshot, slotKey, scratch)
	if err != nil || !found {
		return false, err
	}
	if err := validateStoredSessionSlot(
		state, slot, sessionFenceLookup{snapshot: snapshot},
	); err != nil {
		return false, err
	}
	if slot.SessionDigest != session.Digest || slot.AuthorityClass != command.AuthorityClass ||
		slot.ClientEpoch != command.ClientEpoch || slot.ClientSequence != 3 ||
		slot.ResultCode != ResultRouteGate ||
		slot.ReplicaSetVersion != command.ReplicaSetVersion ||
		slot.ActivePolicyGeneration != command.ActivePolicyGeneration ||
		slot.ProtectionEpoch != command.ProtectionEpoch ||
		slot.RoutingVersion != command.RoutingVersion ||
		slot.RouteGeneration != command.RouteGeneration {
		return false, nil
	}

	resultKey, err := routeGateResultStorageKey(slot.SessionDigest, slot.Slot)
	if err != nil {
		return false, err
	}
	var resultBuffer [routeGateResultBytes]byte
	raw, found, err := snapshot.appendRaw(resultBuffer[:0], resultKey[:])
	if err != nil || !found {
		return false, err
	}
	result, err := openRouteGateResult(raw)
	if err != nil {
		return false, err
	}
	if result.SessionDigest != slot.SessionDigest || result.Slot != slot.Slot ||
		result.ClientEpoch != command.ClientEpoch || result.ClientSequence != 3 {
		return false, errors.Join(err, ErrSessionCorrupt)
	}
	switch result.Outcome.Reason {
	case routegate.ReasonReleased, routegate.ReasonAlreadyReleased:
		if result.Outcome.Status.ReleasedPins == 0 {
			return false, nil
		}
		return true, nil
	default:
		return false, nil
	}
}
