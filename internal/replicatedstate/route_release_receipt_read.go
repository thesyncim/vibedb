package replicatedstate

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

var ErrRouteReleaseReceiptRead = errors.New("replicatedstate: invalid route-release receipt read")

// MaxRouteReleaseReceiptReadCommandBytes is the existing bounded route-session
// command ceiling. Receipt recovery never accepts an arbitrary command-sized
// read surface.
const MaxRouteReleaseReceiptReadCommandBytes = requestledger.MaxRouteGatePinCommandBytes

// ValidateRouteReleaseReceiptCommand accepts only the retained route-session
// ReleaseShared command that can identify one committed receipt. It does not
// authorize cleanup or execute the command.
func ValidateRouteReleaseReceiptCommand(data []byte) (replication.CommandView, error) {
	if len(data) == 0 || len(data) > MaxRouteReleaseReceiptReadCommandBytes {
		return replication.CommandView{}, ErrRouteReleaseReceiptRead
	}
	command, err := replication.OpenCommand(data)
	if err != nil {
		return replication.CommandView{}, ErrRouteReleaseReceiptRead
	}
	if command.Kind() != replication.CommandRouteGate ||
		!replication.IsRouteSessionAuthority(command.AuthorityClass) ||
		command.ClientEpoch == 0 || command.ClientSequence != 3 || command.AckThrough != 2 {
		return replication.CommandView{}, ErrRouteReleaseReceiptRead
	}
	gate, err := command.OpenRouteGate()
	if err != nil || gate.Operation != routegate.OperationReleaseShared {
		return replication.CommandView{}, ErrRouteReleaseReceiptRead
	}
	return command, nil
}

// RouteReleaseReceiptReadInto resolves one exact committed ReleaseShared
// completion. Completion bytes are returned only on nil error; in particular,
// LookupCompletion's useful conflict witness is deliberately discarded here.
func (m *Machine) RouteReleaseReceiptReadInto(
	data []byte,
	dst []byte,
) (CompletionLookup, error) {
	command, err := ValidateRouteReleaseReceiptCommand(data)
	if err != nil {
		return CompletionLookup{}, err
	}
	if cap(dst) < MaxRouteGateCompletionEnvelopeBytes {
		return CompletionLookup{}, ErrCompletionBufferSmall
	}
	lookup, err := m.LookupCompletionInto(data, dst[:0:cap(dst)])
	if err != nil {
		return CompletionLookup{}, err
	}
	if len(lookup.Bytes) == 0 {
		return CompletionLookup{}, ErrRouteReleaseReceiptRead
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != ResultRouteGate ||
		completion.ResultFormat != ResultFormatRouteGate ||
		completion.ClientSequence != 3 ||
		completion.ClientEpoch != command.ClientEpoch ||
		completion.ClientID != command.ClientID ||
		completion.Fingerprint != command.Fingerprint ||
		completion.RetryHome != command.RetryHome ||
		!bytes.Equal(completion.Tenant, command.Tenant) {
		return CompletionLookup{}, ErrRouteReleaseReceiptRead
	}
	outcome, outcomeErr := routegate.OpenOutcome(completion.InlineResult)
	if outcomeErr != nil ||
		(outcome.Reason != routegate.ReasonReleased && outcome.Reason != routegate.ReasonAlreadyReleased) ||
		outcome.Status.ReleasedPins == 0 {
		return CompletionLookup{}, ErrRouteReleaseReceiptRead
	}
	return lookup, nil
}
