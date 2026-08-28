package replicatedstate

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// validatedStateSize validates state with the same full validation as AppendState
// and returns the exact length of its binary envelope without encoding it.
// It returns zero on error. Validation may allocate; sizing adds no allocations
// on a valid state and does not allocate a ConfState encoding or envelope.
func validatedStateSize(state State) (int, error) {
	if err := validateState(state); err != nil {
		return 0, err
	}
	// ConfState contains only integer member lists and an optional boolean.
	// Full validation above rejects unknown fields and invalid membership, so
	// its protobuf size is the exact deterministic marshaled length.
	return stateEncodingSize(state, proto.Size(state.ConfState))
}

// stateEncodingHeader is shared by the persisted encoder and admission sizing;
// the longest populated extension determines the unique canonical geometry.
func stateEncodingHeader(state State) int {
	headerBytes := stateHeaderBytes
	if stateHasFence(state) {
		headerBytes = stateFenceHeaderBytes
	} else if stateHasRelationPlacement(state) {
		headerBytes = stateRelationPlacementHeaderBytes
	} else if stateHasExecutionPins(state) {
		headerBytes = stateExecutionPinHeaderBytes
	} else if stateHasRequestLedger(state) {
		headerBytes = stateRequestLedgerHeaderBytes
	} else if stateHasTransactions(state) {
		headerBytes = stateTransactionHeaderBytes
	}
	return headerBytes
}

func stateEncodingSize(state State, confBytes int) (int, error) {
	total := stateEncodingHeader(state) + recordChecksumLen
	for _, size := range [...]int{len(state.Binding.Distribution), len(state.Binding.Shard), confBytes} {
		if size < 0 || size > MaxStateEnvelopeBytes-total {
			return 0, fmt.Errorf("%w: state envelope bytes", ErrAdmissionBound)
		}
		total += size
	}
	return total, nil
}
