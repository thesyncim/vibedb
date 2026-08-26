package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestRequestLedgerCompletionResultRoundTripStrict(t *testing.T) {
	result := RequestLedgerCompletionResult{
		Operation: requestledger.OperationCreate, Phase: requestledger.PhaseSealed,
		ResultCode: ResultApplied, Revision: 7, ExactDuplicate: true,
		KeyDigest: requestledger.Digest{1}, RequestDigest: requestledger.Digest{2},
		PlanRoot: requestledger.Digest{3}, RangeIdentity: requestledger.Digest{4},
		StateDigest: requestledger.Digest{5},
	}
	encoded, err := AppendRequestLedgerCompletionResult(nil, result)
	if err != nil || len(encoded) != RequestLedgerCompletionResultBytes {
		t.Fatalf("encode = %d, %v", len(encoded), err)
	}
	opened, err := OpenRequestLedgerCompletionResult(ResultApplied, encoded)
	if err != nil || opened != result {
		t.Fatalf("open = %+v, %v", opened, err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		value, openErr := OpenRequestLedgerCompletionResult(ResultApplied, encoded)
		if openErr != nil || value != result {
			panic(openErr)
		}
	}); got != 0 {
		t.Fatalf("open allocations = %v, want 0", got)
	}
	for _, offset := range []int{2, 3, 8, 16, 48, 80, 112, 144} {
		candidate := bytes.Clone(encoded)
		if offset == 2 || offset == 3 {
			candidate[offset] |= 0x80
		} else {
			clear(candidate[offset : offset+min(8, len(candidate)-offset)])
		}
		if _, err := OpenRequestLedgerCompletionResult(ResultApplied, candidate); err == nil {
			t.Fatalf("accepted invalid field at %d", offset)
		}
	}
	if _, err := OpenRequestLedgerCompletionResult(ResultRequestLedgerConflict, encoded); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("mismatched result code error = %v", err)
	}
}

func TestRequestLedgerCompletionAcceptsEveryCanonicalOperation(t *testing.T) {
	for operation := requestledger.OperationCreate; operation <= requestledger.OperationExpirePlanning; operation++ {
		result := RequestLedgerCompletionResult{
			Operation: operation, Phase: requestledger.PhaseSealed,
			ResultCode: ResultApplied, Revision: 1,
			KeyDigest: requestledger.Digest{1}, RequestDigest: requestledger.Digest{2},
			PlanRoot: requestledger.Digest{3}, RangeIdentity: requestledger.Digest{4},
			StateDigest: requestledger.Digest{5},
		}
		encoded, err := AppendRequestLedgerCompletionResult(nil, result)
		if err != nil {
			t.Fatalf("operation %d encode: %v", operation, err)
		}
		opened, err := OpenRequestLedgerCompletionResult(ResultApplied, encoded)
		if err != nil || opened != result {
			t.Fatalf("operation %d open = %+v, %v", operation, opened, err)
		}
	}
}

func TestRequestLedgerCapacityCompletionIsExplicitlyStateless(t *testing.T) {
	result := RequestLedgerCompletionResult{
		Operation:  requestledger.OperationCreate,
		ResultCode: ResultRequestLedgerCapacity,
		KeyDigest:  requestledger.Digest{1}, RequestDigest: requestledger.Digest{2},
		PlanRoot: requestledger.Digest{3}, RangeIdentity: requestledger.Digest{4},
	}
	encoded, err := AppendRequestLedgerCompletionResult(nil, result)
	if err != nil {
		t.Fatal(err)
	}
	if opened, openErr := OpenRequestLedgerCompletionResult(ResultRequestLedgerCapacity, encoded); openErr != nil || opened != result {
		t.Fatalf("open stateless capacity = %+v, %v", opened, openErr)
	}
	for _, mutate := range []func(*RequestLedgerCompletionResult){
		func(value *RequestLedgerCompletionResult) { value.Phase = requestledger.PhasePlanning },
		func(value *RequestLedgerCompletionResult) { value.Revision = 1 },
		func(value *RequestLedgerCompletionResult) { value.StateDigest[0] = 1 },
		func(value *RequestLedgerCompletionResult) { value.ExactDuplicate = true },
		func(value *RequestLedgerCompletionResult) { value.Operation = requestledger.OperationSeal },
	} {
		candidate := result
		mutate(&candidate)
		if _, err := AppendRequestLedgerCompletionResult(nil, candidate); err == nil {
			t.Fatalf("accepted stateful capacity completion: %+v", candidate)
		}
	}
}

func TestRequestLedgerCompletionResultBoundsMaxEnvelope(t *testing.T) {
	want := replication.MaxEmptyResultCompletionEnvelopeBytes + RequestLedgerCompletionResultBytes
	if MaxCompletionEnvelopeBytes != want ||
		RequestLedgerCompletionResultBytes <= transactionCompletionResultBytes {
		t.Fatalf("completion geometry max=%d result=%d", MaxCompletionEnvelopeBytes, RequestLedgerCompletionResultBytes)
	}
}
