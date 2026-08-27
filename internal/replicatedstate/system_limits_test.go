package replicatedstate

import "testing"

func TestRequiredSystemCollectionLimitsCoverDurableBatchGeometry(t *testing.T) {
	for _, ledger := range []bool{false, true} {
		for retry := uint16(1); retry <= MaxSessionRetryWindow; retry++ {
			limits, ok := RequiredSystemCollectionLimits(retry, ledger)
			if !ok {
				t.Fatalf("ledger=%t retry=%d: missing limits", ledger, retry)
			}
			minimum := limits.MaxDocumentBytes + limits.MaxDistinctMutations*limits.MaxKeyBytes
			if limits.MaxBatchBytes < minimum {
				t.Fatalf("ledger=%t retry=%d: batch=%d needs at least %d", ledger, retry, limits.MaxBatchBytes, minimum)
			}
			if limits.MaxBatchBytes > 17<<20 {
				t.Fatalf("ledger=%t retry=%d: system batch grew beyond bounded record geometry: %d", ledger, retry, limits.MaxBatchBytes)
			}
		}
	}
	for _, retry := range []uint16{0, MaxSessionRetryWindow + 1} {
		if _, ok := RequiredSystemCollectionLimits(retry, true); ok {
			t.Fatalf("invalid retry window %d accepted", retry)
		}
	}
}
