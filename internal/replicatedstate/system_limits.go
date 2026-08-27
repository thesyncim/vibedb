package replicatedstate

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

// RequiredSystemCollectionLimits returns the sole exact hidden-collection
// profile for replicated apply. Shipped owners use this instead of copying
// codec geometry, so adding a bounded control record cannot silently leave a
// narrower durable collection that fails only after activation.
func RequiredSystemCollectionLimits(
	retryWindow uint16,
	requestLedger bool,
) (CollectionLimits, bool) {
	if retryWindow == 0 || retryWindow > MaxSessionRetryWindow {
		return CollectionLimits{}, false
	}
	maxKeyBytes := max(
		executionPinActiveStorageKeyBytes,
		routeGatePinKeyBytes,
		routeGateResultKeyBytes,
	)
	maxDocumentBytes := max(
		MaxStateEnvelopeBytes, MaxSessionRecordBytes, MaxSessionSlotRecordBytes,
		MaxAuthorityBindingBytes, routegate.HeadBytes, routegate.StoredPinBytes,
		routeGateResultBytes, executionpin.RecordBytes,
	)
	hotBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxAuthorityBindingBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes
	releaseBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + int(retryWindow)*(sha256.Size+3)
	executionPinBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes +
		executionPinRecordStorageKeyBytes + executionpin.RecordBytes +
		executionPinActiveStorageKeyBytes + executionPinActiveValueBytes
	routeGateBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes +
		len(routeGateHeadKey) + routegate.HeadBytes +
		routeGatePinKeyBytes + routegate.StoredPinBytes +
		routeGateResultKeyBytes + routeGateResultBytes
	maxDocuments := max(6, int(retryWindow)+2)
	maxBatchBytes := max(
		hotBatchBytes, releaseBatchBytes, executionPinBatchBytes, routeGateBatchBytes,
	)
	if requestLedger {
		maxKeyBytes = max(
			maxKeyBytes, requestledger.FixedStorageKeyBytes,
			requestledger.PageStorageKeyBytes, requestledger.PayloadStorageKeyBytes,
			requestledger.ReadyStorageKeyBytes, requestledger.PlanningExpiryKeyBytes,
			requestledger.PrincipalQuotaKeyBytes, requestledger.IssuerHighwaterKeyBytes,
			requestledger.IssuerSequenceKeyBytes,
		)
		maxDocumentBytes = max(maxDocumentBytes, requestledger.MaxCommandBytes)
		maxDocuments = max(maxDocuments, MaxDistinctMutations+1)
		maxBatchBytes = max(
			maxBatchBytes,
			requestledger.MaxCommandBytes+MaxStateEnvelopeBytes+
				MaxDistinctMutations*requestledger.PageStorageKeyBytes,
		)
	}
	// The durable collection admits one maximum value together with every
	// distinct key in a batch. Use the largest supported key, not just the
	// ledger page-key width: payload keys carry a longer identity.
	maxBatchBytes = max(maxBatchBytes, maxDocumentBytes+maxDocuments*maxKeyBytes)
	return CollectionLimits{
		MaxKeyBytes: maxKeyBytes, MaxDocumentBytes: maxDocumentBytes,
		MaxDistinctMutations: maxDocuments, MaxBatchBytes: maxBatchBytes,
	}, true
}
