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
		len(sessionFenceKey(0, 0)),
	)
	maxDocumentBytes := max(
		MaxStateEnvelopeBytes, MaxSessionRecordBytes, MaxSessionSlotRecordBytes,
		MaxAuthorityBindingBytes, routegate.HeadBytes, routegate.StoredPinBytes,
		routeGateResultBytes, executionpin.RecordBytes, sessionFenceBytes,
	)
	// Overwriting an old retry slot updates its historical fence. Releasing a
	// session may update every historical fence rather than delete it when
	// another session still retains references, so reserve complete values.
	fenceRowBytes := len(sessionFenceKey(0, 0)) + sessionFenceBytes
	hotBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxAuthorityBindingBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes + fenceRowBytes
	releaseBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + int(retryWindow)*(sha256.Size+3+fenceRowBytes)
	executionPinBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes +
		executionPinRecordStorageKeyBytes + executionpin.RecordBytes +
		executionPinActiveStorageKeyBytes + executionPinActiveValueBytes + fenceRowBytes
	routeGateBatchBytes := len(stateKey) + MaxStateEnvelopeBytes +
		sha256.Size + 1 + MaxSessionRecordBytes +
		sha256.Size + 3 + MaxSessionSlotRecordBytes +
		len(routeGateHeadKey) + routegate.HeadBytes +
		routeGatePinKeyBytes + routegate.StoredPinBytes +
		routeGateResultKeyBytes + routeGateResultBytes + fenceRowBytes
	maxDocuments := max(7, 2*int(retryWindow)+2)
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
