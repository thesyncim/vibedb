package replicatedstate

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

// RequiredSystemCollectionLimits returns the minimum hidden-collection profile
// for session/control apply. Owners serving distributed transactions must also
// use RequiredTransactionSystemCollectionLimits: staged relation images and
// their intents do not fit the compact session profile.
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
		2*(sha256.Size+1) + int(retryWindow)*(sha256.Size+3+fenceRowBytes+routeGateResultKeyBytes)
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
	maxDocuments := max(7, 3*int(retryWindow)+3)
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

// MaxTransactionSystemDocuments bounds a single transaction staging command:
// one intent per mutation, one packed payload per relation, two controls, one
// coordinator payload, a bounded initial manifest pack, and the state row.
const MaxTransactionSystemDocuments = MaxDistinctMutations*replication.MaxRelationsPerBundle +
	replication.MaxRelationsPerBundle + distributedtxn.MaxManifestSegmentsPerCommand + 4

// RequiredTransactionSystemCollectionLimits derives the hidden collection from
// the owner's already-frozen transaction document budget. It does not raise
// admission quotas. The command carries at most MaxCommandBytes of payload;
// storing it adds relation/control/page framing and a second copy of each
// intent key. Reserving only the wire byte count misses this amplification.
func RequiredTransactionSystemCollectionLimits(
	retryWindow uint16,
	requestLedger bool,
	transactionDocuments int,
) (CollectionLimits, bool) {
	limits, ok := RequiredSystemCollectionLimits(retryWindow, requestLedger)
	if !ok || transactionDocuments < limits.MaxDistinctMutations {
		return CollectionLimits{}, false
	}
	limits.MaxDistinctMutations = min(transactionDocuments, MaxTransactionSystemDocuments)
	limits.MaxKeyBytes = max(limits.MaxKeyBytes, transactionIntentKeyBytes)
	// A packed relation omits the larger outer command header, so its retained
	// header and checksum still fit within the original command byte ceiling.
	limits.MaxDocumentBytes = max(limits.MaxDocumentBytes, replication.MaxCommandBytes)
	framing := len(stateKey) + MaxStateEnvelopeBytes +
		2*(transactionControlStorageKeyBytes+MaxTransactionControlRecordBytes) +
		transactionPayloadStorageKeyBytes + transactionPayloadHeaderBytes + recordChecksumLen +
		replication.MaxRelationsPerBundle*(transactionRelationPayloadKeyBytes+transactionRelationPayloadHeaderBytes+recordChecksumLen) +
		distributedtxn.MaxManifestSegmentsPerCommand*(transactionManifestKeyBytes+transactionManifestHeaderBytes+recordChecksumLen)
	intents := min(limits.MaxDistinctMutations, MaxDistinctMutations*replication.MaxRelationsPerBundle)
	limits.MaxBatchBytes = max(limits.MaxBatchBytes,
		replication.MaxCommandBytes+framing+intents*(transactionIntentKeyBytes+MaxTransactionIntentRecordBytes),
		limits.MaxDocumentBytes+limits.MaxDistinctMutations*limits.MaxKeyBytes)
	return limits, true
}
