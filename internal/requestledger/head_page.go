package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
)

const (
	headHeaderBytes             = 880
	pageHeaderBytes             = 208
	PlanPageRecordOverheadBytes = pageHeaderBytes + checksumBytes
	MaxHeadRecordBytes          = headHeaderBytes + MaxInlinePlanBytes + checksumBytes
)

var (
	headMagic       = [4]byte{'V', 'R', 'L', 'H'}
	pageMagic       = [4]byte{'V', 'R', 'L', 'P'}
	pageChainDomain = []byte("vibedb/request-ledger/page-chain\x00")
)

// HeadRecord is the compact authoritative request state. For a paged plan,
// Appended* and PageChain are updated atomically with every accepted page batch.
// A sealed head therefore validates completeness in O(1), without rereading
// immutable pages.
type HeadRecord struct {
	Key                               RequestKey
	KeyDigest                         Digest
	RequestDigest                     Digest
	TerminalContractDigest            Digest
	PlanRoot                          Digest
	Revision                          uint64
	Phase                             Phase
	TotalPlanBytes                    uint64
	PlanPageCount                     uint64
	AppendedPlanBytes                 uint64
	AppendedPageCount                 uint64
	PageChain                         Digest
	NextStepOrdinal                   uint64
	ContinuationRevision              uint64
	ContinuationDigest                Digest
	CatalogGeneration                 uint64
	PinID                             PinID
	PinDigest                         Digest
	RouteSchemaCertificateDigest      Digest
	MaxPendingWaveBytes               uint64
	MaxContinuationBytes              uint64
	MaxTerminalBytes                  uint64
	MaxActivePayloadBytes             uint64
	MaxActivePayloadChunks            uint64
	PlanBuildID                       Digest
	PlanBuildGeneration               uint64
	PlanningLeaseExpiryIndex          uint64
	PlanningLeaseGeneration           uint64
	PlanCRC32C                        uint32
	PlanCRCBytes                      uint64
	PlanFramingValid                  bool
	TerminalTransitionTag             uint32
	FinalWaveCount                    uint64
	TerminalStateDigest               Digest
	TerminalSummaryDigest             Digest
	AbortTerminalTransitionTag        uint32
	AbortFinalWaveCount               uint64
	AbortTerminalStateDigest          Digest
	CleanupBuildDigest                Digest
	CleanupNextChunk                  uint64
	CleanupChunkCount                 uint64
	CleanupPayloadBytes               uint64
	CleanupTotalDataBytes             uint64
	OutstandingRoutePinDigest         Digest
	PreparedTerminalDigest            Digest
	SchemaPinReleaseCertificateDigest Digest
	ExpiredCleanupNextPage            uint64
	InlinePlan                        []byte
}

type ExecutionContract struct {
	CatalogGeneration            uint64
	PinID                        PinID
	PinDigest                    Digest
	RouteSchemaCertificateDigest Digest
	MaxPendingWaveBytes          uint64
	MaxContinuationBytes         uint64
	MaxTerminalBytes             uint64
	MaxActivePayloadBytes        uint64
	MaxActivePayloadChunks       uint64
	PlanBuildID                  Digest
	PlanBuildGeneration          uint64
	PlanningLeaseExpiryIndex     uint64
	PlanningLeaseGeneration      uint64
	TerminalTransitionTag        uint32
	FinalWaveCount               uint64
	TerminalStateDigest          Digest
	TerminalSummaryDigest        Digest
	AbortTerminalTransitionTag   uint32
	AbortFinalWaveCount          uint64
	AbortTerminalStateDigest     Digest
}

func defaultExecutionContract(root Digest) ExecutionContract {
	var pin PinID
	copy(pin[:], root[:16])
	return ExecutionContract{
		CatalogGeneration: 1, PinID: pin, PinDigest: root,
		RouteSchemaCertificateDigest: root,
		MaxPendingWaveBytes:          MaxPendingWaveRecordBytes,
		MaxContinuationBytes:         MaxContinuationRecordBytes,
		MaxTerminalBytes:             MaxLifecyclePayloadBytes,
		MaxActivePayloadBytes:        0,
		MaxActivePayloadChunks:       0,
		PlanBuildID:                  root, PlanBuildGeneration: 1, PlanningLeaseExpiryIndex: ^uint64(0),
		PlanningLeaseGeneration: 1,
		TerminalTransitionTag:   1, FinalWaveCount: 1,
		TerminalStateDigest: root, TerminalSummaryDigest: root,
		AbortTerminalTransitionTag: 2, AbortFinalWaveCount: 1,
		AbortTerminalStateDigest: root,
	}
}

// NewHead constructs the canonical initial planning head. InlinePlan aliases
// plan; AppendHead copies it into the destination.
func NewHead(key RequestKey, plan []byte) (HeadRecord, error) {
	keyDigest, err := KeyDigest(key)
	if err != nil {
		return HeadRecord{}, err
	}
	root, err := PlanRoot(keyDigest, plan)
	if err != nil {
		return HeadRecord{}, err
	}
	return NewHeadWithContract(key, root, root, plan)
}

// NewHeadWithRequestDigest binds retries to the original canonical client
// request independently of the derived outbound execution plan.
func NewHeadWithRequestDigest(key RequestKey, requestDigest Digest, plan []byte) (HeadRecord, error) {
	return NewHeadWithContract(key, requestDigest, requestDigest, plan)
}

// NewHeadWithContract binds terminal settlement to the exact catalog,
// participant, and result-shape contract selected before any outbound step.
func NewHeadWithContract(
	key RequestKey,
	requestDigest Digest,
	terminalContractDigest Digest,
	plan []byte,
) (HeadRecord, error) {
	keyDigest, err := KeyDigest(key)
	if err != nil {
		return HeadRecord{}, err
	}
	root, err := PlanRoot(keyDigest, plan)
	if err != nil {
		return HeadRecord{}, err
	}
	return NewHeadWithExecutionContract(
		key, requestDigest, terminalContractDigest, defaultExecutionContract(root), plan,
	)
}

func NewHeadWithExecutionContract(
	key RequestKey,
	requestDigest Digest,
	terminalContractDigest Digest,
	contract ExecutionContract,
	plan []byte,
) (HeadRecord, error) {
	_, err := OpenPlan(plan)
	if err != nil {
		return HeadRecord{}, err
	}
	keyDigest, err := KeyDigest(key)
	if err != nil {
		return HeadRecord{}, err
	}
	root, err := PlanRoot(keyDigest, plan)
	if err != nil {
		return HeadRecord{}, err
	}
	if !nonzeroDigest(requestDigest) || !nonzeroDigest(terminalContractDigest) {
		return HeadRecord{}, ErrCorrupt
	}
	head := HeadRecord{
		Key: key, KeyDigest: keyDigest, RequestDigest: requestDigest,
		TerminalContractDigest: terminalContractDigest,
		PlanRoot:               root, Revision: 1,
		Phase: PhasePlanning, TotalPlanBytes: uint64(len(plan)),
		CatalogGeneration: contract.CatalogGeneration, PinID: contract.PinID,
		PinDigest:                    contract.PinDigest,
		RouteSchemaCertificateDigest: contract.RouteSchemaCertificateDigest,
		MaxPendingWaveBytes:          contract.MaxPendingWaveBytes,
		MaxContinuationBytes:         contract.MaxContinuationBytes,
		MaxTerminalBytes:             contract.MaxTerminalBytes,
		MaxActivePayloadBytes:        contract.MaxActivePayloadBytes,
		MaxActivePayloadChunks:       contract.MaxActivePayloadChunks,
		PlanBuildID:                  contract.PlanBuildID,
		PlanBuildGeneration:          contract.PlanBuildGeneration,
		PlanningLeaseExpiryIndex:     contract.PlanningLeaseExpiryIndex,
		PlanningLeaseGeneration:      contract.PlanningLeaseGeneration,
		TerminalTransitionTag:        contract.TerminalTransitionTag,
		FinalWaveCount:               contract.FinalWaveCount,
		TerminalStateDigest:          contract.TerminalStateDigest,
		TerminalSummaryDigest:        contract.TerminalSummaryDigest,
		AbortTerminalTransitionTag:   contract.AbortTerminalTransitionTag,
		AbortFinalWaveCount:          contract.AbortFinalWaveCount,
		AbortTerminalStateDigest:     contract.AbortTerminalStateDigest,
	}
	if len(plan) <= MaxInlinePlanBytes {
		head.Phase = PhaseSealed
		head.InlinePlan = plan[:len(plan):len(plan)]
		head.AppendedPlanBytes = uint64(len(plan))
		head.PlanCRC32C = binary.LittleEndian.Uint32(plan[len(plan)-checksumBytes:])
		head.PlanCRCBytes = uint64(len(plan) - checksumBytes)
		head.PlanFramingValid = true
	} else {
		head.PlanPageCount = uint64((len(plan) + MaxPlanPageBytes - 1) / MaxPlanPageBytes)
	}
	return head, nil
}

// NewPagedHead constructs Create state after a first streaming pass measured
// the exact canonical plan and computed its expected final page-chain root.
// No aggregate plan allocation is required.
func NewPagedHead(
	key RequestKey,
	requestDigest Digest,
	terminalContractDigest Digest,
	totalPlanBytes uint64,
	planRoot Digest,
) (HeadRecord, error) {
	return NewPagedHeadWithExecutionContract(
		key, requestDigest, terminalContractDigest, totalPlanBytes, planRoot,
		defaultExecutionContract(planRoot),
	)
}

func NewPagedHeadWithExecutionContract(
	key RequestKey,
	requestDigest Digest,
	terminalContractDigest Digest,
	totalPlanBytes uint64,
	planRoot Digest,
	contract ExecutionContract,
) (HeadRecord, error) {
	keyDigest, err := KeyDigest(key)
	if err != nil || !nonzeroDigest(requestDigest) ||
		!nonzeroDigest(terminalContractDigest) || !nonzeroDigest(planRoot) ||
		totalPlanBytes <= MaxInlinePlanBytes || totalPlanBytes > MaxPlanBytes {
		return HeadRecord{}, ErrCorrupt
	}
	return HeadRecord{
		Key: key, KeyDigest: keyDigest, RequestDigest: requestDigest,
		TerminalContractDigest: terminalContractDigest, PlanRoot: planRoot,
		Revision: 1, Phase: PhasePlanning, TotalPlanBytes: totalPlanBytes,
		PlanPageCount:     (totalPlanBytes + MaxPlanPageBytes - 1) / MaxPlanPageBytes,
		CatalogGeneration: contract.CatalogGeneration, PinID: contract.PinID,
		PinDigest:                    contract.PinDigest,
		RouteSchemaCertificateDigest: contract.RouteSchemaCertificateDigest,
		MaxPendingWaveBytes:          contract.MaxPendingWaveBytes,
		MaxContinuationBytes:         contract.MaxContinuationBytes,
		MaxTerminalBytes:             contract.MaxTerminalBytes,
		MaxActivePayloadBytes:        contract.MaxActivePayloadBytes,
		MaxActivePayloadChunks:       contract.MaxActivePayloadChunks,
		PlanBuildID:                  contract.PlanBuildID,
		PlanBuildGeneration:          contract.PlanBuildGeneration,
		PlanningLeaseExpiryIndex:     contract.PlanningLeaseExpiryIndex,
		PlanningLeaseGeneration:      contract.PlanningLeaseGeneration,
		TerminalTransitionTag:        contract.TerminalTransitionTag,
		FinalWaveCount:               contract.FinalWaveCount,
		TerminalStateDigest:          contract.TerminalStateDigest,
		TerminalSummaryDigest:        contract.TerminalSummaryDigest,
		AbortTerminalTransitionTag:   contract.AbortTerminalTransitionTag,
		AbortFinalWaveCount:          contract.AbortFinalWaveCount,
		AbortTerminalStateDigest:     contract.AbortTerminalStateDigest,
	}, nil
}

func AppendHead(dst []byte, head HeadRecord) ([]byte, error) {
	if err := validateHead(head); err != nil {
		return dst, err
	}
	if len(head.InlinePlan) > MaxCommandBytes-headHeaderBytes-checksumBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, headHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], headMagic[:])
	out[8] = byte(head.Phase)
	binary.LittleEndian.PutUint64(out[16:24], head.Revision)
	binary.LittleEndian.PutUint64(out[24:32], head.NextStepOrdinal)
	binary.LittleEndian.PutUint64(out[32:40], head.ContinuationRevision)
	binary.LittleEndian.PutUint64(out[40:48], head.TotalPlanBytes)
	binary.LittleEndian.PutUint64(out[48:56], head.PlanPageCount)
	binary.LittleEndian.PutUint64(out[56:64], head.AppendedPlanBytes)
	binary.LittleEndian.PutUint64(out[64:72], head.AppendedPageCount)
	binary.LittleEndian.PutUint64(out[72:80], head.Key.IssuerEpoch)
	binary.LittleEndian.PutUint64(out[80:88], head.Key.IssuerSequence)
	binary.LittleEndian.PutUint64(out[88:96], head.CatalogGeneration)
	binary.LittleEndian.PutUint64(out[96:104], head.MaxPendingWaveBytes)
	binary.LittleEndian.PutUint64(out[104:112], head.MaxContinuationBytes)
	binary.LittleEndian.PutUint64(out[112:120], head.MaxTerminalBytes)
	out[120] = byte(head.Key.Scope)
	copy(out[128:144], head.Key.Principal[:])
	copy(out[144:160], head.Key.Request[:])
	putDigest(out[160:192], head.Key.TenantDigest)
	putDigest(out[192:224], head.KeyDigest)
	putDigest(out[224:256], head.RequestDigest)
	putDigest(out[256:288], head.TerminalContractDigest)
	putDigest(out[288:320], head.PlanRoot)
	putDigest(out[320:352], head.PageChain)
	putDigest(out[352:384], head.ContinuationDigest)
	copy(out[384:400], head.PinID[:])
	copy(out[400:408], head.Key.IssuerLane[:])
	putDigest(out[416:448], head.PinDigest)
	putDigest(out[448:480], head.RouteSchemaCertificateDigest)
	binary.LittleEndian.PutUint64(out[480:488], uint64(len(head.InlinePlan)))
	binary.LittleEndian.PutUint32(out[488:492], head.PlanCRC32C)
	binary.LittleEndian.PutUint64(out[496:504], head.PlanCRCBytes)
	if head.PlanFramingValid {
		out[504] = 1
	}
	binary.LittleEndian.PutUint32(out[512:516], head.TerminalTransitionTag)
	binary.LittleEndian.PutUint64(out[520:528], head.FinalWaveCount)
	putDigest(out[528:560], head.TerminalStateDigest)
	putDigest(out[560:592], head.TerminalSummaryDigest)
	binary.LittleEndian.PutUint64(out[592:600], head.MaxActivePayloadBytes)
	binary.LittleEndian.PutUint64(out[600:608], head.MaxActivePayloadChunks)
	putDigest(out[608:640], head.PlanBuildID)
	binary.LittleEndian.PutUint64(out[640:648], head.PlanningLeaseExpiryIndex)
	binary.LittleEndian.PutUint64(out[648:656], head.PlanningLeaseGeneration)
	binary.LittleEndian.PutUint32(out[656:660], head.AbortTerminalTransitionTag)
	binary.LittleEndian.PutUint64(out[664:672], head.AbortFinalWaveCount)
	putDigest(out[672:704], head.AbortTerminalStateDigest)
	putDigest(out[704:736], head.CleanupBuildDigest)
	binary.LittleEndian.PutUint64(out[736:744], head.CleanupNextChunk)
	binary.LittleEndian.PutUint64(out[744:752], head.CleanupChunkCount)
	binary.LittleEndian.PutUint64(out[752:760], head.CleanupPayloadBytes)
	binary.LittleEndian.PutUint64(out[760:768], head.CleanupTotalDataBytes)
	putDigest(out[768:800], head.OutstandingRoutePinDigest)
	putDigest(out[800:832], head.PreparedTerminalDigest)
	putDigest(out[832:864], head.SchemaPinReleaseCertificateDigest)
	binary.LittleEndian.PutUint64(out[864:872], head.PlanBuildGeneration)
	binary.LittleEndian.PutUint64(out[872:880], head.ExpiredCleanupNextPage)
	dst = append(dst, head.InlinePlan...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenHead(raw []byte) (HeadRecord, error) {
	if len(raw) < headHeaderBytes+checksumBytes || len(raw) > MaxCommandBytes ||
		!magicOK(raw, headMagic) || !zeroBytes(raw[4:8]) ||
		!zeroBytes(raw[9:16]) || !zeroBytes(raw[121:128]) ||
		!zeroBytes(raw[408:416]) || !zeroBytes(raw[492:496]) || raw[504] > 1 ||
		!zeroBytes(raw[505:512]) || !zeroBytes(raw[516:520]) ||
		!zeroBytes(raw[660:664]) || !checksumOK(raw) {
		return HeadRecord{}, ErrCorrupt
	}
	inlineBytes := binary.LittleEndian.Uint64(raw[480:488])
	want, ok := exactLength(headHeaderBytes+checksumBytes, inlineBytes)
	if !ok || want != len(raw) || inlineBytes > MaxInlinePlanBytes {
		return HeadRecord{}, ErrCorrupt
	}
	head := HeadRecord{
		Revision:             binary.LittleEndian.Uint64(raw[16:24]),
		NextStepOrdinal:      binary.LittleEndian.Uint64(raw[24:32]),
		ContinuationRevision: binary.LittleEndian.Uint64(raw[32:40]),
		TotalPlanBytes:       binary.LittleEndian.Uint64(raw[40:48]),
		PlanPageCount:        binary.LittleEndian.Uint64(raw[48:56]),
		AppendedPlanBytes:    binary.LittleEndian.Uint64(raw[56:64]),
		AppendedPageCount:    binary.LittleEndian.Uint64(raw[64:72]),
		Phase:                Phase(raw[8]),
		Key: RequestKey{
			Scope: ScopeKind(raw[120]), TenantDigest: readDigest(raw[160:192]),
			IssuerEpoch:    binary.LittleEndian.Uint64(raw[72:80]),
			IssuerSequence: binary.LittleEndian.Uint64(raw[80:88]),
		},
		CatalogGeneration:    binary.LittleEndian.Uint64(raw[88:96]),
		MaxPendingWaveBytes:  binary.LittleEndian.Uint64(raw[96:104]),
		MaxContinuationBytes: binary.LittleEndian.Uint64(raw[104:112]),
		MaxTerminalBytes:     binary.LittleEndian.Uint64(raw[112:120]),
		KeyDigest:            readDigest(raw[192:224]), RequestDigest: readDigest(raw[224:256]),
		TerminalContractDigest: readDigest(raw[256:288]), PlanRoot: readDigest(raw[288:320]),
		PageChain: readDigest(raw[320:352]), ContinuationDigest: readDigest(raw[352:384]),
		PinDigest:                         readDigest(raw[416:448]),
		RouteSchemaCertificateDigest:      readDigest(raw[448:480]),
		PlanCRC32C:                        binary.LittleEndian.Uint32(raw[488:492]),
		PlanCRCBytes:                      binary.LittleEndian.Uint64(raw[496:504]),
		PlanFramingValid:                  raw[504] == 1,
		TerminalTransitionTag:             binary.LittleEndian.Uint32(raw[512:516]),
		FinalWaveCount:                    binary.LittleEndian.Uint64(raw[520:528]),
		TerminalStateDigest:               readDigest(raw[528:560]),
		TerminalSummaryDigest:             readDigest(raw[560:592]),
		MaxActivePayloadBytes:             binary.LittleEndian.Uint64(raw[592:600]),
		MaxActivePayloadChunks:            binary.LittleEndian.Uint64(raw[600:608]),
		PlanBuildID:                       readDigest(raw[608:640]),
		PlanningLeaseExpiryIndex:          binary.LittleEndian.Uint64(raw[640:648]),
		PlanningLeaseGeneration:           binary.LittleEndian.Uint64(raw[648:656]),
		AbortTerminalTransitionTag:        binary.LittleEndian.Uint32(raw[656:660]),
		AbortFinalWaveCount:               binary.LittleEndian.Uint64(raw[664:672]),
		AbortTerminalStateDigest:          readDigest(raw[672:704]),
		CleanupBuildDigest:                readDigest(raw[704:736]),
		CleanupNextChunk:                  binary.LittleEndian.Uint64(raw[736:744]),
		CleanupChunkCount:                 binary.LittleEndian.Uint64(raw[744:752]),
		CleanupPayloadBytes:               binary.LittleEndian.Uint64(raw[752:760]),
		CleanupTotalDataBytes:             binary.LittleEndian.Uint64(raw[760:768]),
		OutstandingRoutePinDigest:         readDigest(raw[768:800]),
		PreparedTerminalDigest:            readDigest(raw[800:832]),
		SchemaPinReleaseCertificateDigest: readDigest(raw[832:864]),
		PlanBuildGeneration:               binary.LittleEndian.Uint64(raw[864:872]),
		ExpiredCleanupNextPage:            binary.LittleEndian.Uint64(raw[872:880]),
	}
	copy(head.Key.Principal[:], raw[128:144])
	copy(head.Key.Request[:], raw[144:160])
	copy(head.PinID[:], raw[384:400])
	copy(head.Key.IssuerLane[:], raw[400:408])
	head.InlinePlan = raw[headHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes]
	if err := validateHead(head); err != nil {
		return HeadRecord{}, ErrCorrupt
	}
	return head, nil
}

func validateHead(head HeadRecord) error {
	derived, err := KeyDigest(head.Key)
	if err != nil || derived != head.KeyDigest || !nonzeroDigest(head.RequestDigest) ||
		!nonzeroDigest(head.TerminalContractDigest) ||
		!nonzeroDigest(head.PlanRoot) ||
		head.CatalogGeneration == 0 || head.PinID == (PinID{}) ||
		!nonzeroDigest(head.PinDigest) || !nonzeroDigest(head.RouteSchemaCertificateDigest) ||
		head.MaxPendingWaveBytes < pendingWaveHeaderBytes+stepRefBytes+checksumBytes ||
		head.MaxPendingWaveBytes > MaxPendingWaveRecordBytes ||
		head.MaxContinuationBytes < continuationHeaderBytes+1+checksumBytes ||
		head.MaxContinuationBytes > MaxContinuationRecordBytes ||
		head.MaxTerminalBytes < terminalHeaderBytes+checksumBytes ||
		head.MaxTerminalBytes > MaxLifecyclePayloadBytes ||
		(head.MaxActivePayloadBytes == 0) != (head.MaxActivePayloadChunks == 0) ||
		head.MaxActivePayloadBytes > MaxDynamicWavePayloadBytes ||
		head.MaxActivePayloadChunks > MaxDynamicWavePayloadChunks ||
		!nonzeroDigest(head.PlanBuildID) || head.PlanBuildGeneration == 0 ||
		head.PlanningLeaseExpiryIndex == 0 ||
		head.PlanningLeaseGeneration == 0 ||
		head.AbortTerminalTransitionTag == 0 || head.AbortFinalWaveCount == 0 ||
		!nonzeroDigest(head.AbortTerminalStateDigest) ||
		head.TerminalTransitionTag == 0 || head.FinalWaveCount == 0 ||
		!nonzeroDigest(head.TerminalStateDigest) || !nonzeroDigest(head.TerminalSummaryDigest) ||
		head.Revision == 0 ||
		head.TotalPlanBytes == 0 || head.TotalPlanBytes > MaxPlanBytes ||
		!head.Phase.Valid() {
		return ErrCorrupt
	}
	if nonzeroDigest(head.CleanupBuildDigest) {
		if head.CleanupChunkCount == 0 || head.CleanupNextChunk > head.CleanupChunkCount ||
			head.CleanupPayloadBytes == 0 || head.CleanupTotalDataBytes == 0 ||
			head.CleanupTotalDataBytes > head.MaxActivePayloadBytes ||
			head.CleanupChunkCount != (head.CleanupTotalDataBytes+MaxPlanPageBytes-1)/MaxPlanPageBytes ||
			head.Phase != PhaseSealed {
			return ErrCorrupt
		}
		overhead, multiplyErr := checkedMul(head.CleanupChunkCount,
			uint64(PayloadStorageKeyBytes+payloadChunkHeaderBytes+checksumBytes))
		initial, sumErr := checkedSum(head.CleanupTotalDataBytes, overhead,
			uint64(FixedStorageKeyBytes+payloadBuildBytes))
		if multiplyErr != nil || sumErr != nil || head.CleanupPayloadBytes > initial {
			return ErrCorrupt
		}
	} else if head.CleanupNextChunk != 0 || head.CleanupChunkCount != 0 ||
		head.CleanupPayloadBytes != 0 || head.CleanupTotalDataBytes != 0 {
		return ErrCorrupt
	}
	if (head.Phase == PhasePrepared || head.Phase == PhaseTerminal) &&
		nonzeroDigest(head.OutstandingRoutePinDigest) {
		return ErrCorrupt
	}
	inline := uint64(len(head.InlinePlan))
	if head.TotalPlanBytes <= MaxInlinePlanBytes {
		if inline != head.TotalPlanBytes || head.PlanPageCount != 0 ||
			head.AppendedPageCount != 0 || head.AppendedPlanBytes != inline ||
			nonzeroDigest(head.PageChain) || !head.PlanFramingValid ||
			head.PlanCRCBytes != head.TotalPlanBytes-checksumBytes {
			return ErrCorrupt
		}
		_, openErr := OpenPlan(head.InlinePlan)
		root, rootErr := PlanRoot(head.KeyDigest, head.InlinePlan)
		if openErr != nil || rootErr != nil || root != head.PlanRoot {
			return ErrCorrupt
		}
	} else {
		pageCount := (head.TotalPlanBytes + MaxPlanPageBytes - 1) / MaxPlanPageBytes
		if inline != 0 || head.PlanPageCount != pageCount ||
			head.AppendedPageCount > head.PlanPageCount ||
			head.AppendedPlanBytes > head.TotalPlanBytes ||
			(head.AppendedPageCount == 0) != !nonzeroDigest(head.PageChain) ||
			(head.AppendedPageCount == head.PlanPageCount) !=
				(head.AppendedPlanBytes == head.TotalPlanBytes) {
			return ErrCorrupt
		}
	}
	complete := head.AppendedPlanBytes == head.TotalPlanBytes &&
		head.AppendedPageCount == head.PlanPageCount
	switch head.Phase {
	case PhasePlanning:
		if head.NextStepOrdinal != 0 || head.ContinuationRevision != 0 ||
			nonzeroDigest(head.ContinuationDigest) || nonzeroDigest(head.PreparedTerminalDigest) ||
			nonzeroDigest(head.SchemaPinReleaseCertificateDigest) || head.ExpiredCleanupNextPage != 0 {
			return ErrCorrupt
		}
	case PhaseExpired:
		if head.TotalPlanBytes <= MaxInlinePlanBytes || head.NextStepOrdinal != 0 ||
			head.ContinuationRevision != 0 || nonzeroDigest(head.ContinuationDigest) ||
			nonzeroDigest(head.PreparedTerminalDigest) ||
			nonzeroDigest(head.SchemaPinReleaseCertificateDigest) ||
			head.ExpiredCleanupNextPage > head.AppendedPageCount {
			return ErrCorrupt
		}
	case PhaseSealed:
		if !complete || head.NextStepOrdinal == 0 &&
			(head.ContinuationRevision != 0 || nonzeroDigest(head.ContinuationDigest)) ||
			head.NextStepOrdinal != 0 &&
				(head.ContinuationRevision == 0 || !nonzeroDigest(head.ContinuationDigest)) ||
			nonzeroDigest(head.PreparedTerminalDigest) ||
			nonzeroDigest(head.SchemaPinReleaseCertificateDigest) || head.ExpiredCleanupNextPage != 0 {
			return ErrIncomplete
		}
	case PhasePrepared:
		if !complete || head.NextStepOrdinal == 0 || head.ContinuationRevision == 0 ||
			!nonzeroDigest(head.ContinuationDigest) || !nonzeroDigest(head.PreparedTerminalDigest) ||
			head.ExpiredCleanupNextPage != 0 {
			return ErrIncomplete
		}
	case PhaseTerminal:
		if !complete || head.NextStepOrdinal == 0 &&
			(head.ContinuationRevision != 0 || nonzeroDigest(head.ContinuationDigest)) ||
			head.NextStepOrdinal != 0 &&
				(head.ContinuationRevision == 0 || !nonzeroDigest(head.ContinuationDigest)) ||
			!nonzeroDigest(head.PreparedTerminalDigest) ||
			!nonzeroDigest(head.SchemaPinReleaseCertificateDigest) || head.ExpiredCleanupNextPage != 0 {
			return ErrIncomplete
		}
	default:
		return ErrCorrupt
	}
	return nil
}

// SealHead returns the only valid planning-to-sealed transition.
func SealHead(head HeadRecord, revision uint64) (HeadRecord, error) {
	if err := validateHead(head); err != nil || head.Phase != PhasePlanning ||
		head.AppendedPlanBytes != head.TotalPlanBytes ||
		head.AppendedPageCount != head.PlanPageCount ||
		head.PlanPageCount != 0 && head.PageChain != head.PlanRoot ||
		!head.PlanFramingValid ||
		!nextRevision(head.Revision, revision) {
		return HeadRecord{}, ErrIncomplete
	}
	head.Phase = PhaseSealed
	head.Revision = revision
	return head, nil
}

// PlanPageRecord is one immutable slice of a paged plan. Chain authenticates
// PreviousChain, all fixed placement fields, and Data.
type PlanPageRecord struct {
	KeyDigest     Digest
	PlanRoot      Digest
	PlanBuildID   Digest
	Ordinal       uint64
	Count         uint64
	Offset        uint64
	TotalBytes    uint64
	PreviousChain Digest
	Chain         Digest
	Data          []byte
}

func NewPlanPage(head HeadRecord, plan []byte, ordinal uint64, previous Digest) (PlanPageRecord, error) {
	if err := validateHead(head); err != nil || len(head.InlinePlan) != 0 ||
		uint64(len(plan)) != head.TotalPlanBytes || ordinal >= head.PlanPageCount {
		return PlanPageRecord{}, ErrCorrupt
	}
	offset := ordinal * MaxPlanPageBytes
	end := min(offset+MaxPlanPageBytes, uint64(len(plan)))
	return NewPlanPageData(head, ordinal, previous, plan[offset:end:end])
}

// NewPlanPageData is the allocation-free second-pass page emitter.
func NewPlanPageData(
	head HeadRecord,
	ordinal uint64,
	previous Digest,
	data []byte,
) (PlanPageRecord, error) {
	if err := validateHead(head); err != nil || len(head.InlinePlan) != 0 ||
		head.Phase != PhasePlanning || ordinal >= head.PlanPageCount {
		return PlanPageRecord{}, ErrCorrupt
	}
	offset := ordinal * MaxPlanPageBytes
	page := PlanPageRecord{
		KeyDigest: head.KeyDigest, PlanRoot: head.PlanRoot,
		PlanBuildID: head.PlanBuildID,
		Ordinal:     ordinal, Count: head.PlanPageCount, Offset: offset,
		TotalBytes: head.TotalPlanBytes, PreviousChain: previous,
		Data: data[:len(data):len(data)],
	}
	page.Chain = planPageChain(page)
	if err := validatePlanPage(page); err != nil {
		return PlanPageRecord{}, err
	}
	return page, nil
}

func AppendPlanPage(dst []byte, page PlanPageRecord) ([]byte, error) {
	if err := validatePlanPage(page); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, pageHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], pageMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], page.Ordinal)
	binary.LittleEndian.PutUint64(out[16:24], page.Count)
	binary.LittleEndian.PutUint64(out[24:32], page.Offset)
	binary.LittleEndian.PutUint64(out[32:40], page.TotalBytes)
	binary.LittleEndian.PutUint64(out[40:48], uint64(len(page.Data)))
	putDigest(out[48:80], page.KeyDigest)
	putDigest(out[80:112], page.PlanRoot)
	putDigest(out[112:144], page.PreviousChain)
	putDigest(out[144:176], page.Chain)
	putDigest(out[176:208], page.PlanBuildID)
	dst = append(dst, page.Data...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPlanPage(raw []byte) (PlanPageRecord, error) {
	if len(raw) < pageHeaderBytes+1+checksumBytes ||
		len(raw) > pageHeaderBytes+MaxPlanPageBytes+checksumBytes ||
		!magicOK(raw, pageMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return PlanPageRecord{}, ErrCorrupt
	}
	dataBytes := binary.LittleEndian.Uint64(raw[40:48])
	want, ok := exactLength(pageHeaderBytes+checksumBytes, dataBytes)
	if !ok || want != len(raw) || dataBytes > MaxPlanPageBytes {
		return PlanPageRecord{}, ErrCorrupt
	}
	page := PlanPageRecord{
		Ordinal:       binary.LittleEndian.Uint64(raw[8:16]),
		Count:         binary.LittleEndian.Uint64(raw[16:24]),
		Offset:        binary.LittleEndian.Uint64(raw[24:32]),
		TotalBytes:    binary.LittleEndian.Uint64(raw[32:40]),
		KeyDigest:     readDigest(raw[48:80]),
		PlanRoot:      readDigest(raw[80:112]),
		PreviousChain: readDigest(raw[112:144]),
		Chain:         readDigest(raw[144:176]),
		PlanBuildID:   readDigest(raw[176:208]),
		Data:          raw[pageHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}
	if err := validatePlanPage(page); err != nil {
		return PlanPageRecord{}, ErrCorrupt
	}
	return page, nil
}

func validatePlanPage(page PlanPageRecord) error {
	if !nonzeroDigest(page.KeyDigest) || !nonzeroDigest(page.PlanRoot) || !nonzeroDigest(page.PlanBuildID) ||
		page.Count == 0 || page.Ordinal >= page.Count || page.TotalBytes <= MaxInlinePlanBytes ||
		page.TotalBytes > MaxPlanBytes || len(page.Data) == 0 || len(page.Data) > MaxPlanPageBytes ||
		page.Offset != page.Ordinal*MaxPlanPageBytes ||
		page.Offset >= page.TotalBytes || uint64(len(page.Data)) > page.TotalBytes-page.Offset ||
		(page.Ordinal+1 == page.Count) != (page.Offset+uint64(len(page.Data)) == page.TotalBytes) ||
		(page.Ordinal == 0) != !nonzeroDigest(page.PreviousChain) ||
		page.Chain != planPageChain(page) {
		return ErrCorrupt
	}
	return nil
}

func planPageChain(page PlanPageRecord) Digest {
	return PlanPageChain(page.KeyDigest, page.Ordinal, page.Count, page.Offset,
		page.TotalBytes, page.PreviousChain, page.Data)
}

// PlanPageChain is the sole expected-root and page-chain algorithm. It excludes
// declared PlanRoot to avoid self-reference; the record binds that root and
// Seal compares the final accumulated chain with it.
func PlanPageChain(
	key Digest,
	ordinal, count, offset, total uint64,
	previous Digest,
	data []byte,
) Digest {
	// Hash the potentially large payload once, then authenticate that digest in
	// a fixed stack frame. This preserves content addressing while keeping the
	// decode/validation path allocation-free.
	dataDigest := sha256.Sum256(data)
	const domain = "vibedb/request-ledger/page-chain\x00"
	var framed [len(domain) + 32 + 40 + 32 + 32]byte
	at := copy(framed[:], pageChainDomain)
	at += copy(framed[at:], key[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], ordinal)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], count)
	binary.LittleEndian.PutUint64(framed[at+16:at+24], offset)
	binary.LittleEndian.PutUint64(framed[at+24:at+32], total)
	binary.LittleEndian.PutUint64(framed[at+32:at+40], uint64(len(data)))
	at += 40
	at += copy(framed[at:], previous[:])
	copy(framed[at:], dataDigest[:])
	return Digest(sha256.Sum256(framed[:]))
}

// AdvanceHeadPages validates one exact continuation and updates the compact
// seal witness without rereading any prior page.
func AdvanceHeadPage(head HeadRecord, page PlanPageRecord, revision uint64) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validatePlanPage(page)) != nil ||
		head.Phase != PhasePlanning || !nextRevision(head.Revision, revision) ||
		page.KeyDigest != head.KeyDigest || page.PlanRoot != head.PlanRoot ||
		page.PlanBuildID != head.PlanBuildID ||
		page.Count != head.PlanPageCount || page.TotalBytes != head.TotalPlanBytes ||
		page.Ordinal != head.AppendedPageCount || page.Offset != head.AppendedPlanBytes ||
		page.PreviousChain != head.PageChain {
		return HeadRecord{}, ErrInvalidState
	}
	if page.Ordinal == 0 && (len(page.Data) < planHeaderBytes || !magicOK(page.Data, planMagic) ||
		!zeroBytes(page.Data[4:8]) ||
		binary.LittleEndian.Uint64(page.Data[8:16]) != head.TotalPlanBytes ||
		binary.LittleEndian.Uint64(page.Data[16:24]) != head.TotalPlanBytes-planHeaderBytes-checksumBytes) {
		return HeadRecord{}, ErrCorrupt
	}
	crcData := page.Data
	if page.Ordinal+1 == page.Count {
		if len(crcData) < checksumBytes {
			return HeadRecord{}, ErrCorrupt
		}
		crcData = crcData[:len(crcData)-checksumBytes]
	}
	head.PlanCRC32C = crc32.Update(head.PlanCRC32C, castagnoli, crcData)
	head.PlanCRCBytes += uint64(len(crcData))
	if page.Ordinal+1 == page.Count {
		stored := binary.LittleEndian.Uint32(page.Data[len(page.Data)-checksumBytes:])
		if head.PlanCRCBytes != head.TotalPlanBytes-checksumBytes || head.PlanCRC32C != stored {
			return HeadRecord{}, ErrCorrupt
		}
		head.PlanFramingValid = true
	}
	head.Revision = revision
	head.AppendedPageCount++
	head.AppendedPlanBytes += uint64(len(page.Data))
	head.PageChain = page.Chain
	return head, nil
}

// AdvanceHeadPageBatch applies one packed proposal with exactly one durable
// revision change. seal atomically publishes a complete final batch.
func AdvanceHeadPageBatch(
	head HeadRecord,
	batch PlanPageBatchView,
	revision uint64,
	seal bool,
) (HeadRecord, error) {
	if !nextRevision(head.Revision, revision) || batch.Count() == 0 {
		return HeadRecord{}, ErrRevision
	}
	baseRevision := head.Revision
	iter := batch.Iter()
	for applied := uint64(0); applied < batch.Count(); applied++ {
		page, _, ok := iter.Next()
		if !ok {
			return HeadRecord{}, ErrCorrupt
		}
		head.Revision = baseRevision
		var err error
		head, err = AdvanceHeadPage(head, page, revision)
		if err != nil {
			return HeadRecord{}, err
		}
	}
	head.Revision = revision
	if seal {
		if head.AppendedPlanBytes != head.TotalPlanBytes ||
			head.AppendedPageCount != head.PlanPageCount || head.PageChain != head.PlanRoot ||
			!head.PlanFramingValid {
			return HeadRecord{}, ErrIncomplete
		}
		head.Phase = PhaseSealed
	}
	return head, nil
}

func zeroBytes(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}

func errOrNil(err error) error { return err }
