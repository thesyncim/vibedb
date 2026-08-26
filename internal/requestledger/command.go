package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	CommandFormat            = 1
	commandHeaderBytes       = 232
	MaxLifecyclePayloadBytes = MaxCommandBytes - commandHeaderBytes - checksumBytes
	AckRequestBytes          = 72
)

var commandMagic = [4]byte{'V', 'R', 'L', 'X'}

type Operation uint8

const (
	OperationInvalid Operation = iota
	OperationCreate
	OperationAppendPages
	OperationSeal
	OperationPutPending
	OperationAdvance
	OperationComplete
	OperationAck
	OperationGC
	OperationStagePayloadChunk
	OperationSealPayload
	OperationBeginPayloadBuild
	OperationExpirePlanning
	OperationBeginRoutePinAcquire
	OperationRecordRoutePinAcquired
	OperationBeginRoutePinRelease
	OperationRecordRoutePinReleased
	OperationCleanupPayload
	OperationPrepareTerminal
	OperationBeginSchemaPinRelease
	OperationRecordSchemaPinReleased
	OperationRestartPlanning
	OperationCleanupPlanning
	OperationAdvanceIssuerHighwater

	// LastOperation is the sole inclusive admission bound for the current
	// request-ledger command grammar. Integrations must not duplicate a numeric
	// operation ceiling in completion or apply validation.
	LastOperation = OperationAdvanceIssuerHighwater
)

type Command struct {
	Operation             Operation
	ExpectedRevision      uint64
	Revision              uint64
	KeyDigest             Digest
	RequestDigest         Digest
	PlanRoot              Digest
	SubjectDigest         Digest
	ExpectedRangeIdentity Digest
	Home                  LedgerHome
	Payload               []byte
	Seal                  bool
}

type AckRequest struct {
	TerminalRevision uint64
	ResultDigest     Digest
	AckToken         AckToken
}

func AppendAckRequest(dst []byte, request AckRequest) ([]byte, error) {
	if request.TerminalRevision == 0 || !nonzeroDigest(request.ResultDigest) ||
		request.AckToken == (AckToken{}) {
		return dst, ErrCorrupt
	}
	dst = appendU64(dst, request.TerminalRevision)
	dst = append(dst, request.ResultDigest[:]...)
	return append(dst, request.AckToken[:]...), nil
}

func OpenAckRequest(raw []byte) (AckRequest, error) {
	if len(raw) != AckRequestBytes {
		return AckRequest{}, ErrCorrupt
	}
	request := AckRequest{TerminalRevision: binary.LittleEndian.Uint64(raw[:8]),
		ResultDigest: readDigest(raw[8:40])}
	copy(request.AckToken[:], raw[40:72])
	if request.TerminalRevision == 0 || !nonzeroDigest(request.ResultDigest) ||
		request.AckToken == (AckToken{}) {
		return AckRequest{}, ErrCorrupt
	}
	return request, nil
}

var ackRequestDomain = []byte("vibedb/request-ledger/ack-request\x00")

func AckRequestDigest(request AckRequest) Digest {
	const domain = "vibedb/request-ledger/ack-request\x00"
	var framed [len(domain) + 8 + 2*32]byte
	at := copy(framed[:], ackRequestDomain)
	binary.LittleEndian.PutUint64(framed[at:at+8], request.TerminalRevision)
	at += 8
	at += copy(framed[at:], request.ResultDigest[:])
	tokenDigest := AckTokenDigest(request.AckToken)
	copy(framed[at:], tokenDigest[:])
	return Digest(sha256.Sum256(framed[:]))
}

type CommandView struct {
	Command
	raw             []byte
	head            HeadRecord
	pages           PlanPageBatchView
	pending         PendingWaveView
	continuation    ContinuationRecord
	terminal        TerminalRecord
	ack             AckRequest
	gc              GCRequest
	payloadChunk    PayloadChunkRecord
	payloadBuild    PayloadBuildRecord
	expiry          PlanningExpiryRequest
	routePin        RoutePinRecord
	cleanup         PayloadCleanupRequest
	prepared        PreparedTerminalRecord
	schemaPin       SchemaPinReleaseRecord
	restart         PlanningRestartRequest
	planningCleanup PlanningCleanupRequest
	issuerAdvance   IssuerAdvanceRequest
}

func (view CommandView) Bytes() []byte { return view.raw[:len(view.raw):len(view.raw)] }
func (view CommandView) Head() (HeadRecord, bool) {
	return view.head, view.Operation == OperationCreate
}
func (view CommandView) Pages() (PlanPageBatchView, bool) {
	return view.pages, view.Operation == OperationAppendPages
}
func (view CommandView) Pending() (PendingWaveView, bool) {
	return view.pending, view.Operation == OperationPutPending
}
func (view CommandView) Continuation() (ContinuationRecord, bool) {
	return view.continuation, view.Operation == OperationAdvance
}
func (view CommandView) Terminal() (TerminalRecord, bool) {
	return view.terminal, view.Operation == OperationComplete
}
func (view CommandView) AckRequest() (AckRequest, bool) {
	return view.ack, view.Operation == OperationAck
}
func (view CommandView) GCRequest() (GCRequest, bool) { return view.gc, view.Operation == OperationGC }
func (view CommandView) PayloadChunk() (PayloadChunkRecord, bool) {
	return view.payloadChunk, view.Operation == OperationStagePayloadChunk
}
func (view CommandView) PayloadBuild() (PayloadBuildRecord, bool) {
	return view.payloadBuild, view.Operation == OperationBeginPayloadBuild || view.Operation == OperationSealPayload
}
func (view CommandView) PlanningExpiry() (PlanningExpiryRequest, bool) {
	return view.expiry, view.Operation == OperationExpirePlanning
}
func (view CommandView) RoutePin() (RoutePinRecord, bool) {
	return view.routePin, view.Operation >= OperationBeginRoutePinAcquire &&
		view.Operation <= OperationRecordRoutePinReleased
}
func (view CommandView) PayloadCleanup() (PayloadCleanupRequest, bool) {
	return view.cleanup, view.Operation == OperationCleanupPayload
}
func (view CommandView) PreparedTerminal() (PreparedTerminalRecord, bool) {
	return view.prepared, view.Operation == OperationPrepareTerminal
}
func (view CommandView) SchemaPinRelease() (SchemaPinReleaseRecord, bool) {
	return view.schemaPin, view.Operation == OperationBeginSchemaPinRelease ||
		view.Operation == OperationRecordSchemaPinReleased
}
func (view CommandView) PlanningRestart() (PlanningRestartRequest, bool) {
	return view.restart, view.Operation == OperationRestartPlanning
}
func (view CommandView) PlanningCleanup() (PlanningCleanupRequest, bool) {
	return view.planningCleanup, view.Operation == OperationCleanupPlanning
}
func (view CommandView) IssuerAdvance() (IssuerAdvanceRequest, bool) {
	return view.issuerAdvance, view.Operation == OperationAdvanceIssuerHighwater
}

func AppendCommand(dst []byte, command Command) ([]byte, error) {
	if err := validateCommandShape(command); err != nil {
		return dst, err
	}
	if len(command.Payload) > MaxLifecyclePayloadBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, commandHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], commandMagic[:])
	out[8] = byte(command.Operation)
	if command.Seal {
		out[9] = 1
	}
	binary.LittleEndian.PutUint64(out[16:24], command.ExpectedRevision)
	binary.LittleEndian.PutUint64(out[24:32], command.Revision)
	putDigest(out[32:64], command.KeyDigest)
	putDigest(out[64:96], command.RequestDigest)
	putDigest(out[96:128], command.PlanRoot)
	putDigest(out[128:160], command.SubjectDigest)
	putDigest(out[160:192], command.ExpectedRangeIdentity)
	copy(out[192:224], command.Home[:])
	binary.LittleEndian.PutUint64(out[224:232], uint64(len(command.Payload)))
	dst = append(dst, command.Payload...)
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenCommandInto(raw []byte, stepScratch []StepRef) (CommandView, error) {
	if len(raw) < commandHeaderBytes+checksumBytes || len(raw) > MaxCommandBytes ||
		!magicOK(raw, commandMagic) || !zeroBytes(raw[4:8]) || raw[9] > 1 || !zeroBytes(raw[10:16]) ||
		!checksumOK(raw) {
		return CommandView{}, ErrCorrupt
	}
	payloadBytes := binary.LittleEndian.Uint64(raw[224:232])
	want, ok := exactLength(commandHeaderBytes+checksumBytes, payloadBytes)
	if !ok || want != len(raw) || payloadBytes > MaxLifecyclePayloadBytes {
		return CommandView{}, ErrCorrupt
	}
	payload := raw[commandHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes]
	command := Command{
		Operation: Operation(raw[8]), ExpectedRevision: binary.LittleEndian.Uint64(raw[16:24]),
		Revision: binary.LittleEndian.Uint64(raw[24:32]), KeyDigest: readDigest(raw[32:64]),
		RequestDigest: readDigest(raw[64:96]), PlanRoot: readDigest(raw[96:128]),
		SubjectDigest: readDigest(raw[128:160]), Payload: payload,
		ExpectedRangeIdentity: readDigest(raw[160:192]), Seal: raw[9] == 1,
	}
	copy(command.Home[:], raw[192:224])
	if err := validateCommandShape(command); err != nil {
		return CommandView{}, ErrCorrupt
	}
	view := CommandView{Command: command, raw: raw[:len(raw):len(raw)]}
	var err error
	switch command.Operation {
	case OperationCreate:
		view.head, err = OpenHead(payload)
		if err == nil && (view.head.Revision != command.Revision || view.head.KeyDigest != command.KeyDigest ||
			view.head.RequestDigest != command.RequestDigest || view.head.PlanRoot != command.PlanRoot ||
			view.head.TerminalContractDigest != command.SubjectDigest ||
			(view.head.Phase != PhasePlanning && view.head.Phase != PhaseSealed)) {
			err = ErrCorrupt
		}
		if err == nil {
			home, homeErr := Home(view.head.Key)
			if homeErr != nil || home != command.Home {
				err = ErrCorrupt
			}
		}
	case OperationAppendPages:
		view.pages, err = OpenPlanPageBatch(payload)
		if err == nil {
			iter := view.pages.Iter()
			page, _, present := iter.Next()
			if !present || page.KeyDigest != command.KeyDigest || page.PlanRoot != command.PlanRoot ||
				page.PlanBuildID != command.SubjectDigest {
				err = ErrCorrupt
			}
		}
	case OperationSeal:
		if len(payload) != 0 {
			err = ErrCorrupt
		}
	case OperationPutPending:
		if len(stepScratch) == 0 {
			err = ValidatePendingWaveBytes(payload)
			if err == nil && (readDigest(payload[32:64]) != command.KeyDigest ||
				readDigest(payload[64:96]) != command.RequestDigest ||
				readDigest(payload[96:128]) != command.PlanRoot ||
				readDigest(payload[160:192]) != command.SubjectDigest) {
				err = ErrCorrupt
			}
		} else {
			view.pending, err = OpenPendingWaveInto(payload, stepScratch)
			if err == nil && (view.pending.Key() != command.KeyDigest || view.pending.Request() != command.RequestDigest ||
				view.pending.Root() != command.PlanRoot || view.pending.Digest() != command.SubjectDigest) {
				err = ErrCorrupt
			}
		}
	case OperationAdvance:
		view.continuation, err = OpenContinuation(payload)
		if err == nil && (view.continuation.KeyDigest != command.KeyDigest ||
			view.continuation.RequestDigest != command.RequestDigest || view.continuation.PlanRoot != command.PlanRoot ||
			view.continuation.ContinuationDigest != command.SubjectDigest) {
			err = ErrCorrupt
		}
	case OperationComplete:
		view.terminal, err = OpenTerminal(payload)
		if err == nil && (view.terminal.KeyDigest != command.KeyDigest || view.terminal.RequestDigest != command.RequestDigest ||
			view.terminal.PlanRoot != command.PlanRoot || view.terminal.ResultDigest != command.SubjectDigest) {
			err = ErrCorrupt
		}
	case OperationAck:
		view.ack, err = OpenAckRequest(payload)
		if err == nil && AckRequestDigest(view.ack) != command.SubjectDigest {
			err = ErrCorrupt
		}
	case OperationGC:
		view.gc, err = OpenGCRequest(payload)
		if err == nil && (view.gc.ExpectedAckDigest != command.SubjectDigest ||
			view.gc.Action != GCActionCollect) {
			err = ErrCorrupt
		}
	case OperationStagePayloadChunk:
		view.payloadChunk, err = OpenPayloadChunk(payload)
		if err == nil && (view.payloadChunk.KeyDigest != command.KeyDigest ||
			view.payloadChunk.PlanRoot != command.PlanRoot || view.payloadChunk.BuildDigest != command.SubjectDigest) {
			err = ErrCorrupt
		}
	case OperationBeginPayloadBuild, OperationSealPayload:
		view.payloadBuild, err = OpenPayloadBuild(payload)
		wantPhase := PayloadBuildStaging
		if command.Operation == OperationSealPayload {
			wantPhase = PayloadBuildSealed
		}
		if err == nil && (view.payloadBuild.KeyDigest != command.KeyDigest ||
			view.payloadBuild.RequestDigest != command.RequestDigest ||
			view.payloadBuild.PlanRoot != command.PlanRoot ||
			view.payloadBuild.BuildDigest != command.SubjectDigest || view.payloadBuild.Phase != wantPhase) {
			err = ErrCorrupt
		}
	case OperationExpirePlanning:
		view.expiry, err = OpenPlanningExpiryRequest(payload)
		if err == nil && (view.expiry.KeyDigest != command.KeyDigest ||
			view.expiry.PlanBuildID != command.SubjectDigest) {
			err = ErrCorrupt
		}
	case OperationBeginRoutePinAcquire, OperationRecordRoutePinAcquired,
		OperationBeginRoutePinRelease, OperationRecordRoutePinReleased:
		view.routePin, err = OpenRoutePin(payload)
		wantPhase := RoutePinAcquiring
		switch command.Operation {
		case OperationRecordRoutePinAcquired:
			wantPhase = RoutePinAcquired
		case OperationBeginRoutePinRelease:
			wantPhase = RoutePinReleasing
		case OperationRecordRoutePinReleased:
			wantPhase = RoutePinReleased
		}
		if err == nil && (view.routePin.KeyDigest != command.KeyDigest ||
			view.routePin.RequestDigest != command.RequestDigest ||
			view.routePin.PlanRoot != command.PlanRoot ||
			view.routePin.RecordDigest != command.SubjectDigest ||
			view.routePin.Phase != wantPhase) {
			err = ErrCorrupt
		}
	case OperationCleanupPayload:
		view.cleanup, err = OpenPayloadCleanupRequest(payload)
		if err == nil && view.cleanup.BuildDigest != command.SubjectDigest {
			err = ErrCorrupt
		}
	case OperationPrepareTerminal:
		view.prepared, err = OpenPreparedTerminal(payload)
		if err == nil && (view.prepared.KeyDigest != command.KeyDigest ||
			view.prepared.RequestDigest != command.RequestDigest ||
			view.prepared.PlanRoot != command.PlanRoot ||
			view.prepared.PreparedDigest != command.SubjectDigest ||
			view.prepared.Revision != command.Revision) {
			err = ErrCorrupt
		}
	case OperationBeginSchemaPinRelease, OperationRecordSchemaPinReleased:
		view.schemaPin, err = OpenSchemaPinRelease(payload)
		wantPhase := SchemaPinReleasing
		if command.Operation == OperationRecordSchemaPinReleased {
			wantPhase = SchemaPinReleased
		}
		if err == nil && (view.schemaPin.KeyDigest != command.KeyDigest ||
			view.schemaPin.RequestDigest != command.RequestDigest ||
			view.schemaPin.PlanRoot != command.PlanRoot ||
			view.schemaPin.RecordDigest != command.SubjectDigest ||
			view.schemaPin.Revision != command.Revision || view.schemaPin.Phase != wantPhase) {
			err = ErrCorrupt
		}
	case OperationRestartPlanning:
		view.restart, err = OpenPlanningRestartRequest(payload)
		if err == nil && (view.restart.KeyDigest != command.KeyDigest ||
			view.restart.NextPlanBuildID != command.SubjectDigest) {
			err = ErrCorrupt
		}
	case OperationCleanupPlanning:
		view.planningCleanup, err = OpenPlanningCleanupRequest(payload)
		if err == nil && view.planningCleanup.PlanBuildID != command.SubjectDigest {
			err = ErrCorrupt
		}
	case OperationAdvanceIssuerHighwater:
		view.issuerAdvance, err = OpenIssuerAdvanceRequest(payload)
		if err == nil && view.issuerAdvance.ExpectedHighwaterDigest != command.SubjectDigest {
			err = ErrCorrupt
		}
	default:
		err = ErrCorrupt
	}
	if err != nil {
		return CommandView{}, ErrCorrupt
	}
	return view, nil
}

// ValidateCommand fully validates a nested lifecycle command without
// materializing pending StepRefs. Apply callers use OpenCommandInto with their
// own scratch when they need the decoded wave.
func ValidateCommand(raw []byte) error {
	_, err := OpenCommandInto(raw, nil)
	return err
}

func validateCommandShape(command Command) error {
	if command.Operation < OperationCreate || command.Operation > LastOperation ||
		!nonzeroDigest(command.KeyDigest) || !nonzeroDigest(command.RequestDigest) ||
		!nonzeroDigest(command.PlanRoot) || !nonzeroDigest(command.SubjectDigest) ||
		!nonzeroDigest(command.ExpectedRangeIdentity) || command.Home == (LedgerHome{}) {
		return ErrCorrupt
	}
	if command.Operation == OperationCreate || command.Operation == OperationBeginPayloadBuild {
		if command.ExpectedRevision != 0 || command.Revision != 1 {
			return ErrRevision
		}
	} else if !nextRevision(command.ExpectedRevision, command.Revision) {
		return ErrRevision
	}
	if command.Seal && command.Operation != OperationAppendPages {
		return ErrCorrupt
	}
	return nil
}

// SemanticsDigest binds the sole command grammar, operation codes, record
// magics, and byte bounds for build/capability negotiation.
func SemanticsDigest() Digest {
	return semanticsDigestWithPerturb(-1, 0)
}

func semanticsDigestWithPerturb(perturb int, xor uint64) Digest {
	digest, _ := semanticsDigestWithPerturbAndCount(perturb, xor)
	return digest
}

func semanticsDigestWithPerturbAndCount(perturb int, xor uint64) (Digest, int) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/request-ledger/semantics\x00"))
	for _, magic := range [...][4]byte{
		commandMagic, headMagic, pageMagic, planMagic, pageBatchMagic,
		pendingWaveMagic, continuationMagic, terminalMagic, ackMagic,
		gcRequestMagic, payloadBuildMagic, payloadChunkMagic,
		routePinMagic, payloadCleanupMagic, preparedTerminalMagic, schemaPinReleaseMagic,
		planningExpiryMagic, planningRestartMagic, planningCleanupMagic, planningExpiryIndexMagic,
		readyMagic, principalQuotaMagic,
		issuerHighwaterMagic, issuerSequenceMagic, issuerAdvanceMagic,
	} {
		_, _ = hash.Write(magic[:])
	}
	values := [...]uint64{
		CommandFormat, MaxCommandBytes, MaxLifecyclePayloadBytes,
		MaxInlinePlanBytes, MaxPlanPageBytes, MaxPlanBytes, MaxTargetBytes,
		MaxPendingWaveSteps, MaxPendingWaveRecordBytes, MaxContinuationCursorBytes,
		MaxContinuationObservationBytes, MaxContinuationRecordBytes,
		MaxTerminalResultBytes, AckRequestBytes, AckRecordBytes,
		MaxDynamicWavePayloadBytes, MaxDynamicWavePayloadChunks,
		MaxRouteGateReleaseCommandBytes, MaxRouteGateReleaseCompletionBytes,
		MaxGCRequestBytes, MaxAckGCDeleteRows,
		MaxRouteGatePinCommandBytes, MaxRouteGatePinCompletionBytes,
		MaxRoutePinRecordBytes, PayloadCleanupRequestBytes,
		MaxPreparedTerminalResultBytes, MaxPreparedTerminalRecordBytes,
		MaxSchemaPinReleaseRecordBytes,
		PlanningExpiryRequestBytes, PlanningRestartRequestBytes, PlanningCleanupRequestBytes,
		PlanningExpiryKeyBytes, PlanningExpiryRecordBytes,
		ReadyStorageKeyBytes, ReadyRecordBytes, PrincipalQuotaKeyBytes, PrincipalQuotaRecordBytes,
		IssuerHighwaterKeyBytes, IssuerHighwaterRecordBytes,
		IssuerSequenceKeyBytes, IssuerSequenceRecordBytes, IssuerAdvanceRequestBytes,
		IssuerSequenceReservationBytes, IssuerHighwaterResidentBytes,
		RoutePinReservationBytes, PreparedTerminalReservationBytes,
		SchemaPinReleaseReservationBytes, ReadyReservationBytes,
		commandHeaderBytes, headHeaderBytes, pageHeaderBytes, planHeaderBytes,
		pageBatchHeaderBytes, pendingWaveHeaderBytes, stepRefBytes,
		continuationHeaderBytes, terminalHeaderBytes, payloadBuildBytes,
		payloadChunkHeaderBytes, gcRequestHeaderBytes, routePinHeaderBytes,
		preparedTerminalHeaderBytes, schemaPinReleaseHeaderBytes,
		uint64(StoragePrefix), uint64(StorageHead), uint64(StoragePlanPage), uint64(StoragePending), uint64(StorageTerminal),
		uint64(StorageAck), uint64(StorageContinuation), uint64(StoragePayloadChunk), uint64(StoragePayloadBuild),
		uint64(StorageRoutePin),
		uint64(StoragePrepared), uint64(StorageSchemaPin),
		FixedStorageKeyBytes, PageStorageKeyBytes, PayloadStorageKeyBytes,
		uint64(ScopeAuthenticated), uint64(ScopeLocalInstall),
		uint64(PhasePlanning), uint64(PhaseExpired), uint64(PhaseSealed), uint64(PhasePrepared),
		uint64(PhaseTerminal), uint64(PhaseAcked),
		uint64(OutcomeCommitted), uint64(OutcomeAborted),
		uint64(PayloadSourcePlan), uint64(PayloadSourceDynamic),
		uint64(PayloadBuildStaging), uint64(PayloadBuildSealed),
		uint64(AckGCCollecting), uint64(AckGCComplete),
		uint64(GCActionReleasePin), uint64(GCActionCollect),
		uint64(OperationCreate), uint64(OperationAppendPages), uint64(OperationSeal),
		uint64(OperationPutPending), uint64(OperationAdvance), uint64(OperationComplete),
		uint64(OperationAck), uint64(OperationGC), uint64(OperationStagePayloadChunk),
		uint64(OperationSealPayload), uint64(OperationBeginPayloadBuild),
		uint64(OperationExpirePlanning),
		uint64(OperationBeginRoutePinAcquire), uint64(OperationRecordRoutePinAcquired),
		uint64(OperationBeginRoutePinRelease), uint64(OperationRecordRoutePinReleased),
		uint64(OperationCleanupPayload),
		uint64(OperationPrepareTerminal), uint64(OperationBeginSchemaPinRelease),
		uint64(OperationRecordSchemaPinReleased),
		uint64(OperationRestartPlanning), uint64(OperationCleanupPlanning),
		uint64(OperationAdvanceIssuerHighwater),
		uint64(RoutePinAcquiring), uint64(RoutePinAcquired),
		uint64(RoutePinReleasing), uint64(RoutePinReleased),
		uint64(SchemaPinReleasing), uint64(SchemaPinReleased),
		uint64(IssuerSequenceActive), uint64(IssuerSequenceGCComplete),
		uint64(IssuerHighwaterStoragePrefix), uint64(IssuerSequenceStoragePrefix),
		uint64(ReadyStoragePrefix), uint64(PlanningExpiryStoragePrefix), uint64(PrincipalQuotaStoragePrefix),
		uint64(ReadinessDeriveWave), uint64(ReadinessDispatchPending),
		uint64(ReadinessPlanningExpiry), uint64(ReadinessRestartPlanning),
		uint64(ReadinessPinAcquiring), uint64(ReadinessPinRelease),
		uint64(ReadinessDynamicBuild), uint64(ReadinessPayloadCleanup),
		uint64(ReadinessTerminalPrepared), uint64(ReadinessComplete),
	}
	var fixed [8]byte
	for index, value := range values {
		if index == perturb {
			value ^= xor
		}
		binary.LittleEndian.PutUint64(fixed[:], value)
		_, _ = hash.Write(fixed[:])
	}
	var digest Digest
	_ = hash.Sum(digest[:0])
	return digest, len(values)
}
