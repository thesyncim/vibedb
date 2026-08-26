package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"hash/crc32"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	durableRequestMaxPlanPageCount = (requestledger.MaxPlanBytes + requestledger.MaxPlanPageBytes - 1) /
		requestledger.MaxPlanPageBytes
	durableRequestReaderMaxLiveBytes = requestledger.MaxPlanPageBytes +
		durableRequestMaxParticipantFrameBytes +
		durableRequestMaxPlanPageCount*len(requestledger.Digest{}) +
		replication.MaxIdentityBytes +
		distributedtxn.MaxIntentScopes*int(unsafe.Sizeof(distributedtxn.IntentScope{})) +
		replication.MaxRelationBatches*int(unsafe.Sizeof(replication.RelationMutationBatch{})) +
		replication.MaxMutations*int(unsafe.Sizeof(replication.Mutation{}))
)

type durableRequestPlanPageSource interface {
	Get(ordinal uint32) ([]byte, error)
}

type durableRequestPlanPageSourceFunc func(uint32) ([]byte, error)

func (function durableRequestPlanPageSourceFunc) Get(ordinal uint32) ([]byte, error) {
	return function(ordinal)
}

// durableRequestRecipeStreamReader authenticates the complete immutable page
// chain and performs one bounded semantic validation pass before it exposes the
// first participant. Every lazy reload rechecks that page's authenticated chain
// witness, preventing source substitution after validation.
type durableRequestRecipeStreamReader struct {
	descriptor DurableRequestPlanDescriptor
	source     durableRequestPlanPageSource
	keyDigest  requestledger.Digest
	pageChains []requestledger.Digest
	cursor     durableRequestPlanCursor

	CatalogGeneration uint64
	Identity          ReplicatedTransactionIdentity
	Contract          DurableRequestExecutionContract
	ParticipantCount  uint64
	Tenant            []byte
	KeyDigest         replication.Digest
	RequestID         replication.ID128
	RequestDigest     replication.Digest

	ordinal   uint64
	frame     []byte
	scopes    []distributedtxn.IntentScope
	batches   []replication.RelationMutationBatch
	mutations []replication.Mutation
	current   DurableRequestLogicalParticipant
	err       error
	done      bool
}

func openDurableRequestRecipeStream(
	key DurableRequestLedgerKey,
	descriptor DurableRequestPlanDescriptor,
	source durableRequestPlanPageSource,
) (*durableRequestRecipeStreamReader, error) {
	pageChains, err := authenticateDurableRequestPlan(key, descriptor, source)
	if err != nil {
		return nil, err
	}
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		return nil, errors.Join(err, ErrDurableRequest)
	}
	reader := &durableRequestRecipeStreamReader{
		descriptor: descriptor, source: source, keyDigest: keyDigest, pageChains: pageChains,
	}
	if err := reader.resetAndReadHeader(); err != nil {
		return nil, err
	}
	if replication.Digest(keyDigest) != reader.KeyDigest ||
		key.Digest != reader.RequestDigest ||
		key.Request != requestledger.RequestID(reader.RequestID) ||
		key.TenantDigest != requestledger.Digest(sha256.Sum256(reader.Tenant)) {
		return nil, ErrDurableRequestConflict
	}
	validator := newDurableRequestStreamContractValidator(*reader)
	for reader.Next() {
		if err := validator.observe(reader.ordinal-1, &reader.current); err != nil {
			return nil, err
		}
	}
	if reader.Err() != nil || !reader.Complete() {
		return nil, errors.Join(reader.Err(), ErrDurableRequestConflict)
	}
	if err := validator.finish(reader.Contract); err != nil {
		return nil, err
	}
	if err := reader.resetAndReadHeader(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (reader *durableRequestRecipeStreamReader) resetAndReadHeader() error {
	cursor, err := newDurableRequestPlanCursor(
		reader.descriptor, reader.source, reader.keyDigest, reader.pageChains,
	)
	if err != nil {
		return err
	}
	reader.cursor = cursor
	reader.ordinal, reader.err, reader.done = 0, nil, false
	reader.current = DurableRequestLogicalParticipant{}
	var headers [durableRequestPlanHeaderBytes + durableRequestLogicalRecipeHeaderBytes]byte
	if err := reader.cursor.readFull(headers[:]); err != nil {
		return err
	}
	plan := headers[:durableRequestPlanHeaderBytes]
	recipe := headers[durableRequestPlanHeaderBytes:]
	if !bytes.Equal(plan[:4], durableRequestPlanMagic[:]) || !allZeroDurableRequest(plan[4:8]) ||
		binary.LittleEndian.Uint64(plan[8:16]) != reader.descriptor.TotalBytes ||
		binary.LittleEndian.Uint64(plan[16:24])+durableRequestPlanHeaderBytes+
			durableRequestPlanTrailerBytes != reader.descriptor.TotalBytes ||
		!bytes.Equal(recipe[:8], durableRequestLogicalRecipeMagic[:]) ||
		binary.LittleEndian.Uint64(recipe[8:16]) != binary.LittleEndian.Uint64(plan[16:24]) ||
		!allZeroDurableRequest(recipe[26:32]) || !allZeroDurableRequest(recipe[76:80]) {
		return ErrDurableRequestConflict
	}
	reader.ParticipantCount = binary.LittleEndian.Uint64(recipe[16:24])
	tenantBytes := int(binary.LittleEndian.Uint16(recipe[24:26]))
	copy(reader.Identity.ID[:], recipe[32:48])
	copy(reader.Identity.RetryHome[:], recipe[48:56])
	reader.Identity.CatalogGeneration = binary.LittleEndian.Uint64(recipe[56:64])
	reader.Identity.RecoveryDeadline = int64(binary.LittleEndian.Uint64(recipe[64:72]))
	reader.Identity.CoordinatorOrdinal = binary.LittleEndian.Uint32(recipe[72:76])
	offset := 80
	contract := &reader.Contract
	contract.CatalogGeneration = binary.LittleEndian.Uint64(recipe[offset : offset+8])
	offset += 8
	digests := []*replication.Digest{
		&contract.KeyDigest, &contract.RequestDigest, &contract.KernelSemanticsDigest,
		&contract.ApplyContractDigest, &contract.TransactionManifestDigest,
		&contract.RetryHomeDerivationDigest, &contract.ClockContractDigest,
		&contract.CoordinatorIdentityDigest, &contract.SchemaManifestDigest,
		&contract.LineageForwardingDigest, &contract.InitialStateDigest,
		&contract.CommitTerminalStateDigest, &contract.AbortTerminalStateDigest,
		&contract.TerminalSummaryDigest,
	}
	for _, digest := range digests {
		copy(digest[:], recipe[offset:offset+32])
		offset += 32
	}
	copy(contract.PinID[:], recipe[offset:offset+16])
	offset += 16
	contract.PinEpoch = binary.LittleEndian.Uint64(recipe[offset : offset+8])
	offset += 8
	digests = []*replication.Digest{
		&contract.PinDigest, &contract.RouteSchemaCertificateDigest,
		&contract.TerminalContractDigest, &contract.ProtocolProgramDigest,
		&contract.ResultGrammarDigest, &contract.RetirementWitnessDigest,
	}
	for _, digest := range digests {
		copy(digest[:], recipe[offset:offset+32])
		offset += 32
	}
	contract.CommitTransitionTag = binary.LittleEndian.Uint32(recipe[offset : offset+4])
	offset += 4
	contract.AbortTransitionTag = binary.LittleEndian.Uint32(recipe[offset : offset+4])
	offset += 4
	values := []*uint64{
		&contract.ParticipantCount, &contract.CommitFinalWaveCount,
		&contract.AbortFinalWaveCount, &contract.MaxPendingWaveBytes,
		&contract.MaxContinuationBytes, &contract.MaxTerminalBytes,
		&contract.MaxActivePayloadBytes, &contract.MaxActivePayloadChunks,
	}
	for _, value := range values {
		*value = binary.LittleEndian.Uint64(recipe[offset : offset+8])
		offset += 8
	}
	copy(contract.PlanBuildID[:], recipe[offset:offset+32])
	offset += 32
	contract.PlanningLeaseExpiryIndex = binary.LittleEndian.Uint64(recipe[offset : offset+8])
	offset += 8
	contract.PlanningLeaseGeneration = binary.LittleEndian.Uint64(recipe[offset : offset+8])
	offset += 8
	copy(reader.KeyDigest[:], recipe[offset:offset+32])
	offset += 32
	copy(reader.RequestID[:], recipe[offset:offset+16])
	offset += 16
	copy(reader.RequestDigest[:], recipe[offset:offset+32])
	reader.CatalogGeneration = reader.Identity.CatalogGeneration
	recipeBytes := binary.LittleEndian.Uint64(plan[16:24])
	frameBudget := recipeBytes - durableRequestLogicalRecipeHeaderBytes - durableRequestRecipeTrailerBytes
	if reader.ParticipantCount == 0 || tenantBytes <= 0 || tenantBytes > replication.MaxIdentityBytes ||
		uint64(reader.Identity.CoordinatorOrdinal) >= reader.ParticipantCount ||
		uint64(tenantBytes) > frameBudget ||
		reader.ParticipantCount > (frameBudget-uint64(tenantBytes))/durableRequestLogicalParticipantHeaderBytes ||
		!validDurableRequestStreamFixedContract(*reader) {
		return ErrDurableRequestConflict
	}
	if cap(reader.Tenant) < tenantBytes {
		reader.Tenant = make([]byte, tenantBytes)
	} else {
		reader.Tenant = reader.Tenant[:tenantBytes]
	}
	if err := reader.cursor.readFull(reader.Tenant); err != nil {
		return err
	}
	return nil
}

func validDurableRequestStreamFixedContract(reader durableRequestRecipeStreamReader) bool {
	identity, contract := reader.Identity, reader.Contract
	if identity.ID == (distributedtxn.ID{}) ||
		identity.ID != durableRequestTransactionID(reader.KeyDigest, reader.RequestDigest) ||
		identity.RetryHome == (replication.RetryHome{}) ||
		identity.RetryHome != durableRequestRetryHome(reader.KeyDigest, identity.ID) ||
		identity.CatalogGeneration == 0 || identity.RecoveryDeadline <= 0 ||
		reader.KeyDigest == (replication.Digest{}) || reader.RequestID == (replication.ID128{}) ||
		reader.RequestDigest == (replication.Digest{}) ||
		contract.CatalogGeneration != identity.CatalogGeneration ||
		contract.KeyDigest != reader.KeyDigest || contract.RequestDigest != reader.RequestDigest ||
		contract.ParticipantCount != reader.ParticipantCount ||
		contract.KernelSemanticsDigest != replication.Digest(requestledger.SemanticsDigest()) ||
		contract.PinID != durableRequestPinID(reader.KeyDigest, reader.RequestDigest) ||
		contract.PinEpoch == 0 ||
		contract.PinDigest == (replication.Digest{}) ||
		contract.RouteSchemaCertificateDigest == (replication.Digest{}) ||
		contract.ApplyContractDigest == (replication.Digest{}) ||
		contract.ResultGrammarDigest != durableRequestResultGrammarDigest() ||
		contract.RetirementWitnessDigest == (replication.Digest{}) ||
		contract.InitialStateDigest == (replication.Digest{}) ||
		contract.CommitTerminalStateDigest == (replication.Digest{}) ||
		contract.AbortTerminalStateDigest == (replication.Digest{}) ||
		contract.TerminalSummaryDigest == (replication.Digest{}) ||
		contract.CommitTerminalStateDigest == contract.AbortTerminalStateDigest ||
		contract.CommitTransitionTag == 0 || contract.AbortTransitionTag == 0 ||
		contract.CommitTransitionTag == contract.AbortTransitionTag ||
		contract.CommitFinalWaveCount == 0 || contract.AbortFinalWaveCount == 0 ||
		contract.MaxPendingWaveBytes == 0 ||
		contract.MaxPendingWaveBytes > requestledger.MaxPendingWaveRecordBytes ||
		contract.MaxContinuationBytes == 0 ||
		contract.MaxContinuationBytes > requestledger.MaxContinuationRecordBytes ||
		contract.MaxTerminalBytes == 0 || contract.MaxTerminalBytes > requestledger.MaxLifecyclePayloadBytes ||
		(contract.MaxActivePayloadBytes == 0) != (contract.MaxActivePayloadChunks == 0) ||
		contract.MaxActivePayloadBytes > requestledger.MaxDynamicWavePayloadBytes ||
		contract.MaxActivePayloadChunks > requestledger.MaxDynamicWavePayloadChunks ||
		contract.PlanBuildID != durableRequestPlanBuildID(reader.KeyDigest, reader.RequestDigest) ||
		contract.PlanningLeaseExpiryIndex == 0 || contract.PlanningLeaseGeneration == 0 {
		return false
	}
	program := DurableRequestLogicalProgram{
		Identity: identity, Contract: contract, KeyDigest: reader.KeyDigest,
		RequestDigest: reader.RequestDigest,
	}
	return contract.RetryHomeDerivationDigest == durableRequestRetryHomeContractDigest(program) &&
		contract.ClockContractDigest == durableRequestClockContractDigest(program) &&
		contract.ProtocolProgramDigest == durableRequestProtocolProgramDigest(contract) &&
		contract.TerminalContractDigest == durableRequestTerminalContractDigest(contract)
}

func authenticateDurableRequestPlan(
	key DurableRequestLedgerKey,
	descriptor DurableRequestPlanDescriptor,
	source durableRequestPlanPageSource,
) ([]requestledger.Digest, error) {
	if descriptor.TotalBytes < durableRequestPlanHeaderBytes+durableRequestLogicalRecipeHeaderBytes+
		durableRequestRecipeTrailerBytes+durableRequestPlanTrailerBytes+1 ||
		descriptor.TotalBytes > requestledger.MaxPlanBytes || descriptor.Root == (replication.Digest{}) {
		return nil, ErrDurableRequestBound
	}
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		return nil, errors.Join(err, ErrDurableRequest)
	}
	physicalPages := uint32((descriptor.TotalBytes + requestledger.MaxPlanPageBytes - 1) /
		requestledger.MaxPlanPageBytes)
	inline := len(descriptor.Inline) != 0
	if inline != (descriptor.TotalBytes <= requestledger.MaxInlinePlanBytes) ||
		(inline && (descriptor.PageCount != 0 || uint64(len(descriptor.Inline)) != descriptor.TotalBytes)) ||
		(!inline && (source == nil || descriptor.PageCount != physicalPages)) {
		return nil, ErrDurableRequestConflict
	}
	collector, err := newDurableRequestRootCollector(keyDigest, descriptor.TotalBytes, physicalPages)
	if err != nil {
		return nil, err
	}
	pageChains := make([]requestledger.Digest, physicalPages, durableRequestMaxPlanPageCount)
	var pageChain requestledger.Digest
	planCRC, recipeCRC := uint32(0), uint32(0)
	var prefix [durableRequestPlanHeaderBytes + durableRequestLogicalRecipeHeaderBytes]byte
	var tail [8]byte
	offset := uint64(0)
	for ordinal := uint32(0); ordinal < physicalPages; ordinal++ {
		var page []byte
		if inline {
			page = descriptor.Inline
		} else {
			page, err = source.Get(ordinal)
			if err != nil {
				return nil, errors.Join(err, ErrDurableRequestConflict)
			}
		}
		want := int(min(uint64(requestledger.MaxPlanPageBytes), descriptor.TotalBytes-offset))
		if len(page) != want {
			return nil, ErrDurableRequestConflict
		}
		page = page[:len(page):len(page)]
		if err := collector.put(ordinal, page); err != nil {
			return nil, err
		}
		pageChain = requestledger.PlanPageChain(
			keyDigest, uint64(ordinal), uint64(physicalPages), offset,
			descriptor.TotalBytes, pageChain, page,
		)
		pageChains[ordinal] = pageChain
		copyDurableRequestIntersection(prefix[:], 0, page, offset)
		copyDurableRequestIntersection(tail[:], descriptor.TotalBytes-8, page, offset)
		planCRC = updateDurableRequestCRCWindow(
			planCRC, durableRequestPlanCRC, page, offset, 0, descriptor.TotalBytes-4,
		)
		recipeCRC = updateDurableRequestCRCWindow(
			recipeCRC, durableRequestLogicalRecipeCRC, page, offset,
			durableRequestPlanHeaderBytes, descriptor.TotalBytes-8,
		)
		offset += uint64(len(page))
		if inline {
			break
		}
	}
	root, err := collector.root()
	if err != nil || replication.Digest(root) != descriptor.Root ||
		binary.LittleEndian.Uint32(tail[:4]) != recipeCRC ||
		binary.LittleEndian.Uint32(tail[4:]) != planCRC {
		return nil, errors.Join(err, ErrDurableRequestConflict)
	}
	plan := prefix[:durableRequestPlanHeaderBytes]
	recipe := prefix[durableRequestPlanHeaderBytes:]
	recipeBytes := binary.LittleEndian.Uint64(plan[16:24])
	if !bytes.Equal(plan[:4], durableRequestPlanMagic[:]) || !allZeroDurableRequest(plan[4:8]) ||
		binary.LittleEndian.Uint64(plan[8:16]) != descriptor.TotalBytes ||
		recipeBytes+durableRequestPlanHeaderBytes+durableRequestPlanTrailerBytes != descriptor.TotalBytes ||
		!bytes.Equal(recipe[:8], durableRequestLogicalRecipeMagic[:]) ||
		binary.LittleEndian.Uint64(recipe[8:16]) != recipeBytes {
		return nil, ErrDurableRequestConflict
	}
	return pageChains, nil
}

func updateDurableRequestCRCWindow(
	current uint32, table *crc32.Table, page []byte, pageOffset, start, end uint64,
) uint32 {
	pageEnd := pageOffset + uint64(len(page))
	from, to := max(pageOffset, start), min(pageEnd, end)
	if from >= to {
		return current
	}
	return crc32.Update(current, table, page[from-pageOffset:to-pageOffset])
}

func copyDurableRequestIntersection(dst []byte, dstOffset uint64, page []byte, pageOffset uint64) {
	dstEnd := dstOffset + uint64(len(dst))
	pageEnd := pageOffset + uint64(len(page))
	from, to := max(dstOffset, pageOffset), min(dstEnd, pageEnd)
	if from < to {
		copy(dst[from-dstOffset:to-dstOffset], page[from-pageOffset:to-pageOffset])
	}
}

func allZeroDurableRequest(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}

type durableRequestPlanCursor struct {
	descriptor  DurableRequestPlanDescriptor
	source      durableRequestPlanPageSource
	keyDigest   requestledger.Digest
	pageChains  []requestledger.Digest
	page        []byte
	pageOrdinal uint32
	pageOffset  int
	absolute    uint64
}

func newDurableRequestPlanCursor(
	descriptor DurableRequestPlanDescriptor,
	source durableRequestPlanPageSource,
	keyDigest requestledger.Digest,
	pageChains []requestledger.Digest,
) (durableRequestPlanCursor, error) {
	cursor := durableRequestPlanCursor{
		descriptor: descriptor, source: source, keyDigest: keyDigest, pageChains: pageChains,
	}
	if err := cursor.load(0); err != nil {
		return durableRequestPlanCursor{}, err
	}
	return cursor, nil
}

func (cursor *durableRequestPlanCursor) load(ordinal uint32) error {
	if ordinal >= uint32((cursor.descriptor.TotalBytes+requestledger.MaxPlanPageBytes-1)/
		requestledger.MaxPlanPageBytes) || int(ordinal) >= len(cursor.pageChains) {
		return ErrDurableRequestConflict
	}
	if len(cursor.descriptor.Inline) != 0 {
		if ordinal != 0 {
			return ErrDurableRequestConflict
		}
		cursor.page = cursor.descriptor.Inline
	} else {
		page, err := cursor.source.Get(ordinal)
		if err != nil {
			return errors.Join(err, ErrDurableRequestConflict)
		}
		cursor.page = page
	}
	offset := uint64(ordinal) * requestledger.MaxPlanPageBytes
	want := int(min(uint64(requestledger.MaxPlanPageBytes), cursor.descriptor.TotalBytes-offset))
	if len(cursor.page) != want {
		return ErrDurableRequestConflict
	}
	cursor.page = cursor.page[:len(cursor.page):len(cursor.page)]
	var previous requestledger.Digest
	if ordinal != 0 {
		previous = cursor.pageChains[ordinal-1]
	}
	chain := requestledger.PlanPageChain(
		cursor.keyDigest, uint64(ordinal), uint64(len(cursor.pageChains)), offset,
		cursor.descriptor.TotalBytes, previous, cursor.page,
	)
	if chain != cursor.pageChains[ordinal] {
		return ErrDurableRequestConflict
	}
	cursor.pageOrdinal, cursor.pageOffset = ordinal, 0
	return nil
}

func (cursor *durableRequestPlanCursor) readFull(dst []byte) error {
	if cursor.absolute > cursor.descriptor.TotalBytes ||
		uint64(len(dst)) > cursor.descriptor.TotalBytes-cursor.absolute {
		return ErrDurableRequestConflict
	}
	for len(dst) != 0 {
		if cursor.pageOffset == len(cursor.page) {
			if err := cursor.load(cursor.pageOrdinal + 1); err != nil {
				return err
			}
		}
		take := min(len(dst), len(cursor.page)-cursor.pageOffset)
		copy(dst[:take], cursor.page[cursor.pageOffset:cursor.pageOffset+take])
		cursor.pageOffset += take
		cursor.absolute += uint64(take)
		dst = dst[take:]
	}
	return nil
}

func (reader *durableRequestRecipeStreamReader) Next() bool {
	if reader == nil || reader.err != nil || reader.done {
		return false
	}
	if reader.ordinal == reader.ParticipantCount {
		var trailers [durableRequestRecipeTrailerBytes + durableRequestPlanTrailerBytes]byte
		if reader.err = reader.cursor.readFull(trailers[:]); reader.err == nil &&
			reader.cursor.absolute != reader.descriptor.TotalBytes {
			reader.err = ErrDurableRequestConflict
		}
		reader.done = true
		return false
	}
	var header [durableRequestLogicalParticipantHeaderBytes]byte
	if reader.err = reader.cursor.readFull(header[:]); reader.err != nil {
		return false
	}
	frameBytes := int(binary.LittleEndian.Uint32(header[:4]))
	if frameBytes < durableRequestLogicalParticipantHeaderBytes ||
		frameBytes > durableRequestMaxParticipantFrameBytes ||
		reader.cursor.absolute > reader.descriptor.TotalBytes ||
		uint64(frameBytes-durableRequestLogicalParticipantHeaderBytes) >
			reader.descriptor.TotalBytes-reader.cursor.absolute {
		reader.err = ErrDurableRequestConflict
		return false
	}
	if cap(reader.frame) < frameBytes {
		reader.frame = make([]byte, frameBytes)
	} else {
		reader.frame = reader.frame[:frameBytes]
	}
	copy(reader.frame[:durableRequestLogicalParticipantHeaderBytes], header[:])
	reader.err = reader.cursor.readFull(reader.frame[durableRequestLogicalParticipantHeaderBytes:])
	if reader.err != nil {
		return false
	}
	reader.current, reader.err = reader.openParticipant(reader.frame)
	if reader.err != nil {
		return false
	}
	reader.ordinal++
	return true
}

func (reader *durableRequestRecipeStreamReader) Current() DurableRequestLogicalParticipant {
	if reader == nil || reader.err != nil || reader.ordinal == 0 {
		return DurableRequestLogicalParticipant{}
	}
	return reader.current
}

func (reader *durableRequestRecipeStreamReader) Err() error {
	if reader == nil {
		return ErrDurableRequest
	}
	return reader.err
}

func (reader *durableRequestRecipeStreamReader) Complete() bool {
	return reader != nil && reader.err == nil && reader.done &&
		reader.ordinal == reader.ParticipantCount && reader.cursor.absolute == reader.descriptor.TotalBytes
}

func (reader *durableRequestRecipeStreamReader) BufferedBytes() int {
	if reader == nil {
		return 0
	}
	return cap(reader.frame) + len(reader.cursor.page) +
		cap(reader.pageChains)*len(requestledger.Digest{}) + cap(reader.Tenant) +
		cap(reader.scopes)*int(unsafe.Sizeof(distributedtxn.IntentScope{})) +
		cap(reader.batches)*int(unsafe.Sizeof(replication.RelationMutationBatch{})) +
		cap(reader.mutations)*int(unsafe.Sizeof(replication.Mutation{}))
}

func (reader *durableRequestRecipeStreamReader) openParticipant(
	frame []byte,
) (DurableRequestLogicalParticipant, error) {
	if len(frame) < durableRequestLogicalParticipantHeaderBytes ||
		int(binary.LittleEndian.Uint32(frame[:4])) != len(frame) {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	distributionBytes := int(binary.LittleEndian.Uint16(frame[4:6]))
	shardBytes := int(binary.LittleEndian.Uint16(frame[6:8]))
	scopeCount := int(binary.LittleEndian.Uint16(frame[8:10]))
	layout := replication.TransactionMutationBytesLayout{
		RelationCount:    binary.LittleEndian.Uint16(frame[10:12]),
		MutationCount:    binary.LittleEndian.Uint32(frame[12:16]),
		Bytes:            int(binary.LittleEndian.Uint32(frame[16:20])),
		InlineRelationID: replication.RelationID(binary.LittleEndian.Uint16(frame[24:26])),
	}
	bucketBits := frame[20]
	if !allZeroDurableRequest(frame[21:24]) || !allZeroDurableRequest(frame[26:32]) ||
		distributionBytes == 0 || distributionBytes > replication.MaxIdentityBytes ||
		shardBytes == 0 || shardBytes > replication.MaxIdentityBytes ||
		scopeCount < 0 || scopeCount > maxDurableRequestScopeWave ||
		layout.Bytes <= 0 || layout.Bytes > replication.MaxCommandBytes ||
		layout.RelationCount == 0 || layout.MutationCount == 0 ||
		layout.MutationCount > replication.MaxMutations {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	cursor := durableRequestLogicalParticipantHeaderBytes
	if distributionBytes > len(frame)-cursor {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	distributionEnd := cursor + distributionBytes
	distributionRaw := frame[cursor:distributionEnd:distributionEnd]
	cursor = distributionEnd
	if shardBytes > len(frame)-cursor {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	shardEnd := cursor + shardBytes
	shardRaw := frame[cursor:shardEnd:shardEnd]
	cursor = shardEnd
	if scopeCount > (len(frame)-cursor-layout.Bytes)/8 ||
		layout.Bytes > len(frame)-cursor-scopeCount*8 {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	if cap(reader.scopes) < scopeCount {
		reader.scopes = make([]distributedtxn.IntentScope, scopeCount)
	} else {
		reader.scopes = reader.scopes[:scopeCount]
	}
	for index := range reader.scopes {
		reader.scopes[index] = distributedtxn.IntentScope{
			Start: binary.LittleEndian.Uint32(frame[cursor : cursor+4]),
			End:   binary.LittleEndian.Uint32(frame[cursor+4 : cursor+8]),
		}
		cursor += 8
	}
	if cursor+layout.Bytes != len(frame) {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	mutationRaw := frame[cursor : cursor+layout.Bytes : cursor+layout.Bytes]
	view, err := replication.OpenTransactionMutationBytes(mutationRaw, layout)
	if err != nil {
		return DurableRequestLogicalParticipant{}, errors.Join(err, ErrDurableRequestConflict)
	}
	if cap(reader.mutations) < int(layout.MutationCount) {
		reader.mutations = make([]replication.Mutation, 0, int(layout.MutationCount))
	} else {
		reader.mutations = reader.mutations[:0]
	}
	if cap(reader.batches) < int(layout.RelationCount) {
		reader.batches = make([]replication.RelationMutationBatch, 0, int(layout.RelationCount))
	} else {
		reader.batches = reader.batches[:0]
	}
	batchIterator := view.RelationBatches()
	for batchIterator.Next() {
		batchView := batchIterator.Batch()
		start := len(reader.mutations)
		mutationIterator := batchView.Mutations()
		for mutationIterator.Next() {
			mutation := mutationIterator.Mutation()
			reader.mutations = append(reader.mutations, replication.Mutation{
				Kind: mutation.Kind, Key: mutation.Key, Value: mutation.Value,
				ExpectedValueLength: mutation.ExpectedValueLength,
				ExpectedValueDigest: mutation.ExpectedValueDigest,
			})
		}
		reader.batches = append(reader.batches, replication.RelationMutationBatch{
			Relation:  batchView.Relation,
			Mutations: reader.mutations[start:len(reader.mutations):len(reader.mutations)],
		})
	}
	participant := DurableRequestLogicalParticipant{
		Distribution:     distribution.DistributionName(byteview.String(distributionRaw)),
		Shard:            distribution.ShardID(byteview.String(shardRaw)),
		Group:            openDurableRequestGroup(frame[64:136]),
		SchemaGeneration: binary.LittleEndian.Uint64(frame[136:144]),
		BucketBits:       bucketBits, IntentScopes: reader.scopes, Batches: reader.batches,
	}
	copy(participant.RangeIdentity[:], frame[32:64])
	copy(participant.RelationManifestDigest[:], frame[144:176])
	copy(participant.LineageDigest[:], frame[176:208])
	copy(participant.ForwardingRuleDigest[:], frame[208:240])
	copy(participant.MutationDigest[:], frame[240:272])
	if !validDurableRequestLogicalGroup(participant.Group) || participant.SchemaGeneration == 0 ||
		participant.RangeIdentity == (replication.Digest{}) ||
		participant.RelationManifestDigest == (replication.Digest{}) ||
		participant.LineageDigest == (replication.Digest{}) ||
		participant.ForwardingRuleDigest == (replication.Digest{}) ||
		!distributedtxn.ValidateIntentScopes(participant.IntentScopes, participant.BucketBits) ||
		view.Digest() != participant.MutationDigest {
		return DurableRequestLogicalParticipant{}, ErrDurableRequestConflict
	}
	return participant, nil
}

type durableRequestStreamContractValidator struct {
	manifest                  hash.Hash
	schema                    hash.Hash
	lineage                   hash.Hash
	coordinator               hash.Hash
	coordinatorOrdinal        uint32
	previousDistribution      [replication.MaxIdentityBytes]byte
	previousShard             [replication.MaxIdentityBytes]byte
	previousDistributionBytes int
	previousShardBytes        int
	previousRange             replication.Digest
	hasPrevious               bool
	scratch                   [8]byte
}

func newDurableRequestStreamContractValidator(
	reader durableRequestRecipeStreamReader,
) durableRequestStreamContractValidator {
	validator := durableRequestStreamContractValidator{
		manifest: sha256.New(), schema: sha256.New(), lineage: sha256.New(), coordinator: sha256.New(),
		coordinatorOrdinal: reader.Identity.CoordinatorOrdinal,
	}
	_, _ = validator.manifest.Write(byteview.Bytes(durableRequestManifestDomain))
	writeDurableRequestIdentity(validator.manifest, &validator.scratch, reader.Identity)
	_, _ = validator.manifest.Write(reader.KeyDigest[:])
	_, _ = validator.manifest.Write(reader.RequestDigest[:])
	_, _ = validator.schema.Write(byteview.Bytes(durableRequestSchemaDomain))
	writeDurableRequestU64(validator.schema, &validator.scratch, reader.ParticipantCount)
	_, _ = validator.lineage.Write(byteview.Bytes(durableRequestLineageDomain))
	writeDurableRequestU64(validator.lineage, &validator.scratch, reader.ParticipantCount)
	_, _ = validator.coordinator.Write(byteview.Bytes(durableRequestCoordinatorDomain))
	writeDurableRequestU64(validator.coordinator, &validator.scratch, uint64(reader.Identity.CoordinatorOrdinal))
	return validator
}

func (validator *durableRequestStreamContractValidator) observe(
	ordinal uint64,
	participant *DurableRequestLogicalParticipant,
) error {
	distributionRaw := byteview.Bytes(string(participant.Distribution))
	shardRaw := byteview.Bytes(string(participant.Shard))
	if validator.hasPrevious {
		order := bytes.Compare(
			validator.previousDistribution[:validator.previousDistributionBytes], distributionRaw,
		)
		if order == 0 {
			order = bytes.Compare(validator.previousShard[:validator.previousShardBytes], shardRaw)
		}
		if order == 0 {
			order = bytes.Compare(validator.previousRange[:], participant.RangeIdentity[:])
		}
		if order >= 0 {
			return ErrDurableRequestConflict
		}
	}
	validator.previousDistributionBytes = copy(validator.previousDistribution[:], distributionRaw)
	validator.previousShardBytes = copy(validator.previousShard[:], shardRaw)
	validator.previousRange = participant.RangeIdentity
	validator.hasPrevious = true
	writeDurableRequestParticipantIdentity(validator.manifest, &validator.scratch, participant)
	writeDurableRequestU64(validator.schema, &validator.scratch, participant.SchemaGeneration)
	_, _ = validator.schema.Write(participant.RelationManifestDigest[:])
	_, _ = validator.lineage.Write(participant.RangeIdentity[:])
	_, _ = validator.lineage.Write(participant.LineageDigest[:])
	_, _ = validator.lineage.Write(participant.ForwardingRuleDigest[:])
	if ordinal == uint64(validator.coordinatorOrdinal) {
		writeDurableRequestParticipantIdentity(validator.coordinator, &validator.scratch, participant)
	}
	return nil
}

func (validator *durableRequestStreamContractValidator) finish(
	contract DurableRequestExecutionContract,
) error {
	if sumDurableRequestDigest(validator.manifest) != contract.TransactionManifestDigest ||
		sumDurableRequestDigest(validator.schema) != contract.SchemaManifestDigest ||
		sumDurableRequestDigest(validator.lineage) != contract.LineageForwardingDigest ||
		sumDurableRequestDigest(validator.coordinator) != contract.CoordinatorIdentityDigest {
		return ErrDurableRequestConflict
	}
	return nil
}
