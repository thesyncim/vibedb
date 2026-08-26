package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	durableRequestPlanHeaderBytes               = 24
	durableRequestPlanTrailerBytes              = 4
	durableRequestRecipeTrailerBytes            = 4
	durableRequestLogicalRecipeHeaderBytes      = 952
	durableRequestLogicalParticipantHeaderBytes = 272
	maxDurableRequestScopeWave                  = distributedtxn.MaxIntentScopes
	durableRequestMaxParticipantFrameBytes      = durableRequestLogicalParticipantHeaderBytes +
		2*replication.MaxIdentityBytes + distributedtxn.MaxIntentScopes*8 +
		replication.MaxCommandBytes
)

var durableRequestPlanMagic = [4]byte{'V', 'R', 'P', 'L'}
var durableRequestLogicalRecipeMagic = [8]byte{'V', 'D', 'B', 'D', 'R', 'Q', 0, 0}
var durableRequestPlanCRC = crc32.MakeTable(crc32.Castagnoli)
var durableRequestLogicalRecipeCRC = crc32.MakeTable(crc32.Castagnoli)

type durableRequestPlanMeasurement struct {
	RecipeBytes      uint64
	PlanBytes        uint64
	PhysicalPages    uint32
	Root             replication.Digest
	Inline           []byte
	keyDigest        requestledger.Digest
	maxMutationBytes int
	contract         DurableRequestExecutionContract
}

func (measurement durableRequestPlanMeasurement) descriptor() DurableRequestPlanDescriptor {
	descriptor := DurableRequestPlanDescriptor{
		TotalBytes: measurement.PlanBytes, Root: measurement.Root, Contract: measurement.contract,
	}
	if len(measurement.Inline) != 0 {
		descriptor.Inline = bytes.Clone(measurement.Inline)
	} else {
		descriptor.PageCount = measurement.PhysicalPages
	}
	return descriptor
}

type durableRequestPlanPageSink interface {
	Put(ordinal uint32, page []byte) error
}

type durableRequestPlanPageSinkFunc func(uint32, []byte) error

func (function durableRequestPlanPageSinkFunc) Put(ordinal uint32, page []byte) error {
	return function(ordinal, page)
}

// measureDurableRequestPlan computes exact canonical bytes and the authenticated
// page root while retaining only one page and one participant mutation payload.
func measureDurableRequestPlan(
	key DurableRequestLedgerKey,
	program DurableRequestLogicalProgram,
) (durableRequestPlanMeasurement, error) {
	keyDigest, err := validateDurableRequestProgramKey(key, program)
	if err != nil {
		return durableRequestPlanMeasurement{}, err
	}
	recipeBytes, maxMutationBytes, err := measureValidatedDurableRequestRecipeBytes(program)
	if err != nil {
		return durableRequestPlanMeasurement{}, err
	}
	planBytes, pages, err := durableRequestPlanSizes(recipeBytes)
	if err != nil {
		return durableRequestPlanMeasurement{}, err
	}
	measurement := durableRequestPlanMeasurement{
		RecipeBytes: recipeBytes, PlanBytes: planBytes, PhysicalPages: pages,
		keyDigest: keyDigest, maxMutationBytes: maxMutationBytes, contract: program.Contract,
	}
	collector, err := newDurableRequestRootCollector(keyDigest, planBytes, pages)
	if err != nil {
		return durableRequestPlanMeasurement{}, err
	}
	if planBytes <= requestledger.MaxInlinePlanBytes {
		collector.inline = make([]byte, 0, int(planBytes))
	}
	if err := encodeDurableRequestPlan(measurement, program, durableRequestPlanPageSinkFunc(collector.put)); err != nil {
		return durableRequestPlanMeasurement{}, err
	}
	root, err := collector.root()
	if err != nil {
		return durableRequestPlanMeasurement{}, err
	}
	measurement.Root = replication.Digest(root)
	measurement.Inline = collector.inline
	return measurement, nil
}

// streamDurableRequestPlan emits byte-identical pages and rejects input changed
// between the measure and emit passes.
func streamDurableRequestPlan(
	measurement durableRequestPlanMeasurement,
	program DurableRequestLogicalProgram,
	sink durableRequestPlanPageSink,
) error {
	if sink == nil || measurement.PlanBytes == 0 || measurement.Root == (replication.Digest{}) {
		return ErrDurableRequest
	}
	if measurement.contract != program.Contract {
		return ErrDurableRequestConflict
	}
	recipeBytes, maxMutationBytes, err := measureDurableRequestRecipeBytes(program)
	if err != nil || recipeBytes != measurement.RecipeBytes ||
		maxMutationBytes != measurement.maxMutationBytes {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	planBytes, pages, err := durableRequestPlanSizes(recipeBytes)
	if err != nil || planBytes != measurement.PlanBytes || pages != measurement.PhysicalPages {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	collector, err := newDurableRequestRootCollector(measurement.keyDigest, planBytes, pages)
	if err != nil {
		return err
	}
	tee := durableRequestPlanPageSinkFunc(func(ordinal uint32, page []byte) error {
		if putErr := collector.put(ordinal, page); putErr != nil {
			return putErr
		}
		return sink.Put(ordinal, page)
	})
	if err := encodeDurableRequestPlan(measurement, program, tee); err != nil {
		return err
	}
	root, err := collector.root()
	if err != nil || replication.Digest(root) != measurement.Root {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	return nil
}

func validateDurableRequestProgramKey(
	key DurableRequestLedgerKey,
	program DurableRequestLogicalProgram,
) (requestledger.Digest, error) {
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		return requestledger.Digest{}, errors.Join(err, ErrDurableRequest)
	}
	tenantDigest := requestledger.Digest(sha256.Sum256(program.Tenant))
	if !validDurableRequestLogicalProgram(program) ||
		replication.Digest(keyDigest) != program.KeyDigest ||
		key.Digest != program.RequestDigest ||
		key.Request != requestledger.RequestID(program.RequestID) ||
		key.TenantDigest != tenantDigest {
		return requestledger.Digest{}, ErrDurableRequestConflict
	}
	return keyDigest, nil
}

func durableRequestPlanSizes(recipeBytes uint64) (uint64, uint32, error) {
	const overhead = durableRequestPlanHeaderBytes + durableRequestPlanTrailerBytes
	if recipeBytes == 0 || recipeBytes > MaxDurableRequestRecipeBytes ||
		recipeBytes > requestledger.MaxPlanBytes-overhead {
		return 0, 0, ErrDurableRequestBound
	}
	total := recipeBytes + overhead
	pages := (total + requestledger.MaxPlanPageBytes - 1) / requestledger.MaxPlanPageBytes
	if pages == 0 || pages > math.MaxUint32 {
		return 0, 0, ErrDurableRequestBound
	}
	return total, uint32(pages), nil
}

func measureDurableRequestRecipeBytes(program DurableRequestLogicalProgram) (uint64, int, error) {
	if !validDurableRequestLogicalProgram(program) {
		return 0, 0, ErrDurableRequest
	}
	return measureValidatedDurableRequestRecipeBytes(program)
}

func measureValidatedDurableRequestRecipeBytes(program DurableRequestLogicalProgram) (uint64, int, error) {
	total := uint64(durableRequestLogicalRecipeHeaderBytes + len(program.Tenant) + durableRequestRecipeTrailerBytes)
	maxMutationBytes := 0
	for index := range program.Participants {
		participant := &program.Participants[index]
		layout, err := replication.MeasureTransactionMutationBytes(participant.Batches)
		if err != nil || layout.Bytes <= 0 || layout.Bytes > replication.MaxCommandBytes ||
			layout.RelationCount == 0 || layout.MutationCount == 0 {
			return 0, 0, errors.Join(err, ErrDurableRequest)
		}
		frameBytes := uint64(durableRequestLogicalParticipantHeaderBytes) +
			uint64(len(participant.Distribution)) + uint64(len(participant.Shard)) +
			uint64(len(participant.IntentScopes))*8 + uint64(layout.Bytes)
		if frameBytes > durableRequestMaxParticipantFrameBytes || frameBytes > math.MaxUint32 ||
			frameBytes > MaxDurableRequestRecipeBytes || total > MaxDurableRequestRecipeBytes-frameBytes {
			return 0, 0, ErrDurableRequestBound
		}
		total += frameBytes
		maxMutationBytes = max(maxMutationBytes, layout.Bytes)
	}
	return total, maxMutationBytes, nil
}

type durableRequestPageWriter struct {
	total   uint64
	offset  uint64
	ordinal uint32
	page    []byte
	sink    durableRequestPlanPageSink
}

func (writer *durableRequestPageWriter) write(raw []byte) error {
	if writer == nil || writer.sink == nil || writer.offset > writer.total ||
		uint64(len(raw)) > writer.total-writer.offset {
		return ErrDurableRequestBound
	}
	for len(raw) != 0 {
		room := requestledger.MaxPlanPageBytes - len(writer.page)
		take := min(room, len(raw))
		writer.page = append(writer.page, raw[:take]...)
		writer.offset += uint64(take)
		raw = raw[take:]
		if len(writer.page) == requestledger.MaxPlanPageBytes {
			if err := writer.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (writer *durableRequestPageWriter) flush() error {
	if len(writer.page) == 0 {
		return nil
	}
	page := writer.page[:len(writer.page):len(writer.page)]
	if err := writer.sink.Put(writer.ordinal, page); err != nil {
		return err
	}
	writer.ordinal++
	writer.page = writer.page[:0]
	return nil
}

func (writer *durableRequestPageWriter) finish(expectedPages uint32) error {
	if err := writer.flush(); err != nil {
		return err
	}
	if writer.offset != writer.total || writer.ordinal != expectedPages {
		return ErrDurableRequestBound
	}
	return nil
}

func encodeDurableRequestPlan(
	measurement durableRequestPlanMeasurement,
	program DurableRequestLogicalProgram,
	sink durableRequestPlanPageSink,
) error {
	if measurement.RecipeBytes == 0 || measurement.PlanBytes == 0 ||
		measurement.PhysicalPages == 0 || measurement.maxMutationBytes <= 0 || sink == nil {
		return ErrDurableRequest
	}
	writer := durableRequestPageWriter{
		total: measurement.PlanBytes,
		page:  make([]byte, 0, requestledger.MaxPlanPageBytes), sink: sink,
	}
	framer, err := requestledger.NewPlanStreamFramer(measurement.RecipeBytes)
	if err != nil || framer.TotalBytes() != measurement.PlanBytes {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	planFrame, err := framer.AppendHeader(make([]byte, 0, durableRequestPlanHeaderBytes))
	if err != nil || len(planFrame) != durableRequestPlanHeaderBytes {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	if err := writer.write(planFrame); err != nil {
		return err
	}
	var recipeHeader [durableRequestLogicalRecipeHeaderBytes]byte
	appendDurableRequestLogicalHeader(recipeHeader[:], measurement.RecipeBytes, program)
	recipeCRC := uint32(0)
	if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, recipeHeader[:]); err != nil {
		return err
	}
	if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, program.Tenant); err != nil {
		return err
	}
	mutationScratch := make([]byte, 0, measurement.maxMutationBytes)
	var participantHeader [durableRequestLogicalParticipantHeaderBytes]byte
	var scope [8]byte
	for index := range program.Participants {
		participant := &program.Participants[index]
		encoded, layout, err := replication.AppendTransactionMutationBytes(mutationScratch[:0], participant.Batches)
		if err != nil || len(encoded) > measurement.maxMutationBytes {
			return errors.Join(err, ErrDurableRequestConflict)
		}
		mutationScratch = encoded[:0:cap(encoded)]
		frameBytes := durableRequestLogicalParticipantHeaderBytes + len(participant.Distribution) +
			len(participant.Shard) + len(participant.IntentScopes)*8 + len(encoded)
		if frameBytes > durableRequestMaxParticipantFrameBytes || frameBytes > math.MaxUint32 {
			return ErrDurableRequestBound
		}
		appendDurableRequestLogicalParticipantHeader(participantHeader[:], frameBytes, layout, *participant)
		if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, participantHeader[:]); err != nil {
			return err
		}
		distributionBytes := byteview.Bytes(string(participant.Distribution))
		if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, distributionBytes); err != nil {
			return err
		}
		shardBytes := byteview.Bytes(string(participant.Shard))
		if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, shardBytes); err != nil {
			return err
		}
		for scopeIndex := range participant.IntentScopes {
			binary.LittleEndian.PutUint32(scope[:4], participant.IntentScopes[scopeIndex].Start)
			binary.LittleEndian.PutUint32(scope[4:8], participant.IntentScopes[scopeIndex].End)
			if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, scope[:]); err != nil {
				return err
			}
		}
		if err := observeDurableRequestRecipePart(&recipeCRC, &framer, &writer, encoded); err != nil {
			return err
		}
	}
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], recipeCRC)
	if err := framer.ObserveRecipe(checksum[:]); err != nil {
		return err
	}
	if err := writer.write(checksum[:]); err != nil {
		return err
	}
	planFrame, err = framer.AppendTrailer(planFrame[:0])
	if err != nil || len(planFrame) != durableRequestPlanTrailerBytes {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	if err := writer.write(planFrame); err != nil {
		return err
	}
	return writer.finish(measurement.PhysicalPages)
}

func observeDurableRequestRecipePart(
	recipeCRC *uint32,
	framer *requestledger.PlanStreamFramer,
	writer *durableRequestPageWriter,
	raw []byte,
) error {
	*recipeCRC = crc32.Update(*recipeCRC, durableRequestLogicalRecipeCRC, raw)
	if err := framer.ObserveRecipe(raw); err != nil {
		return err
	}
	return writer.write(raw)
}

func appendDurableRequestLogicalHeader(raw []byte, recipeBytes uint64, program DurableRequestLogicalProgram) {
	clear(raw)
	copy(raw[:8], durableRequestLogicalRecipeMagic[:])
	binary.LittleEndian.PutUint64(raw[8:16], recipeBytes)
	binary.LittleEndian.PutUint64(raw[16:24], uint64(len(program.Participants)))
	binary.LittleEndian.PutUint16(raw[24:26], uint16(len(program.Tenant)))
	copy(raw[32:48], program.Identity.ID[:])
	copy(raw[48:56], program.Identity.RetryHome[:])
	binary.LittleEndian.PutUint64(raw[56:64], program.Identity.CatalogGeneration)
	binary.LittleEndian.PutUint64(raw[64:72], uint64(program.Identity.RecoveryDeadline))
	binary.LittleEndian.PutUint32(raw[72:76], program.Identity.CoordinatorOrdinal)
	offset := 80
	contract := program.Contract
	binary.LittleEndian.PutUint64(raw[offset:offset+8], contract.CatalogGeneration)
	offset += 8
	for _, digest := range [...]replication.Digest{
		contract.KeyDigest, contract.RequestDigest, contract.KernelSemanticsDigest,
		contract.ApplyContractDigest, contract.TransactionManifestDigest,
		contract.RetryHomeDerivationDigest, contract.ClockContractDigest,
		contract.CoordinatorIdentityDigest, contract.SchemaManifestDigest,
		contract.LineageForwardingDigest, contract.InitialStateDigest,
		contract.CommitTerminalStateDigest, contract.AbortTerminalStateDigest,
		contract.TerminalSummaryDigest,
	} {
		copy(raw[offset:offset+32], digest[:])
		offset += 32
	}
	copy(raw[offset:offset+16], contract.PinID[:])
	offset += 16
	binary.LittleEndian.PutUint64(raw[offset:offset+8], contract.PinEpoch)
	offset += 8
	for _, digest := range [...]replication.Digest{
		contract.PinDigest, contract.RouteSchemaCertificateDigest,
		contract.TerminalContractDigest, contract.ProtocolProgramDigest,
		contract.ResultGrammarDigest, contract.RetirementWitnessDigest,
	} {
		copy(raw[offset:offset+32], digest[:])
		offset += 32
	}
	binary.LittleEndian.PutUint32(raw[offset:offset+4], contract.CommitTransitionTag)
	offset += 4
	binary.LittleEndian.PutUint32(raw[offset:offset+4], contract.AbortTransitionTag)
	offset += 4
	for _, value := range [...]uint64{
		contract.ParticipantCount, contract.CommitFinalWaveCount,
		contract.AbortFinalWaveCount, contract.MaxPendingWaveBytes,
		contract.MaxContinuationBytes, contract.MaxTerminalBytes,
		contract.MaxActivePayloadBytes, contract.MaxActivePayloadChunks,
	} {
		binary.LittleEndian.PutUint64(raw[offset:offset+8], value)
		offset += 8
	}
	copy(raw[offset:offset+32], contract.PlanBuildID[:])
	offset += 32
	binary.LittleEndian.PutUint64(raw[offset:offset+8], contract.PlanningLeaseExpiryIndex)
	offset += 8
	binary.LittleEndian.PutUint64(raw[offset:offset+8], contract.PlanningLeaseGeneration)
	offset += 8
	copy(raw[offset:offset+32], program.KeyDigest[:])
	offset += 32
	copy(raw[offset:offset+16], program.RequestID[:])
	offset += 16
	copy(raw[offset:offset+32], program.RequestDigest[:])
}

func appendDurableRequestLogicalParticipantHeader(
	raw []byte,
	frameBytes int,
	layout replication.TransactionMutationBytesLayout,
	participant DurableRequestLogicalParticipant,
) {
	clear(raw)
	binary.LittleEndian.PutUint32(raw[:4], uint32(frameBytes))
	binary.LittleEndian.PutUint16(raw[4:6], uint16(len(participant.Distribution)))
	binary.LittleEndian.PutUint16(raw[6:8], uint16(len(participant.Shard)))
	binary.LittleEndian.PutUint16(raw[8:10], uint16(len(participant.IntentScopes)))
	binary.LittleEndian.PutUint16(raw[10:12], layout.RelationCount)
	binary.LittleEndian.PutUint32(raw[12:16], layout.MutationCount)
	binary.LittleEndian.PutUint32(raw[16:20], uint32(layout.Bytes))
	raw[20] = participant.BucketBits
	binary.LittleEndian.PutUint16(raw[24:26], uint16(layout.InlineRelationID))
	copy(raw[32:64], participant.RangeIdentity[:])
	appendDurableRequestGroup(raw[64:136], participant.Group)
	binary.LittleEndian.PutUint64(raw[136:144], participant.SchemaGeneration)
	copy(raw[144:176], participant.RelationManifestDigest[:])
	copy(raw[176:208], participant.LineageDigest[:])
	copy(raw[208:240], participant.ForwardingRuleDigest[:])
	copy(raw[240:272], participant.MutationDigest[:])
}

type durableRequestRootCollector struct {
	total       uint64
	count       uint32
	offset      uint64
	ordinal     uint32
	key         requestledger.Digest
	chain       requestledger.Digest
	accumulator *requestledger.PlanRootAccumulator
	inline      []byte
}

func newDurableRequestRootCollector(key requestledger.Digest, total uint64, count uint32) (*durableRequestRootCollector, error) {
	collector := &durableRequestRootCollector{key: key, total: total, count: count}
	if total > requestledger.MaxInlinePlanBytes {
		accumulator, err := requestledger.NewPlanRootAccumulator(key, total)
		if err != nil {
			return nil, err
		}
		collector.accumulator = &accumulator
	}
	return collector, nil
}

func (collector *durableRequestRootCollector) put(ordinal uint32, page []byte) error {
	if collector == nil || ordinal != collector.ordinal || len(page) == 0 ||
		len(page) > requestledger.MaxPlanPageBytes || collector.offset > collector.total ||
		uint64(len(page)) > collector.total-collector.offset {
		return ErrDurableRequestConflict
	}
	if collector.accumulator != nil {
		if err := collector.accumulator.Append(page); err != nil {
			return err
		}
	} else {
		collector.chain = requestledger.PlanPageChain(
			collector.key, uint64(ordinal), uint64(collector.count), collector.offset,
			collector.total, collector.chain, page,
		)
		collector.inline = append(collector.inline, page...)
	}
	collector.offset += uint64(len(page))
	collector.ordinal++
	return nil
}

func (collector *durableRequestRootCollector) root() (requestledger.Digest, error) {
	if collector == nil || collector.offset != collector.total || collector.ordinal != collector.count {
		return requestledger.Digest{}, ErrDurableRequestConflict
	}
	if collector.accumulator != nil {
		return collector.accumulator.Root()
	}
	if collector.chain == (requestledger.Digest{}) {
		return requestledger.Digest{}, ErrDurableRequestConflict
	}
	return collector.chain, nil
}
