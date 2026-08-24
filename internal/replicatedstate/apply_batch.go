package replicatedstate

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

var _ raftmodel.NormalBatchStateMachine = (*Machine)(nil)

type normalBatchWorkspace struct {
	system         logicalOverlay
	user           logicalOverlay
	attempted      logicalOverlay
	relationExtra  []logicalOverlay
	attemptedExtra []logicalOverlay
	relationMarks  []logicalOverlayMark
	attemptedMarks []logicalOverlayMark
	plan           commandPlanScratch
	state          []byte
	keys           []finalMutation
}

type normalBatchTelemetry struct {
	logicalValueReads          uint32
	physicalBaseValueReads     uint32
	logicalValueHashes         uint32
	logicalAfterValueHashes    uint32
	hybridClassificationPasses uint32
	hybridDescriptorRereads    uint32
	retainedBytes              uint64
}

var normalBatchWorkspacePool = sync.Pool{
	New: func() any { return new(normalBatchWorkspace) },
}

func (workspace *normalBatchWorkspace) release() uint64 {
	if workspace == nil {
		return 0
	}
	clear(workspace.keys)
	workspace.keys = workspace.keys[:0]
	workspace.system.release()
	workspace.user.release()
	workspace.attempted.release()
	relationBank := workspace.relationExtra[:cap(workspace.relationExtra)]
	attemptedBank := workspace.attemptedExtra[:cap(workspace.attemptedExtra)]
	for i := range relationBank {
		relationBank[i].release()
	}
	for i := range attemptedBank {
		attemptedBank[i].release()
	}
	workspace.plan.release()
	clear(workspace.relationMarks)
	clear(workspace.attemptedMarks)
	workspace.relationMarks = workspace.relationMarks[:0]
	workspace.attemptedMarks = workspace.attemptedMarks[:0]
	clear(workspace.state)
	workspace.state = workspace.state[:0]
	if cap(workspace.state) > maxNormalBatchRetainedBufferBytes {
		workspace.state = nil
	}
	if cap(workspace.keys) > maxNormalBatchRetainedOverlayEntries {
		workspace.keys = nil
	}
	retained := uint64(workspace.retainedBytes())
	workspace.relationExtra = workspace.relationExtra[:0]
	workspace.attemptedExtra = workspace.attemptedExtra[:0]
	return retained
}

func (workspace *normalBatchWorkspace) retainedBytes() uintptr {
	if workspace == nil {
		return 0
	}
	overlayBytes := func(overlay *logicalOverlay) uintptr {
		return uintptr(cap(overlay.slots))*unsafe.Sizeof(uint32(0)) +
			uintptr(cap(overlay.entries))*unsafe.Sizeof(logicalOverlayEntry{}) +
			uintptr(cap(overlay.arena)+cap(overlay.probe)) +
			uintptr(cap(overlay.order))*unsafe.Sizeof(int(0)) +
			uintptr(cap(overlay.undo))*unsafe.Sizeof(logicalOverlayUndo{})
	}
	plan := &workspace.plan
	retained := overlayBytes(&workspace.system) + overlayBytes(&workspace.user) +
		overlayBytes(&workspace.attempted)
	relationBank := workspace.relationExtra[:cap(workspace.relationExtra)]
	attemptedBank := workspace.attemptedExtra[:cap(workspace.attemptedExtra)]
	for i := range relationBank {
		retained += overlayBytes(&relationBank[i])
	}
	for i := range attemptedBank {
		retained += overlayBytes(&attemptedBank[i])
	}
	return retained +
		uintptr(cap(plan.sessionRead)+cap(plan.slotRead)+cap(plan.sessionRecord)+
			cap(plan.slotRecord)+cap(plan.currentValue)+cap(plan.decodeRead)+
			cap(workspace.state)) +
		uintptr(cap(plan.descriptors))*unsafe.Sizeof(mutationValueDescriptor{}) +
		uintptr(cap(workspace.relationMarks)+cap(workspace.attemptedMarks))*
			unsafe.Sizeof(logicalOverlayMark{}) +
		uintptr(cap(workspace.keys))*unsafe.Sizeof(finalMutation{})
}

func (workspace *normalBatchWorkspace) prepareRelationOverlays(
	snapshots relationPointSnapshots,
) bool {
	if workspace == nil || snapshots.count == 0 ||
		int(snapshots.count) > replication.MaxRelationsPerBundle {
		return false
	}
	extra := int(snapshots.count) - 1
	if cap(workspace.relationExtra) < extra {
		workspace.relationExtra = append(
			workspace.relationExtra[:cap(workspace.relationExtra)],
			make([]logicalOverlay, extra-cap(workspace.relationExtra))...,
		)
	} else {
		workspace.relationExtra = workspace.relationExtra[:extra]
	}
	if cap(workspace.attemptedExtra) < extra {
		workspace.attemptedExtra = append(
			workspace.attemptedExtra[:cap(workspace.attemptedExtra)],
			make([]logicalOverlay, extra-cap(workspace.attemptedExtra))...,
		)
	} else {
		workspace.attemptedExtra = workspace.attemptedExtra[:extra]
	}
	count := int(snapshots.count)
	if cap(workspace.relationMarks) < count {
		workspace.relationMarks = make([]logicalOverlayMark, count)
	} else {
		workspace.relationMarks = workspace.relationMarks[:count]
	}
	if cap(workspace.attemptedMarks) < count {
		workspace.attemptedMarks = make([]logicalOverlayMark, count)
	} else {
		workspace.attemptedMarks = workspace.attemptedMarks[:count]
	}
	workspace.user.reset(snapshots.values[0].value)
	workspace.attempted.reset(nil)
	for i := range extra {
		workspace.relationExtra[i].reset(snapshots.values[i+1].value)
		workspace.attemptedExtra[i].reset(nil)
	}
	return true
}

func (workspace *normalBatchWorkspace) relationOverlay(ordinal int) *logicalOverlay {
	if workspace == nil || ordinal < 0 {
		return nil
	}
	if ordinal == 0 {
		return &workspace.user
	}
	ordinal--
	if ordinal >= len(workspace.relationExtra) {
		return nil
	}
	return &workspace.relationExtra[ordinal]
}

func (workspace *normalBatchWorkspace) attemptedOverlay(ordinal int) *logicalOverlay {
	if workspace == nil || ordinal < 0 {
		return nil
	}
	if ordinal == 0 {
		return &workspace.attempted
	}
	ordinal--
	if ordinal >= len(workspace.attemptedExtra) {
		return nil
	}
	return &workspace.attemptedExtra[ordinal]
}

// ApplyNormalBatch plans a bounded consecutive normal-entry run against one
// coherent durable cut plus binary-key logical overlays, then publishes the
// exact accepted prefix through one CheckpointGroup transaction. It is
// available only on the replay-backed checkpoint lane. Ownership transitions
// are singleton control boundaries. Empty normal entries are ordinary
// batchable applied-index transitions. A selected physical batch contains at
// most one non-empty command for each stable session digest so every selected
// command outcome remains retry-readable until the caller settles it.
func (m *Machine) ApplyNormalBatch(
	entries []raftmodel.NormalApply,
	dataChainWitnesses [][32]byte,
) (int, raftmodel.Publication, error) {
	if len(entries) == 0 {
		return 0, raftmodel.Publication{}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.releaseMutationPlan()
	m.batchTelemetry = normalBatchTelemetry{}
	clear(dataChainWitnesses[:min(len(entries), len(dataChainWitnesses))])
	if err := m.checkUsable(); err != nil {
		return 0, raftmodel.Publication{}, err
	}
	if len(entries) > raftmodel.MaxNormalApplyBatchEntries ||
		len(dataChainWitnesses) < len(entries) {
		return 0, raftmodel.Publication{}, ErrAdmissionBound
	}
	totalBytes := 0
	for i := range entries {
		if len(entries[i].Data) > raftmodel.MaxNormalApplyBatchBytes-totalBytes {
			return 0, raftmodel.Publication{}, ErrAdmissionBound
		}
		totalBytes += len(entries[i].Data)
	}
	if m.checkpointGroup == nil || m.capture != nil || len(m.relations) == 0 {
		return 0, raftmodel.Publication{}, nil
	}
	first := entries[0]
	if first.Meta.Type != pb.EntryNormal || IsOwnershipTransition(first.Data) {
		return 0, raftmodel.Publication{}, nil
	}
	if !m.initialized {
		return 0, raftmodel.Publication{}, m.fail(fmt.Errorf(
			"%w: static bootstrap is not installed", ErrApplySequence,
		))
	}
	if first.Meta.Index <= m.state.Applied {
		return 0, raftmodel.Publication{}, nil
	}

	systemSnapshot, relationSnapshots, err := m.captureHotBundleApplyCutLocked()
	if err != nil {
		return 0, raftmodel.Publication{}, m.fail(err)
	}
	batch := normalBatchWorkspacePool.Get().(*normalBatchWorkspace)
	defer func() {
		telemetry := normalBatchTelemetry{
			logicalValueReads:          batch.plan.logicalValueReads,
			physicalBaseValueReads:     batch.plan.physicalBaseValueReads,
			logicalValueHashes:         batch.plan.logicalValueHashes,
			logicalAfterValueHashes:    batch.plan.logicalAfterValueHashes,
			hybridClassificationPasses: batch.plan.hybridClassificationPasses,
			hybridDescriptorRereads:    batch.plan.hybridDescriptorRereads,
		}
		telemetry.retainedBytes = batch.release()
		m.batchTelemetry = telemetry
		normalBatchWorkspacePool.Put(batch)
	}()
	batch.system.reset(systemSnapshot)
	if !batch.prepareRelationOverlays(relationSnapshots) {
		return 0, raftmodel.Publication{}, m.fail(errors.Join(
			ErrInconsistentSnapshot, m.applyCut.Close(),
		))
	}
	planningSnapshots := relationSnapshots
	for ordinal := range m.relations {
		planningSnapshots.values[ordinal].overlay = batch.relationOverlay(ordinal)
	}
	var found bool
	var stateErr error
	batch.state, found, stateErr = batch.system.appendRaw(
		batch.state[:0], stateKey,
	)
	if stateErr != nil || !found {
		if stateErr == nil {
			stateErr = ErrStateCorrupt
		}
		return 0, raftmodel.Publication{}, m.fail(errors.Join(stateErr, m.applyCut.Close()))
	}
	stateEnvelopeBytes := len(batch.state)
	batch.state = batch.state[:0]
	working := m.state
	planned := 0
	selectedSessions := 0
	// Stack-owned fixed binary scratch avoids a per-Machine density tax while
	// keeping duplicate-session selection allocation-free.
	var selectedSessionDigests [raftmodel.MaxNormalApplyBatchEntries][32]byte
	batch.plan.logicalValueReads = 0
	batch.plan.physicalBaseValueReads = 0
	batch.plan.logicalValueHashes = 0
	batch.plan.logicalAfterValueHashes = 0
	batch.plan.hybridClassificationPasses = 0
	batch.plan.hybridDescriptorRereads = 0
	var deferredErr error
	for index := range entries {
		entry := entries[index]
		meta, data := entry.Meta, entry.Data
		if meta.Type != pb.EntryNormal || IsOwnershipTransition(data) {
			break
		}
		if meta.Term == 0 || meta.Term == math.MaxUint64 {
			deferredErr = fmt.Errorf("%w: invalid normal metadata", ErrApplySequence)
			break
		}
		if len(data) > replication.MaxCommandBytes {
			deferredErr = ErrAdmissionBound
			break
		}
		if working.Applied == math.MaxUint64-1 || meta.Index != working.Applied+1 ||
			meta.Index == math.MaxUint64 || meta.Term < working.LastTerm {
			deferredErr = fmt.Errorf(
				"%w: have %d, entry %d", ErrApplySequence, working.Applied, meta.Index,
			)
			break
		}

		digest := normalEntryDigest(meta, data)
		next := m.nextBatchState(working, meta, digest)
		plan := commandPlan{dataChainDigest: working.DataChainDigest}
		if len(data) != 0 {
			command, openErr := replication.OpenCommand(data)
			if openErr != nil {
				deferredErr = openErr
				break
			}
			if !m.immutableBindingMatches(command) {
				deferredErr = ErrWrongBinding
				break
			}
			sessionDigest := SessionKey(command.Tenant, command.ClientID)
			seen := false
			for selected := 0; selected < selectedSessions; selected++ {
				if selectedSessionDigests[selected] == sessionDigest {
					seen = true
					break
				}
			}
			if seen {
				break
			}
			selectedSessionDigests[selectedSessions] = sessionDigest
			selectedSessions++
			plan, deferredErr = m.planBundleCommand(
				command, meta.Index, working,
				pointSnapshot{value: systemSnapshot, overlay: &batch.system},
				planningSnapshots,
				&batch.plan,
			)
			if deferredErr != nil {
				break
			}
			if deferredErr = applyCommandPlanToState(&next, plan); deferredErr != nil {
				break
			}
		}

		systemMark := batch.system.mark()
		for ordinal := range m.relations {
			batch.relationMarks[ordinal] = batch.relationOverlay(ordinal).mark()
			batch.attemptedMarks[ordinal] = batch.attemptedOverlay(ordinal).mark()
		}
		if deferredErr = m.recordBatchPlan(batch, plan); deferredErr != nil {
			batch.system.rollback(systemMark)
			for ordinal := range m.relations {
				batch.relationOverlay(ordinal).rollback(batch.relationMarks[ordinal])
				batch.attemptedOverlay(ordinal).rollback(batch.attemptedMarks[ordinal])
			}
			break
		}
		if !m.batchFits(batch, 1, len(stateKey)+stateEnvelopeBytes) {
			batch.system.rollback(systemMark)
			for ordinal := range m.relations {
				batch.relationOverlay(ordinal).rollback(batch.relationMarks[ordinal])
				batch.attemptedOverlay(ordinal).rollback(batch.attemptedMarks[ordinal])
			}
			deferredErr = ErrAdmissionBound
			break
		}
		batch.system.commit(systemMark)
		for ordinal := range m.relations {
			batch.relationOverlay(ordinal).commit(batch.relationMarks[ordinal])
			batch.attemptedOverlay(ordinal).commit(batch.attemptedMarks[ordinal])
		}
		working = next
		dataChainWitnesses[planned] = working.DataChainDigest
		planned++
	}
	var finalizeErr error
	if planned != 0 {
		batch.state, finalizeErr = AppendState(batch.state[:0], working)
		if finalizeErr == nil && len(batch.state) != stateEnvelopeBytes {
			finalizeErr = fmt.Errorf(
				"%w: normal state envelope changed size", ErrStateCorrupt,
			)
		}
		if finalizeErr == nil {
			finalizeErr = batch.system.record(stateKey, batch.state, false)
		}
		if finalizeErr == nil && !m.batchFits(batch, 0, 0) {
			finalizeErr = ErrAdmissionBound
		}
	}
	closeErr := m.applyCut.Close()
	if finalizeErr != nil || closeErr != nil {
		clear(dataChainWitnesses[:planned])
		return 0, raftmodel.Publication{}, m.fail(errors.Join(finalizeErr, closeErr))
	}
	if planned == 0 {
		if deferredErr == nil {
			return 0, raftmodel.Publication{}, nil
		}
		return 0, raftmodel.Publication{}, m.fail(deferredErr)
	}

	m.transitionMembers = m.transitionMembers[:0]
	m.transitionMembers = append(m.transitionMembers, durable.NamedCollection{
		Name: systemCollectionName, Collection: m.system.Collection,
		BatchDocumentsHint: batch.system.netDocuments,
	})
	for ordinal := range m.relations {
		overlay := batch.relationOverlay(ordinal)
		if overlay.netDocuments == 0 {
			continue
		}
		relation := &m.relations[ordinal]
		m.transitionMembers = append(m.transitionMembers, durable.NamedCollection{
			Name: relation.name, Collection: relation.target.Collection,
			BatchDocumentsHint: overlay.netDocuments,
		})
	}
	updateErr := m.checkpointGroup.UpdateConsecutive(
		entries[0].Meta.Index, working.Applied, m.transitionMembers,
		m.options.TxnLimits,
		func(transaction *durable.DatabaseBatch) error {
			systemBatch, batchErr := transaction.CollectionHandle(m.system.Collection)
			if batchErr != nil {
				return batchErr
			}
			if batchErr = batch.system.writeNet(systemBatch); batchErr != nil {
				return batchErr
			}
			for ordinal := range m.relations {
				overlay := batch.relationOverlay(ordinal)
				if overlay.netDocuments == 0 {
					continue
				}
				relationBatch, relationErr := transaction.CollectionHandle(
					m.relations[ordinal].target.Collection,
				)
				if relationErr != nil {
					return relationErr
				}
				if relationErr = overlay.writeNet(relationBatch); relationErr != nil {
					return relationErr
				}
			}
			return nil
		},
	)
	anyAttempted := false
	for ordinal := range m.relations {
		attempted := batch.attemptedOverlay(ordinal)
		if len(attempted.entries) == 0 {
			continue
		}
		anyAttempted = true
		observer := m.relations[ordinal].target.ObserveMutationAttempt
		if ordinal == 0 {
			// user remains the canonical singleton integration handle. Tests and
			// internal integrations may install its synchronous observer after
			// Open; the dense first-relation mirror is otherwise immutable.
			observer = m.user.ObserveMutationAttempt
		}
		if observer == nil {
			continue
		}
		batch.keys = attempted.appendAttempted(batch.keys[:0])
		observer(AttemptedMutationKeys{changes: batch.keys}, updateErr)
	}
	if updateErr != nil {
		clear(dataChainWitnesses[:planned])
		return 0, raftmodel.Publication{}, m.fail(updateErr)
	}

	if !anyAttempted && m.openedBundleImageCurrent() {
		m.openedImageApplied = working.Applied
		for ordinal := range m.relations {
			m.relations[ordinal].openedApplied = working.Applied
		}
	} else {
		m.openedImageApplied = 0
		m.openedImageGeneration = 0
		for ordinal := range m.relations {
			if len(batch.attemptedOverlay(ordinal).entries) == 0 {
				continue
			}
			m.relations[ordinal].openedImage = [32]byte{}
			m.relations[ordinal].openedApplied = 0
			m.relations[ordinal].openedGen = 0
		}
	}
	m.state = working
	m.initialized = true
	m.publication = publicationFromState(working)
	return planned, clonePublication(m.publication), nil
}

func (m *Machine) nextBatchState(
	current State,
	meta raftmodel.ApplyMeta,
	digest [32]byte,
) State {
	return State{
		Binding: current.Binding, Applied: meta.Index, LastTerm: meta.Term,
		LastKind: RecordNormal, LastEntryType: meta.Type, LastEntryDigest: digest,
		DataChainDigest: current.DataChainDigest, ConfState: current.ConfState,
		ApplyContractDigest:   current.ApplyContractDigest,
		ReplicaSetVersion:     current.ReplicaSetVersion,
		BootstrapDigest:       current.BootstrapDigest,
		SnapshotBaseDigest:    current.SnapshotBaseDigest,
		SessionCount:          current.SessionCount,
		SessionSlotCount:      current.SessionSlotCount,
		SessionEpochHighWater: current.SessionEpochHighWater,
	}
}

func (m *Machine) recordBatchPlan(
	batch *normalBatchWorkspace,
	plan commandPlan,
) error {
	if batch == nil {
		return ErrInvalidCollection
	}
	if plan.writeSession {
		if err := batch.system.record(plan.sessionKey[:], plan.sessionRecord, false); err != nil {
			return err
		}
	}
	if plan.writeSlot {
		if err := batch.system.record(plan.slotKey[:], plan.slotRecord, false); err != nil {
			return err
		}
	}
	if plan.deleteSession {
		if err := batch.system.record(plan.sessionKey[:], nil, true); err != nil {
			return err
		}
		for slot := uint16(0); slot < plan.deleteSlots; slot++ {
			key, keyErr := SessionSlotStorageKey(plan.sessionDigest, slot)
			if keyErr != nil {
				return keyErr
			}
			if err := batch.system.record(key[:], nil, true); err != nil {
				return err
			}
		}
	}
	if err := m.validateRelationChangePlan(plan.changes, plan.relations); err != nil {
		return err
	}
	for i := range plan.relations {
		span := plan.relations[i]
		ordinal := int(span.ordinal)
		relation := batch.relationOverlay(ordinal)
		attempted := batch.attemptedOverlay(ordinal)
		if relation == nil || attempted == nil {
			return ErrInvalidCollection
		}
		for mutationIndex := span.start; mutationIndex < span.end; mutationIndex++ {
			mutation := plan.changes[mutationIndex]
			if err := relation.recordMutation(mutation, batch.plan.descriptors); err != nil {
				return err
			}
			if err := attempted.record(mutation.key, nil, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Machine) batchFits(
	batch *normalBatchWorkspace,
	systemExtraDocuments, systemExtraBytes int,
) bool {
	if systemExtraDocuments < 0 || systemExtraBytes < 0 ||
		batch == nil ||
		batch.system.netDocuments > math.MaxInt-systemExtraDocuments ||
		batch.system.netBytes > math.MaxInt-systemExtraBytes {
		return false
	}
	systemDocuments := batch.system.netDocuments + systemExtraDocuments
	systemBytes := batch.system.netBytes + systemExtraBytes
	if systemDocuments == 0 ||
		systemDocuments > m.system.Limits.MaxDistinctMutations ||
		systemBytes > m.system.Limits.MaxBatchBytes {
		return false
	}
	collections := 1
	documents := systemDocuments
	bytes := systemBytes
	for ordinal := range m.relations {
		overlay := batch.relationOverlay(ordinal)
		if overlay == nil || overlay.netDocuments < 0 || overlay.netBytes < 0 {
			return false
		}
		limits := m.relations[ordinal].target.Limits
		if overlay.netDocuments > limits.MaxDistinctMutations ||
			overlay.netBytes > limits.MaxBatchBytes ||
			documents > math.MaxInt-overlay.netDocuments ||
			bytes > math.MaxInt-overlay.netBytes {
			return false
		}
		if overlay.netDocuments != 0 {
			collections++
		}
		documents += overlay.netDocuments
		bytes += overlay.netBytes
	}
	return collections <= m.options.TxnLimits.MaxCollections &&
		documents <= m.options.TxnLimits.MaxDocuments &&
		int64(bytes) <= m.options.TxnLimits.MaxBytes
}
