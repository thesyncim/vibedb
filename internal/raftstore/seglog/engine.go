package seglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	ErrWaveConflict = errors.New("seglog: wave ID reused with different payload")
	ErrRaftState    = errors.New("seglog: illegal Raft durable state")
	ErrBackpressure = fmt.Errorf("%w: durability capacity maintenance pending", ErrBounds)
)

const engineWaveGroup = ^uint64(0)

type WaveID [16]byte

type Entry struct {
	Index, Term                         uint64
	Type                                pb.EntryType
	Data                                []byte
	ExtentID, ExtentOffset, ExtentBytes uint64
	DataOffset, DataBytes               uint64
	dataOffset                          uint64
}
type HardState struct{ Term, Vote, Commit uint64 }
type Checkpoint struct {
	ID          [16]byte
	Index, Term uint64
}
type ReadyBatch struct {
	GroupID                     uint64
	BeginIncarnation            uint64
	NodeIncarnation, ReadyID    uint64
	ReadyDigest                 [16]byte
	ReplaceFrom                 uint64
	Entries                     []Entry
	Hard                        *HardState
	TruncateIndex, TruncateTerm uint64
	Checkpoint                  *Checkpoint
}
type Wave struct {
	ID      WaveID
	Batches []ReadyBatch
	// Blob is the wave-shared sequence of independently authenticated,
	// entry-aligned extents. Entry extent offsets are relative to Blob.
	Blob []byte
}

type EntryLocation struct {
	SegmentID, Offset, Bytes, Index, Term uint64
	Type                                  pb.EntryType
	DataOffset, DataBytes                 uint64
	BatchID                               WaveID
	ExtentID                              uint64
}
type GroupState struct {
	Hard                        HardState
	TruncateIndex, TruncateTerm uint64
	Checkpoint                  Checkpoint
	Entries                     []EntryLocation
	NodeIncarnation, ReadyID    uint64
	ReadyDigest                 [16]byte
	ReadyWaveID                 WaveID
}

type GroupSummary struct {
	LastIndex                uint64
	NodeIncarnation, ReadyID uint64
	ReadyDigest              [16]byte
}

type GroupMetadata struct {
	Hard                        HardState
	TruncateIndex, TruncateTerm uint64
	Checkpoint                  Checkpoint
	FirstIndex, LastIndex       uint64
	NodeIncarnation, ReadyID    uint64
	ReadyDigest                 [16]byte
}

type engineGroup struct {
	GroupState
	entryLimit          int
	sealed              []sealedRunRef
	lastIndex, lastTerm uint64
	buildSegmentID      uint64
	buildSlot           int
	owner               *Engine
	id                  uint64
	latestWaveID        WaveID
	latestWaveDigest    [32]byte
	latestWaveSequence  uint64
}
type segmentReader struct {
	id     uint64
	file   *os.File
	routes *LazyRouteReader
}

type sealRequest struct {
	base    metadataState
	pending SegmentMeta
	events  []segmentEvent
	hook    func(RotationPhase) error
	build   *segmentBuildArena
}

type segmentGroupBuild struct {
	GroupID uint64
	Final   sealedRunSummary
}

type segmentBuildArena struct {
	groups       []segmentGroupBuild
	lastSequence uint64
}

func groupSealSummary(group *engineGroup) sealedRunSummary {
	lastIndex, lastTerm := durableLast(group), group.lastTerm
	if len(group.Entries) != 0 {
		lastTerm = group.Entries[len(group.Entries)-1].Term
	} else if lastIndex == group.Checkpoint.Index {
		lastTerm = group.Checkpoint.Term
	} else if lastIndex == group.TruncateIndex {
		lastTerm = group.TruncateTerm
	}
	return sealedRunSummary{LastIndex: lastIndex, LastTerm: lastTerm, Hard: group.Hard, TruncateIndex: group.TruncateIndex, TruncateTerm: group.TruncateTerm, Checkpoint: group.Checkpoint, NodeIncarnation: group.NodeIncarnation, ReadyID: group.ReadyID, ReadyDigest: group.ReadyDigest, ReadyWaveID: group.ReadyWaveID, LatestWaveID: group.latestWaveID, LatestWaveDigest: group.latestWaveDigest, LatestWaveSequence: group.latestWaveSequence}
}

func (arena *segmentBuildArena) touch(group *engineGroup, segmentID uint64) bool {
	if group.buildSegmentID == segmentID {
		return group.buildSlot >= 0 && group.buildSlot < len(arena.groups)
	}
	if len(arena.groups) == cap(arena.groups) {
		return false
	}
	group.buildSegmentID, group.buildSlot = segmentID, len(arena.groups)
	arena.groups = append(arena.groups, segmentGroupBuild{GroupID: group.id, Final: groupSealSummary(group)})
	return true
}

func (arena *segmentBuildArena) update(group *engineGroup, segmentID uint64) bool {
	if group.buildSegmentID != segmentID || group.buildSlot < 0 || group.buildSlot >= len(arena.groups) {
		return false
	}
	arena.groups[group.buildSlot].Final = groupSealSummary(group)
	return true
}

func (arena *segmentBuildArena) clear() {
	arena.groups = arena.groups[:0]
	arena.lastSequence = 0
}

type Engine struct {
	writeMu             sync.Mutex
	sealRequests        chan sealRequest
	reclaimRequests     chan chan error
	sealResults         chan error
	sealStop            chan struct{}
	sealerDone          chan struct{}
	sealPending         bool
	maintenanceBusy     bool
	activeBuild         *segmentBuildArena
	spareBuild          *segmentBuildArena
	sealBuildHookTest   func()
	sealOpenHookTest    func(string)
	log                 *Log
	groups              map[uint64]*engineGroup
	waves               map[WaveID]waveState
	waveLimit           int
	sealHeadroom        uint64
	sequence            uint64
	frameBuf            []byte
	eventScratch        []segmentEvent
	syncData            func(*os.File) error
	writeAt             func(*os.File, []byte, int64) (int, error)
	readers             []segmentReader
	readerNext          int
	authMAC             hash.Hash
	authKey             [32]byte
	authContext         [40]byte
	authSum             [32]byte
	sealSummaryOverride map[uint64]sealedRunSummary
	recoveryIO          *recoveryIOCounters
	liveSealed          map[uint64]uint32
	reclaimAfter        []uint64
	sealedSummaries     map[uint64]sealedRunSummary
	sealedSummaryOrder  []uint64
	sealedSequence      uint64
	applySequence       uint64
	entryArenas         [2][]EntryLocation
	entryArenaActive    uint8
	entryArenaReady     bool
}

type waveState struct {
	digest   [32]byte
	sequence uint64
	refs     uint32
}

type recoveryIOCounters struct {
	pendingPayloadBytes                                       uint64
	activeScanBytes                                           uint64
	sealedMetadataBytes, sealedMetadataReads                  uint64
	pendingSealBytes                                          uint64
	pendingPromotions                                         uint64
	maintenanceReserveAttempts, maintenanceCheckpointAttempts uint64
}

func CreateEngineAuthenticated(dir string, logID [16]byte, authKey [32]byte, segmentCapacity uint64) (*Engine, error) {
	if logID == ([16]byte{}) || authKey == ([32]byte{}) || segmentCapacity < segmentHeaderBytes || segmentCapacity >= 1<<32 {
		return nil, ErrBounds
	}
	l, err := createLog(dir, logID, authKey, segmentCapacity)
	if err != nil {
		return nil, err
	}
	e := &Engine{log: l, groups: make(map[uint64]*engineGroup), waves: make(map[WaveID]waveState), liveSealed: make(map[uint64]uint32), reclaimAfter: make([]uint64, len(l.state.Segments), cap(l.state.Segments)), sealedSummaries: make(map[uint64]sealedRunSummary), syncData: syncActiveData, writeAt: func(f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }, authMAC: hmac.New(sha256.New, authKey[:]), authKey: authKey}
	e.startSealer()
	return e, nil
}

func (e *Engine) startSealer() {
	e.sealRequests = make(chan sealRequest, 1)
	e.reclaimRequests = make(chan chan error, 1)
	e.sealResults = make(chan error, 1)
	e.sealStop = make(chan struct{})
	e.sealerDone = make(chan struct{})
	go func() {
		defer close(e.sealerDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case request := <-e.sealRequests:
				err := e.finishFrozenSeal(request.base, request.pending, request.events, request.build, request.hook, false)
				if err != nil {
					e.writeMu.Lock()
					e.log.poison(err)
					e.writeMu.Unlock()
				}
				e.sealResults <- err
			case result := <-e.reclaimRequests:
				result <- e.reclaimDeadPrefix()
			case <-ticker.C:
				e.runMetadataMaintenance()
			case <-e.sealStop:
				return
			}
		}
	}()
}

func (e *Engine) waveDigest(payload []byte, sequence uint64, id WaveID) [32]byte {
	// The nil-state path is restricted to the internal encode-only sizing seam;
	// every creatable/openable Engine installs the keyed MAC and a Log.
	if e.authMAC == nil || e.log == nil {
		return sha256.Sum256(payload)
	}
	e.authMAC.Reset()
	copy(e.authContext[:16], e.log.state.LogID[:])
	binary.LittleEndian.PutUint64(e.authContext[16:24], sequence)
	copy(e.authContext[24:40], id[:])
	_, _ = e.authMAC.Write(e.authContext[:])
	_, _ = e.authMAC.Write(payload)
	_ = e.authMAC.Sum(e.authSum[:0])
	return e.authSum
}

func (e *Engine) Close() error {
	var err error
	if e.sealPending {
		err = <-e.sealResults
		e.sealPending = false
	}
	if e.sealStop != nil {
		close(e.sealStop)
		<-e.sealerDone
		e.sealStop = nil
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	for i := range e.readers {
		if e.readers[i].file != nil {
			err = errors.Join(err, e.readers[i].file.Close())
			e.readers[i] = segmentReader{}
		}
	}
	return errors.Join(err, e.log.Close())
}

// WaitSeal is a control-plane fence for rotation tests, shutdown, and callers
// that need the frozen segment published before reclamation. PersistWave does
// not wait for it.
func (e *Engine) WaitSeal() error {
	e.writeMu.Lock()
	if !e.sealPending {
		err := e.log.usable()
		e.writeMu.Unlock()
		return err
	}
	e.writeMu.Unlock()
	err := <-e.sealResults
	e.writeMu.Lock()
	e.sealPending = false
	if err == nil {
		err = e.log.usable()
	}
	e.writeMu.Unlock()
	return err
}

// ReserveReaders fixes the maximum number of retained sealed descriptors.
// Cache misses are explicit control-plane operations through PrepareSegment.
func (e *Engine) ReserveReaders(count int) error {
	if err := e.log.usable(); err != nil {
		return err
	}
	if count < 0 {
		return ErrBounds
	}
	for i := count; i < len(e.readers); i++ {
		if e.readers[i].file != nil {
			_ = e.readers[i].file.Close()
		}
	}
	if count != len(e.readers) {
		e.readers = make([]segmentReader, count)
		e.readerNext = 0
	}
	return nil
}

func (e *Engine) PrepareSegment(segmentID uint64) error {
	if err := e.log.usable(); err != nil {
		return err
	}
	if segmentID == e.log.state.ActiveID {
		return nil
	}
	for i := range e.readers {
		if e.readers[i].id == segmentID && e.readers[i].file != nil {
			return nil
		}
	}
	found := false
	for _, meta := range e.log.state.Segments {
		if meta.ID == segmentID {
			found = true
			break
		}
	}
	if !found || len(e.readers) == 0 {
		return ErrBounds
	}
	fileID, ok := e.fileIDForSegment(segmentID)
	if !ok {
		return ErrBounds
	}
	f, err := os.Open(segmentPath(e.log.dir, fileID))
	if err != nil {
		return err
	}
	slot := e.readerNext % len(e.readers)
	e.readerNext++
	if e.readers[slot].file != nil {
		_ = e.readers[slot].file.Close()
	}
	routes, err := newLazyRouteReader(f, e.authKey, e.log.state.LogID, segmentID, 4, true)
	if err != nil {
		_ = f.Close()
		return err
	}
	e.readers[slot] = segmentReader{id: segmentID, file: f, routes: routes}
	return nil
}

func (e *Engine) fileIDForSegment(segmentID uint64) (fileID, bool) {
	if segmentID <= e.log.state.AnchorID {
		return fileID{}, false
	}
	ordinal := segmentID - e.log.state.AnchorID - 1
	if ordinal >= uint64(len(e.log.state.Segments)) {
		return fileID{}, false
	}
	meta := e.log.state.Segments[ordinal]
	if meta.ID != segmentID || meta.FileID == (fileID{}) {
		return fileID{}, false
	}
	return meta.FileID, true
}

// ReadLocation reads exactly one entry value without decoding its containing
// wave. Returned bytes alias dst and remain valid until the caller reuses dst.
func (e *Engine) ReadLocation(location EntryLocation, dst []byte) ([]byte, error) {
	if err := e.log.usable(); err != nil {
		return nil, err
	}
	if location.Bytes > uint64(len(dst)) {
		return nil, ErrBounds
	}
	var file *os.File
	if location.SegmentID == e.log.state.ActiveID {
		file = e.log.active
	} else {
		for i := range e.readers {
			if e.readers[i].id == location.SegmentID {
				file = e.readers[i].file
				break
			}
		}
	}
	if file == nil {
		return nil, ErrBounds
	}
	result := dst[:location.Bytes]
	if _, err := file.ReadAt(result, int64(location.Offset)); err != nil {
		return nil, err
	}
	return result, nil
}

// Reserve moves every unbounded allocation off the steady PersistWave path.
func (e *Engine) Reserve(frameBytes, events, waveIDs int) error {
	return e.reserve(frameBytes, events, waveIDs, nil)
}

// ReserveWithFrameArena installs caller-owned frame scratch. This is intended
// for fused storage stacks that can reuse plaintext after encryption instead
// of retaining a third maximum-wave buffer solely for log framing. The caller
// must keep the arena alive and must not use it concurrently with PersistWave.
func (e *Engine) ReserveWithFrameArena(frame []byte, events, waveIDs int) error {
	if cap(frame) < 72 {
		return ErrBounds
	}
	return e.reserve(cap(frame), events, waveIDs, frame[:0])
}

func (e *Engine) reserve(frameBytes, events, waveIDs int, frame []byte) error {
	if err := e.log.usable(); err != nil {
		return err
	}
	if frameBytes < 72 || events < len(e.eventScratch) || events < len(e.log.events) || waveIDs < len(e.waves) {
		return ErrBounds
	}
	// The seal/index bound is charged before accepting any append. Route
	// descriptors, compact payload, top directory, and footer are conservatively
	// bounded here; actual seal geometry is checked again before publication.
	headroom := uint64(sealedIndexHeaderBytes+segmentFooterBytes) + uint64(events)*96 + 4096
	if headroom < e.sealHeadroom && e.log.activeOffset > segmentHeaderBytes {
		return ErrBounds
	}
	if uint64(frameBytes) > e.log.state.SegmentCapacity-segmentHeaderBytes || headroom >= e.log.state.SegmentCapacity-segmentHeaderBytes || uint64(frameBytes)+headroom > e.log.state.SegmentCapacity-segmentHeaderBytes {
		return ErrBounds
	}
	e.sealHeadroom = headroom
	if frame != nil {
		e.frameBuf = frame
	} else if frameBytes > cap(e.frameBuf) {
		e.frameBuf = make([]byte, 0, frameBytes)
	}
	if events > cap(e.eventScratch) {
		e.eventScratch = make([]segmentEvent, 0, events)
	}
	if events > cap(e.log.events) {
		if err := e.log.ReserveEvents(events); err != nil {
			return err
		}
	}
	if events > len(e.entryArenas[0]) {
		first := make([]EntryLocation, events)
		second := make([]EntryLocation, events)
		e.entryArenas = [2][]EntryLocation{first, second}
		e.entryArenaActive = 0
		e.entryArenaReady = false
		if !e.repackEntryArena(Wave{}) {
			return ErrBounds
		}
	}
	if err := e.reserveBuildGroups(min(len(e.groups), events)); err != nil {
		return err
	}
	if waveIDs > len(e.waves) {
		replacement := make(map[WaveID]waveState, waveIDs)
		for id, state := range e.waves {
			replacement[id] = state
		}
		e.waves = replacement
	}
	e.waveLimit = waveIDs
	return nil
}

// reserveBuildGroups sizes seal summaries by registered groups, not segment
// events. Each group occupies at most one summary slot in a segment regardless
// of how many entries or metadata events it appends.
func (e *Engine) reserveBuildGroups(groups int) error {
	if groups < 0 || e.log == nil || groups > cap(e.log.events) {
		return ErrBounds
	}
	capacity := entryArenaCapacity(groups, cap(e.log.events))
	if e.activeBuild == nil || cap(e.activeBuild.groups) < groups {
		// Rebuilding the active arena must also move the per-group slot tags.
		// A tag points into one concrete arena, so retaining it while replacing
		// that arena would silently omit the group from the frozen summary.
		for i := range e.log.events {
			if e.log.events[i].GroupID == engineWaveGroup {
				continue
			}
			if group := e.groups[e.log.events[i].GroupID]; group != nil && group.buildSegmentID == e.log.state.ActiveID {
				group.buildSegmentID = 0
				group.buildSlot = 0
			}
		}
		e.activeBuild = &segmentBuildArena{groups: make([]segmentGroupBuild, 0, capacity)}
		for i := range e.log.events {
			if e.log.events[i].GroupID == engineWaveGroup {
				continue
			}
			group := e.groups[e.log.events[i].GroupID]
			if group != nil && group.buildSegmentID != e.log.state.ActiveID {
				if !e.activeBuild.touch(group, e.log.state.ActiveID) {
					return ErrBounds
				}
			}
		}
	}
	if e.spareBuild == nil || cap(e.spareBuild.groups) < groups {
		e.spareBuild = &segmentBuildArena{groups: make([]segmentGroupBuild, 0, capacity)}
	}
	return nil
}

func (e *Engine) ReserveGroup(group uint64, entries int) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.log.usable(); err != nil {
		return err
	}
	if group == 0 || group == engineWaveGroup || entries < 0 {
		return ErrBounds
	}
	g := e.groups[group]
	if g == nil {
		buildGroups := min(len(e.groups)+1, cap(e.log.events))
		if err := e.reserveBuildGroups(buildGroups); err != nil {
			return err
		}
		e.groups[group] = &engineGroup{entryLimit: entries, owner: e, id: group}
		return nil
	}
	if entries < len(g.Entries) {
		return ErrBounds
	}
	g.entryLimit = entries
	return nil
}

func waveGroupEntryNeed(group *engineGroup, wave Wave) (int, bool) {
	if group == nil {
		return 0, false
	}
	for index := range wave.Batches {
		batch := &wave.Batches[index]
		if batch.GroupID != group.id {
			continue
		}
		return len(group.Entries) - replacementCount(group.Entries, batch.ReplaceFrom) + len(batch.Entries), true
	}
	return len(group.Entries), false
}

func entryArenaCapacity(needed, limit int) int {
	if needed <= 0 {
		return 0
	}
	capacity := 1
	for capacity < needed && capacity <= limit/2 {
		capacity *= 2
	}
	if capacity < needed || capacity > limit {
		return needed
	}
	return capacity
}

// repackEntryArena moves every active-segment location between two fixed
// node-wide arenas. The arenas are bounded by the segment event reservation;
// no group receives its worst-case slice until it actually becomes hot.
func (e *Engine) repackEntryArena(wave Wave) bool {
	if len(e.entryArenas[0]) == 0 || len(e.entryArenas[1]) != len(e.entryArenas[0]) {
		return false
	}
	total := 0
	for _, group := range e.groups {
		needed, targeted := waveGroupEntryNeed(group, wave)
		if targeted && (group.entryLimit == 0 || needed > group.entryLimit) {
			return false
		}
		capacity := len(group.Entries)
		if e.entryArenaReady {
			capacity = cap(group.Entries)
		}
		if targeted && capacity < needed {
			capacity = entryArenaCapacity(needed, group.entryLimit)
		}
		if capacity > len(e.entryArenas[0])-total {
			total = len(e.entryArenas[0]) + 1
			break
		}
		total += capacity
	}
	compact := total > len(e.entryArenas[0])
	if compact {
		total = 0
		for _, group := range e.groups {
			needed, targeted := waveGroupEntryNeed(group, wave)
			capacity := len(group.Entries)
			if targeted && capacity < needed {
				capacity = entryArenaCapacity(needed, group.entryLimit)
			}
			if capacity > len(e.entryArenas[0])-total {
				return false
			}
			total += capacity
		}
	}
	target := e.entryArenaActive ^ 1
	arena, cursor := e.entryArenas[target], 0
	for _, group := range e.groups {
		needed, targeted := waveGroupEntryNeed(group, wave)
		capacity := len(group.Entries)
		if e.entryArenaReady && !compact {
			capacity = cap(group.Entries)
		}
		if targeted && capacity < needed {
			capacity = entryArenaCapacity(needed, group.entryLimit)
		}
		copy(arena[cursor:cursor+len(group.Entries)], group.Entries)
		group.Entries = arena[cursor : cursor+len(group.Entries) : cursor+capacity]
		cursor += capacity
	}
	e.entryArenaActive, e.entryArenaReady = target, true
	return true
}

func (e *Engine) ensureWaveEntryCapacity(wave Wave) bool {
	allFit := true
	for index := range wave.Batches {
		group := e.groups[wave.Batches[index].GroupID]
		needed, _ := waveGroupEntryNeed(group, wave)
		if group == nil || group.entryLimit == 0 || needed > group.entryLimit {
			return false
		}
		allFit = allFit && needed <= cap(group.Entries)
	}
	return allFit || e.repackEntryArena(wave)
}

func (e *Engine) Group(group uint64) (GroupState, bool) {
	if e == nil || e.log.usable() != nil {
		return GroupState{}, false
	}
	g, ok := e.groups[group]
	if !ok {
		return GroupState{}, false
	}
	result := g.GroupState
	sealedEntries := 0
	for i := range g.sealed {
		sealedEntries += int(g.sealed[i].Last - g.sealed[i].First + 1)
	}
	result.Entries = make([]EntryLocation, 0, sealedEntries+len(g.Entries))
	for i := range g.sealed {
		run := g.sealed[i]
		for index := run.First; index <= run.Last; index++ {
			location, _, compacted, found := e.Lookup(group, index)
			if compacted {
				continue
			}
			if !found {
				return GroupState{}, false
			}
			result.Entries = append(result.Entries, location)
			if index == ^uint64(0) {
				break
			}
		}
	}
	result.Entries = append(result.Entries, g.Entries...)
	return result, true
}

// Summary returns the mutation admission cursor without cloning entry indexes.
func (e *Engine) Summary(group uint64) (GroupSummary, bool) {
	if e == nil || e.log.usable() != nil {
		return GroupSummary{}, false
	}
	g, ok := e.groups[group]
	if !ok {
		return GroupSummary{}, false
	}
	last := durableLast(g)
	return GroupSummary{LastIndex: last, NodeIncarnation: g.NodeIncarnation, ReadyID: g.ReadyID, ReadyDigest: g.ReadyDigest}, true
}

func (e *Engine) Metadata(group uint64) (GroupMetadata, bool) {
	if e == nil || e.log.usable() != nil {
		return GroupMetadata{}, false
	}
	g, ok := e.groups[group]
	if !ok {
		return GroupMetadata{}, false
	}
	base := max(g.Checkpoint.Index, g.TruncateIndex)
	return GroupMetadata{Hard: g.Hard, TruncateIndex: g.TruncateIndex, TruncateTerm: g.TruncateTerm, Checkpoint: g.Checkpoint, FirstIndex: base + 1, LastIndex: durableLast(g), NodeIncarnation: g.NodeIncarnation, ReadyID: g.ReadyID, ReadyDigest: g.ReadyDigest}, true
}

// GroupIDs returns recovered group IDs in canonical order. The allocation is
// confined to the Open/control path.
func (e *Engine) GroupIDs() []uint64 {
	ids := make([]uint64, 0, len(e.groups))
	for id := range e.groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// Lookup returns one immutable location without cloning the group's index.
// compacted distinguishes a missing historical index from an unavailable one.
func (e *Engine) Lookup(group, index uint64) (location EntryLocation, term uint64, compacted, ok bool) {
	location, term, compacted, ok, err := e.LookupExact(group, index)
	if err != nil {
		_ = e.log.poison(err)
		return EntryLocation{}, 0, false, false
	}
	return location, term, compacted, ok
}

func (e *Engine) LookupExact(group, index uint64) (location EntryLocation, term uint64, compacted, ok bool, err error) {
	if e == nil || e.log.usable() != nil {
		return EntryLocation{}, 0, false, false, ErrPoisoned
	}
	g, exists := e.groups[group]
	if !exists {
		return EntryLocation{}, 0, false, false, nil
	}
	base := max(g.Checkpoint.Index, g.TruncateIndex)
	if index < base {
		return EntryLocation{}, 0, true, false, nil
	}
	if index == base {
		baseTerm := g.TruncateTerm
		if g.Checkpoint.Index == base {
			baseTerm = g.Checkpoint.Term
		}
		return EntryLocation{}, baseTerm, false, true, nil
	}
	position := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= index })
	if position < len(g.Entries) && g.Entries[position].Index == index {
		location = g.Entries[position]
		return location, location.Term, false, true, nil
	}
	for i := len(g.sealed) - 1; i >= 0; i-- {
		run := g.sealed[i]
		if index < run.First || index > run.Last {
			continue
		}
		if err := e.PrepareSegment(run.SegmentID); err != nil {
			return EntryLocation{}, 0, false, false, err
		}
		for slot := range e.readers {
			reader := &e.readers[slot]
			if reader.id != run.SegmentID || reader.routes == nil {
				continue
			}
			route, err := reader.routes.Point(run, index)
			if err != nil {
				return EntryLocation{}, 0, false, false, err
			}
			return EntryLocation{SegmentID: run.SegmentID, Offset: route.ExtentOffset, Bytes: route.ExtentBytes, Index: index, Term: route.Term, Type: pb.EntryType(route.Type), DataOffset: route.DataOffset, DataBytes: route.DataBytes, BatchID: route.BatchID, ExtentID: route.ExtentID}, route.Term, false, true, nil
		}
	}
	return EntryLocation{}, 0, false, false, nil
}

func (e *Engine) Sequence() uint64 {
	if e == nil || e.log.usable() != nil {
		return 0
	}
	return e.sequence
}

func (e *Engine) LogID() [16]byte { return e.log.state.LogID }

// FatalError reports whether an ambiguous storage mutation poisoned the
// handle. A nil result means a returned operation error was pre-mutation.
func (e *Engine) FatalError() error {
	if e == nil || e.log == nil {
		return os.ErrClosed
	}
	return e.log.usable()
}

// SetDataSyncForTesting installs a deterministic durability fault seam.
func (e *Engine) SetDataSyncForTesting(sync func(*os.File) error) { e.syncData = sync }

// PersistWave makes a caller-sorted, multi-group Ready wave durable with one
// append and one data-sync. The frame is the acknowledgement boundary: its
// checksum and canonical payload digest cover every batch, so recovery applies
// all batches or none. A sync error poisons this handle because the complete
// frame may nevertheless be durable; OpenEngine recovers it, after which an
// exact WaveID+payload retry is a no-op. Reusing the ID for other bytes is fatal.
//
// Callers must reserve buffers, group entry capacity, and WaveID map capacity
// before entering the steady path. They must also supply collision-resistant
// nonzero WaveIDs and strictly increasing GroupIDs within each wave. Success is
// the fence before dependent messages or Ready completion may be published.
func (e *Engine) PersistWave(w Wave) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := e.log.usable(); err != nil {
		return err
	}
	if w.ID == (WaveID{}) || len(w.Batches) == 0 {
		return ErrBounds
	}
	if previous, ok := e.waves[w.ID]; ok {
		encoded, _, _, err := e.prepareWave(w, false)
		if err != nil {
			return e.log.poison(ErrWaveConflict)
		}
		digest := e.waveDigest(encoded[72:], previous.sequence, w.ID)
		if previous.digest == digest {
			return nil
		}
		return e.log.poison(ErrWaveConflict)
	}
	if e.waveLimit == 0 || len(e.waves)-e.reclaimedWaveStates(w.Batches)+1 > e.waveLimit {
		return ErrBounds
	}
	frame, _, events, err := e.prepareWave(w, true)
	if err != nil {
		return err
	}
	entriesFit := e.ensureWaveEntryCapacity(w)
	if !entriesFit || !e.waveFitsActive(w, len(frame), len(events)) {
		if e.log.activeOffset == segmentHeaderBytes {
			if e.sealPending {
				return ErrBackpressure
			}
			return ErrBounds
		}
		if err = e.rotateLocked(nil); err != nil {
			return err
		}
		if !e.ensureWaveEntryCapacity(w) || !e.waveFitsActive(w, len(frame), len(events)) {
			// Frozen locations remain in the group indexes until the asynchronous
			// seal publishes its compact routes. The device sequencer waits on the
			// seal and retries the exact WaveID; no accepted Ready is discarded.
			return ErrBackpressure
		}
	}
	for i := range w.Batches {
		if !e.activeBuild.touch(e.groups[w.Batches[i].GroupID], e.log.state.ActiveID) {
			return ErrBounds
		}
	}
	offset := e.log.activeOffset
	sequence := e.sequence + 1
	sealWaveHeader(frame, sequence, w.ID)
	n, err := e.writeAt(e.log.active, frame, int64(offset))
	if err != nil {
		return e.log.poison(err)
	}
	if n != len(frame) {
		return e.log.poison(io.ErrShortWrite)
	}
	if _, err = e.log.activeHash.Write(frame); err != nil {
		return e.log.poison(err)
	}
	if err = e.syncData(e.log.active); err != nil {
		return e.log.poison(err)
	}
	e.log.activeOffset += uint64(n)
	e.log.records++
	for i := range events {
		if _, ok := typeForEventKind(events[i].Kind); ok {
			events[i].Offset += offset
		}
	}
	e.log.events = append(e.log.events, events...)
	e.applySequence = sequence
	for _, event := range events {
		if err = e.applyEvent(event, e.log.state.ActiveID); err != nil {
			return e.log.poison(err)
		}
	}
	e.applySequence = 0
	return nil
}

func (e *Engine) waveFitsActive(w Wave, frameBytes, events int) bool {
	if e.sealHeadroom == 0 || frameBytes < 0 || events < 0 ||
		len(e.log.events)+events > cap(e.log.events) ||
		e.log.activeOffset > e.log.state.SegmentCapacity-e.sealHeadroom ||
		uint64(frameBytes) > e.log.state.SegmentCapacity-e.sealHeadroom-e.log.activeOffset {
		return false
	}
	missingBuildGroups := 0
	for i := range w.Batches {
		batch := &w.Batches[i]
		group := e.groups[batch.GroupID]
		if group == nil || len(group.Entries)-replacementCount(group.Entries, batch.ReplaceFrom)+len(batch.Entries) > cap(group.Entries) {
			return false
		}
		if group.buildSegmentID != e.log.state.ActiveID {
			missingBuildGroups++
		}
	}
	return e.activeBuild != nil && missingBuildGroups <= cap(e.activeBuild.groups)-len(e.activeBuild.groups)
}

func (e *Engine) reclaimedWaveStates(batches []ReadyBatch) int {
	reclaimed := 0
	for i := range batches {
		group := e.groups[batches[i].GroupID]
		if group == nil || group.latestWaveID == (WaveID{}) {
			continue
		}
		old := group.latestWaveID
		seen := false
		refs := uint32(0)
		for j := 0; j < i; j++ {
			other := e.groups[batches[j].GroupID]
			if other != nil && other.latestWaveID == old {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		for j := i; j < len(batches); j++ {
			other := e.groups[batches[j].GroupID]
			if other != nil && other.latestWaveID == old {
				refs++
			}
		}
		if state, ok := e.waves[old]; ok && refs == state.refs {
			reclaimed++
		}
	}
	return reclaimed
}

func (e *Engine) prepareWave(w Wave, validateState bool) ([]byte, [32]byte, []segmentEvent, error) {
	e.frameBuf = e.frameBuf[:0]
	encodedBytes, eventCount, err := waveSize(w)
	if err != nil {
		return nil, [32]byte{}, nil, err
	}
	if validateState && (e.log == nil || e.activeBuild == nil) {
		return nil, [32]byte{}, nil, ErrBounds
	}
	if encodedBytes > cap(e.frameBuf) || eventCount > cap(e.eventScratch) || validateState && eventCount > cap(e.log.events) {
		return nil, [32]byte{}, nil, ErrBounds
	}
	if validateState {
		missing := 0
		for i := range w.Batches {
			group := e.groups[w.Batches[i].GroupID]
			if group == nil || group.buildSegmentID != e.log.state.ActiveID {
				missing++
			}
		}
		if missing > cap(e.activeBuild.groups) {
			return nil, [32]byte{}, nil, ErrBounds
		}
	}
	e.frameBuf = e.frameBuf[:72]
	clear(e.frameBuf)
	e.eventScratch = e.eventScratch[:0]
	previousGroup := uint64(0)
	e.frameBuf = appendUvarint(e.frameBuf, uint64(len(w.Batches)))
	for batchIndex := range w.Batches {
		batch := &w.Batches[batchIndex]
		if batch.GroupID == 0 || batch.GroupID == engineWaveGroup || batch.GroupID <= previousGroup {
			return nil, [32]byte{}, nil, ErrRaftState
		}
		group := e.groups[batch.GroupID]
		if validateState && group == nil {
			return nil, [32]byte{}, nil, ErrBounds
		}
		flags := batchFlags(batch)
		if validateState {
			var err error
			flags, err = validateBatch(group, batch)
			if err != nil {
				return nil, [32]byte{}, nil, err
			}
		}
		needed := len(batch.Entries) + 1 // generic latest-wave ref for this group
		if batch.ReplaceFrom != 0 {
			needed++
		}
		if batch.TruncateIndex != 0 {
			needed++
		}
		if batch.Checkpoint != nil {
			needed++
		}
		if batch.Hard != nil {
			needed++
		}
		if flags&batchIdentity != 0 {
			needed++
		}
		if flags&batchBegin != 0 {
			needed++
		}
		if len(e.eventScratch)+needed+1 > cap(e.eventScratch) || validateState && len(batch.Entries) > group.entryLimit {
			return nil, [32]byte{}, nil, ErrBounds
		}
		e.frameBuf = appendUvarint(e.frameBuf, batch.GroupID-previousGroup)
		e.frameBuf = append(e.frameBuf, flags)
		if validateState {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventWaveRef, GroupID: batch.GroupID, Index: e.sequence + 1, Reference: w.ID})
		}
		if flags&batchIdentity != 0 {
			e.frameBuf = appendUvarint(e.frameBuf, batch.NodeIncarnation)
			e.frameBuf = appendUvarint(e.frameBuf, batch.ReadyID)
			e.frameBuf = append(e.frameBuf, batch.ReadyDigest[:]...)
			if validateState {
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventReadyState, GroupID: batch.GroupID, Incarnation: batch.NodeIncarnation, ReadyID: batch.ReadyID, ReadyDigest: batch.ReadyDigest, Reference: w.ID})
			}
		}
		if flags&batchBegin != 0 {
			e.frameBuf = appendUvarint(e.frameBuf, batch.BeginIncarnation)
			if validateState {
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventIncarnation, GroupID: batch.GroupID, Incarnation: batch.BeginIncarnation})
			}
		}
		if batch.ReplaceFrom != 0 {
			e.frameBuf = appendUvarint(e.frameBuf, batch.ReplaceFrom)
			if validateState {
				predecessor, termErr := predecessorTerm(group, batch.ReplaceFrom)
				if termErr != nil {
					return nil, [32]byte{}, nil, termErr
				}
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: RecordTruncateSuffix, GroupID: batch.GroupID, Index: batch.ReplaceFrom, Term: predecessor})
			}
		}
		if batch.TruncateIndex != 0 {
			e.frameBuf = appendUvarint(e.frameBuf, batch.TruncateIndex)
			e.frameBuf = appendUvarint(e.frameBuf, batch.TruncateTerm)
		}
		if batch.Checkpoint != nil {
			e.frameBuf = append(e.frameBuf, batch.Checkpoint.ID[:]...)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Checkpoint.Index)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Checkpoint.Term)
		}
		if batch.Hard != nil {
			e.frameBuf = appendUvarint(e.frameBuf, batch.Hard.Term)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Hard.Vote)
			e.frameBuf = appendUvarint(e.frameBuf, batch.Hard.Commit)
		}
		e.frameBuf = appendUvarint(e.frameBuf, uint64(len(batch.Entries)))
		for _, entry := range batch.Entries {
			if flags&batchBlob != 0 && (entry.ExtentOffset > uint64(len(w.Blob)) || entry.ExtentBytes > uint64(len(w.Blob))-entry.ExtentOffset) {
				return nil, [32]byte{}, nil, ErrBounds
			}
			e.frameBuf = appendUvarint(e.frameBuf, entry.Index)
			e.frameBuf = appendUvarint(e.frameBuf, entry.Term)
			if flags&batchTypes != 0 {
				e.frameBuf = appendUvarint(e.frameBuf, uint64(entry.Type))
			}
			if flags&batchBlob != 0 {
				e.frameBuf = appendUvarint(e.frameBuf, entry.ExtentID)
				e.frameBuf = appendUvarint(e.frameBuf, entry.ExtentOffset)
				e.frameBuf = appendUvarint(e.frameBuf, entry.ExtentBytes)
				e.frameBuf = appendUvarint(e.frameBuf, entry.DataOffset)
				e.frameBuf = appendUvarint(e.frameBuf, entry.DataBytes)
			} else {
				e.frameBuf = appendUvarint(e.frameBuf, uint64(len(entry.Data)))
			}
			dataOffset := uint64(len(e.frameBuf))
			if flags&batchBlob == 0 {
				e.frameBuf = append(e.frameBuf, entry.Data...)
			}
			if len(e.frameBuf) > cap(e.frameBuf) {
				return nil, [32]byte{}, nil, ErrBounds
			}
			if validateState && flags&batchBlob == 0 {
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventKindForType(entry.Type), GroupID: batch.GroupID, Index: entry.Index, Term: entry.Term, Offset: dataOffset, Bytes: uint64(len(entry.Data))})
			}
		}
		if validateState && flags&batchBlob != 0 {
			for _, entry := range batch.Entries {
				e.eventScratch = append(e.eventScratch, segmentEvent{Kind: blobEventKind(entry.Type), GroupID: batch.GroupID, Index: entry.Index, Term: entry.Term, Offset: entry.ExtentOffset, Bytes: entry.ExtentBytes, DataOffset: entry.DataOffset, DataBytes: entry.DataBytes, ReadyID: entry.ExtentID, Reference: w.ID})
			}
		}
		if validateState && batch.TruncateIndex != 0 {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventPrefix, GroupID: batch.GroupID, Index: batch.TruncateIndex, Term: batch.TruncateTerm})
		}
		if validateState && batch.Checkpoint != nil {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventCheckpoint, GroupID: batch.GroupID, Index: batch.Checkpoint.Index, Term: batch.Checkpoint.Term, Reference: batch.Checkpoint.ID})
		}
		if validateState && batch.Hard != nil {
			e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventHardState, GroupID: batch.GroupID, Term: batch.Hard.Term, Vote: batch.Hard.Vote, Commit: batch.Hard.Commit})
		}
		previousGroup = batch.GroupID
	}
	e.frameBuf = appendUvarint(e.frameBuf, uint64(len(w.Blob)))
	blobOffset := uint64(len(e.frameBuf))
	e.frameBuf = append(e.frameBuf, w.Blob...)
	if validateState {
		for i := range e.eventScratch {
			if isBlobEvent(e.eventScratch[i].Kind) {
				e.eventScratch[i].Offset += blobOffset
			}
		}
	}
	if len(e.frameBuf) > maxRecordBytes || len(e.frameBuf) > cap(e.frameBuf) {
		return nil, [32]byte{}, nil, ErrBounds
	}
	digest := e.waveDigest(e.frameBuf[72:], e.sequence+1, w.ID)
	copy(e.frameBuf[40:72], digest[:])
	if validateState {
		for i := range e.eventScratch {
			if e.eventScratch[i].Kind == eventWaveRef {
				e.eventScratch[i].Digest = digest
			}
		}
		e.eventScratch = append(e.eventScratch, segmentEvent{Kind: eventWave, GroupID: engineWaveGroup, Index: e.sequence + 1, Reference: w.ID, Digest: digest})
	}
	return e.frameBuf, digest, e.eventScratch, nil
}

// waveSize is deliberately run before the encoder mutates its reusable slices.
// Insufficient reservations therefore fail without a hidden growth allocation.
func waveSize(w Wave) (int, int, error) {
	if err := validateWaveExtents(w.Batches, uint64(len(w.Blob))); err != nil {
		return 0, 0, err
	}
	bytes, events := 72+uvarintBytes(uint64(len(w.Batches))), 1+len(w.Batches)
	previousGroup := uint64(0)
	for i := range w.Batches {
		b := &w.Batches[i]
		if b.GroupID <= previousGroup {
			return 0, 0, ErrRaftState
		}
		bytes += uvarintBytes(b.GroupID-previousGroup) + 1 + uvarintBytes(uint64(len(b.Entries)))
		if b.ReplaceFrom != 0 {
			bytes += uvarintBytes(b.ReplaceFrom)
			events++
		}
		if b.TruncateIndex != 0 {
			bytes += uvarintBytes(b.TruncateIndex) + uvarintBytes(b.TruncateTerm)
			events++
		}
		if b.Checkpoint != nil {
			bytes += 16 + uvarintBytes(b.Checkpoint.Index) + uvarintBytes(b.Checkpoint.Term)
			events++
		}
		if b.Hard != nil {
			bytes += uvarintBytes(b.Hard.Term) + uvarintBytes(b.Hard.Vote) + uvarintBytes(b.Hard.Commit)
			events++
		}
		if batchFlags(b)&batchIdentity != 0 {
			bytes += uvarintBytes(b.NodeIncarnation) + uvarintBytes(b.ReadyID) + 16
			events++
		}
		if batchFlags(b)&batchBegin != 0 {
			bytes += uvarintBytes(b.BeginIncarnation)
			events++
		}
		hasTypes := batchFlags(b)&batchTypes != 0
		hasBlob := batchFlags(b)&batchBlob != 0
		for j := range b.Entries {
			entry := &b.Entries[j]
			if len(entry.Data) > maxRecordBytes || bytes > maxRecordBytes-len(entry.Data) {
				return 0, 0, ErrBounds
			}
			bytes += uvarintBytes(entry.Index) + uvarintBytes(entry.Term)
			if hasBlob {
				bytes += uvarintBytes(entry.ExtentID) + uvarintBytes(entry.ExtentOffset) + uvarintBytes(entry.ExtentBytes) + uvarintBytes(entry.DataOffset) + uvarintBytes(entry.DataBytes)
			} else {
				bytes += uvarintBytes(uint64(len(entry.Data))) + len(entry.Data)
			}
			if hasTypes {
				bytes += uvarintBytes(uint64(entry.Type))
			}
			events++
		}
		previousGroup = b.GroupID
	}
	if len(w.Blob) > maxRecordBytes || bytes > maxRecordBytes-len(w.Blob) {
		return 0, 0, ErrBounds
	}
	bytes += uvarintBytes(uint64(len(w.Blob))) + len(w.Blob)
	if bytes > maxRecordBytes {
		return 0, 0, ErrBounds
	}
	return bytes, events, nil
}

// validateWaveExtents makes the shared blob a canonical sequence of disjoint
// authenticated extents. Entries may share an extent, but cannot disagree on
// its identity/geometry or overlap slices within it.
func validateWaveExtents(batches []ReadyBatch, blobBytes uint64) error {
	var previousOffset, previousBytes, previousID, previousDataEnd uint64
	haveExtent := false
	for i := range batches {
		for j := range batches[i].Entries {
			entry := batches[i].Entries[j]
			if entry.ExtentBytes == 0 {
				if entry.ExtentID != 0 || entry.ExtentOffset != 0 || entry.DataOffset != 0 || entry.DataBytes != 0 {
					return ErrRaftState
				}
				continue
			}
			if entry.ExtentID == 0 || entry.ExtentBytes < 16 || entry.ExtentOffset > blobBytes || entry.ExtentBytes > blobBytes-entry.ExtentOffset || entry.DataOffset > entry.ExtentBytes-16 || entry.DataBytes > entry.ExtentBytes-16-entry.DataOffset {
				return ErrRaftState
			}
			if haveExtent && entry.ExtentOffset == previousOffset {
				if entry.ExtentID != previousID || entry.ExtentBytes != previousBytes || entry.DataOffset != previousDataEnd {
					return ErrRaftState
				}
			} else {
				if !haveExtent {
					if entry.ExtentOffset != 0 || entry.ExtentID != 1 {
						return ErrRaftState
					}
				} else if previousDataEnd != previousBytes-16 || entry.ExtentOffset != previousOffset+previousBytes || entry.ExtentID != previousID+1 {
					return ErrRaftState
				}
				previousOffset, previousBytes, previousID, previousDataEnd, haveExtent = entry.ExtentOffset, entry.ExtentBytes, entry.ExtentID, 0, true
				if entry.DataOffset != 0 {
					return ErrRaftState
				}
			}
			previousDataEnd = entry.DataOffset + entry.DataBytes
		}
	}
	if haveExtent != (blobBytes != 0) {
		return ErrRaftState
	}
	if haveExtent && (previousDataEnd != previousBytes-16 || previousOffset+previousBytes != blobBytes) {
		return ErrRaftState
	}
	return nil
}

func uvarintBytes(value uint64) int {
	bytes := 1
	for value >= 0x80 {
		value >>= 7
		bytes++
	}
	return bytes
}

const (
	batchReplace    = 1 << 0
	batchPrefix     = 1 << 1
	batchCheckpoint = 1 << 2
	batchHard       = 1 << 3
	batchTypes      = 1 << 4
	batchIdentity   = 1 << 5
	batchBlob       = 1 << 6
	batchBegin      = 1 << 7
)

func batchFlags(b *ReadyBatch) byte {
	var flags byte
	if b.ReplaceFrom != 0 {
		flags |= batchReplace
	}
	if b.TruncateIndex != 0 {
		flags |= batchPrefix
	}
	if b.Checkpoint != nil {
		flags |= batchCheckpoint
	}
	if b.Hard != nil {
		flags |= batchHard
	}
	if b.NodeIncarnation != 0 || b.ReadyID != 0 || b.ReadyDigest != ([16]byte{}) {
		flags |= batchIdentity
	}
	if b.BeginIncarnation != 0 {
		flags |= batchBegin
	}
	for i := range b.Entries {
		if b.Entries[i].ExtentBytes != 0 {
			flags |= batchBlob
		}
		if b.Entries[i].Type != pb.EntryNormal {
			flags |= batchTypes
			break
		}
	}
	return flags
}

func validEntryType(kind pb.EntryType) bool {
	return kind == pb.EntryNormal || kind == pb.EntryConfChange || kind == pb.EntryConfChangeV2
}

func eventKindForType(kind pb.EntryType) uint16 {
	switch kind {
	case pb.EntryConfChange:
		return eventWaveEntryConf
	case pb.EntryConfChangeV2:
		return eventWaveEntryConfV2
	default:
		return eventWaveEntry
	}
}

func typeForEventKind(kind uint16) (pb.EntryType, bool) {
	switch kind {
	case eventWaveEntry:
		return pb.EntryNormal, true
	case eventWaveEntryConf:
		return pb.EntryConfChange, true
	case eventWaveEntryConfV2:
		return pb.EntryConfChangeV2, true
	case eventBlobEntry:
		return pb.EntryNormal, true
	case eventBlobEntryConf:
		return pb.EntryConfChange, true
	case eventBlobEntryConfV2:
		return pb.EntryConfChangeV2, true
	default:
		return 0, false
	}
}

func blobEventKind(kind pb.EntryType) uint16 {
	switch kind {
	case pb.EntryConfChange:
		return eventBlobEntryConf
	case pb.EntryConfChangeV2:
		return eventBlobEntryConfV2
	default:
		return eventBlobEntry
	}
}

func isBlobEvent(kind uint16) bool { return kind >= eventBlobEntry && kind <= eventBlobEntryConfV2 }

func validateBatch(g *engineGroup, b *ReadyBatch) (byte, error) {
	flags := batchFlags(b)
	if flags&batchBegin != 0 {
		if b.BeginIncarnation != g.NodeIncarnation+1 || len(b.Entries) != 0 || b.ReplaceFrom != 0 || b.TruncateIndex != 0 || b.TruncateTerm != 0 {
			return 0, ErrRaftState
		}
		if flags == batchBegin {
			return flags, nil
		}
		// Only a virgin group may combine its first incarnation fence with a
		// checkpoint and initial HardState. Existing groups still require a
		// standalone fence: no append, vote change or reset can hitch a ride.
		if flags != batchBegin|batchCheckpoint|batchHard || g.NodeIncarnation != 0 ||
			g.Hard != (HardState{}) || durableLast(g) != 0 || g.Checkpoint != (Checkpoint{}) ||
			g.TruncateIndex != 0 || g.ReadyID != 0 ||
			b.Hard.Vote != 0 || b.Hard.Term < b.Checkpoint.Term || b.Hard.Commit != b.Checkpoint.Index {
			return 0, ErrRaftState
		}
	}
	if flags&batchIdentity != 0 {
		if b.NodeIncarnation == 0 || b.ReadyID == 0 || b.ReadyDigest == ([16]byte{}) {
			return 0, ErrRaftState
		}
		if b.NodeIncarnation == g.NodeIncarnation {
			if b.ReadyID != g.ReadyID+1 {
				return 0, ErrRaftState
			}
		} else if b.NodeIncarnation <= g.NodeIncarnation || b.ReadyID != 1 {
			return 0, ErrRaftState
		}
	}
	last := durableLast(g)
	effectiveCommit := g.Hard.Commit
	if b.Hard != nil {
		effectiveCommit = b.Hard.Commit
	}
	if len(b.Entries) != 0 {
		first := b.Entries[0].Index
		if first <= last {
			if b.ReplaceFrom != first || b.ReplaceFrom <= g.Hard.Commit {
				return 0, ErrRaftState
			}
		} else if first != last+1 || b.ReplaceFrom != 0 {
			return 0, ErrRaftState
		}
		for i, entry := range b.Entries {
			if entry.Index != first+uint64(i) || entry.Term == 0 || !validEntryType(entry.Type) {
				return 0, ErrRaftState
			}
			if flags&batchBlob != 0 {
				if len(entry.Data) != 0 || entry.ExtentBytes < 16 || entry.ExtentOffset > ^uint64(0)-entry.ExtentBytes || entry.DataOffset > ^uint64(0)-entry.DataBytes || entry.DataOffset+entry.DataBytes > entry.ExtentBytes-16 {
					return 0, ErrRaftState
				}
			} else if entry.ExtentOffset != 0 || entry.ExtentBytes != 0 || entry.DataOffset != 0 || entry.DataBytes != 0 {
				return 0, ErrRaftState
			}
		}
		last = b.Entries[len(b.Entries)-1].Index
	} else if b.ReplaceFrom != 0 {
		return 0, ErrRaftState
	}
	if b.Checkpoint != nil {
		checkpointTerm, err := termAt(g, b, b.Checkpoint.Index)
		if err != nil {
			return 0, err
		}
		if b.Checkpoint.ID == ([16]byte{}) || b.Checkpoint.Index == 0 || b.Checkpoint.Term == 0 || b.Checkpoint.Index < g.Checkpoint.Index || b.Checkpoint.Index > effectiveCommit || (b.Checkpoint.Index <= last && checkpointTerm != b.Checkpoint.Term) {
			return 0, ErrRaftState
		}
		if b.Checkpoint.Index > last {
			last = b.Checkpoint.Index
		}
	}
	if b.TruncateIndex != 0 {
		truncateTerm, err := termAt(g, b, b.TruncateIndex)
		if err != nil {
			return 0, err
		}
		if b.TruncateTerm == 0 || b.TruncateIndex < g.TruncateIndex || b.TruncateIndex > last || b.TruncateIndex > effectiveCommit || (b.Checkpoint == nil || b.TruncateIndex != b.Checkpoint.Index) && truncateTerm != b.TruncateTerm {
			return 0, ErrRaftState
		}
	}
	if b.Hard != nil {
		h := b.Hard
		if h.Term < g.Hard.Term || h.Commit < g.Hard.Commit || h.Commit > last || h.Term == 0 && h.Vote != 0 {
			return 0, ErrRaftState
		}
		if h.Term == g.Hard.Term && g.Hard.Vote != 0 && h.Vote != g.Hard.Vote {
			return 0, ErrRaftState
		}
		if len(b.Entries) != 0 && h.Term < b.Entries[len(b.Entries)-1].Term {
			return 0, ErrRaftState
		}
	}
	return flags, nil
}

func durableLast(g *engineGroup) uint64 {
	if len(g.Entries) != 0 {
		return g.Entries[len(g.Entries)-1].Index
	}
	if g.lastIndex != 0 {
		return g.lastIndex
	}
	if g.Checkpoint.Index > g.TruncateIndex {
		return g.Checkpoint.Index
	}
	return g.TruncateIndex
}
func replacementCount(entries []EntryLocation, from uint64) int {
	if from == 0 {
		return 0
	}
	cut := sort.Search(len(entries), func(i int) bool { return entries[i].Index >= from })
	return len(entries) - cut
}
func predecessorTerm(g *engineGroup, from uint64) (uint64, error) {
	if from <= 1 {
		return 0, nil
	}
	for i := len(g.Entries) - 1; i >= 0; i-- {
		if g.Entries[i].Index == from-1 {
			return g.Entries[i].Term, nil
		}
	}
	if term, ok, err := g.sealedTerm(from - 1); err != nil || ok {
		return term, err
	}
	if g.Checkpoint.Index == from-1 {
		return g.Checkpoint.Term, nil
	}
	if g.TruncateIndex == from-1 {
		return g.TruncateTerm, nil
	}
	return 0, nil
}
func termAt(g *engineGroup, b *ReadyBatch, index uint64) (uint64, error) {
	if b != nil {
		for _, entry := range b.Entries {
			if entry.Index == index {
				return entry.Term, nil
			}
		}
	}
	for i := len(g.Entries) - 1; i >= 0; i-- {
		if g.Entries[i].Index == index {
			return g.Entries[i].Term, nil
		}
	}
	if term, ok, err := g.sealedTerm(index); err != nil || ok {
		return term, err
	}
	// The authenticated checkpoint-base summary retains the exact terminal
	// index/term even when every route carrying that entry was reclaimed. It is
	// sufficient for validating a checkpoint or append at the durable boundary;
	// older non-base terms still require a retained route/checkpoint.
	if g.lastIndex == index {
		return g.lastTerm, nil
	}
	if g.Checkpoint.Index == index {
		return g.Checkpoint.Term, nil
	}
	if g.TruncateIndex == index {
		return g.TruncateTerm, nil
	}
	return 0, nil
}

func (g *engineGroup) sealedTerm(index uint64) (uint64, bool, error) {
	for i := len(g.sealed) - 1; i >= 0; i-- {
		run := g.sealed[i]
		if index < run.First || index > run.Last {
			continue
		}
		if g.owner == nil {
			return 0, false, ErrBounds
		}
		if err := g.owner.PrepareSegment(run.SegmentID); err != nil {
			return 0, false, err
		}
		for slot := range g.owner.readers {
			reader := &g.owner.readers[slot]
			if reader.id != run.SegmentID || reader.routes == nil {
				continue
			}
			route, err := reader.routes.Point(run, index)
			if err != nil {
				return 0, false, err
			}
			return route.Term, true, nil
		}
	}
	return 0, false, nil
}

func sealWaveHeader(frame []byte, sequence uint64, id WaveID) {
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(frame)))
	binary.LittleEndian.PutUint16(frame[4:6], RecordWave)
	binary.LittleEndian.PutUint16(frame[6:8], canonicalFormatMarker)
	binary.LittleEndian.PutUint64(frame[8:16], sequence)
	copy(frame[16:32], id[:])
	binary.LittleEndian.PutUint32(frame[32:36], crc32.Checksum(frame[40:], crcTable))
	binary.LittleEndian.PutUint32(frame[36:40], crc32.Checksum(frame[:36], crcTable))
}

func (e *Engine) applyEvent(event segmentEvent, segmentID uint64) error {
	if event.Kind == eventWave {
		id := WaveID(event.Reference)
		if previous, ok := e.waves[id]; ok && previous.digest != ([32]byte{}) && previous.digest != event.Digest {
			return ErrWaveConflict
		}
		if state, ok := e.waves[id]; ok {
			state.digest, state.sequence = event.Digest, event.Index
			e.waves[id] = state
		}
		if event.Index != e.sequence+1 {
			return ErrCorrupt
		}
		e.sequence = event.Index
		return nil
	}
	g := e.groups[event.GroupID]
	if g == nil {
		g = &engineGroup{owner: e, id: event.GroupID}
		e.groups[event.GroupID] = g
	}
	switch event.Kind {
	case eventWaveRef:
		id := WaveID(event.Reference)
		if id == (WaveID{}) || event.Digest == ([32]byte{}) || event.Index != e.sequence+1 {
			return ErrCorrupt
		}
		if g.latestWaveID != id {
			e.releaseWaveReference(g.latestWaveID)
			state := e.waves[id]
			if state.digest != ([32]byte{}) && (state.digest != event.Digest || state.sequence != event.Index) {
				return ErrWaveConflict
			}
			state.digest, state.sequence, state.refs = event.Digest, event.Index, state.refs+1
			e.waves[id] = state
		}
		g.latestWaveID, g.latestWaveDigest, g.latestWaveSequence = id, event.Digest, event.Index
	case eventIncarnation:
		if event.Incarnation != g.NodeIncarnation+1 {
			return ErrRaftState
		}
		g.NodeIncarnation, g.ReadyID, g.ReadyDigest, g.ReadyWaveID = event.Incarnation, 0, [16]byte{}, WaveID{}
	case eventReadyState:
		if event.Incarnation == 0 || event.ReadyID == 0 || event.ReadyDigest == ([16]byte{}) {
			return ErrRaftState
		}
		if event.Incarnation == g.NodeIncarnation {
			if event.ReadyID != g.ReadyID+1 {
				return ErrRaftState
			}
		} else if event.Incarnation <= g.NodeIncarnation || event.ReadyID != 1 {
			return ErrRaftState
		}
		newWave := WaveID(event.Reference)
		g.NodeIncarnation, g.ReadyID, g.ReadyDigest, g.ReadyWaveID = event.Incarnation, event.ReadyID, event.ReadyDigest, newWave
	case RecordTruncateSuffix:
		predecessor, err := predecessorTerm(g, event.Index)
		if err != nil {
			return err
		}
		if event.Index == 0 || event.Index <= g.Hard.Commit || event.Index > durableLast(g)+1 || predecessor != event.Term {
			return ErrRaftState
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index >= event.Index })
		g.Entries = slices.Delete(g.Entries, cut, len(g.Entries))
		g.clipSealedSuffix(event.Index - 1)
		g.lastIndex, g.lastTerm = event.Index-1, event.Term
	case eventWaveEntry, eventWaveEntryConf, eventWaveEntryConfV2, eventBlobEntry, eventBlobEntryConf, eventBlobEntryConfV2:
		if event.Index != durableLast(g)+1 || event.Term == 0 {
			return ErrRaftState
		}
		entryType, ok := typeForEventKind(event.Kind)
		if !ok {
			return ErrCorrupt
		}
		g.Entries = append(g.Entries, EntryLocation{SegmentID: segmentID, Offset: event.Offset, Bytes: event.Bytes, Index: event.Index, Term: event.Term, Type: entryType, DataOffset: event.DataOffset, DataBytes: event.DataBytes, BatchID: WaveID(event.Reference), ExtentID: event.ReadyID})
		g.lastIndex, g.lastTerm = event.Index, event.Term
	case eventPrefix:
		term, err := termAt(g, nil, event.Index)
		if err != nil {
			return err
		}
		if event.Index < g.TruncateIndex || event.Index > durableLast(g) || term != event.Term {
			return ErrRaftState
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index > event.Index })
		g.Entries = slices.Delete(g.Entries, 0, cut)
		g.clipSealedThrough(event.Index)
		g.TruncateIndex, g.TruncateTerm = event.Index, event.Term
		if len(g.Entries) == 0 && g.lastIndex < event.Index {
			g.lastIndex, g.lastTerm = event.Index, event.Term
		}
	case eventCheckpoint:
		if event.Reference == ([16]byte{}) || event.Index < g.Checkpoint.Index || event.Term == 0 {
			return ErrRaftState
		}
		cut := sort.Search(len(g.Entries), func(i int) bool { return g.Entries[i].Index > event.Index })
		g.Entries = slices.Delete(g.Entries, 0, cut)
		g.clipSealedThrough(event.Index)
		g.TruncateIndex, g.TruncateTerm = event.Index, event.Term
		g.Checkpoint = Checkpoint{ID: event.Reference, Index: event.Index, Term: event.Term}
		if len(g.Entries) == 0 && g.lastIndex < event.Index {
			g.lastIndex, g.lastTerm = event.Index, event.Term
		}
	case eventHardState:
		if event.Term < g.Hard.Term || event.Commit < g.Hard.Commit || event.Commit > durableLast(g) || event.Term == g.Hard.Term && g.Hard.Vote != 0 && event.Vote != g.Hard.Vote {
			return ErrRaftState
		}
		g.Hard = HardState{Term: event.Term, Vote: event.Vote, Commit: event.Commit}
	default:
		return ErrCorrupt
	}
	if e.activeBuild != nil && !e.activeBuild.update(g, e.log.state.ActiveID) {
		return ErrBounds
	}
	return nil
}

func (e *Engine) releaseWaveReference(id WaveID) {
	if id == (WaveID{}) {
		return
	}
	state, ok := e.waves[id]
	if !ok || state.refs <= 1 {
		delete(e.waves, id)
		return
	}
	state.refs--
	e.waves[id] = state
}

func (g *engineGroup) clipSealedSuffix(last uint64) {
	kept := g.sealed[:0]
	for i := range g.sealed {
		run := g.sealed[i]
		if run.First > last {
			g.dropSealedRun(run)
			continue
		}
		if run.Last > last {
			run.Last = last
		}
		kept = append(kept, run)
	}
	g.sealed = kept
}

func (g *engineGroup) clipSealedThrough(index uint64) {
	kept := g.sealed[:0]
	for i := range g.sealed {
		run := g.sealed[i]
		if run.Last <= index {
			g.dropSealedRun(run)
			continue
		}
		if run.First <= index {
			run.First = index + 1
		}
		kept = append(kept, run)
	}
	g.sealed = kept
}

func (g *engineGroup) addSealedRun(run sealedRunRef) {
	g.sealed = append(g.sealed, run)
	if g.owner != nil {
		g.owner.liveSealed[run.SegmentID]++
		g.owner.setReclaimFence(run.SegmentID, 0)
	}
}

func (g *engineGroup) dropSealedRun(run sealedRunRef) {
	if g.owner == nil {
		return
	}
	count := g.owner.liveSealed[run.SegmentID]
	if count <= 1 {
		delete(g.owner.liveSealed, run.SegmentID)
		g.owner.setReclaimFence(run.SegmentID, g.owner.applySequence)
	} else {
		g.owner.liveSealed[run.SegmentID] = count - 1
	}
}

func (e *Engine) setReclaimFence(segmentID, sequence uint64) {
	if e == nil || e.log == nil || segmentID <= e.log.state.AnchorID {
		return
	}
	ordinal := segmentID - e.log.state.AnchorID - 1
	if ordinal >= uint64(len(e.log.state.Segments)) || ordinal >= uint64(len(e.reclaimAfter)) || e.log.state.Segments[ordinal].ID != segmentID {
		return
	}
	e.reclaimAfter[ordinal] = sequence
}

func (e *Engine) recordSealedSummary(groupID uint64, summary sealedRunSummary, sequence uint64) {
	if e.sealedSummaries == nil || groupID == 0 || sequence == 0 {
		return
	}
	if _, exists := e.sealedSummaries[groupID]; !exists {
		at := sort.Search(len(e.sealedSummaryOrder), func(i int) bool { return e.sealedSummaryOrder[i] >= groupID })
		e.sealedSummaryOrder = append(e.sealedSummaryOrder, 0)
		copy(e.sealedSummaryOrder[at+1:], e.sealedSummaryOrder[at:])
		e.sealedSummaryOrder[at] = groupID
	}
	summary.RetryOrdinal = 0
	e.sealedSummaries[groupID] = summary
	if sequence > e.sealedSequence {
		e.sealedSequence = sequence
	}
}

func (e *Engine) Rotate(hook func(RotationPhase) error) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	return e.rotateLocked(hook)
}

func (e *Engine) rotateLocked(hook func(RotationPhase) error) error {
	if e.sealPending {
		select {
		case err := <-e.sealResults:
			e.sealPending = false
			if err != nil {
				return e.log.poison(err)
			}
		default:
			return ErrBackpressure
		}
	}
	l := e.log
	if e.maintenanceBusy || l.metadata.needsHealing || l.metadata.slot.ReclaimPhase != reclaimNone || l.metadata.slot.RetiredCheckpointCount != 0 || len(l.state.Segments) == cap(l.state.Segments) || catalogSuffixRecords(l.metadata.slot) >= catalogCheckpointHardRecords || !l.state.Reserves[0].Ready || !l.state.Reserves[1].Ready || l.reserveFiles[0] == nil || l.reserveFiles[1] == nil {
		return ErrBackpressure
	}
	if cap(l.eventSpare) < cap(l.events) || e.activeBuild == nil || e.spareBuild == nil {
		return ErrBounds
	}
	if err := e.log.Sync(); err != nil {
		return err
	}
	dataBytes, frozenID, frozenGeneration := l.activeOffset, l.state.ActiveID, l.state.ActiveGeneration
	var sum [32]byte
	copy(sum[:], l.activeHash.Sum(l.digestScratch[:0]))
	nextID, nextGeneration := frozenID+1, frozenGeneration+1
	nextHeader := segmentHeader{ID: nextID, Generation: nextGeneration, PreviousID: frozenID, PreviousHash: sum, LogID: l.state.LogID, FileID: l.state.Reserves[0].FileID, Capacity: l.state.SegmentCapacity}
	nextFile := l.reserveFiles[0]
	var err error
	if err = activateReserve(nextFile, nextHeader, e.authKey); err != nil {
		return l.poison(err)
	}
	if hook != nil {
		if err = hook(RotationNextPublished); err != nil {
			_ = nextFile.Close()
			return l.poison(err)
		}
	}
	pending := SegmentMeta{ID: frozenID, Generation: frozenGeneration, Bytes: dataBytes, Records: l.records, PreviousHash: l.expectedPreviousHash(), Hash: sum, FileID: l.state.ActiveFileID, State: SegmentFrozenPending}
	next := l.state
	next.Generation++
	next.ActiveID, next.ActiveGeneration = nextID, nextGeneration
	next.ActiveFileID = next.Reserves[0].FileID
	next.Reserves[0], next.Reserves[1] = next.Reserves[1], reserveDescriptor{}
	next.DurableSegmentID, next.DurableOffset = nextID, segmentHeaderBytes
	next.Segments = next.Segments[:len(next.Segments)+1]
	next.Segments[len(next.Segments)-1] = pending
	e.reclaimAfter = e.reclaimAfter[:len(e.reclaimAfter)+1]
	e.reclaimAfter[len(e.reclaimAfter)-1] = 0
	if err = l.publish(next); err != nil {
		_ = nextFile.Close()
		return l.poison(err)
	}
	old := l.active
	frozenEvents := l.events
	frozenBuild := e.activeBuild
	frozenBuild.lastSequence = e.sequence
	e.activeBuild, e.spareBuild = e.spareBuild, nil
	l.events, l.eventSpare = l.eventSpare[:0], nil
	l.active, l.activeOffset, l.records, l.state = nextFile, segmentHeaderBytes, 0, next
	l.reserveFiles[0], l.reserveFiles[1] = l.reserveFiles[1], nil
	l.activeHash = sha256.New()
	prefix := marshalSegmentPrefix(nextHeader, e.authKey)
	_, _ = l.activeHash.Write(prefix[:])
	if err = old.Close(); err != nil {
		return l.poison(err)
	}
	if hook != nil {
		if err = hook(RotationPendingMetadataPublished); err != nil {
			return l.poison(err)
		}
	}
	e.sealPending = true
	e.sealRequests <- sealRequest{base: next, pending: pending, events: frozenEvents, hook: hook, build: frozenBuild}
	return nil
}

func (e *Engine) finishFrozenSeal(base metadataState, pending SegmentMeta, events []segmentEvent, build *segmentBuildArena, hook func(RotationPhase) error, deferMaintenance bool) (result error) {
	if build == nil {
		return ErrBounds
	}
	if build.lastSequence < pending.Records {
		return ErrCorrupt
	}
	if e.sealBuildHookTest != nil {
		e.sealBuildHookTest()
	}
	builderLog := &Log{dir: e.log.dir, state: base, authKey: e.authKey, events: events}
	builderLog.state.ActiveID, builderLog.state.ActiveGeneration = pending.ID, pending.Generation
	builder := &Engine{log: builderLog, groups: make(map[uint64]*engineGroup, len(build.groups)), sequence: build.lastSequence, authKey: e.authKey, sealSummaryOverride: make(map[uint64]sealedRunSummary, len(build.groups))}
	for i := range build.groups {
		item := build.groups[i]
		final := item.Final
		builder.sealSummaryOverride[item.GroupID] = final
		builder.groups[item.GroupID] = &engineGroup{id: item.GroupID, GroupState: GroupState{Hard: final.Hard, TruncateIndex: final.TruncateIndex, TruncateTerm: final.TruncateTerm, Checkpoint: final.Checkpoint, NodeIncarnation: final.NodeIncarnation, ReadyID: final.ReadyID, ReadyDigest: final.ReadyDigest, ReadyWaveID: final.ReadyWaveID}, lastIndex: final.LastIndex, lastTerm: final.LastTerm, latestWaveID: final.LatestWaveID, latestWaveDigest: final.LatestWaveDigest, latestWaveSequence: final.LatestWaveSequence}
	}
	for i := range events {
		event := events[i]
		group := builder.groups[event.GroupID]
		if group == nil {
			continue
		}
		switch event.Kind {
		case RecordTruncateSuffix:
			cut := sort.Search(len(group.Entries), func(i int) bool { return group.Entries[i].Index >= event.Index })
			group.Entries = group.Entries[:cut]
		case eventPrefix, eventCheckpoint:
			cut := sort.Search(len(group.Entries), func(i int) bool { return group.Entries[i].Index > event.Index })
			group.Entries = group.Entries[cut:]
		case eventWaveEntry, eventWaveEntryConf, eventWaveEntryConfV2, eventBlobEntry, eventBlobEntryConf, eventBlobEntryConfV2:
			entryType, ok := typeForEventKind(event.Kind)
			if !ok {
				return ErrCorrupt
			}
			group.Entries = append(group.Entries, EntryLocation{SegmentID: pending.ID, Offset: event.Offset, Bytes: event.Bytes, Index: event.Index, Term: event.Term, Type: entryType, DataOffset: event.DataOffset, DataBytes: event.DataBytes, BatchID: WaveID(event.Reference), ExtentID: event.ReadyID})
		}
	}
	index, topBytes, err := builder.marshalEngineSealedIndex(pending.Bytes)
	if err != nil {
		return err
	}
	if pending.Bytes > base.SegmentCapacity || uint64(len(index)) > base.SegmentCapacity-pending.Bytes || segmentFooterBytes > base.SegmentCapacity-pending.Bytes-uint64(len(index)) {
		return ErrBounds
	}
	header, err := unmarshalSealedIndexHeader(index[:sealedIndexHeaderBytes])
	if err != nil {
		return err
	}
	runs, err := decodeSealedDirectory(index[sealedIndexHeaderBytes:topBytes], header)
	if err != nil {
		return err
	}
	pendingPath := segmentPath(e.log.dir, pending.FileID)
	if e.sealOpenHookTest != nil {
		e.sealOpenHookTest(pendingPath)
	}
	f, err := os.OpenFile(pendingPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err = f.Truncate(int64(pending.Bytes)); err == nil {
		_, err = f.WriteAt(index, int64(pending.Bytes))
	}
	footer := segmentFooter{ID: pending.ID, Generation: pending.Generation, Records: pending.Records, DataBytes: pending.Bytes, Hash: pending.Hash, IndexOffset: pending.Bytes, IndexBytes: uint64(len(index)), Events: uint64(len(events))}
	segmentIdentity := segmentHeader{ID: pending.ID, Generation: pending.Generation, PreviousID: pending.ID - 1, PreviousHash: pending.PreviousHash, LogID: base.LogID, FileID: pending.FileID, Capacity: base.SegmentCapacity}
	footer.Auth = segmentSealedMetadataMAC(e.authKey, segmentIdentity, index[:topBytes], footer)
	footerBytes := marshalSegmentFooter(footer)
	if err == nil {
		if e.recoveryIO != nil {
			e.recoveryIO.pendingSealBytes += uint64(len(index) + len(footerBytes))
		}
		_, err = f.WriteAt(footerBytes, int64(pending.Bytes)+int64(len(index)))
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil && hook != nil {
		err = hook(RotationSealedSynced)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if hook != nil {
		err = hook(RotationSealFileClosed)
	}
	if err != nil {
		return err
	}
	sealed := pending
	sealed.State = SegmentSealed
	sealed.IndexOffset, sealed.IndexBytes = pending.Bytes, uint64(len(index))
	sealed.Bytes = pending.Bytes + uint64(len(index)) + segmentFooterBytes
	recordSegment := sealed
	recordSegment.PreviousHash, recordSegment.FileID = [32]byte{}, fileID{}
	record := catalogRecord{Kind: catalogSeal, Segment: recordSegment, FileID: sealed.FileID}
	nextGeneration := e.log.metadata.slot.Generation + 1
	predictedTail, predictedHash, previewErr := e.log.metadata.previewRecord(record, nextGeneration)
	if previewErr != nil {
		return previewErr
	}
	var checkpointID fileID
	var checkpointHash [32]byte
	var checkpointCandidate catalogCheckpoint
	if suffix := (predictedTail - max(uint64(metadataCatalogStart), e.log.metadata.slot.CheckpointTail)) / catalogRecordBytes; !deferMaintenance && suffix >= catalogCheckpointSoftRecords {
		if e.recoveryIO != nil {
			e.recoveryIO.maintenanceCheckpointAttempts++
		}
		checkpointID = derivedCheckpointID(e.authKey, base.LogID, nextGeneration, predictedTail, predictedHash, base.AnchorID, base.AnchorGeneration, base.AnchorHash, checkpointRoleOrdinary)
		checkpointCandidate = catalogCheckpoint{ID: checkpointID, LogID: base.LogID, Generation: nextGeneration, Tail: predictedTail, CatalogHash: predictedHash, AnchorID: base.AnchorID, AnchorGeneration: base.AnchorGeneration, AnchorHash: base.AnchorHash, Segments: base.Segments, Final: sealed, BaseSequence: e.sealedSequence, GroupIDs: e.sealedSummaryOrder, GroupSummaries: e.sealedSummaries}
		checkpointHash, err = catalogCheckpointWriter(e.log.dir, checkpointCandidate, e.authKey)
		// Checkpoint creation is capacity maintenance, not log corruption. The
		// seal remains publishable through the exact bounded suffix limit; a
		// later Rotate backpressures before mutation if maintenance stays down.
		if err != nil {
			if checkpointHash != ([32]byte{}) {
				cleanupUnpublishedCheckpoint(e.log.dir, checkpointCandidate, checkpointHash, e.authKey)
			}
			checkpointID, checkpointHash = fileID{}, [32]byte{}
		}
	}
	// Reserve preparation is deliberately outside writeMu: physical allocation,
	// file sync, and directory sync must never stall active appends. Failure is
	// capacity backpressure for the next Rotate, not corruption of this seal.
	var reserveFile *os.File
	var reserve reserveDescriptor
	var reserveErr error
	if !deferMaintenance {
		if e.recoveryIO != nil {
			e.recoveryIO.maintenanceReserveAttempts++
		}
		reserveFile, reserve, reserveErr = prepareReserve(e.log.dir, e.log.state.SegmentCapacity, e.log.state.LogID, e.authKey)
	}
	var grownSegments []SegmentMeta
	var grownFences []uint64
	if !deferMaintenance && len(e.log.state.Segments) == cap(e.log.state.Segments) {
		capacity := max(256, cap(e.log.state.Segments)*2)
		grownSegments = make([]SegmentMeta, len(e.log.state.Segments), capacity)
		copy(grownSegments, e.log.state.Segments)
		grownFences = make([]uint64, len(e.log.state.Segments), capacity)
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	state := e.log.state
	if grownSegments != nil {
		copy(grownFences, e.reclaimAfter)
		e.reclaimAfter = grownFences
		state.Segments = grownSegments
	}
	if len(state.Segments) == 0 || state.Segments[len(state.Segments)-1] != pending {
		if reserveFile != nil {
			cleanupUnpublishedFile(reserveFile, segmentPath(e.log.dir, reserve.FileID), e.log.dir)
		}
		if checkpointHash != ([32]byte{}) {
			cleanupUnpublishedCheckpoint(e.log.dir, checkpointCandidate, checkpointHash, e.authKey)
		}
		return ErrCorrupt
	}
	nextSlot := e.log.metadata.slot
	nextSlot.Generation++
	nextSlot.HasPending = false
	nextSlot.Pending = pendingDescriptor{}
	reserveIndex := -1
	if reserveErr == nil {
		for i := range nextSlot.Reserves {
			if !nextSlot.Reserves[i].Ready {
				nextSlot.Reserves[i], reserveIndex = reserve, i
				break
			}
		}
	}
	if checkpointID != (fileID{}) {
		nextSlot.PreviousCheckpointID = nextSlot.CheckpointID
		nextSlot.PreviousCheckpointTail = nextSlot.CheckpointTail
		nextSlot.PreviousCheckpointHash = nextSlot.CheckpointHash
		nextSlot.CheckpointID = [16]byte(checkpointID)
		nextSlot.CheckpointTail = predictedTail
		nextSlot.CheckpointHash = checkpointHash
	}
	if err = e.log.metadata.publish(nextSlot, &record); err != nil {
		if reserveFile != nil {
			_ = reserveFile.Close()
		}
		return err
	}
	if hook != nil {
		if err = hook(RotationSealedMetadataPublished); err != nil {
			if reserveFile != nil {
				_ = reserveFile.Close()
			}
			return err
		}
	}
	state.Generation = nextSlot.Generation
	state.Segments[len(state.Segments)-1] = sealed
	if reserveIndex >= 0 {
		state.Reserves[reserveIndex] = reserve
		e.log.reserveFiles[reserveIndex] = reserveFile
	} else if reserveFile != nil {
		cleanupUnpublishedFile(reserveFile, segmentPath(e.log.dir, reserve.FileID), e.log.dir)
	}
	e.log.state = state
	if deferMaintenance {
		e.log.metadata.needsHealing = true
	}
	for i := range runs {
		run := runs[i]
		e.recordSealedSummary(run.GroupID, run.Summary, header.LastSequence)
		group := e.groups[run.GroupID]
		if group == nil {
			continue
		}
		first, last := uint64(0), uint64(0)
		for j := range group.Entries {
			if group.Entries[j].SegmentID == pending.ID {
				if first == 0 {
					first = group.Entries[j].Index
				}
				last = group.Entries[j].Index
			}
		}
		if first != 0 {
			first, last = max(first, run.First), min(last, run.Last)
			if first <= last {
				group.addSealedRun(sealedRunRef{SegmentID: pending.ID, GroupID: run.GroupID, First: first, Last: last, RouteFirst: run.First, RouteLast: run.Last, DescriptorBase: pending.Bytes + uint64(header.DescriptorOffset) + run.DescriptorOrdinal*sealedRouteDescriptorBytes, DescriptorCount: run.DescriptorCount, BlockEntries: run.BlockEntries, ExtentOffset: run.ExtentOffset, ExtentBytes: run.ExtentBytes, Inline: run.Inline})
			}
		}
		kept := group.Entries[:0]
		for j := range group.Entries {
			if group.Entries[j].SegmentID != pending.ID {
				kept = append(kept, group.Entries[j])
			}
		}
		group.Entries = kept
	}
	if e.liveSealed[pending.ID] == 0 {
		e.setReclaimFence(pending.ID, header.LastSequence)
	}
	e.log.eventSpare = events[:0]
	build.clear()
	e.spareBuild = build
	return nil
}

// runMetadataMaintenance is the serial sealer's autonomous capacity lane.
// It performs all physical allocation/checkpoint I/O without writeMu and only
// holds the mutex for the fixed-size authenticated slot publication.
func (e *Engine) runMetadataMaintenance() {
	e.writeMu.Lock()
	if e.log != nil && e.log.metadata != nil && (e.log.metadata.slot.ReclaimPhase != reclaimNone || e.log.metadata.slot.RetiredCheckpointCount != 0) {
		if e.maintenanceBusy || e.sealPending || e.log.usable() != nil || e.log.metadata.slot.HasPending {
			e.writeMu.Unlock()
			return
		}
		e.maintenanceBusy = true
		e.writeMu.Unlock()
		_ = e.resumeReclaim()
		e.writeMu.Lock()
		e.maintenanceBusy = false
		e.writeMu.Unlock()
		return
	}
	if e.maintenanceBusy || e.log == nil || e.log.usable() != nil || e.log.metadata == nil || e.log.metadata.slot.HasPending {
		e.writeMu.Unlock()
		return
	}
	missing := [2]bool{}
	needWork := e.log.metadata.needsHealing || catalogSuffixRecords(e.log.metadata.slot) >= catalogCheckpointSoftRecords || len(e.log.state.Segments) == cap(e.log.state.Segments)
	for i := range missing {
		missing[i] = !e.log.metadata.slot.Reserves[i].Ready || e.log.reserveFiles[i] == nil
		needWork = needWork || missing[i]
	}
	if !needWork {
		e.writeMu.Unlock()
		return
	}
	e.maintenanceBusy = true
	baseSlot := e.log.metadata.slot
	segments := e.log.state.Segments
	dir, logID, capacity := e.log.dir, e.log.state.LogID, e.log.state.SegmentCapacity
	e.writeMu.Unlock()
	var grownSegments []SegmentMeta
	var grownFences []uint64
	if len(segments) == cap(segments) {
		grownCapacity := max(256, cap(segments)*2)
		grownSegments = make([]SegmentMeta, len(segments), grownCapacity)
		copy(grownSegments, segments)
		grownFences = make([]uint64, len(segments), grownCapacity)
	}

	var reserveFiles [2]*os.File
	var reserves [2]reserveDescriptor
	for i := range missing {
		if missing[i] {
			reserveFiles[i], reserves[i], _ = prepareReserve(dir, capacity, logID, e.authKey)
		}
	}
	var checkpointID fileID
	var checkpointHash [32]byte
	var checkpointCandidate catalogCheckpoint
	if e.log.metadata.needsHealing || catalogSuffixRecords(baseSlot) >= catalogCheckpointSoftRecords {
		candidateID := derivedCheckpointID(e.authKey, logID, baseSlot.Generation, baseSlot.CatalogTail, baseSlot.CatalogHash, baseSlot.AnchorID, baseSlot.AnchorGeneration, baseSlot.AnchorHash, checkpointRoleOrdinary)
		checkpointCandidate = catalogCheckpoint{ID: candidateID, LogID: logID, Generation: baseSlot.Generation, Tail: baseSlot.CatalogTail, CatalogHash: baseSlot.CatalogHash, AnchorID: baseSlot.AnchorID, AnchorGeneration: baseSlot.AnchorGeneration, AnchorHash: baseSlot.AnchorHash, Segments: segments, BaseSequence: e.sealedSequence, GroupIDs: e.sealedSummaryOrder, GroupSummaries: e.sealedSummaries}
		if digest, writeErr := catalogCheckpointWriter(dir, checkpointCandidate, e.authKey); writeErr == nil {
			checkpointID, checkpointHash = candidateID, digest
		}
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.maintenanceBusy = false
	if e.log.metadata.slot.Generation != baseSlot.Generation || e.log.metadata.slot.CatalogTail != baseSlot.CatalogTail {
		for i := range reserveFiles {
			if reserveFiles[i] != nil {
				cleanupUnpublishedFile(reserveFiles[i], segmentPath(dir, reserves[i].FileID), dir)
			}
		}
		if checkpointHash != ([32]byte{}) {
			cleanupUnpublishedCheckpoint(dir, checkpointCandidate, checkpointHash, e.authKey)
		}
		return
	}
	next := baseSlot
	changed := false
	for i := range reserveFiles {
		if reserveFiles[i] != nil && missing[i] {
			next.Reserves[i] = reserves[i]
			changed = true
		}
	}
	if checkpointID != (fileID{}) {
		next.PreviousCheckpointID, next.PreviousCheckpointTail, next.PreviousCheckpointHash = next.CheckpointID, next.CheckpointTail, next.CheckpointHash
		next.CheckpointID, next.CheckpointTail, next.CheckpointHash = [16]byte(checkpointID), next.CatalogTail, checkpointHash
		changed = true
	}
	if !changed {
		if grownSegments != nil {
			copy(grownFences, e.reclaimAfter)
			e.log.state.Segments = grownSegments
			e.reclaimAfter = grownFences
		}
		return
	}
	next.Generation++
	if err := e.log.metadata.publish(next, nil); err != nil {
		for i := range reserveFiles {
			if reserveFiles[i] != nil {
				_ = reserveFiles[i].Close()
			}
		}
		e.log.poison(err)
		return
	}
	if grownSegments != nil {
		copy(grownFences, e.reclaimAfter)
		e.log.state.Segments = grownSegments
		e.reclaimAfter = grownFences
	}
	e.log.state.Generation = next.Generation
	for i := range reserveFiles {
		if reserveFiles[i] != nil && missing[i] {
			e.log.reserveFiles[i] = reserveFiles[i]
			e.log.state.Reserves[i] = reserves[i]
		}
	}
}

func OpenEngineAuthenticated(dir string, logID [16]byte, key [32]byte) (*Engine, error) {
	if logID == ([16]byte{}) || key == ([32]byte{}) {
		return nil, ErrRaftState
	}
	return openEngineAuthenticated(dir, syncActiveData, logID, key)
}

func openEngine(dir string, startupSync func(*os.File) error, logID [16]byte, key [32]byte) (*Engine, error) {
	return openEngineAuthenticated(dir, startupSync, logID, key)
}

func openEngineAuthenticated(dir string, startupSync func(*os.File) error, logID [16]byte, key [32]byte) (*Engine, error) {
	return openEngineAuthenticatedObserved(dir, startupSync, logID, key, nil)
}

func openEngineAuthenticatedObserved(dir string, startupSync func(*os.File) error, logID [16]byte, key [32]byte, recoveryIO *recoveryIOCounters) (*Engine, error) {
	if logID == ([16]byte{}) || key == ([32]byte{}) {
		return nil, ErrRaftState
	}
	metadata, segments, err := openMetadataStore(dir, logID, key)
	if err != nil {
		return nil, err
	}
	state := stateFromMetadata(metadata.slot, segments)
	l := &Log{dir: dir, state: state, metadata: metadata, authKey: key}
	if err = l.reconcileRotation(); err != nil {
		_ = metadata.Close()
		return nil, err
	}
	for i := range state.Reserves {
		if !state.Reserves[i].Ready {
			continue
		}
		file, openErr := os.OpenFile(segmentPath(dir, state.Reserves[i].FileID), os.O_RDWR, 0)
		var reconcileErr error
		if openErr == nil {
			reconcileErr = reconcileTentativeReserve(file, state.Reserves[i], metadata.slot, key)
		}
		if openErr != nil || reconcileErr != nil {
			if file != nil {
				_ = file.Close()
			}
			state.Reserves[i] = reserveDescriptor{}
			l.state.Reserves[i] = reserveDescriptor{}
			metadata.slot.Reserves[i] = reserveDescriptor{}
			metadata.needsHealing = true
			continue
		}
		l.reserveFiles[i] = file
	}
	e := &Engine{log: l, groups: make(map[uint64]*engineGroup), waves: make(map[WaveID]waveState), liveSealed: make(map[uint64]uint32), reclaimAfter: make([]uint64, len(l.state.Segments), cap(l.state.Segments)), sealedSummaries: make(map[uint64]sealedRunSummary), syncData: startupSync, writeAt: func(f *os.File, b []byte, off int64) (int, error) { return f.WriteAt(b, off) }, recoveryIO: recoveryIO}
	e.authMAC = hmac.New(sha256.New, key[:])
	e.authKey = key
	if err = e.rebuild(); err != nil {
		_ = l.Close()
		return nil, err
	}
	if err = e.resumeReclaim(); err != nil {
		_ = l.Close()
		return nil, err
	}
	e.waveLimit = len(e.waves)
	e.startSealer()
	return e, nil
}

// promoteCompletePending recognizes a fully synced seal suffix left on either
// side of the active-to-sealed rename. It publishes that already-authenticated
// result without truncating, rescanning frames, or rebuilding the index. A
// partial suffix is not an error here: the bounded pending-data recovery path
// below truncates it back to want.Bytes and seals it again.
func (e *Engine) promoteCompletePending(want SegmentMeta, previousID uint64, previousHash [32]byte) (SegmentMeta, segmentFooter, sealedIndexHeader, []sealedGroupRun, bool, error) {
	path := segmentPath(e.log.dir, want.FileID)
	meta, footer, err := readUnpublishedSealed(path, want.FileID, e.log.state.SegmentCapacity, e.log.state.LogID, previousID, previousHash, e.authKey)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, sealedIndexHeader{}, nil, false, nil
	}
	if footer.DataBytes != want.Bytes || footer.Records != want.Records || footer.Hash != want.Hash || meta.ID != want.ID || meta.Generation != want.Generation || meta.PreviousHash != want.PreviousHash {
		return SegmentMeta{}, segmentFooter{}, sealedIndexHeader{}, nil, false, ErrCorrupt
	}
	meta.FileID = want.FileID
	file, err := os.Open(path)
	if err != nil {
		return SegmentMeta{}, segmentFooter{}, sealedIndexHeader{}, nil, false, err
	}
	footer, header, runs, _, readErr := readSealedSealedMetadata(file, meta, e.log.state.SegmentCapacity, e.log.state.LogID, previousID, previousHash, e.authKey)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return SegmentMeta{}, segmentFooter{}, sealedIndexHeader{}, nil, false, errors.Join(readErr, closeErr)
	}
	next := e.log.state
	next.Generation++
	next.Segments = slices.Clone(next.Segments)
	found := false
	for i := range next.Segments {
		if next.Segments[i] == want {
			next.Segments[i] = meta
			found = true
			break
		}
	}
	if !found {
		return SegmentMeta{}, segmentFooter{}, sealedIndexHeader{}, nil, false, ErrCorrupt
	}
	if err = e.log.publish(next); err != nil {
		return SegmentMeta{}, segmentFooter{}, sealedIndexHeader{}, nil, false, err
	}
	e.log.state = next
	if e.recoveryIO != nil {
		e.recoveryIO.pendingPromotions++
	}
	return meta, footer, header, runs, true, nil
}

func (e *Engine) rebuild() error {
	previousID, previousHash := e.log.state.AnchorID, e.log.state.AnchorHash
	baseApplied := e.log.metadata.base.Sequence == 0
	applyBase := func() error {
		if baseApplied {
			return nil
		}
		if e.sequence != 0 && e.sequence != e.log.metadata.base.Sequence {
			return ErrCorrupt
		}
		e.sequence = e.log.metadata.base.Sequence
		for i := range e.log.metadata.base.Groups {
			item := e.log.metadata.base.Groups[i]
			group := e.groups[item.GroupID]
			if group == nil {
				group = &engineGroup{owner: e, id: item.GroupID}
				e.groups[item.GroupID] = group
			}
			if err := e.applySealedRun(group, SegmentMeta{}, sealedIndexHeader{LastSequence: e.sequence}, sealedGroupRun{GroupID: item.GroupID, Summary: item.Summary}); err != nil {
				return err
			}
			e.recordSealedSummary(item.GroupID, item.Summary, e.sequence)
		}
		baseApplied = true
		return nil
	}
	for _, want := range e.log.state.Segments {
		if pendingSegment(want) {
			if err := applyBase(); err != nil {
				return err
			}
			sealed, footer, sealedHeader, runs, promoted, promoteErr := e.promoteCompletePending(want, previousID, previousHash)
			if promoteErr != nil {
				return promoteErr
			}
			if promoted {
				if sealedHeader.LastSequence != e.sequence+footer.Records {
					return ErrCorrupt
				}
				for i := range runs {
					run := runs[i]
					group := e.groups[run.GroupID]
					if group == nil {
						group = &engineGroup{owner: e, id: run.GroupID}
						e.groups[run.GroupID] = group
					}
					if applyErr := e.applySealedRun(group, sealed, sealedHeader, run); applyErr != nil {
						return applyErr
					}
				}
				if e.liveSealed[sealed.ID] == 0 {
					e.setReclaimFence(sealed.ID, sealedHeader.LastSequence)
				}
				e.sequence = sealedHeader.LastSequence
				previousID, previousHash = sealed.ID, sealed.Hash
				continue
			}
			file, err := os.OpenFile(segmentPath(e.log.dir, want.FileID), os.O_RDWR, 0)
			if err != nil {
				return err
			}
			headerBytes := make([]byte, segmentHeaderBytes)
			_, headerErr := file.ReadAt(headerBytes, 0)
			header, decodeErr := unmarshalSegmentHeader(headerBytes)
			certificateOK := validReserveCertificate(headerBytes[segmentIdentityBytes:], reserveDescriptor{FileID: want.FileID, Capacity: e.log.state.SegmentCapacity, Ready: true}, e.log.state.LogID, e.authKey)
			if headerErr != nil || decodeErr != nil || !certificateOK || header.ID != want.ID || header.Generation != want.Generation || header.PreviousID != previousID || header.PreviousHash != previousHash || header.LogID != e.log.state.LogID || header.FileID != want.FileID || header.Capacity != e.log.state.SegmentCapacity {
				_ = file.Close()
				return errors.Join(ErrCorrupt, headerErr, decodeErr)
			}
			st, statErr := file.Stat()
			if statErr != nil || uint64(st.Size()) < want.Bytes {
				_ = file.Close()
				return errors.Join(ErrCorrupt, statErr)
			}
			if uint64(st.Size()) != want.Bytes {
				if err = file.Truncate(int64(want.Bytes)); err != nil {
					_ = file.Close()
					return err
				}
			}
			if e.recoveryIO != nil {
				e.recoveryIO.pendingPayloadBytes += want.Bytes
			}
			sum, hashErr := hashPrefix(file, want.Bytes)
			if hashErr != nil || sum != want.Hash {
				_ = file.Close()
				return errors.Join(ErrCorrupt, hashErr)
			}
			events, records, scanErr := verifyWaveFrames(file, want.Bytes, want.ID, e)
			closeErr := file.Close()
			if scanErr != nil || closeErr != nil || records != want.Records {
				return errors.Join(ErrCorrupt, scanErr, closeErr)
			}
			seen := make(map[uint64]struct{})
			build := &segmentBuildArena{groups: make([]segmentGroupBuild, 0, len(events)), lastSequence: e.sequence}
			for i := range events {
				groupID := events[i].GroupID
				if groupID == engineWaveGroup {
					continue
				}
				if _, ok := seen[groupID]; ok {
					continue
				}
				seen[groupID] = struct{}{}
				if group := e.groups[groupID]; group != nil {
					build.groups = append(build.groups, segmentGroupBuild{GroupID: groupID, Final: groupSealSummary(group)})
				}
			}
			if err = e.finishFrozenSeal(e.log.state, want, events, build, nil, true); err != nil {
				return err
			}
			previousID, previousHash = want.ID, want.Hash
			continue
		}
		file, err := os.Open(segmentPath(e.log.dir, want.FileID))
		if err != nil {
			return err
		}
		footer, header, runs, metadataBytes, readErr := readSealedSealedMetadata(file, want, e.log.state.SegmentCapacity, e.log.state.LogID, previousID, previousHash, e.authKey)
		closeErr := file.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if e.recoveryIO != nil {
			e.recoveryIO.sealedMetadataReads++
			e.recoveryIO.sealedMetadataBytes += metadataBytes
		}
		belowBase := !baseApplied && header.LastSequence <= e.log.metadata.base.Sequence
		if belowBase {
			if footer.Records > header.LastSequence || e.sequence != 0 && header.LastSequence != e.sequence+footer.Records {
				return ErrCorrupt
			}
		} else {
			if err = applyBase(); err != nil {
				return err
			}
			if header.LastSequence != e.sequence+footer.Records {
				return ErrCorrupt
			}
		}
		for i := range runs {
			run := runs[i]
			group := e.groups[run.GroupID]
			if group == nil {
				group = &engineGroup{owner: e, id: run.GroupID}
				e.groups[run.GroupID] = group
			}
			if belowBase {
				err = e.applySealedRoute(group, want, header, run)
			} else {
				err = e.applySealedRun(group, want, header, run)
			}
			if err != nil {
				return err
			}
		}
		if e.liveSealed[want.ID] == 0 {
			e.setReclaimFence(want.ID, header.LastSequence)
		}
		e.sequence = header.LastSequence
		previousID, previousHash = want.ID, want.Hash
	}
	if err := applyBase(); err != nil {
		return err
	}
	if len(e.log.state.Segments) != 0 && len(e.readers) == 0 {
		e.readers = make([]segmentReader, 1)
	}
	headerBytes := make([]byte, segmentHeaderBytes)
	if _, err := e.log.active.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := unmarshalSegmentHeader(headerBytes)
	if err != nil {
		return err
	}
	certificateOK := validReserveCertificate(headerBytes[segmentIdentityBytes:], reserveDescriptor{FileID: e.log.state.ActiveFileID, Capacity: e.log.state.SegmentCapacity, Ready: true}, e.log.state.LogID, e.authKey)
	if !certificateOK || header.ID != e.log.state.ActiveID || header.Generation != e.log.state.ActiveGeneration || header.PreviousID != previousID || header.PreviousHash != previousHash || header.LogID != e.log.state.LogID || header.FileID != e.log.state.ActiveFileID || header.Capacity != e.log.state.SegmentCapacity {
		return ErrCorrupt
	}
	stat, err := e.log.active.Stat()
	if err != nil {
		return err
	}
	if stat.Size() < segmentHeaderBytes || uint64(stat.Size()) > e.log.state.SegmentCapacity {
		return ErrCorrupt
	}
	end := uint64(stat.Size())
	sealMarker := e.log.state.DurableOffset
	scanEnd := end
	pendingSeal := sealMarker > segmentHeaderBytes
	if pendingSeal {
		if sealMarker > end {
			return ErrCorrupt
		}
		scanEnd = sealMarker
	}
	off := uint64(segmentHeaderBytes)
	recoveryHash := sha256.New()
	_, _ = recoveryHash.Write(headerBytes)
	frameHeader := make([]byte, recordHeaderBytes)
	for off < scanEnd {
		remaining := scanEnd - off
		if remaining < recordHeaderBytes {
			break
		}
		if _, err = e.log.active.ReadAt(frameHeader, int64(off)); err != nil {
			return err
		}
		if e.recoveryIO != nil {
			e.recoveryIO.activeScanBytes += recordHeaderBytes
		}
		total, headerErr := inspectWaveHeader(frameHeader)
		if headerErr != nil {
			return headerErr
		}
		if total > remaining {
			break
		}
		frame := make([]byte, total)
		copy(frame, frameHeader)
		if _, err = e.log.active.ReadAt(frame[recordHeaderBytes:], int64(off+recordHeaderBytes)); err != nil {
			return err
		}
		if e.recoveryIO != nil {
			e.recoveryIO.activeScanBytes += total - recordHeaderBytes
		}
		id, digest, batches, parseErr := decodeWaveFrameForEngine(frame, e.sequence+1, e)
		if parseErr != nil {
			return fmt.Errorf("rebuild decode active segment %d sequence %d: %w", header.ID, e.sequence+1, parseErr)
		}
		events, applyErr := e.eventsForDecoded(batches, off, e.sequence+1, id, digest)
		if applyErr != nil {
			return fmt.Errorf("rebuild prepare active segment %d sequence %d: %w", header.ID, e.sequence+1, applyErr)
		}
		for _, event := range events {
			e.applySequence = e.sequence + 1
			if applyErr = e.applyEvent(event, header.ID); applyErr != nil {
				return fmt.Errorf("rebuild active segment %d sequence %d group %d event %d: %w", header.ID, e.sequence+1, event.GroupID, event.Kind, applyErr)
			}
		}
		e.applySequence = 0
		e.log.events = append(e.log.events, events...)
		e.log.records++
		_, _ = recoveryHash.Write(frame)
		off += total
	}
	if pendingSeal && off != scanEnd {
		return ErrCorrupt
	}
	if off != end {
		if err = e.log.active.Truncate(int64(off)); err != nil {
			return err
		}
	}
	if err = restoreActiveAllocation(e.log.active, e.log.state.SegmentCapacity, int64(off)); err != nil {
		return err
	}
	// A process crash can leave a checksum-valid frame only in the page cache.
	// Before recovered state becomes observable, make the accepted prefix a
	// durable startup boundary (also persisting any torn-tail truncation).
	if err = e.syncData(e.log.active); err != nil {
		return err
	}
	if pendingSeal {
		next := e.log.state
		next.Generation++
		next.DurableOffset = segmentHeaderBytes
		if err = e.log.publish(next); err != nil {
			return err
		}
		e.log.state = next
	}
	e.log.activeOffset = off
	e.log.activeHash = recoveryHash
	return nil
}

func inspectWaveHeader(header []byte) (uint64, error) {
	if len(header) != recordHeaderBytes || binary.LittleEndian.Uint16(header[4:6]) != RecordWave || binary.LittleEndian.Uint16(header[6:8]) != canonicalFormatMarker || binary.LittleEndian.Uint32(header[36:40]) != crc32.Checksum(header[:36], crcTable) {
		return 0, ErrCorrupt
	}
	total := uint64(binary.LittleEndian.Uint32(header[:4]))
	if total < 72 || total > maxRecordBytes {
		return 0, ErrCorrupt
	}
	return total, nil
}

func decodeWaveFrame(frame []byte, expectedSequence uint64) (WaveID, [32]byte, []ReadyBatch, error) {
	return decodeWaveFrameForEngine(frame, expectedSequence, nil)
}

func decodeWaveFrameForEngine(frame []byte, expectedSequence uint64, engine *Engine) (WaveID, [32]byte, []ReadyBatch, error) {
	if len(frame) < 72 {
		return WaveID{}, [32]byte{}, nil, ErrCorrupt
	}
	total, err := inspectWaveHeader(frame[:40])
	if err != nil || total != uint64(len(frame)) || binary.LittleEndian.Uint64(frame[8:16]) != expectedSequence || binary.LittleEndian.Uint32(frame[32:36]) != crc32.Checksum(frame[40:], crcTable) {
		return WaveID{}, [32]byte{}, nil, ErrCorrupt
	}
	var id WaveID
	copy(id[:], frame[16:32])
	if id == (WaveID{}) {
		return id, [32]byte{}, nil, ErrCorrupt
	}
	var stored [32]byte
	copy(stored[:], frame[40:72])
	digest := sha256.Sum256(frame[72:])
	if engine != nil {
		digest = engine.waveDigest(frame[72:], expectedSequence, id)
	}
	if stored != digest {
		return id, digest, nil, ErrCorrupt
	}
	cursor := canonicalCursor{data: frame[72:]}
	count, err := cursor.uvarint()
	if err != nil || count == 0 || count > uint64(len(cursor.data)-cursor.off)/3 {
		return id, digest, nil, ErrCorrupt
	}
	batches := make([]ReadyBatch, 0, count)
	previous := uint64(0)
	for range count {
		delta, err := cursor.uvarint()
		if err != nil || delta == 0 || previous > ^uint64(0)-delta {
			return id, digest, nil, ErrCorrupt
		}
		group := previous + delta
		if group == engineWaveGroup {
			return id, digest, nil, ErrCorrupt
		}
		flags, err := cursor.byte()
		if err != nil || flags & ^byte(batchReplace|batchPrefix|batchCheckpoint|batchHard|batchTypes|batchIdentity|batchBlob|batchBegin) != 0 {
			return id, digest, nil, ErrCorrupt
		}
		batch := ReadyBatch{GroupID: group}
		if flags&batchIdentity != 0 {
			batch.NodeIncarnation, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.ReadyID, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			value, takeErr := cursor.take(16)
			if takeErr != nil {
				return id, digest, nil, ErrCorrupt
			}
			copy(batch.ReadyDigest[:], value)
		}
		if flags&batchBegin != 0 {
			batch.BeginIncarnation, err = cursor.uvarint()
			if err != nil || batch.BeginIncarnation == 0 {
				return id, digest, nil, ErrCorrupt
			}
		}
		if flags&batchReplace != 0 {
			batch.ReplaceFrom, err = cursor.uvarint()
			if err != nil || batch.ReplaceFrom == 0 {
				return id, digest, nil, ErrCorrupt
			}
		}
		if flags&batchPrefix != 0 {
			batch.TruncateIndex, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.TruncateTerm, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
		}
		if flags&batchCheckpoint != 0 {
			reference, readErr := cursor.take(16)
			if readErr != nil {
				return id, digest, nil, readErr
			}
			checkpoint := Checkpoint{}
			copy(checkpoint.ID[:], reference)
			checkpoint.Index, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			checkpoint.Term, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.Checkpoint = &checkpoint
		}
		if flags&batchHard != 0 {
			hard := HardState{}
			hard.Term, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			hard.Vote, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			hard.Commit, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			batch.Hard = &hard
		}
		entryCount, err := cursor.uvarint()
		minimumEntryBytes := uint64(3)
		if flags&batchTypes != 0 {
			minimumEntryBytes++
		}
		if flags&batchBlob != 0 {
			minimumEntryBytes++
		}
		if err != nil || entryCount > uint64(len(cursor.data)-cursor.off)/minimumEntryBytes {
			return id, digest, nil, ErrCorrupt
		}
		batch.Entries = make([]Entry, 0, entryCount)
		for range entryCount {
			entry := Entry{}
			entry.Index, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			entry.Term, err = cursor.uvarint()
			if err != nil {
				return id, digest, nil, ErrCorrupt
			}
			if flags&batchTypes != 0 {
				entryType, typeErr := cursor.uvarint()
				if typeErr != nil || entryType > uint64(pb.EntryConfChangeV2) {
					return id, digest, nil, ErrCorrupt
				}
				entry.Type = pb.EntryType(entryType)
			}
			if !validEntryType(entry.Type) {
				return id, digest, nil, ErrCorrupt
			}
			if flags&batchBlob != 0 {
				entry.ExtentID, err = cursor.uvarint()
				if err != nil {
					return id, digest, nil, ErrCorrupt
				}
				entry.ExtentOffset, err = cursor.uvarint()
				if err != nil {
					return id, digest, nil, ErrCorrupt
				}
				entry.ExtentBytes, err = cursor.uvarint()
				if err != nil || entry.ExtentBytes < 16 || entry.ExtentOffset > ^uint64(0)-entry.ExtentBytes {
					return id, digest, nil, ErrCorrupt
				}
				entry.DataOffset, err = cursor.uvarint()
				if err != nil {
					return id, digest, nil, ErrCorrupt
				}
				entry.DataBytes, err = cursor.uvarint()
				if err != nil || entry.DataOffset > ^uint64(0)-entry.DataBytes || entry.DataOffset+entry.DataBytes > entry.ExtentBytes-16 {
					return id, digest, nil, ErrCorrupt
				}
			} else {
				size, readErr := cursor.uvarint()
				if readErr != nil || size > uint64(len(cursor.data)-cursor.off) {
					return id, digest, nil, ErrCorrupt
				}
				entry.dataOffset = uint64(72 + cursor.off)
				data, readErr := cursor.take(int(size))
				if readErr != nil {
					return id, digest, nil, ErrCorrupt
				}
				entry.Data = data
			}
			batch.Entries = append(batch.Entries, entry)
		}
		batches = append(batches, batch)
		previous = group
	}
	blobSize, err := cursor.uvarint()
	if err != nil || blobSize > uint64(len(cursor.data)-cursor.off) {
		return id, digest, nil, ErrCorrupt
	}
	blobOffset := uint64(72 + cursor.off)
	if _, err = cursor.take(int(blobSize)); err != nil {
		return id, digest, nil, ErrCorrupt
	}
	for i := range batches {
		for j := range batches[i].Entries {
			entry := &batches[i].Entries[j]
			if entry.ExtentBytes == 0 {
				continue
			}
			if entry.ExtentOffset > blobSize || entry.ExtentBytes > blobSize-entry.ExtentOffset {
				return id, digest, nil, ErrCorrupt
			}
			entry.dataOffset = blobOffset + entry.ExtentOffset
		}
	}
	if err = validateWaveExtents(batches, blobSize); err != nil {
		return id, digest, nil, ErrCorrupt
	}
	if !cursor.empty() {
		return id, digest, nil, ErrCorrupt
	}
	return id, digest, batches, nil
}

func (e *Engine) eventsForDecoded(batches []ReadyBatch, frameOffset, sequence uint64, id WaveID, digest [32]byte) ([]segmentEvent, error) {
	events := make([]segmentEvent, 0)
	for i := range batches {
		batch := &batches[i]
		g := e.groups[batch.GroupID]
		if g == nil {
			g = &engineGroup{owner: e, id: batch.GroupID}
			e.groups[batch.GroupID] = g
		}
		if _, err := validateBatch(g, batch); err != nil {
			return nil, fmt.Errorf("validate group %d durable-last %d hard=%+v truncate=%d/%d checkpoint=%+v batch entries=%d replace=%d prefix=%d/%d checkpoint=%+v hard=%+v: %w", batch.GroupID, durableLast(g), g.Hard, g.TruncateIndex, g.TruncateTerm, g.Checkpoint, len(batch.Entries), batch.ReplaceFrom, batch.TruncateIndex, batch.TruncateTerm, batch.Checkpoint, batch.Hard, err)
		}
		events = append(events, segmentEvent{Kind: eventWaveRef, GroupID: batch.GroupID, Index: sequence, Reference: id, Digest: digest})
		if batch.NodeIncarnation != 0 {
			events = append(events, segmentEvent{Kind: eventReadyState, GroupID: batch.GroupID, Incarnation: batch.NodeIncarnation, ReadyID: batch.ReadyID, ReadyDigest: batch.ReadyDigest, Reference: id})
		}
		if batch.BeginIncarnation != 0 {
			events = append(events, segmentEvent{Kind: eventIncarnation, GroupID: batch.GroupID, Incarnation: batch.BeginIncarnation})
		}
		if batch.ReplaceFrom != 0 {
			predecessor, termErr := predecessorTerm(g, batch.ReplaceFrom)
			if termErr != nil {
				return nil, termErr
			}
			events = append(events, segmentEvent{Kind: RecordTruncateSuffix, GroupID: batch.GroupID, Index: batch.ReplaceFrom, Term: predecessor})
		}
		for _, entry := range batch.Entries {
			kind, bytes := eventKindForType(entry.Type), uint64(len(entry.Data))
			var reference [16]byte
			if entry.ExtentBytes != 0 {
				kind, bytes = blobEventKind(entry.Type), entry.ExtentBytes
				reference = id
			}
			// Match prepareWave: only authenticated blob extents carry a
			// BatchID in their entry route. Inline payloads have no extent AAD.
			events = append(events, segmentEvent{Kind: kind, GroupID: batch.GroupID, Index: entry.Index, Term: entry.Term, Offset: frameOffset + entry.dataOffset, Bytes: bytes, DataOffset: entry.DataOffset, DataBytes: entry.DataBytes, ReadyID: entry.ExtentID, Reference: reference})
		}
		if batch.TruncateIndex != 0 {
			events = append(events, segmentEvent{Kind: eventPrefix, GroupID: batch.GroupID, Index: batch.TruncateIndex, Term: batch.TruncateTerm})
		}
		if batch.Checkpoint != nil {
			events = append(events, segmentEvent{Kind: eventCheckpoint, GroupID: batch.GroupID, Index: batch.Checkpoint.Index, Term: batch.Checkpoint.Term, Reference: batch.Checkpoint.ID})
		}
		if batch.Hard != nil {
			events = append(events, segmentEvent{Kind: eventHardState, GroupID: batch.GroupID, Term: batch.Hard.Term, Vote: batch.Hard.Vote, Commit: batch.Hard.Commit})
		}
	}
	events = append(events, segmentEvent{Kind: eventWave, GroupID: engineWaveGroup, Index: sequence, Reference: id, Digest: digest})
	return events, nil
}

func (e *Engine) DeepVerify() error {
	if err := e.log.usable(); err != nil {
		return err
	}
	verifier := &Engine{log: e.log, groups: make(map[uint64]*engineGroup), waves: make(map[WaveID]waveState), authKey: e.authKey}
	if e.authKey != ([32]byte{}) {
		verifier.authMAC = hmac.New(sha256.New, e.authKey[:])
	}
	previousID, previousHash := e.log.state.AnchorID, e.log.state.AnchorHash
	for _, want := range e.log.state.Segments {
		path := segmentPath(e.log.dir, want.FileID)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		footer, header, runs, _, err := readSealedSealedMetadata(f, want, e.log.state.SegmentCapacity, e.log.state.LogID, previousID, previousHash, e.authKey)
		if err != nil {
			_ = f.Close()
			return err
		}
		sum, err := hashPrefix(f, footer.DataBytes)
		if err == nil && sum != footer.Hash {
			err = ErrCorrupt
		}
		var scanned []segmentEvent
		var records uint64
		if err == nil {
			scanned, records, err = verifyWaveFrames(f, footer.DataBytes, want.ID, verifier)
			if err != nil {
				err = fmt.Errorf("%w: segment %d frame verification", err, want.ID)
			}
		}
		if err == nil && (records != footer.Records || uint64(len(scanned)) != footer.Events || verifier.sequence != header.LastSequence) {
			err = fmt.Errorf("%w: segment %d frame/event/sequence counts", ErrCorrupt, want.ID)
		}
		if err == nil {
			err = verifySealedRuns(f, want, header, runs, verifier, e.authKey)
			if err != nil {
				err = fmt.Errorf("%w: segment %d route verification", err, want.ID)
			}
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		previousID, previousHash = want.ID, want.Hash
	}
	activeEvents, records, err := verifyWaveFrames(e.log.active, e.log.activeOffset, e.log.state.ActiveID, verifier)
	if err != nil {
		return err
	}
	if records != e.log.records || !slices.Equal(canonicalEventOrder(activeEvents), canonicalEventOrder(e.log.events)) || verifier.sequence != e.sequence {
		return ErrCorrupt
	}
	sum, err := hashPrefix(e.log.active, e.log.activeOffset)
	if err != nil {
		return err
	}
	wantDigest := e.log.activeHash.Sum(nil)
	if !slices.Equal(sum[:], wantDigest) {
		return ErrCorrupt
	}
	return nil
}

func verifyWaveFrames(file *os.File, end, segmentID uint64, verifier *Engine) ([]segmentEvent, uint64, error) {
	off := uint64(segmentHeaderBytes)
	header := make([]byte, recordHeaderBytes)
	var events []segmentEvent
	var records uint64
	for off < end {
		if end-off < recordHeaderBytes {
			return nil, 0, ErrCorrupt
		}
		if _, err := file.ReadAt(header, int64(off)); err != nil {
			return nil, 0, err
		}
		total, err := inspectWaveHeader(header)
		if err != nil || total > end-off {
			return nil, 0, ErrCorrupt
		}
		frame := make([]byte, total)
		copy(frame, header)
		if _, err = file.ReadAt(frame[recordHeaderBytes:], int64(off+recordHeaderBytes)); err != nil {
			return nil, 0, err
		}
		id, digest, batches, err := decodeWaveFrameForEngine(frame, verifier.sequence+1, verifier)
		if err != nil {
			return nil, 0, err
		}
		decoded, err := verifier.eventsForDecoded(batches, off, verifier.sequence+1, id, digest)
		if err != nil {
			return nil, 0, err
		}
		for _, event := range decoded {
			if err = verifier.applyEvent(event, segmentID); err != nil {
				return nil, 0, err
			}
		}
		events = append(events, decoded...)
		records++
		off += total
	}
	if off != end {
		return nil, 0, ErrCorrupt
	}
	return events, records, nil
}
