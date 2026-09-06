package durable

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// ExactRootVectorFilename is the opt-in exact-root recovery sidecar.  The
// existing checkpoint.vgc certificate remains authoritative for production
// journal-backed apply; this file is only consulted by callers that explicitly
// select exact-root recovery.
const ExactRootVectorFilename = "checkpoint.roots"

// These aliases keep the durable owner API usable without making callers
// import the storage codec package merely to describe a cut or member image.
type RootVectorCut = storeio.RootVectorCut
type RootVectorMember = storeio.RootVectorMember
type RootVector = storeio.RootVector
type RootVectorMemberFloor = storeio.RootVectorMemberFloor

// ExactRootVectorCapture is the detached result of an owner capture.
type ExactRootVectorCapture struct {
	Vector  RootVector
	Floors  []RootVectorMemberFloor
	Changed bool
}

var (
	ErrExactRootVectorClosed = errors.New("vibedb: exact-root vector checkpoint is closed")
	ErrExactRootVectorPath   = errors.New("vibedb: invalid exact-root vector path")
)

// ExactRootVectorCheckpoint owns the bounded two-bank sidecar.  Its mutex is
// process-local; the file's complete-bank checksum and independent Sync calls
// are the cross-crash authority.
type ExactRootVectorCheckpoint struct {
	mu           sync.Mutex
	file         *os.File
	fileInfo     os.FileInfo
	path         string
	memberCount  int
	bankBytes    int
	parentDirty  bool
	writerLocked bool
	closed       bool
}

// OpenExactRootVectorCheckpoint opens or creates an exact-root sidecar.  A
// zero member count is allowed only for a new empty file; existing files always
// prove their member count from a complete bank before returning.
func OpenExactRootVectorCheckpoint(path string, memberCount int) (*ExactRootVectorCheckpoint, error) {
	if path == "" || filepath.Base(path) == "." || filepath.Base(path) == ".." {
		return nil, ErrExactRootVectorPath
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	path = absolute
	if memberCount < 0 || memberCount > storeio.RootVectorMaxMembers {
		return nil, fmt.Errorf("%w: member count %d", storeio.ErrRootVectorMember, memberCount)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		file, err = os.OpenFile(path, os.O_RDWR, 0)
	}
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	entry, entryErr := os.Lstat(path)
	if entryErr != nil || !info.Mode().IsRegular() || !entry.Mode().IsRegular() ||
		!os.SameFile(info, entry) {
		_ = file.Close()
		if entryErr != nil {
			return nil, entryErr
		}
		return nil, fmt.Errorf("%w: sidecar entry identity", storeio.ErrRootVectorCorrupt)
	}
	if err := storeio.LockWriter(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	closeFile := func() {
		_ = storeio.UnlockWriter(file)
		_ = file.Close()
	}
	checkpoint := &ExactRootVectorCheckpoint{
		file: file, fileInfo: info, path: path, memberCount: memberCount,
		parentDirty: created || info.Size() == 0, writerLocked: true,
	}
	if info.Size() == 0 {
		return checkpoint, nil
	}
	minimumBank, _ := storeio.RootVectorBankBytes(1)
	maximumBank, _ := storeio.RootVectorBankBytes(storeio.RootVectorMaxMembers)
	if info.Size()%2 != 0 || info.Size() < 2*int64(minimumBank) ||
		info.Size() > 2*int64(maximumBank) {
		closeFile()
		return nil, fmt.Errorf("%w: file size %d", storeio.ErrRootVectorCorrupt, info.Size())
	}
	checkpoint.bankBytes = int(info.Size() / 2)
	if checkpoint.bankBytes%storeio.InlineSuperblockSize != 0 {
		closeFile()
		return nil, fmt.Errorf("%w: bank size %d", storeio.ErrRootVectorCorrupt, checkpoint.bankBytes)
	}
	if checkpoint.bankBytes < minimumBank || checkpoint.bankBytes > maximumBank {
		closeFile()
		return nil, fmt.Errorf("%w: bank size %d", storeio.ErrRootVectorCorrupt, checkpoint.bankBytes)
	}
	if memberCount != 0 {
		want, sizeErr := storeio.RootVectorBankBytes(memberCount)
		if sizeErr != nil || want != checkpoint.bankBytes {
			closeFile()
			return nil, fmt.Errorf("%w: member count or bank size", storeio.ErrRootVectorMember)
		}
	} else {
		for candidate := 1; candidate <= storeio.RootVectorMaxMembers; candidate++ {
			candidateBytes, sizeErr := storeio.RootVectorBankBytes(candidate)
			if sizeErr == nil && candidateBytes == checkpoint.bankBytes {
				checkpoint.memberCount = candidate
				break
			}
		}
		if checkpoint.memberCount == 0 {
			closeFile()
			return nil, fmt.Errorf("%w: unrecognized bank geometry", storeio.ErrRootVectorCorrupt)
		}
	}
	return checkpoint, nil
}

// Path returns the sidecar path.
func (c *ExactRootVectorCheckpoint) Path() string {
	if c == nil {
		return ""
	}
	return c.path
}

// Close releases the sidecar descriptor. It is safe to call repeatedly.
func (c *ExactRootVectorCheckpoint) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.file == nil {
		return nil
	}
	var unlockErr error
	if c.writerLocked {
		unlockErr = storeio.UnlockWriter(c.file)
		c.writerLocked = false
	}
	err := errors.Join(unlockErr, c.file.Close())
	c.file = nil
	c.fileInfo = nil
	return err
}

// Read selects the newest complete compatible bank and returns the retained
// generation floor for every member across all still-selectable banks.
func (c *ExactRootVectorCheckpoint) Read() (RootVector, []RootVectorMemberFloor, error) {
	if c == nil {
		return RootVector{}, nil, ErrExactRootVectorClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readLocked()
}

// Publish captures a complete vector in both banks. The first write leaves the
// previous complete bank untouched; the second write is the same vector at the
// next sequence. If power fails after either write, SelectRootVectorBanks still
// returns one complete coherent vector and floors remain conservative.
func (c *ExactRootVectorCheckpoint) Publish(vector RootVector) error {
	if c == nil {
		return ErrExactRootVectorClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.file == nil {
		return ErrExactRootVectorClosed
	}
	if err := validateRootVectorForPublish(vector); err != nil {
		return err
	}
	if c.memberCount != 0 && c.memberCount != len(vector.Members) {
		return fmt.Errorf("%w: expected=%d got=%d", storeio.ErrRootVectorMember, c.memberCount, len(vector.Members))
	}
	if c.bankBytes != 0 {
		want, err := storeio.RootVectorBankBytes(len(vector.Members))
		if err != nil {
			return err
		}
		if want != c.bankBytes {
			return fmt.Errorf("%w: bank geometry", storeio.ErrRootVectorMember)
		}
	}
	if c.bankBytes == 0 {
		return c.writeInitialBanksLocked(vector)
	}
	current, _, currentSlot, converged, err := c.readSelectedLocked()
	if err != nil {
		if !errors.Is(err, storeio.ErrRootVectorMissing) {
			return err
		}
		empty, emptyErr := c.zeroFilledLocked()
		if emptyErr != nil {
			return emptyErr
		}
		if !empty {
			return err
		}
		c.parentDirty = true
		return c.writeInitialBanksLocked(vector)
	}
	if current.Cut.GroupID != vector.Cut.GroupID || current.Cut.Lineage != vector.Cut.Lineage ||
		!rootVectorMembersSameIdentity(current, vector) {
		return storeio.ErrRootVectorIdentity
	}
	if rootVectorContentEqual(current, vector) && converged {
		return nil
	}
	if err := storeio.ValidateRootVectorSuccessor(current, vector); err != nil {
		return err
	}
	if current.Sequence > ^uint64(0)-2 {
		return storeio.ErrRootVectorSequence
	}
	first := vector
	first.Sequence = current.Sequence + 1
	// Always write the older bank first. This leaves the currently selected
	// complete bank untouched until the successor is durable, so a crash after
	// either write still leaves two consecutive selectable sequences.
	firstSlot := currentSlot ^ 1
	if err := c.writeBankLocked(first, firstSlot); err != nil {
		return err
	}
	second := vector
	second.Sequence = first.Sequence + 1
	return c.writeBankLocked(second, currentSlot)
}

// CaptureExactRootVector folds all fixed members through the existing
// journal-backed checkpoint owner, captures their complete durable inline roots
// at one applied cut, and publishes both exact-root banks. It is intentionally
// an opt-in foundation: no normal apply path calls it and no journal/marker
// authority is changed.
func (g *CheckpointGroup) CaptureExactRootVector(
	path string, cut RootVectorCut,
) (RootVector, []RootVectorMemberFloor, error) {
	result, err := g.CaptureExactRootVectorResult(path, cut)
	if err != nil {
		return RootVector{}, nil, err
	}
	return result.Vector, result.Floors, nil
}

// CaptureExactRootVectorResult is CaptureExactRootVector with a same-content
// change bit. A same-applied-cut physical root change therefore still advances
// both banks; an unchanged vector performs no sidecar write.
func (g *CheckpointGroup) CaptureExactRootVectorResult(
	path string, cut RootVectorCut,
) (ExactRootVectorCapture, error) {
	if g == nil {
		return ExactRootVectorCapture{}, ErrCheckpointGroupOwned
	}
	if path == "" {
		return ExactRootVectorCapture{}, ErrExactRootVectorPath
	}
	if info, statErr := os.Stat(path); statErr == nil && info.IsDir() {
		path = filepath.Join(path, ExactRootVectorFilename)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return ExactRootVectorCapture{}, err
	}
	if cut.Applied != g.applied {
		return ExactRootVectorCapture{}, fmt.Errorf(
			"%w: exact-root cut %d, owner applied %d",
			storeio.ErrRootVectorCut, cut.Applied, g.applied,
		)
	}
	if err := g.checkpointLocked(); err != nil {
		return ExactRootVectorCapture{}, err
	}
	lineage := checkpointGroupCertificateLineageDigest(g.certificateLocked())
	if cut.Lineage == ([32]byte{}) {
		cut.Lineage = lineage
	} else if cut.Lineage != lineage {
		return ExactRootVectorCapture{}, fmt.Errorf(
			"%w: exact-root lineage", storeio.ErrRootVectorIdentity,
		)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/exact-root-vector/group/v1\x00"))
	_, _ = h.Write(g.markerID[:])
	_, _ = h.Write(lineage[:])
	var groupID [32]byte
	copy(groupID[:], h.Sum(nil))
	if cut.GroupID == ([32]byte{}) {
		cut.GroupID = groupID
	} else if cut.GroupID != groupID {
		return ExactRootVectorCapture{}, fmt.Errorf(
			"%w: exact-root group", storeio.ErrRootVectorIdentity,
		)
	}
	vector := RootVector{Cut: cut, Members: make([]RootVectorMember, len(g.members))}
	// Preserve the lock order used by group publication. The owner mutex already
	// excludes a concurrent group update; member writers protect inlineFree and
	// durableState while this exact image is copied.
	g.log.commitMu.Lock()
	order := make([]*Collection, len(g.members))
	for i := range g.members {
		order[i] = g.members[i].collection
	}
	sortCollectionSnapshotOrder(order)
	for _, collection := range order {
		collection.writer.Lock()
	}
	for _, collection := range order {
		collection.snapshotGate.Lock()
	}
	unlockCapture := func() {
		for index := len(order) - 1; index >= 0; index-- {
			order[index].snapshotGate.Unlock()
		}
		for index := len(order) - 1; index >= 0; index-- {
			order[index].writer.Unlock()
		}
		g.log.commitMu.Unlock()
	}
	for i, member := range g.members {
		image, err := exactRootImageLocked(member.collection)
		if err != nil {
			unlockCapture()
			return ExactRootVectorCapture{}, err
		}
		vector.Members[i] = RootVectorMember{
			NameDigest: member.nameDigest,
			StoreID:    member.storeID,
			JournalID:  member.journalID,
			Root:       image,
		}
	}
	unlockCapture()
	// CheckpointGroup membership is name-sorted, while the durable vector uses
	// the digest order required by its canonical codec. Sort only after the
	// coherent images have been copied; the owner-to-image association is already
	// carried by each member descriptor.
	slices.SortFunc(vector.Members, func(left, right RootVectorMember) int {
		if result := bytes.Compare(left.NameDigest[:], right.NameDigest[:]); result != 0 {
			return result
		}
		if result := bytes.Compare(left.StoreID[:], right.StoreID[:]); result != 0 {
			return result
		}
		return bytes.Compare(left.JournalID[:], right.JournalID[:])
	})

	checkpoint, err := OpenExactRootVectorCheckpoint(path, len(vector.Members))
	if err != nil {
		return ExactRootVectorCapture{}, err
	}
	defer checkpoint.Close()
	previous, _, previousErr := checkpoint.Read()
	if previousErr != nil && !errors.Is(previousErr, storeio.ErrRootVectorMissing) {
		return ExactRootVectorCapture{}, previousErr
	}
	if err := checkpoint.Publish(vector); err != nil {
		if errors.Is(err, ErrCommitOutcomeUnknown) {
			return ExactRootVectorCapture{}, g.poisonLocked(err)
		}
		return ExactRootVectorCapture{}, err
	}
	selected, floors, err := checkpoint.Read()
	if err != nil {
		return ExactRootVectorCapture{}, g.poisonLocked(journalCommitOutcomeUnknown(err))
	}
	for _, floor := range floors {
		found := false
		for _, member := range g.members {
			if member.storeID == floor.StoreID && member.nameDigest == floor.NameDigest {
				found = true
				if err := member.collection.installExactRootRecoveryFloor(floor.Generation); err != nil {
					return ExactRootVectorCapture{}, g.poisonLocked(
						journalCommitOutcomeUnknown(err),
					)
				}
				break
			}
		}
		if !found {
			return ExactRootVectorCapture{}, g.poisonLocked(
				journalCommitOutcomeUnknown(storeio.ErrRootVectorMember),
			)
		}
	}
	changed := previousErr != nil || !rootVectorContentEqual(previous, selected)
	return ExactRootVectorCapture{Vector: selected, Floors: floors, Changed: changed}, nil
}

func exactRootImageLocked(collection *Collection) (storeio.InlineSuperblock, error) {
	if collection == nil {
		return storeio.InlineSuperblock{}, ErrCheckpointGroupCorrupt
	}
	state := collection.durableState.Load()
	if state == nil || state.root.Generation == 0 {
		return storeio.InlineSuperblock{}, fmt.Errorf(
			"%w: member has no durable root", ErrCheckpointGroupCorrupt,
		)
	}
	return storeio.InlineSuperblock{
		StoreID:    collection.storeID,
		Generation: state.root.Generation,
		FileEnd:    state.fileEnd,
		PageSize:   state.root.PageSize,
		State:      state.root,
		FreeDelta:  collection.inlineFree,
	}, nil
}

// openAtAuthenticatedRoot repairs one member's mutable root slots from an
// already selected complete vector image before ordinary collection bootstrap.
// The caller must have authenticated the vector/member association; this
// function deliberately performs no fallback or cross-member root selection.
func openAtAuthenticatedRoot(
	file *os.File,
	options Options,
	member RootVectorMember,
	floor uint64,
) (*Collection, error) {
	if file == nil || member.StoreID == ([16]byte{}) || floor == 0 {
		return nil, fmt.Errorf("%w: exact-root member", storeio.ErrRootVectorMember)
	}
	root := member.Root
	if root.StoreID != member.StoreID || root.State.StoreID != member.StoreID ||
		root.State.JournalID != member.JournalID || floor > root.Generation {
		return nil, storeio.ErrRootVectorMember
	}
	return openCollection(file, options, collectionOpenConfig{
		checkpointGroupRecovery: true,
		deferJournalReplay:      true,
		exactRoot:               &root,
		exactRootStoreID:        member.StoreID,
		exactRecoveryFloor:      floor,
	})
}

// OpenCollectionsAtAuthenticatedRoot opens every fixed member from one
// selected vector. It validates the complete member set before exposing any
// collection and closes every partially opened handle on failure. Journals
// must already be empty because this foundation selects a folded root while
// leaving the production journal protocol authoritative.
func OpenCollectionsAtAuthenticatedRoot(
	vector RootVector,
	floors []RootVectorMemberFloor,
	requests []TransactionCollectionOpen,
	names []string,
) ([]*Collection, error) {
	if len(requests) != len(names) || len(requests) != len(vector.Members) || len(requests) == 0 {
		return nil, fmt.Errorf("%w: exact-root request membership", storeio.ErrRootVectorMember)
	}
	if len(floors) != len(vector.Members) {
		return nil, fmt.Errorf("%w: exact-root floor membership", storeio.ErrRootVectorMember)
	}
	bankBytes, err := storeio.RootVectorBankBytes(len(vector.Members))
	if err != nil {
		return nil, err
	}
	if _, err := storeio.EncodeRootVectorBank(make([]byte, bankBytes), vector); err != nil {
		return nil, err
	}
	byName := make(map[[32]byte]int, len(vector.Members))
	byStore := make(map[[16]byte]int, len(vector.Members))
	for i, member := range vector.Members {
		if _, duplicate := byName[member.NameDigest]; duplicate {
			return nil, storeio.ErrRootVectorMember
		}
		if _, duplicate := byStore[member.StoreID]; duplicate {
			return nil, storeio.ErrRootVectorIdentity
		}
		byName[member.NameDigest] = i
		byStore[member.StoreID] = i
	}
	collections := make([]*Collection, len(requests))
	abort := func(cause error, count int) ([]*Collection, error) {
		for i := count - 1; i >= 0; i-- {
			if collections[i] != nil {
				_ = collections[i].closeResources()
			}
		}
		return nil, cause
	}
	seen := make(map[int]struct{}, len(requests))
	for i := range requests {
		if requests[i].File == nil || names[i] == "" {
			return abort(storeio.ErrRootVectorMember, i)
		}
		memberIndex, ok := byName[sha256.Sum256([]byte(names[i]))]
		if !ok || memberIndex < 0 {
			return abort(fmt.Errorf("%w: member name %q", storeio.ErrRootVectorMember, names[i]), i)
		}
		if _, duplicate := seen[memberIndex]; duplicate {
			return abort(storeio.ErrRootVectorMember, i)
		}
		seen[memberIndex] = struct{}{}
		member := vector.Members[memberIndex]
		floor := floors[memberIndex]
		if floor.StoreID != member.StoreID || floor.NameDigest != member.NameDigest ||
			floor.Generation == 0 || floor.Generation > member.Root.Generation {
			return abort(storeio.ErrRootVectorMember, i)
		}
		collection, openErr := openAtAuthenticatedRoot(
			requests[i].File, requests[i].Options, member, floor.Generation,
		)
		if openErr != nil {
			return abort(openErr, i)
		}
		collections[i] = collection
		if collection.storeID != member.StoreID {
			return abort(storeio.ErrRootVectorMember, i+1)
		}
		state := collection.durableState.Load()
		if state == nil {
			return abort(storeio.ErrRootVectorCorrupt, i+1)
		}
		gotImage := storeio.InlineSuperblock{
			StoreID: collection.storeID, Generation: state.root.Generation,
			FileEnd: state.fileEnd, PageSize: state.root.PageSize,
			State: state.root, FreeDelta: collection.inlineFree,
		}
		gotBytes := make([]byte, storeio.RootVectorRootBytes)
		wantBytes := make([]byte, storeio.RootVectorRootBytes)
		if _, encodeErr := storeio.EncodeInlineSuperblock(gotBytes, gotImage); encodeErr != nil {
			return abort(encodeErr, i+1)
		}
		if _, encodeErr := storeio.EncodeInlineSuperblock(wantBytes, member.Root); encodeErr != nil ||
			string(gotBytes) != string(wantBytes) {
			if encodeErr == nil {
				encodeErr = storeio.ErrRootVectorMember
			}
			return abort(encodeErr, i+1)
		}
		if collection.journal != nil && collection.journal.Cursor() != 0 {
			return abort(fmt.Errorf("%w: member %q has an unfurled journal", storeio.ErrRootVectorCorrupt, names[i]), i+1)
		}
	}
	if len(seen) != len(vector.Members) {
		return abort(storeio.ErrRootVectorMember, len(collections))
	}
	return collections, nil
}

func (c *ExactRootVectorCheckpoint) readLocked() (RootVector, []RootVectorMemberFloor, error) {
	vector, floors, _, _, err := c.readSelectedLocked()
	return vector, floors, err
}

func (c *ExactRootVectorCheckpoint) readSelectedLocked() (RootVector, []RootVectorMemberFloor, int, bool, error) {
	if c.closed || c.file == nil {
		return RootVector{}, nil, -1, false, ErrExactRootVectorClosed
	}
	info, err := c.proveEntryLocked()
	if err != nil {
		return RootVector{}, nil, -1, false, err
	}
	if info.Size() == 0 {
		return RootVector{}, nil, -1, false, storeio.ErrRootVectorMissing
	}
	if c.bankBytes == 0 {
		if err := c.setBankGeometryLocked(info.Size()); err != nil {
			return RootVector{}, nil, -1, false, err
		}
	}
	if info.Size() != int64(c.bankBytes*2) {
		return RootVector{}, nil, -1, false, fmt.Errorf("%w: file changed size", storeio.ErrRootVectorCorrupt)
	}
	first := make([]byte, c.bankBytes)
	second := make([]byte, c.bankBytes)
	if err := readFullAt(c.file, first, 0); err != nil {
		return RootVector{}, nil, -1, false, err
	}
	if err := readFullAt(c.file, second, int64(c.bankBytes)); err != nil {
		return RootVector{}, nil, -1, false, err
	}
	vector, slot, err := storeio.SelectRootVectorBanks(first, second)
	if err != nil {
		return RootVector{}, nil, -1, false, err
	}
	floors, err := storeio.RootVectorMemberFloors(first, second)
	if err != nil {
		return RootVector{}, nil, -1, false, err
	}
	converged, err := storeio.RootVectorBanksConverged(first, second)
	if err != nil {
		return RootVector{}, nil, -1, false, err
	}
	if c.memberCount != 0 && len(vector.Members) != c.memberCount {
		return RootVector{}, nil, -1, false, fmt.Errorf("%w: member count", storeio.ErrRootVectorMember)
	}
	return vector, floors, slot, converged, nil
}

func (c *ExactRootVectorCheckpoint) writeBankLocked(vector RootVector, slot int) error {
	if slot < 0 || slot > 1 || c.bankBytes == 0 {
		return storeio.ErrRootVectorCorrupt
	}
	info, err := c.proveEntryLocked()
	if err != nil {
		return err
	}
	if info.Size() != int64(c.bankBytes*2) {
		return fmt.Errorf("%w: file changed size", storeio.ErrRootVectorCorrupt)
	}
	encoded := make([]byte, c.bankBytes)
	if _, err := storeio.EncodeRootVectorBank(encoded, vector); err != nil {
		return err
	}
	if err := writeFullAt(c.file, encoded, int64(slot*c.bankBytes)); err != nil {
		return journalCommitOutcomeUnknown(fmt.Errorf("write exact-root bank: %w", err))
	}
	if err := c.file.Sync(); err != nil {
		return journalCommitOutcomeUnknown(fmt.Errorf("sync exact-root bank: %w", err))
	}
	if _, err := c.proveEntryLocked(); err != nil {
		return journalCommitOutcomeUnknown(fmt.Errorf("prove exact-root bank entry: %w", err))
	}
	return nil
}

func (c *ExactRootVectorCheckpoint) writeInitialBanksLocked(vector RootVector) error {
	bankBytes, err := storeio.RootVectorBankBytes(len(vector.Members))
	if err != nil {
		return err
	}
	if c.bankBytes != 0 && c.bankBytes != bankBytes {
		return fmt.Errorf("%w: initial bank geometry", storeio.ErrRootVectorMember)
	}
	info, err := c.proveEntryLocked()
	if err != nil {
		return err
	}
	c.memberCount = len(vector.Members)
	c.bankBytes = bankBytes
	wantSize := int64(bankBytes * 2)
	if info.Size() == 0 {
		if err := c.file.Truncate(wantSize); err != nil {
			return err
		}
		if err := c.file.Sync(); err != nil {
			return err
		}
	} else if info.Size() != wantSize {
		return fmt.Errorf("%w: initial file size", storeio.ErrRootVectorCorrupt)
	}
	if c.parentDirty {
		if err := syncRecoveryJournalParent(c.path); err != nil {
			return fmt.Errorf("sync exact-root parent directory: %w", err)
		}
		c.parentDirty = false
	}
	first := vector
	first.Sequence = 1
	if err := c.writeBankLocked(first, 0); err != nil {
		return err
	}
	second := vector
	second.Sequence = 2
	return c.writeBankLocked(second, 1)
}

func (c *ExactRootVectorCheckpoint) proveEntryLocked() (os.FileInfo, error) {
	if c == nil || c.file == nil || c.fileInfo == nil {
		return nil, ErrExactRootVectorClosed
	}
	descriptor, err := c.file.Stat()
	if err != nil {
		return nil, err
	}
	entry, err := os.Lstat(c.path)
	if err != nil {
		return nil, err
	}
	if !descriptor.Mode().IsRegular() || !entry.Mode().IsRegular() ||
		!os.SameFile(descriptor, entry) || !os.SameFile(descriptor, c.fileInfo) {
		return nil, fmt.Errorf("%w: sidecar entry changed", storeio.ErrRootVectorCorrupt)
	}
	return descriptor, nil
}

func (c *ExactRootVectorCheckpoint) setBankGeometryLocked(fileBytes int64) error {
	minimumBank, _ := storeio.RootVectorBankBytes(1)
	maximumBank, _ := storeio.RootVectorBankBytes(storeio.RootVectorMaxMembers)
	if fileBytes%2 != 0 || fileBytes < 2*int64(minimumBank) ||
		fileBytes > 2*int64(maximumBank) {
		return fmt.Errorf("%w: file size %d", storeio.ErrRootVectorCorrupt, fileBytes)
	}
	bankBytes := int(fileBytes / 2)
	if bankBytes%storeio.InlineSuperblockSize != 0 {
		return fmt.Errorf("%w: bank size %d", storeio.ErrRootVectorCorrupt, bankBytes)
	}
	c.bankBytes = bankBytes
	return nil
}

func (c *ExactRootVectorCheckpoint) zeroFilledLocked() (bool, error) {
	info, err := c.proveEntryLocked()
	if err != nil {
		return false, err
	}
	if c.bankBytes == 0 || info.Size() != int64(c.bankBytes*2) {
		return false, fmt.Errorf("%w: zero-bank geometry", storeio.ErrRootVectorCorrupt)
	}
	var scratch [storeio.InlineSuperblockSize]byte
	for offset := int64(0); offset < info.Size(); {
		chunk := scratch[:]
		if remaining := info.Size() - offset; remaining < int64(len(chunk)) {
			chunk = chunk[:int(remaining)]
		}
		if err := readFullAt(c.file, chunk, offset); err != nil {
			return false, err
		}
		for _, value := range chunk {
			if value != 0 {
				return false, nil
			}
		}
		offset += int64(len(chunk))
	}
	return true, nil
}

func validateRootVectorForPublish(vector RootVector) error {
	// Encoding through the canonical codec performs all structural validation;
	// the sequence is assigned by this owner and therefore may be zero here.
	if vector.Sequence == 0 {
		vector.Sequence = 1
	}
	bankBytes, err := storeio.RootVectorBankBytes(len(vector.Members))
	if err != nil {
		return err
	}
	_, err = storeio.EncodeRootVectorBank(make([]byte, bankBytes), vector)
	return err
}

func rootVectorContentEqual(left, right RootVector) bool {
	if left.Cut != right.Cut || !rootVectorMembersSameIdentity(left, right) {
		return false
	}
	for i := range left.Members {
		if left.Members[i].NameDigest != right.Members[i].NameDigest ||
			left.Members[i].StoreID != right.Members[i].StoreID ||
			left.Members[i].JournalID != right.Members[i].JournalID {
			return false
		}
		leftImage := make([]byte, storeio.RootVectorRootBytes)
		rightImage := make([]byte, storeio.RootVectorRootBytes)
		if _, err := storeio.EncodeInlineSuperblock(leftImage, left.Members[i].Root); err != nil {
			return false
		}
		if _, err := storeio.EncodeInlineSuperblock(rightImage, right.Members[i].Root); err != nil {
			return false
		}
		if string(leftImage) != string(rightImage) {
			return false
		}
	}
	return true
}

func rootVectorMembersSameIdentity(left, right RootVector) bool {
	if len(left.Members) != len(right.Members) {
		return false
	}
	for i := range left.Members {
		if left.Members[i].NameDigest != right.Members[i].NameDigest ||
			left.Members[i].StoreID != right.Members[i].StoreID ||
			left.Members[i].JournalID != right.Members[i].JournalID {
			return false
		}
	}
	return true
}

func readFullAt(file *os.File, dst []byte, offset int64) error {
	read := 0
	for read < len(dst) {
		n, err := file.ReadAt(dst[read:], offset+int64(read))
		read += n
		if err != nil {
			if errors.Is(err, io.EOF) && read == len(dst) {
				return nil
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func writeFullAt(file *os.File, src []byte, offset int64) error {
	written := 0
	for written < len(src) {
		n, err := file.WriteAt(src[written:], offset+int64(written))
		written += n
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
