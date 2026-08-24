package durable

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
)

// A CheckpointGroup is the fixed-membership durability owner used by local
// replicated apply. It deliberately has one on-disk format (format 0): there is
// no compatibility ladder and no alternate record grammar.
const (
	checkpointGroupFilename           = "checkpoint.vgc"
	checkpointGroupFormat      uint16 = 0
	checkpointGroupSlotBytes          = 4096
	checkpointGroupSlots              = 2
	checkpointGroupFileBytes          = checkpointGroupSlotBytes * checkpointGroupSlots
	checkpointGroupHeaderBytes        = 168
	// Byte 14 is the authenticated maximum logical apply span represented by any
	// one physical transaction in this certificate's history. Byte 15 remains
	// canonical zero padding. A max-span byte of zero is the sole canonical
	// encoding of effective span one and preserves every
	// legacy format-0 certificate byte-for-byte. Values 2..128 enable the
	// bounded consecutive-update grammar. Encoded one and values above the hard
	// bound are authenticated corruption.
	checkpointGroupMaxApplySpanOffset = 14
	checkpointGroupMemberBytes        = 64
	checkpointGroupChecksumOffset     = checkpointGroupSlotBytes - sha256.Size
	// The final 24 authenticated bytes before the checksum are outside even the
	// maximum member bank (which ends at byte 4008). Legacy format-0 files keep
	// them all zero. A widened max span uses them as the exact transaction/range
	// witness that prevents a checksum-valid certificate from merely widening
	// its applied/transaction sanity envelope.
	checkpointGroupMaxSpanWitnessTxnOffset   = checkpointGroupChecksumOffset - 24
	checkpointGroupMaxSpanWitnessFirstOffset = checkpointGroupChecksumOffset - 16
	checkpointGroupMaxSpanWitnessLastOffset  = checkpointGroupChecksumOffset - 8
	checkpointGroupMaxMembers                = (checkpointGroupMaxSpanWitnessTxnOffset - checkpointGroupHeaderBytes) / checkpointGroupMemberBytes
	defaultCheckpointEvery                   = 128
	// MaxCheckpointGroupUpdateEntries bounds the logical index range one
	// physical group transaction may certify. It matches the replicated apply
	// port's independent bound without importing that higher layer.
	MaxCheckpointGroupUpdateEntries = 128
)

var checkpointGroupMagic = [8]byte{'V', 'I', 'B', 'E', 'C', 'P', 'G', 0}
var checkpointGroupDigestDomain = []byte("vibedb/checkpoint-group/format-0\x00")
var checkpointGroupSeedDomain = []byte("vibedb/checkpoint-group/seed-state-envelope/format-0\x00")

var (
	// ErrCheckpointGroupOwned reports a direct generic mutation, Flush, Close,
	// or UpdateCollections call against a collection/TxnLog exclusively attached
	// to a CheckpointGroup.
	ErrCheckpointGroupOwned = errors.New(
		"vibedb: collection and transaction log are exclusively owned by a checkpoint group",
	)
	// ErrCheckpointGroupPressure is a pre-publication signal. A physical
	// checkpoint would otherwise persist a reader-visible replicated suffix past
	// the certified cut. CheckpointGroup catches it, checkpoints the group, and
	// retries; callers outside the group may treat it as bounded admission.
	ErrCheckpointGroupPressure = errors.New(
		"vibedb: checkpoint group must advance before this mutation can be admitted",
	)
	// ErrCheckpointGroupCorrupt reports a missing, torn, mismatched, or
	// non-contiguous format-0 certificate/decision prefix.
	ErrCheckpointGroupCorrupt = errors.New(
		"vibedb: corrupt checkpoint group certificate or decision prefix",
	)
	// ErrCheckpointGroupMissing distinguishes a clean pre-activation replicated
	// store from a checkpoint-group reopen. Callers may create format 0 only when
	// every intended member is empty and the marker has no decision.
	ErrCheckpointGroupMissing = errors.New(
		"vibedb: checkpoint group certificate is missing",
	)
	// ErrCheckpointGroupRecoveryRequired reports use of NewCheckpointGroup
	// beside an existing certificate. Existing format 0 must be opened through
	// OpenCollectionsWithCheckpointGroup so no generic recovery can commit an
	// uncertified marker suffix before the certificate is consulted.
	ErrCheckpointGroupRecoveryRequired = errors.New(
		"vibedb: checkpoint group requires certificate-authoritative recovery",
	)
	// ErrCheckpointGroupSequence reports a non-consecutive replicated apply cut
	// or exhausted internal transaction sequence. Update permits same-index
	// metadata publications or an advance of exactly +1. UpdateConsecutive
	// permits one bounded range beginning exactly at the next apply index.
	ErrCheckpointGroupSequence = errors.New(
		"vibedb: invalid checkpoint group apply sequence",
	)
	// ErrCheckpointGroupSeedChanged reports that an imported member image moved
	// after it was authenticated but before fixed group ownership could be
	// attached. No certificate is published for this pre-activation refusal.
	ErrCheckpointGroupSeedChanged = errors.New(
		"vibedb: checkpoint group seed image changed before activation",
	)
)

// CheckpointGroupOptions fixes the periodic certificate cadence in physical
// group transactions. Zero selects 128 transactions. Since one consecutive
// update may cover MaxCheckpointGroupUpdateEntries logical apply indices, the
// default certificate lag can reach 128*MaxCheckpointGroupUpdateEntries
// logical indices. Pressure may checkpoint earlier. It never checkpoints after
// publishing the mutation that encountered pressure.
type CheckpointGroupOptions struct {
	CheckpointEvery uint64
}

// CheckpointGroupSeed binds already durable member images to the one imported
// replicated-state row that may establish a group above cut one.
// Envelope is borrowed only for the constructor call. Its domain-separated
// commitment, the exact member name, and Applied are authenticated by the
// format-0 certificate before any group-owned mutation is admitted.
type CheckpointGroupSeed struct {
	Applied  uint64
	Member   string
	Envelope []byte
	// Images form the transient activation fence for every already durable
	// imported member. They are never encoded in format 0: the constructor checks
	// the exact generations while it holds every member writer, then publishes
	// group ownership before releasing those writers. Images must name every fixed
	// member except Member exactly once. The slice is borrowed for the constructor
	// call and is ignored by Seed and ValidateSeedState.
	Images []CheckpointGroupSeedImage
}

// CheckpointGroupSeedImage binds one imported collection handle to the exact
// generation authenticated by its preparation scan. Collection identity avoids
// a second string-keyed membership domain at the scan-to-ownership handoff.
type CheckpointGroupSeedImage struct {
	Collection *Collection
	Generation uint64
}

func (o CheckpointGroupOptions) normalized() (CheckpointGroupOptions, error) {
	if o.CheckpointEvery == 0 {
		o.CheckpointEvery = defaultCheckpointEvery
	}
	if o.CheckpointEvery == math.MaxUint64 {
		return CheckpointGroupOptions{}, ErrCheckpointGroupSequence
	}
	return o, nil
}

func checkpointGroupSeedProfile(seed CheckpointGroupSeed) (
	applied uint64,
	state [sha256.Size]byte,
	member [sha256.Size]byte,
	err error,
) {
	if seed.Applied <= 1 || seed.Applied == math.MaxUint64 ||
		seed.Member == "" || len(seed.Envelope) == 0 {
		return 0, [sha256.Size]byte{}, [sha256.Size]byte{}, ErrCheckpointGroupSequence
	}
	h := sha256.New()
	_, _ = h.Write(checkpointGroupSeedDomain)
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(seed.Envelope)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(seed.Envelope)
	_ = h.Sum(state[:0])
	member = sha256.Sum256([]byte(seed.Member))
	return seed.Applied, state, member, nil
}

// CheckpointGroupStats is a detached counter snapshot. BarrierSyncs counts the
// K participant-journal Syncs plus the one certificate Sync used by ordinary
// checkpoints. MarkerSyncs counts only exceptional marker recycling; the
// marker is not commit authority and is never synced by the normal barrier.
// A normal transition leaves every Sync counter unchanged.
type CheckpointGroupStats struct {
	AppliedIndex           uint64
	CheckpointAppliedIndex uint64
	TransactionHighWater   uint64
	CheckpointTransactions uint64
	Updates                uint64
	// LargestUpdateSpan is the largest number of consecutive logical applied
	// indices represented by one successful physical Update transaction. It is
	// zero before the first transaction and one for legacy/single-entry history.
	// Updates itself retains its physical-transaction meaning.
	LargestUpdateSpan   uint64
	Checkpoints         uint64
	JournalSyncs        uint64
	MarkerSyncs         uint64
	CertificateSyncs    uint64
	BarrierSyncs        uint64
	PhysicalCheckpoints uint64
}

type checkpointGroupMember struct {
	name       string
	nameDigest [sha256.Size]byte
	collection *Collection
	storeID    [16]byte
	journalID  [16]byte
}

type checkpointGroupCertificate struct {
	sequence     uint64
	applied      uint64
	txnHighWater uint64
	txnBase      uint64
	maxApplySpan uint8
	maxSpanTxn   uint64
	maxSpanFirst uint64
	maxSpanLast  uint64
	markerEpoch  uint64
	markerID     [16]byte
	seedApplied  uint64
	seedState    [sha256.Size]byte
	seedMember   [sha256.Size]byte
	members      []checkpointGroupMember
}

// CheckpointGroup exclusively owns one TxnLog and a fixed collection set. It
// uses an internal transaction ordinal, separate from the Raft index, so a
// same-index snapshot-base metadata publication can be certified without
// weakening consecutive Raft apply validation.
type CheckpointGroup struct {
	mu sync.Mutex

	log      *TxnLog
	members  []checkpointGroupMember
	byName   map[string]*Collection
	opts     CheckpointGroupOptions
	file     *os.File
	fileInfo os.FileInfo

	sequence     uint64
	txnBase      uint64
	markerEpoch  uint64
	markerID     [16]byte
	txn          uint64
	applied      uint64
	maxApplySpan uint8
	maxSpanTxn   uint64
	maxSpanFirst uint64
	maxSpanLast  uint64
	foldedTxn    uint64
	seedApplied  uint64
	seedState    [sha256.Size]byte
	seedMember   [sha256.Size]byte
	closed       bool
	closeErr     error
	poison       error

	visibleApplied atomic.Uint64
	visibleTxn     atomic.Uint64
	certApplied    atomic.Uint64
	certTxn        atomic.Uint64

	updates             atomic.Uint64
	checkpoints         atomic.Uint64
	journalSyncs        atomic.Uint64
	markerSyncs         atomic.Uint64
	certificateSyncs    atomic.Uint64
	barrierSyncs        atomic.Uint64
	physicalCheckpoints atomic.Uint64
}

// checkpointGroupFaultPoint is a package test seam. Production leaves the hook
// nil. Each point is after the named operation has completed.
type checkpointGroupFaultPoint uint8

const (
	checkpointGroupAfterJournalSync checkpointGroupFaultPoint = iota + 1
	checkpointGroupAfterMarkerSync
	checkpointGroupAfterCertificateWrite
	checkpointGroupAfterCertificateSync
	checkpointGroupAfterPhysicalCheckpoint
	checkpointGroupAfterCertificateRename
	checkpointGroupAfterPrepareAppend
	checkpointGroupAfterDecisionAppend
)

var (
	checkpointGroupFaultHook                        func(checkpointGroupFaultPoint) error
	checkpointGroupAfterInitialValidationHook       func()
	checkpointGroupAfterDirectoryMembershipHook     func()
	checkpointGroupCertificateCloseHook             func(*os.File) error
	checkpointGroupGenericDatabaseAfterPrecheckHook func()
	// checkpointGroupNamespaceLease serializes the in-process engine entry
	// points that can create/open a collection with the activation scan and
	// fixed-name certificate publication. It is intentionally process-global:
	// activation is rare, and avoiding a path-keyed lease keeps symlink/rename
	// aliases from splitting the fence. Cross-process catalog ownership remains
	// the caller's existing exclusive-directory responsibility.
	checkpointGroupNamespaceLease sync.RWMutex
)

// rejectCheckpointGroupCertificate prevents every generic catalog recovery
// path from consulting txn.vtm or member journals while the format-0
// certificate is present. The certificate, not txn.vtm, is the commit
// authority; even a read that later fails may otherwise fold an uncertified
// suffix before discovering that it used the wrong resolver.
func rejectCheckpointGroupCertificate(root *os.Root) error {
	if root == nil {
		return ErrCheckpointGroupCorrupt
	}
	_, err := root.Lstat(checkpointGroupFilename)
	switch {
	case err == nil:
		return ErrCheckpointGroupRecoveryRequired
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return err
	}
}

// rejectCheckpointGroupCertificateForFile extends the persistent fence to the
// standalone collection opener. A caller-owned descriptor does not retain its
// parent directory, so prove its current regular entry and directory identity
// before trusting the sibling-certificate lookup. This is the same descriptor
// identity contract used by transaction recovery.
func rejectCheckpointGroupCertificateForFile(file *os.File) error {
	if file == nil {
		return nil
	}
	root, err := os.OpenRoot(filepath.Dir(file.Name()))
	if err != nil {
		return err
	}
	defer root.Close()
	entry, err := root.Lstat(filepath.Base(file.Name()))
	if err != nil {
		return err
	}
	if !entry.Mode().IsRegular() {
		return ErrTransactionLogDirectoryMismatch
	}
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(opened, entry) {
		return ErrTransactionLogDirectoryMismatch
	}
	return rejectCheckpointGroupCertificate(root)
}

func acquireCheckpointGroupGenericNamespace(file *os.File) (func(), error) {
	checkpointGroupNamespaceLease.RLock()
	if err := rejectCheckpointGroupCertificateForFile(file); err != nil {
		checkpointGroupNamespaceLease.RUnlock()
		return nil, err
	}
	return checkpointGroupNamespaceLease.RUnlock, nil
}

func acquireCheckpointGroupGenericCatalogNamespace(root *os.Root) (func(), error) {
	checkpointGroupNamespaceLease.RLock()
	if err := rejectCheckpointGroupCertificate(root); err != nil {
		checkpointGroupNamespaceLease.RUnlock()
		return nil, err
	}
	return checkpointGroupNamespaceLease.RUnlock, nil
}

func acquireCheckpointGroupRecoveryNamespace() (func(), error) {
	checkpointGroupNamespaceLease.RLock()
	return checkpointGroupNamespaceLease.RUnlock, nil
}

// NewCheckpointGroup creates format 0 at cut zero. An existing certificate is
// never adopted from generic-recovered handles: callers must reopen it through
// OpenCollectionsWithCheckpointGroup so an uncertified marker suffix cannot be
// folded before the certificate is consulted. Format 0 is created only when
// every member is logically empty and txn.vtm has no decision; this is the
// crash-safe first-activation seam.
func NewCheckpointGroup(
	log *TxnLog,
	members []NamedCollection,
	options CheckpointGroupOptions,
) (*CheckpointGroup, error) {
	return newCheckpointGroup(log, members, options, nil)
}

// NewSeededCheckpointGroup creates format 0 at certified cut zero around clean,
// already durable imported images. The named seed member itself must be empty;
// every other fixed member is covered by an exact generation witness and may
// already contain durable rows. The seed's exact state envelope is committed to
// the initial certificate, but is not written until Seed publishes it through
// the exclusively attached group.
func NewSeededCheckpointGroup(
	log *TxnLog,
	members []NamedCollection,
	seed CheckpointGroupSeed,
	options CheckpointGroupOptions,
) (*CheckpointGroup, error) {
	return newCheckpointGroup(log, members, options, &seed)
}

func newCheckpointGroup(
	log *TxnLog,
	members []NamedCollection,
	options CheckpointGroupOptions,
	seed *CheckpointGroupSeed,
) (*CheckpointGroup, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	ordered, err := checkpointGroupMembers(members)
	if err != nil {
		return nil, err
	}
	if log == nil {
		return nil, fmt.Errorf("%w: nil transaction log", ErrCheckpointGroupOwned)
	}
	if err := log.ValidateCollections(members); err != nil {
		return nil, err
	}
	var seedApplied uint64
	var seedState, seedMember [sha256.Size]byte
	if seed != nil {
		seedApplied, seedState, seedMember, err = checkpointGroupSeedProfile(*seed)
		if err != nil {
			return nil, err
		}
		var seedCollection *Collection
		for _, member := range ordered {
			if member.name == seed.Member && member.nameDigest == seedMember {
				seedCollection = member.collection
			}
		}
		if seedCollection == nil {
			return nil, fmt.Errorf("%w: seed member is outside fixed membership", ErrCheckpointGroupOwned)
		}
		if len(seed.Images) != len(ordered)-1 {
			return nil, fmt.Errorf("%w: imported image witness count", ErrCheckpointGroupSeedChanged)
		}
		for i, image := range seed.Images {
			if image.Collection == nil || image.Collection == seedCollection || image.Generation == 0 {
				return nil, fmt.Errorf("%w: invalid imported image witness", ErrCheckpointGroupSeedChanged)
			}
			owned := false
			for _, member := range ordered {
				if member.collection == image.Collection {
					owned = true
					break
				}
			}
			if !owned {
				return nil, fmt.Errorf("%w: imported image witness is outside fixed membership", ErrCheckpointGroupSeedChanged)
			}
			for prior := range i {
				if seed.Images[prior].Collection == image.Collection {
					return nil, fmt.Errorf("%w: duplicate imported image witness", ErrCheckpointGroupSeedChanged)
				}
			}
		}
		// Staging uses ordinary single-collection journaled writes. Fold only a
		// member that actually carries such a suffix before fixed-group
		// activation; already-clean images incur no redundant Flush or scan.
		for _, member := range ordered {
			member.collection.writer.Lock()
			pending := member.collection.journal == nil || member.collection.journal.Cursor() != 0
			member.collection.writer.Unlock()
			if pending {
				if err := member.collection.Flush(); err != nil {
					return nil, err
				}
			}
		}
	}
	if checkpointGroupAfterInitialValidationHook != nil {
		checkpointGroupAfterInitialValidationHook()
	}
	checkpointGroupNamespaceLease.Lock()
	defer checkpointGroupNamespaceLease.Unlock()
	// Marker mint is a namespace mutation too. Keep it behind the same writer
	// lease as the directory scan and fixed-name certificate publication so a
	// generic recovery admitted on an absent certificate must finish (including
	// mint-residue removal) before activation can create txn.vtm.
	if err := log.EnsureMinted(); err != nil {
		return nil, err
	}

	log.commitMu.Lock()
	defer log.commitMu.Unlock()
	if log.checkpointGroup != nil || log.checkpointGroupRetired {
		return nil, ErrCheckpointGroupOwned
	}
	// Validate again under the commit fence and atomically claim the complete
	// registered catalog. The preliminary proof is only an early diagnostic: an
	// AdoptCollection racing between it and this lock must not leave a generic
	// nonmember handle beside a newly published fixed-membership certificate.
	if err := log.validateCollectionsLocked(members); err != nil {
		return nil, err
	}
	if log.marker == nil {
		return nil, fmt.Errorf("%w: transaction marker is not open", ErrCheckpointGroupCorrupt)
	}
	order := make([]*Collection, len(ordered))
	for i := range ordered {
		order[i] = ordered[i].collection
	}
	sortCollectionSnapshotOrder(order)
	for _, collection := range order {
		collection.writer.Lock()
	}
	defer func() {
		for i := len(order) - 1; i >= 0; i-- {
			order[i].writer.Unlock()
		}
	}()
	for _, collection := range order {
		if collection.closed || collection.checkpointGroup.Load() != nil ||
			collection.checkpointGroupRetired.Load() {
			return nil, ErrCheckpointGroupOwned
		}
	}
	if seed != nil {
		for _, image := range seed.Images {
			if image.Collection.Generation() != image.Generation {
				return nil, ErrCheckpointGroupSeedChanged
			}
		}
	}
	if err := claimCheckpointGroupCatalogLocked(log, ordered); err != nil {
		return nil, err
	}
	if checkpointGroupAfterDirectoryMembershipHook != nil {
		checkpointGroupAfterDirectoryMembershipHook()
	}
	file, certificate, openErr := openCheckpointGroupCertificate(log)
	if openErr == nil {
		cause := ErrCheckpointGroupRecoveryRequired
		terminalFenceCheckpointGroupActivationLocked(log, order, cause)
		return nil, errors.Join(cause, file.Close())
	}
	if !errors.Is(openErr, os.ErrNotExist) {
		// A malformed, inaccessible, or otherwise uncertain fixed-name entry is
		// still a persistent ownership claim. Returning mutable generic handles
		// here would recreate the same post-publication escape as a failed
		// directory Sync.
		terminalFenceCheckpointGroupActivationLocked(log, order, openErr)
		return nil, openErr
	}
	published := false
	decisions, scanErr := rescanTxnLogMarker(log)
	if scanErr != nil {
		return nil, scanErr
	}
	if decisions.MaxTxnID() != 0 || log.marker.Cursor() != 0 {
		return nil, fmt.Errorf("%w: missing certificate beside a non-empty marker", ErrCheckpointGroupCorrupt)
	}
	for _, member := range ordered {
		if member.collection.journal == nil || member.collection.journal.Cursor() != 0 {
			return nil, fmt.Errorf("%w: missing certificate beside live member journal %q", ErrCheckpointGroupCorrupt, member.name)
		}
		if seed == nil || member.nameDigest == seedMember {
			if member.collection.Len() != 0 {
				return nil, fmt.Errorf("%w: missing certificate beside non-empty member %q", ErrCheckpointGroupCorrupt, member.name)
			}
		}
	}
	holds, holdsErr := directoryHoldsAnyConditional(log.root)
	if holdsErr != nil {
		return nil, holdsErr
	}
	if holds {
		return nil, fmt.Errorf(
			"%w: activation directory contains an unowned conditional journal",
			ErrCheckpointGroupCorrupt,
		)
	}
	certificate = checkpointGroupCertificate{
		sequence: 1, markerID: log.marker.Header().MarkerID,
		markerEpoch: log.marker.Header().Epoch, members: ordered,
		seedApplied: seedApplied, seedState: seedState, seedMember: seedMember,
	}
	file, published, openErr = createCheckpointGroupCertificate(log, certificate)
	if openErr != nil {
		if published {
			terminalFenceCheckpointGroupActivationLocked(log, order, openErr)
		}
		return nil, openErr
	}
	group, err := checkpointGroupFromCertificateLocked(
		log, file, certificate, ordered, options,
	)
	if err != nil {
		terminalFenceCheckpointGroupActivationLocked(log, order, err)
		return nil, errors.Join(err, file.Close())
	}
	if err := group.validateRecoveredMembersLocked(true); err != nil {
		terminalFenceCheckpointGroupActivationLocked(log, order, err)
		return nil, errors.Join(err, file.Close())
	}
	for _, collection := range order {
		collection.checkpointGroup.Store(group)
	}
	log.checkpointGroup = group
	return group, nil
}

// claimCheckpointGroupCatalogLocked turns an empty/subset registration into
// the exact fixed membership while commitMu excludes Adopt/Detach/commit. A
// pre-existing extra registration is a definite pre-publication refusal; it is
// intentionally not terminally fenced because no certificate exists yet.
func claimCheckpointGroupCatalogLocked(
	log *TxnLog, members []checkpointGroupMember,
) error {
	if log == nil {
		return ErrCheckpointGroupCorrupt
	}
	memberSet := make(map[*Collection]struct{}, len(members))
	for _, member := range members {
		memberSet[member.collection] = struct{}{}
	}
	log.regMu.Lock()
	for registered := range log.registered {
		if _, ok := memberSet[registered]; !ok {
			log.regMu.Unlock()
			return fmt.Errorf(
				"%w: registered collection outside fixed checkpoint membership",
				ErrTxnParticipant,
			)
		}
	}
	for _, member := range members {
		if _, ok := log.registered[member.collection]; ok {
			continue
		}
		log.registered[member.collection] = struct{}{}
		log.collections = append(log.collections, member.collection)
	}
	log.regMu.Unlock()

	return validateCheckpointGroupDirectoryMembership(log, members)
}

// validateCheckpointGroupDirectoryMembership rejects discovered engine files
// outside the fixed set at activation and recovery. The persistent certificate
// fences the whole physical directory on generic reopen, so it must not strand
// an already-open unregistered page file outside its membership.
func validateCheckpointGroupDirectoryMembership(
	log *TxnLog, members []checkpointGroupMember,
) error {
	allowed := make(map[string]struct{}, len(members)*2)
	for _, member := range members {
		primary := filepath.Base(member.collection.file.Name())
		allowed[primary] = struct{}{}
		allowed[filepath.Base(RecoveryJournalPath(member.collection.file.Name()))] = struct{}{}
	}
	entries, err := fs.ReadDir(log.root.FS(), ".")
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(allowed))
	for _, entry := range entries {
		name := entry.Name()
		if _, ok := allowed[name]; ok {
			info, infoErr := entry.Info()
			if infoErr != nil {
				return infoErr
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("%w: checkpoint member entry %q is not regular", ErrTxnParticipant, name)
			}
			seen[name] = struct{}{}
			continue
		}
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, collectionname.PrimarySuffix) ||
			strings.HasSuffix(lower, collectionname.JournalSuffix) {
			return fmt.Errorf(
				"%w: unowned collection entry %q outside fixed checkpoint membership",
				ErrTxnParticipant, name,
			)
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if !info.Mode().IsRegular() || name == txnMarkerFilename ||
			name == checkpointGroupFilename {
			continue
		}
		candidate, openErr := log.root.OpenFile(name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		_, discoverErr := storeio.DiscoverMutableInlineBootstrap(candidate)
		closeErr := candidate.Close()
		if closeErr != nil {
			return closeErr
		}
		if discoverErr == nil || errors.Is(discoverErr, storeio.ErrSuperblockConflict) {
			return fmt.Errorf(
				"%w: unowned format-0 collection %q outside fixed checkpoint membership",
				ErrTxnParticipant, name,
			)
		}
	}
	if len(seen) != len(allowed) {
		return fmt.Errorf("%w: fixed checkpoint member entry is missing", ErrTxnParticipant)
	}
	return nil
}

// terminalFenceCheckpointGroupActivationLocked is used only after the fixed
// certificate may have become visible. commitMu and every member writer are
// held, so waiters can observe only the pre-publication generic state or this
// permanent retired state—never mutable handles beside an uncertain cert.
func terminalFenceCheckpointGroupActivationLocked(
	log *TxnLog, order []*Collection, cause error,
) {
	for _, collection := range order {
		collection.checkpointGroupRetired.Store(true)
		collection.checkpointGroup.Store(nil)
	}
	log.checkpointGroupRetired = true
	log.checkpointGroup = nil
	if cause != nil && log.poison == nil {
		log.poison = cause
	}
}

func checkpointGroupMembers(members []NamedCollection) ([]checkpointGroupMember, error) {
	if len(members) == 0 || len(members) > checkpointGroupMaxMembers {
		return nil, fmt.Errorf("%w: fixed membership size %d", ErrTxnParticipant, len(members))
	}
	validated, err := validateTxnMembers(members)
	if err != nil {
		return nil, err
	}
	if len(validated) == 0 || len(validated) > checkpointGroupMaxMembers {
		return nil, fmt.Errorf("%w: fixed membership size %d", ErrTxnParticipant, len(validated))
	}
	slices.SortFunc(validated, func(a, b NamedCollection) int {
		return strings.Compare(a.Name, b.Name)
	})
	result := make([]checkpointGroupMember, len(validated))
	for i := range validated {
		c := validated[i].Collection
		if !databaseTxnLaneSupported(c) || !c.journalEnabled() {
			return nil, fmt.Errorf("%w: member %q has no conditional journal", ErrDatabaseTransactionUnsupportedLane, validated[i].Name)
		}
		result[i] = checkpointGroupMember{
			name: strings.Clone(validated[i].Name), nameDigest: sha256.Sum256([]byte(validated[i].Name)),
			collection: c, storeID: c.storeID, journalID: c.journalID,
		}
	}
	return result, nil
}

func checkpointGroupFromCertificateLocked(
	log *TxnLog,
	file *os.File,
	certificate checkpointGroupCertificate,
	members []checkpointGroupMember,
	options CheckpointGroupOptions,
) (*CheckpointGroup, error) {
	if log == nil || log.marker == nil || file == nil {
		return nil, ErrCheckpointGroupCorrupt
	}
	if err := validateCheckpointGroupCertificate(certificate, log.marker.Header(), members); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	byName := make(map[string]*Collection, len(members))
	for _, member := range members {
		byName[member.name] = member.collection
	}
	group := &CheckpointGroup{
		log: log, members: members, byName: byName, opts: options,
		file: file, fileInfo: info, sequence: certificate.sequence,
		txnBase: certificate.txnBase, markerEpoch: certificate.markerEpoch,
		markerID: certificate.markerID, txn: certificate.txnHighWater,
		applied: certificate.applied, maxApplySpan: certificate.maxApplySpan,
		maxSpanTxn: certificate.maxSpanTxn, maxSpanFirst: certificate.maxSpanFirst,
		maxSpanLast: certificate.maxSpanLast, foldedTxn: certificate.txnHighWater,
		seedApplied: certificate.seedApplied, seedState: certificate.seedState,
		seedMember: certificate.seedMember,
	}
	group.certApplied.Store(certificate.applied)
	group.certTxn.Store(certificate.txnHighWater)
	group.visibleApplied.Store(certificate.applied)
	group.visibleTxn.Store(certificate.txnHighWater)
	if certificate.txnHighWater == math.MaxUint64 {
		// An empty marker anchored at the terminal transaction has no record
		// whose ID can seed generic marker recovery. Preserve the checkpoint
		// certificate's exhausted durable counter instead of wrapping to one.
		log.nextTxnID = math.MaxUint64
	}
	return group, nil
}

func (g *CheckpointGroup) attachLocked() error {
	if g == nil || g.log == nil || g.log.marker == nil ||
		g.log.checkpointGroup != nil || g.log.checkpointGroupRetired {
		return ErrCheckpointGroupOwned
	}
	order := make([]*Collection, len(g.members))
	for i := range g.members {
		order[i] = g.members[i].collection
	}
	sortCollectionSnapshotOrder(order)
	for _, c := range order {
		c.writer.Lock()
	}
	defer func() {
		for i := len(order) - 1; i >= 0; i-- {
			order[i].writer.Unlock()
		}
	}()
	for _, c := range order {
		if c.closed || c.checkpointGroup.Load() != nil || c.checkpointGroupRetired.Load() {
			return ErrCheckpointGroupOwned
		}
	}
	for _, c := range order {
		c.checkpointGroup.Store(g)
	}
	g.log.checkpointGroup = g
	return nil
}

func (g *CheckpointGroup) validateRecoveredMembersLocked(requireEmptyJournal bool) error {
	if g == nil || g.log == nil || g.log.marker == nil {
		return ErrCheckpointGroupCorrupt
	}
	header := g.log.marker.Header()
	if header.MarkerID != g.markerID {
		return fmt.Errorf("%w: marker identity", ErrCheckpointGroupCorrupt)
	}
	if header.Epoch != g.markerEpoch {
		// The only accepted mismatch is the crash interval after an empty marker
		// recycle and before the same-cut certificate rewrite.
		if header.Epoch != g.markerEpoch+1 || g.log.marker.Cursor() != 0 {
			return fmt.Errorf("%w: marker epoch", ErrCheckpointGroupCorrupt)
		}
		for _, member := range g.members {
			if member.collection.journal == nil || member.collection.journal.Cursor() != 0 {
				return fmt.Errorf("%w: marker rollover with a live journal", ErrCheckpointGroupCorrupt)
			}
		}
		g.markerEpoch = header.Epoch
		g.txnBase = g.txn
	}
	if requireEmptyJournal {
		for _, member := range g.members {
			if member.collection.journal == nil || member.collection.journal.Cursor() != 0 {
				return fmt.Errorf("%w: member %q was not recovered", ErrCheckpointGroupCorrupt, member.name)
			}
		}
	}
	return nil
}

// Owns reports whether the exact name/handle set is the group's fixed
// membership. It performs no I/O.
func (g *CheckpointGroup) Owns(members []NamedCollection) bool {
	if g == nil {
		return false
	}
	ordered, err := checkpointGroupMembers(members)
	if err != nil || len(ordered) != len(g.members) {
		return false
	}
	for i := range ordered {
		if ordered[i].name != g.members[i].name || ordered[i].collection != g.members[i].collection {
			return false
		}
	}
	return true
}

// SeedPending reports an authenticated format-0 seed certificate whose state
// transition has not yet reached the certified cut. It performs no I/O and is
// used only to keep staged images non-serving until exact activation resumes.
func (g *CheckpointGroup) SeedPending() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	pending := !g.closed && g.seedApplied > 1 && g.certApplied.Load() == 0
	g.mu.Unlock()
	return pending
}

// SeedAppliedIndex reports the imported cut authenticated by this group. The
// boolean is false for an ordinary format-0 group.
func (g *CheckpointGroup) SeedAppliedIndex() (uint64, bool) {
	if g == nil {
		return 0, false
	}
	g.mu.Lock()
	applied := g.seedApplied
	seeded := !g.closed && applied > 1
	g.mu.Unlock()
	return applied, seeded
}

// SeedStateAuthoritative reports the unique certified transaction that wrote
// the seed envelope itself. The later snapshot-base binding is a second
// same-Applied group transition, so its State envelope must not be compared to
// the original seed commitment.
func (g *CheckpointGroup) SeedStateAuthoritative() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	authoritative := !g.closed && g.seedApplied > 1 &&
		g.applied == g.seedApplied && g.txn == 1 &&
		g.certApplied.Load() == g.seedApplied && g.certTxn.Load() == 1
	g.mu.Unlock()
	return authoritative
}

// SeedActivationPending reports either the C0 seed interval or a certified
// prefix that does not yet include the required second same-index immutable
// snapshot-base transition. It deliberately reads certificate high-water, not
// the newer visible suffix: a crash discards any published but uncertified
// second transition and must reopen non-serving.
func (g *CheckpointGroup) SeedActivationPending() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	pending := !g.closed && g.seedApplied > 1 &&
		(g.certTxn.Load() < 2 || g.certApplied.Load() < g.seedApplied)
	g.mu.Unlock()
	return pending
}

// ValidateSeedState proves that an imported State envelope is exactly the one
// committed by this group's seed certificate. seeded is false for an ordinary
// format-0 group; callers must not infer seed authority from cut shape alone.
func (g *CheckpointGroup) ValidateSeedState(
	applied uint64,
	member string,
	envelope []byte,
) (seeded bool, err error) {
	if g == nil {
		return false, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return false, err
	}
	if g.seedApplied == 0 {
		return false, nil
	}
	wantApplied, wantState, wantMember, err := checkpointGroupSeedProfile(
		CheckpointGroupSeed{Applied: applied, Member: member, Envelope: envelope},
	)
	if err != nil || wantApplied != g.seedApplied || wantState != g.seedState ||
		wantMember != g.seedMember {
		return true, fmt.Errorf("%w: imported state differs from seed certificate", ErrCheckpointGroupCorrupt)
	}
	return true, nil
}

// qualifyLog is the read-only qualification half of TxnLog.EnsureMinted for an
// authenticated active group. It retains the canonical g.mu -> log.commitMu
// order through every certificate descriptor and membership read, including
// the close interval after terminal retirement releases commitMu but before it
// closes and clears the certificate descriptor.
func (g *CheckpointGroup) qualifyLog(log *TxnLog) error {
	if g == nil || log == nil {
		return ErrCheckpointGroupOwned
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	log.commitMu.Lock()
	defer log.commitMu.Unlock()
	if g == nil || log == nil || g.log != log || log.checkpointGroup != g ||
		g.file == nil || g.fileInfo == nil || log.marker == nil {
		return ErrCheckpointGroupOwned
	}
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	header := log.marker.Header()
	if header.MarkerID != g.markerID || header.Epoch != g.markerEpoch {
		return fmt.Errorf("%w: transaction marker identity", ErrCheckpointGroupCorrupt)
	}
	current, err := log.root.Lstat(checkpointGroupFilename)
	if err != nil {
		return err
	}
	descriptor, err := g.file.Stat()
	if err != nil {
		return err
	}
	if !current.Mode().IsRegular() || !descriptor.Mode().IsRegular() ||
		!os.SameFile(current, descriptor) || !os.SameFile(current, g.fileInfo) {
		return fmt.Errorf("%w: certificate entry changed", ErrCheckpointGroupCorrupt)
	}
	log.regMu.Lock()
	if len(log.registered) != len(g.members) {
		log.regMu.Unlock()
		return fmt.Errorf("%w: fixed checkpoint registration count", ErrTxnParticipant)
	}
	for _, member := range g.members {
		if _, ok := log.registered[member.collection]; !ok ||
			member.collection.checkpointGroup.Load() != g {
			log.regMu.Unlock()
			return fmt.Errorf("%w: fixed checkpoint member %q", ErrTxnParticipant, member.name)
		}
	}
	log.regMu.Unlock()
	if err := log.validateCollectionsLocked(checkpointNamedMembers(g.members)); err != nil {
		return err
	}
	return validateCheckpointGroupDirectoryMembership(log, g.members)
}

// AppliedIndex is the current reader-visible group cut.
func (g *CheckpointGroup) AppliedIndex() uint64 {
	if g == nil {
		return 0
	}
	return g.visibleApplied.Load()
}

// CheckpointAppliedIndex is the certificate-backed contiguous apply cut. It
// moves immediately after the authenticated certificate Sync, even if a later
// physical fold must be retried; the synced participant journals and decision
// prefix already make that cut replayable. WAL deletion requires additional
// Raft term, configuration, lineage, witness, and retained-suffix proofs.
func (g *CheckpointGroup) CheckpointAppliedIndex() uint64 {
	if g == nil {
		return 0
	}
	return g.certApplied.Load()
}

// Stats returns a detached counter snapshot.
func (g *CheckpointGroup) Stats() CheckpointGroupStats {
	if g == nil {
		return CheckpointGroupStats{}
	}
	g.mu.Lock()
	applied, txn, maxApplySpan := g.applied, g.txn, g.maxApplySpan
	g.mu.Unlock()
	largestUpdateSpan := uint64(maxApplySpan)
	if largestUpdateSpan == 0 && txn != 0 {
		largestUpdateSpan = 1
	}
	journal := g.journalSyncs.Load()
	marker := g.markerSyncs.Load()
	certificate := g.certificateSyncs.Load()
	return CheckpointGroupStats{
		AppliedIndex: applied, CheckpointAppliedIndex: g.certApplied.Load(),
		TransactionHighWater: txn, CheckpointTransactions: g.certTxn.Load(),
		Updates: g.updates.Load(), LargestUpdateSpan: largestUpdateSpan,
		Checkpoints:  g.checkpoints.Load(),
		JournalSyncs: journal, MarkerSyncs: marker,
		CertificateSyncs: certificate, BarrierSyncs: g.barrierSyncs.Load(),
		PhysicalCheckpoints: g.physicalCheckpoints.Load(),
	}
}

// Seed publishes the sole non-consecutive first transition admitted by a
// seeded format-0 group. The certificate already binds Applied, Member, and
// the exact Envelope. Seed writes exactly one key in that member, then
// certifies and physically folds the transition before returning success.
// Retrying after a certified outcome is idempotent only when the rooted value
// is byte-identical to the certificate commitment.
func (g *CheckpointGroup) Seed(
	seed CheckpointGroupSeed,
	member NamedCollection,
	limits TxnLimits,
	key []byte,
) error {
	if g == nil || member.Collection == nil || len(key) == 0 {
		return ErrCheckpointGroupOwned
	}
	applied, stateCommitment, memberDigest, err := checkpointGroupSeedProfile(seed)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	owned := g.byName[member.Name]
	if owned == nil || owned != member.Collection || member.Name != seed.Member ||
		g.seedApplied != applied || g.seedState != stateCommitment ||
		g.seedMember != memberDigest {
		return fmt.Errorf("%w: seed profile or member", ErrCheckpointGroupCorrupt)
	}
	if g.applied == applied && g.certApplied.Load() == applied {
		value, found, readErr := owned.AppendRaw(nil, key)
		if readErr != nil || !found || !slices.Equal(value, seed.Envelope) {
			return errors.Join(
				fmt.Errorf("%w: certified seed row differs", ErrCheckpointGroupCorrupt),
				readErr,
			)
		}
		return g.checkpointLocked()
	}
	if g.applied != 0 || g.txn != 0 || g.foldedTxn != 0 ||
		g.certApplied.Load() != 0 || g.certTxn.Load() != 0 {
		return fmt.Errorf("%w: seed requires cut zero", ErrCheckpointGroupSequence)
	}

	wb := &WriteBatch{
		collection: owned,
		position:   make(map[string]int, 1),
		active:     true,
	}
	defer closeDurableWriteBatches([]*WriteBatch{wb})
	if err := wb.Put(key, seed.Envelope); err != nil {
		return err
	}
	if wb.Len() != 1 {
		return fmt.Errorf("%w: seed must contain exactly one state row", ErrCheckpointGroupSequence)
	}
	if err := checkTxnLimits(
		limits, 1, 1, int64(len(wb.keys)+len(wb.values)),
	); err != nil {
		return err
	}
	dirty := []NamedCollection{{Name: member.Name, Collection: owned}}
	byName := map[string]*WriteBatch{member.Name: wb}
	for attempt := 0; attempt < 2; attempt++ {
		room, roomErr := g.markerRoomLocked(1)
		if roomErr != nil {
			return roomErr
		}
		if !room {
			if err := g.checkpointLocked(); err != nil {
				return err
			}
			if err := g.recycleMarkerLocked(); err != nil {
				return err
			}
			continue
		}
		err = g.commitTransitionLocked(1, applied, dirty, byName)
		if errors.Is(err, ErrCheckpointGroupPressure) {
			if err := g.checkpointLocked(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		g.txn = 1
		g.applied = applied
		g.updates.Add(1)
		return g.checkpointLocked()
	}
	return ErrCheckpointGroupPressure
}

// Update publishes one fixed-group transition. The normal per-transition path
// appends conditional participant records and one decision with zero Sync. All
// fallible work precedes the simultaneous snapshot-gate publication.
func (g *CheckpointGroup) Update(
	applied uint64,
	members []NamedCollection,
	limits TxnLimits,
	fn func(*DatabaseBatch) error,
) error {
	if g == nil || fn == nil {
		return ErrCheckpointGroupOwned
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.updateLocked(checkpointGroupUpdate{
		firstApplied: applied, lastApplied: applied,
	}, members, limits, fn)
}

// UpdateConsecutive publishes one physical fixed-group transaction for a
// bounded, non-empty range of consecutive replicated apply indices. The
// caller has already evaluated every logical transition in [firstApplied,
// lastApplied] in order and fn must write their exact final net state. The
// certificate and reader-visible group cut advance directly to lastApplied.
// recovery either retains the complete transaction or none of it.
//
// Same-index metadata transitions deliberately remain on Update. A seeded
// group must also bind its required same-index snapshot base through Update
// before this method can advance beyond the imported cut.
func (g *CheckpointGroup) UpdateConsecutive(
	firstApplied, lastApplied uint64,
	members []NamedCollection,
	limits TxnLimits,
	fn func(*DatabaseBatch) error,
) error {
	if g == nil || fn == nil {
		return ErrCheckpointGroupOwned
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.updateLocked(checkpointGroupUpdate{
		firstApplied: firstApplied, lastApplied: lastApplied, consecutive: true,
	}, members, limits, fn)
}

type checkpointGroupUpdate struct {
	firstApplied uint64
	lastApplied  uint64
	consecutive  bool
}

func (g *CheckpointGroup) updateLocked(
	update checkpointGroupUpdate,
	members []NamedCollection,
	limits TxnLimits,
	fn func(*DatabaseBatch) error,
) error {
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	if g.seedApplied > 1 {
		switch {
		case g.txn == 0:
			// The cut-zero certificate grants exactly one mutation authority:
			// Seed must publish the committed state envelope. Letting an
			// ordinary update consume txn 1 would strand or replace that proof.
			return fmt.Errorf(
				"%w: seeded cut zero requires Seed", ErrCheckpointGroupSequence,
			)
		case g.txn == 1 && g.applied == g.seedApplied &&
			(update.consecutive || update.lastApplied != g.seedApplied):
			// The first transition after Seed binds the immutable snapshot
			// base at the imported cut. It cannot advance apply and thereby
			// make an unbound child appear activation-complete.
			return fmt.Errorf(
				"%w: seeded snapshot-base binding must remain at applied %d",
				ErrCheckpointGroupSequence, g.seedApplied,
			)
		}
	}
	// Periodic barriers are admission work for the next physical transaction.
	// This keeps every checkpoint failure pre-mutation: a caller never receives
	// a terminal checkpoint error after its requested publication is visible.
	if g.txn-g.certTxn.Load() >= g.opts.CheckpointEvery {
		if err := g.checkpointLocked(); err != nil {
			return err
		}
	}
	if update.consecutive {
		if g.applied == math.MaxUint64 || update.firstApplied != g.applied+1 ||
			update.lastApplied < update.firstApplied ||
			update.lastApplied == math.MaxUint64 ||
			update.lastApplied-update.firstApplied >= MaxCheckpointGroupUpdateEntries {
			return fmt.Errorf(
				"%w: have %d, consecutive range [%d,%d]",
				ErrCheckpointGroupSequence, g.applied,
				update.firstApplied, update.lastApplied,
			)
		}
	} else if update.lastApplied < g.applied ||
		update.lastApplied > g.applied+1 || update.lastApplied == math.MaxUint64 {
		return fmt.Errorf(
			"%w: have %d, transition %d",
			ErrCheckpointGroupSequence, g.applied, update.lastApplied,
		)
	}
	ordered, err := validateTxnMembers(members)
	if err != nil {
		return err
	}
	for _, member := range ordered {
		if owned := g.byName[member.Name]; owned == nil || owned != member.Collection {
			return fmt.Errorf(
				"%w: member %q is outside the fixed group",
				ErrCheckpointGroupOwned, member.Name,
			)
		}
	}

	batch := &DatabaseBatch{byName: make(map[string]*WriteBatch, len(ordered))}
	batches := make([]*WriteBatch, len(ordered))
	for i, member := range ordered {
		hint := member.BatchDocumentsHint
		if hint == 0 {
			hint = member.Collection.options.MaxBatchDocuments
		}
		wb := &WriteBatch{
			collection: member.Collection,
			position:   make(map[string]int, hint), active: true,
		}
		batches[i] = wb
		batch.byName[member.Name] = wb
	}
	defer closeDurableWriteBatches(batches)
	if err := fn(batch); err != nil {
		return err
	}

	dirty := make([]NamedCollection, 0, len(ordered))
	totalDocs := 0
	var totalBytes int64
	for _, member := range ordered {
		wb := batch.byName[member.Name]
		if wb.Len() == 0 {
			continue
		}
		dirty = append(dirty, member)
		totalDocs += wb.Len()
		totalBytes += int64(len(wb.keys) + len(wb.values))
	}
	if len(dirty) == 0 {
		return fmt.Errorf("%w: transition has no durable mutation", ErrCheckpointGroupSequence)
	}
	if err := checkTxnLimits(limits, len(dirty), totalDocs, totalBytes); err != nil {
		return err
	}
	if g.txn >= math.MaxUint64-1 {
		return ErrCheckpointGroupSequence
	}

	for attempt := 0; attempt < 2; attempt++ {
		room, roomErr := g.markerRoomLocked(len(dirty))
		if roomErr != nil {
			return roomErr
		}
		if !room {
			if err := g.checkpointLocked(); err != nil {
				return err
			}
			if err := g.recycleMarkerLocked(); err != nil {
				return err
			}
			room, roomErr = g.markerRoomLocked(len(dirty))
			if roomErr != nil {
				return roomErr
			}
			if !room {
				return ErrTxnTooLarge
			}
		}
		err = g.commitTransitionLocked(g.txn+1, update.lastApplied, dirty, batch.byName)
		if !errors.Is(err, ErrCheckpointGroupPressure) {
			if err == nil {
				if update.consecutive {
					span := update.lastApplied - update.firstApplied + 1
					if span > 1 && uint8(span) > g.maxApplySpan {
						g.maxApplySpan = uint8(span)
						g.maxSpanTxn = g.txn + 1
						g.maxSpanFirst = update.firstApplied
						g.maxSpanLast = update.lastApplied
					}
				}
				g.txn++
				g.applied = update.lastApplied
				// Updates retains its format-0 meaning: physical group
				// transactions, not the number of logical entries represented.
				g.updates.Add(1)
			}
			return err
		}
		if err := g.checkpointLocked(); err != nil {
			return err
		}
	}
	return ErrCheckpointGroupPressure
}

func (g *CheckpointGroup) commitTransitionLocked(
	txnID uint64,
	applied uint64,
	dirty []NamedCollection,
	byName map[string]*WriteBatch,
) (err error) {
	log := g.log
	log.commitMu.Lock()
	defer log.commitMu.Unlock()
	if log.checkpointGroup != g {
		return ErrCheckpointGroupOwned
	}
	if err := log.validateCollectionsLocked(checkpointNamedMembers(g.members)); err != nil {
		return err
	}
	if log.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, log.poison)
	}
	if log.marker == nil || log.marker.Header().MarkerID != g.markerID ||
		log.marker.Header().Epoch != g.markerEpoch {
		return fmt.Errorf("%w: transaction marker identity", ErrCheckpointGroupCorrupt)
	}

	order := make([]*Collection, len(dirty))
	nameOf := make(map[*Collection]string, len(dirty))
	for i := range dirty {
		order[i] = dirty[i].Collection
		nameOf[dirty[i].Collection] = dirty[i].Name
	}
	sortCollectionSnapshotOrder(order)
	for _, c := range order {
		c.writer.Lock()
	}
	defer func() {
		for i := len(order) - 1; i >= 0; i-- {
			order[i].writer.Unlock()
		}
	}()

	staged := make([]stagedPrimaryBatch, len(order))
	stagedLive := 0
	defer func() {
		if err == nil {
			return
		}
		for i := stagedLive - 1; i >= 0; i-- {
			order[i].unwindStagedPrimaryBatch(&staged[i])
		}
	}()
	for i, c := range order {
		if c.checkpointGroup.Load() != g || c.closed {
			return ErrCheckpointGroupOwned
		}
		st, stageErr := c.stagePrimaryBatchConditionalLocked(byName[nameOf[c]])
		if stageErr != nil {
			return stageErr
		}
		if !st.live {
			return fmt.Errorf("%w: staged empty member %q", ErrTxnParticipant, nameOf[c])
		}
		staged[i] = st
		stagedLive = i + 1
	}

	header := log.marker.Header()
	participants := make([]storeio.TxnParticipant, len(order))
	for i, c := range order {
		if prepErr := c.preparePrimaryBatchConditionalLocked(
			&staged[i], header.MarkerID, header.Epoch, txnID, false,
		); prepErr != nil {
			return prepErr
		}
		participants[i] = storeio.TxnParticipant{
			StoreID: c.storeID, JournalID: c.journalID,
			PreparedGeneration: staged[i].generation,
		}
	}
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterPrepareAppend); err != nil {
			return err
		}
	}
	if _, appendErr := log.marker.AppendDecision(txnID, participants); appendErr != nil {
		poisoned := journalCommitOutcomeUnknown(appendErr)
		log.poison = poisoned
		g.poison = poisoned
		return poisoned
	}
	if checkpointGroupFaultHook != nil {
		if hookErr := checkpointGroupFaultHook(checkpointGroupAfterDecisionAppend); hookErr != nil {
			poisoned := journalCommitOutcomeUnknown(hookErr)
			log.poison = poisoned
			g.poison = poisoned
			return poisoned
		}
	}

	for _, c := range order {
		c.snapshotGate.Lock()
	}
	for i, c := range order {
		c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
		c.publishPrimaryBatchGateHeld(staged[i])
		staged[i].live = false
	}
	// Publish the physical-durability fence before releasing any snapshot gate.
	// A snapshot/materialization waiter can otherwise observe the new collection
	// roots with the old txn cut and persist an uncertified suffix.
	g.visibleTxn.Store(txnID)
	g.visibleApplied.Store(applied)
	for i := len(order) - 1; i >= 0; i-- {
		order[i].snapshotGate.Unlock()
	}
	stagedLive = 0
	return nil
}

func checkpointNamedMembers(members []checkpointGroupMember) []NamedCollection {
	result := make([]NamedCollection, len(members))
	for i := range members {
		result[i] = NamedCollection{Name: members[i].name, Collection: members[i].collection}
	}
	return result
}

func (g *CheckpointGroup) markerRoomLocked(participantCount int) (bool, error) {
	padded, ok := txnDecisionRecordBytes(participantCount)
	if !ok {
		return false, ErrTxnTooLarge
	}
	g.log.commitMu.Lock()
	defer g.log.commitMu.Unlock()
	if g.log.marker == nil || g.log.checkpointGroup != g {
		return false, ErrCheckpointGroupOwned
	}
	marker := g.log.marker
	if marker.NextSequence() == 0 {
		return false, ErrCheckpointGroupSequence
	}
	if uint64(padded) > marker.Header().Capacity {
		return false, ErrTxnTooLarge
	}
	return marker.Cursor()+uint64(padded) <= marker.Header().Capacity, nil
}

// MaybeCheckpoint applies the configured physical-transaction cadence.
func (g *CheckpointGroup) MaybeCheckpoint() error {
	if g == nil {
		return ErrCheckpointGroupOwned
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	if g.txn-g.certTxn.Load() < g.opts.CheckpointEvery {
		return nil
	}
	return g.checkpointLocked()
}

// Checkpoint durably certifies the current contiguous cut and folds every
// participant. K journal Syncs and one authenticated certificate Sync are
// ordered before any participant may recycle its journal. txn.vtm is an
// unsynced, recyclable implementation log; its records are not commit
// authority.
func (g *CheckpointGroup) Checkpoint() error {
	if g == nil {
		return ErrCheckpointGroupOwned
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	return g.checkpointLocked()
}

func (g *CheckpointGroup) checkpointLocked() error {
	if g.foldedTxn == g.txn && g.certTxn.Load() == g.txn {
		return nil
	}
	if g.certTxn.Load() != g.txn {
		g.log.commitMu.Lock()
		for _, member := range g.members {
			c := member.collection
			c.writer.Lock()
			if failure := c.PersistenceError(); failure != nil {
				c.writer.Unlock()
				g.log.commitMu.Unlock()
				return failure
			}
			err := c.journal.Sync(c.journalPowerSafe)
			c.writer.Unlock()
			if err != nil {
				g.log.commitMu.Unlock()
				return g.poisonLocked(journalCommitOutcomeUnknown(err))
			}
			g.journalSyncs.Add(1)
			if checkpointGroupFaultHook != nil {
				if err := checkpointGroupFaultHook(checkpointGroupAfterJournalSync); err != nil {
					g.log.commitMu.Unlock()
					return g.poisonLocked(err)
				}
			}
		}
		g.sequence++
		certificate := g.certificateLocked()
		if err := g.writeCertificateLocked(certificate); err != nil {
			g.log.commitMu.Unlock()
			return g.poisonLocked(err)
		}
		g.certTxn.Store(g.txn)
		g.certApplied.Store(g.applied)
		g.barrierSyncs.Add(uint64(len(g.members) + 1))
		g.checkpoints.Add(1)
		g.log.commitMu.Unlock()
	}

	// Certificate durability is already sufficient for recovery. Physical folds
	// can therefore be retried independently without ever moving the accepted cut
	// backwards or exposing a partially durable multi-collection state.
	g.log.commitMu.Lock()
	for _, member := range g.members {
		c := member.collection
		c.writer.Lock()
		err := c.checkpointPastCertifiedPrefixConditionalsLocked(
			checkpointCertificateResolver(
				g.markerID, g.markerEpoch, g.txnBase, g.txn,
			), g.markerEpoch,
		)
		c.writer.Unlock()
		if err != nil {
			g.log.commitMu.Unlock()
			return err
		}
		g.physicalCheckpoints.Add(1)
		if checkpointGroupFaultHook != nil {
			if err = checkpointGroupFaultHook(checkpointGroupAfterPhysicalCheckpoint); err != nil {
				g.log.commitMu.Unlock()
				return err
			}
		}
	}
	g.foldedTxn = g.txn
	g.log.undischarged = 0
	g.log.commitMu.Unlock()
	return nil
}

func (g *CheckpointGroup) recycleMarkerLocked() error {
	if g.foldedTxn != g.txn || g.certTxn.Load() != g.txn {
		return fmt.Errorf("%w: recycle before complete checkpoint", ErrCheckpointGroupCorrupt)
	}
	g.log.commitMu.Lock()
	defer g.log.commitMu.Unlock()
	header := g.log.marker.Header()
	if header.Epoch == math.MaxUint64 {
		return ErrCheckpointGroupSequence
	}
	for _, member := range g.members {
		if member.collection.journal.Cursor() != 0 {
			return fmt.Errorf("%w: recycle with live participant journal", ErrCheckpointGroupCorrupt)
		}
	}
	directoryHolds, err := directoryHoldsAnyConditional(g.log.root)
	if err != nil {
		return err
	}
	if directoryHolds {
		return fmt.Errorf(
			"%w: marker recycle blocked by an unowned conditional journal",
			ErrCheckpointGroupCorrupt,
		)
	}
	if err := g.log.marker.Recycle(header.Epoch + 1); err != nil {
		return g.poisonLocked(journalCommitOutcomeUnknown(err))
	}
	g.markerSyncs.Add(1)
	g.markerEpoch = header.Epoch + 1
	g.txnBase = g.txn
	g.sequence++
	if err := g.writeCertificateLocked(g.certificateLocked()); err != nil {
		return g.poisonLocked(err)
	}
	return nil
}

func (g *CheckpointGroup) certificateLocked() checkpointGroupCertificate {
	return checkpointGroupCertificate{
		sequence: g.sequence, applied: g.applied,
		txnHighWater: g.txn, txnBase: g.txnBase, maxApplySpan: g.maxApplySpan,
		maxSpanTxn: g.maxSpanTxn, maxSpanFirst: g.maxSpanFirst,
		maxSpanLast: g.maxSpanLast,
		markerEpoch: g.markerEpoch, markerID: g.markerID,
		seedApplied: g.seedApplied, seedState: g.seedState, seedMember: g.seedMember,
		members: g.members,
	}
}

func (g *CheckpointGroup) writeCertificateLocked(c checkpointGroupCertificate) error {
	if g.file == nil || g.fileInfo == nil {
		return ErrCheckpointGroupCorrupt
	}
	current, err := g.log.root.Lstat(checkpointGroupFilename)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(current, g.fileInfo) {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: certificate entry changed", ErrCheckpointGroupCorrupt)
	}
	encoded, err := encodeCheckpointGroupCertificate(c)
	if err != nil {
		return err
	}
	slot := c.sequence % checkpointGroupSlots
	if _, err := g.file.WriteAt(encoded, int64(slot*checkpointGroupSlotBytes)); err != nil {
		return journalCommitOutcomeUnknown(err)
	}
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterCertificateWrite); err != nil {
			return err
		}
	}
	if err := g.file.Sync(); err != nil {
		return journalCommitOutcomeUnknown(err)
	}
	g.certificateSyncs.Add(1)
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterCertificateSync); err != nil {
			return err
		}
	}
	return nil
}

func (g *CheckpointGroup) checkUsableLocked() error {
	if g.closed || g.log == nil || g.log.checkpointGroup != g {
		return ErrCheckpointGroupOwned
	}
	if g.poison != nil {
		return fmt.Errorf("%w: %w", ErrTxnLogPoisoned, g.poison)
	}
	return nil
}

func (g *CheckpointGroup) poisonLocked(err error) error {
	if err == nil {
		return nil
	}
	if g.poison == nil {
		g.poison = err
	}
	if g.log != nil && g.log.poison == nil {
		g.log.poison = err
	}
	return err
}

// Close gracefully checkpoints and then replaces exclusive ownership with a
// terminal mutation fence. It does not close the caller-owned collections or
// TxnLog; their resource Close paths become legal only after this succeeds,
// while generic mutation can never resume beside the persistent certificate.
func (g *CheckpointGroup) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return g.closeErr
	}
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	if err := g.checkpointLocked(); err != nil {
		return err
	}
	g.log.commitMu.Lock()
	order := make([]*Collection, len(g.members))
	for i := range g.members {
		order[i] = g.members[i].collection
	}
	sortCollectionSnapshotOrder(order)
	for _, collection := range order {
		collection.writer.Lock()
	}
	for _, member := range g.members {
		if member.collection.checkpointGroup.Load() != g {
			for i := len(order) - 1; i >= 0; i-- {
				order[i].writer.Unlock()
			}
			g.log.commitMu.Unlock()
			return ErrCheckpointGroupOwned
		}
	}
	for _, member := range g.members {
		member.collection.checkpointGroupRetired.Store(true)
		member.collection.checkpointGroup.Store(nil)
	}
	for i := len(order) - 1; i >= 0; i-- {
		order[i].writer.Unlock()
	}
	g.log.checkpointGroupRetired = true
	g.log.checkpointGroup = nil
	g.log.commitMu.Unlock()
	g.closed = true
	if checkpointGroupCertificateCloseHook != nil {
		g.closeErr = checkpointGroupCertificateCloseHook(g.file)
	} else {
		g.closeErr = g.file.Close()
	}
	g.file = nil
	return g.closeErr
}

// CloseCompleted reports whether Close completed the terminal ownership
// transition. A certificate-descriptor Close error is sticky, but the member
// resources are already safe to close once this returns true.
func (g *CheckpointGroup) CloseCompleted() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	closed := g.closed
	g.mu.Unlock()
	return closed
}

func (c *Collection) checkpointGroupPhysicalFence() error {
	if c == nil {
		return ErrClosed
	}
	group := c.checkpointGroup.Load()
	if group == nil {
		return nil
	}
	if group.visibleTxn.Load() > group.certTxn.Load() {
		return ErrCheckpointGroupPressure
	}
	return nil
}

func (c *Collection) rejectCheckpointGroupOwner() error {
	if c != nil && (c.checkpointGroup.Load() != nil || c.checkpointGroupRetired.Load()) {
		return ErrCheckpointGroupOwned
	}
	return nil
}

func (c *Collection) rejectActiveCheckpointGroupOwner() error {
	if c != nil && c.checkpointGroup.Load() != nil {
		return ErrCheckpointGroupOwned
	}
	return nil
}

func checkpointCertificateResolver(
	markerID [16]byte,
	markerEpoch, txnBase, txnHighWater uint64,
) recoveryJournalDecisionResolver {
	return func(recordMarkerID [16]byte, epoch, txnID, _ uint64) (bool, error) {
		if recordMarkerID != markerID || epoch != markerEpoch {
			return false, fmt.Errorf(
				"%w: conditional transaction marker identity",
				ErrCheckpointGroupCorrupt,
			)
		}
		if txnID <= txnBase {
			return false, fmt.Errorf(
				"%w: conditional transaction %d is not after epoch base %d",
				ErrCheckpointGroupCorrupt, txnID, txnBase,
			)
		}
		return txnID <= txnHighWater, nil
	}
}

// validateCheckpointMarkerRecords validates every implementation-log record
// which happened to survive. It deliberately does not require the log to reach
// the certified high-water: txn.vtm is not synced by a group checkpoint, so a
// crash may retain any valid prefix (including none). The authenticated
// certificate is the authority that the in-memory transition ordinals through
// its high-water were consecutive and committed.
func validateCheckpointMarkerRecords(
	decisions *storeio.TxnDecisions,
	base uint64,
	members []checkpointGroupMember,
) error {
	if decisions == nil {
		return ErrCheckpointGroupCorrupt
	}
	if decisions.RetirementCount() != 0 {
		return fmt.Errorf(
			"%w: fixed-membership marker contains retirement records",
			ErrCheckpointGroupCorrupt,
		)
	}
	memberByStore := make(map[[16]byte]checkpointGroupMember, len(members))
	for _, member := range members {
		memberByStore[member.storeID] = member
	}
	expected := base + 1
	if expected == 0 {
		if decisions.MaxDCSN() != 0 || decisions.MaxTxnID() != 0 {
			return fmt.Errorf(
				"%w: exhausted marker transaction prefix is not empty",
				ErrCheckpointGroupCorrupt,
			)
		}
		return nil
	}
	var result error
	decisions.RangeDecisions(func(txnID uint64, participants []storeio.TxnParticipant) bool {
		if txnID != expected {
			result = fmt.Errorf(
				"%w: marker transaction prefix wants %d, found %d",
				ErrCheckpointGroupCorrupt, expected, txnID,
			)
			return false
		}
		if len(participants) == 0 || len(participants) > len(members) {
			result = fmt.Errorf("%w: transaction %d participant count", ErrCheckpointGroupCorrupt, txnID)
			return false
		}
		seen := make(map[[16]byte]struct{}, len(participants))
		for _, participant := range participants {
			member, ok := memberByStore[participant.StoreID]
			if !ok || member.journalID != participant.JournalID {
				result = fmt.Errorf("%w: transaction %d participant binding", ErrCheckpointGroupCorrupt, txnID)
				return false
			}
			if _, duplicate := seen[participant.StoreID]; duplicate {
				result = fmt.Errorf("%w: transaction %d duplicate participant", ErrCheckpointGroupCorrupt, txnID)
				return false
			}
			seen[participant.StoreID] = struct{}{}
		}
		expected++
		if expected == 0 {
			result = fmt.Errorf(
				"%w: exhausted marker transaction prefix",
				ErrCheckpointGroupCorrupt,
			)
			return false
		}
		return true
	})
	if result != nil {
		return result
	}
	return nil
}

func encodeCheckpointGroupCertificate(c checkpointGroupCertificate) ([]byte, error) {
	if len(c.members) == 0 || len(c.members) > checkpointGroupMaxMembers ||
		checkpointGroupHeaderBytes+len(c.members)*checkpointGroupMemberBytes > checkpointGroupMaxSpanWitnessTxnOffset ||
		c.sequence == 0 || c.markerID == ([16]byte{}) || c.markerEpoch == 0 ||
		c.txnBase > c.txnHighWater || !validCheckpointGroupSeedCertificate(c) {
		return nil, ErrCheckpointGroupCorrupt
	}
	buf := make([]byte, checkpointGroupSlotBytes)
	copy(buf[0:8], checkpointGroupMagic[:])
	binary.LittleEndian.PutUint16(buf[8:10], checkpointGroupFormat)
	binary.LittleEndian.PutUint16(buf[10:12], checkpointGroupHeaderBytes)
	binary.LittleEndian.PutUint16(buf[12:14], uint16(len(c.members)))
	buf[checkpointGroupMaxApplySpanOffset] = c.maxApplySpan
	binary.LittleEndian.PutUint64(buf[16:24], c.sequence)
	binary.LittleEndian.PutUint64(buf[24:32], c.applied)
	binary.LittleEndian.PutUint64(buf[32:40], c.txnHighWater)
	binary.LittleEndian.PutUint64(buf[40:48], c.txnBase)
	binary.LittleEndian.PutUint64(buf[48:56], c.markerEpoch)
	copy(buf[56:72], c.markerID[:])
	binary.LittleEndian.PutUint64(buf[72:80], c.seedApplied)
	copy(buf[80:112], c.seedState[:])
	copy(buf[112:144], c.seedMember[:])
	membership := sha256.New()
	for i, member := range c.members {
		off := checkpointGroupHeaderBytes + i*checkpointGroupMemberBytes
		copy(buf[off:off+32], member.nameDigest[:])
		copy(buf[off+32:off+48], member.storeID[:])
		copy(buf[off+48:off+64], member.journalID[:])
		_, _ = membership.Write(buf[off : off+checkpointGroupMemberBytes])
	}
	copy(buf[144:168], membership.Sum(nil)[:24])
	binary.LittleEndian.PutUint64(
		buf[checkpointGroupMaxSpanWitnessTxnOffset:checkpointGroupMaxSpanWitnessFirstOffset],
		c.maxSpanTxn,
	)
	binary.LittleEndian.PutUint64(
		buf[checkpointGroupMaxSpanWitnessFirstOffset:checkpointGroupMaxSpanWitnessLastOffset],
		c.maxSpanFirst,
	)
	binary.LittleEndian.PutUint64(
		buf[checkpointGroupMaxSpanWitnessLastOffset:checkpointGroupChecksumOffset],
		c.maxSpanLast,
	)
	h := sha256.New()
	_, _ = h.Write(checkpointGroupDigestDomain)
	_, _ = h.Write(buf[:checkpointGroupChecksumOffset])
	copy(buf[checkpointGroupChecksumOffset:], h.Sum(nil))
	return buf, nil
}

func decodeCheckpointGroupCertificate(buf []byte) (checkpointGroupCertificate, error) {
	if len(buf) != checkpointGroupSlotBytes ||
		!slices.Equal(buf[:8], checkpointGroupMagic[:]) ||
		binary.LittleEndian.Uint16(buf[8:10]) != checkpointGroupFormat ||
		binary.LittleEndian.Uint16(buf[10:12]) != checkpointGroupHeaderBytes {
		return checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	h := sha256.New()
	_, _ = h.Write(checkpointGroupDigestDomain)
	_, _ = h.Write(buf[:checkpointGroupChecksumOffset])
	if !slices.Equal(buf[checkpointGroupChecksumOffset:], h.Sum(nil)) {
		return checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	count := int(binary.LittleEndian.Uint16(buf[12:14]))
	if count == 0 || count > checkpointGroupMaxMembers ||
		checkpointGroupHeaderBytes+count*checkpointGroupMemberBytes > checkpointGroupMaxSpanWitnessTxnOffset {
		return checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	c := checkpointGroupCertificate{
		sequence:     binary.LittleEndian.Uint64(buf[16:24]),
		applied:      binary.LittleEndian.Uint64(buf[24:32]),
		txnHighWater: binary.LittleEndian.Uint64(buf[32:40]),
		txnBase:      binary.LittleEndian.Uint64(buf[40:48]),
		maxApplySpan: buf[checkpointGroupMaxApplySpanOffset],
		maxSpanTxn: binary.LittleEndian.Uint64(
			buf[checkpointGroupMaxSpanWitnessTxnOffset:checkpointGroupMaxSpanWitnessFirstOffset],
		),
		maxSpanFirst: binary.LittleEndian.Uint64(
			buf[checkpointGroupMaxSpanWitnessFirstOffset:checkpointGroupMaxSpanWitnessLastOffset],
		),
		maxSpanLast: binary.LittleEndian.Uint64(
			buf[checkpointGroupMaxSpanWitnessLastOffset:checkpointGroupChecksumOffset],
		),
		markerEpoch: binary.LittleEndian.Uint64(buf[48:56]),
		seedApplied: binary.LittleEndian.Uint64(buf[72:80]),
		members:     make([]checkpointGroupMember, count),
	}
	copy(c.markerID[:], buf[56:72])
	copy(c.seedState[:], buf[80:112])
	copy(c.seedMember[:], buf[112:144])
	if c.sequence == 0 || c.markerEpoch == 0 || c.markerID == ([16]byte{}) ||
		c.txnBase > c.txnHighWater || !validCheckpointGroupSeedCertificate(c) {
		return checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	membership := sha256.New()
	for i := range c.members {
		off := checkpointGroupHeaderBytes + i*checkpointGroupMemberBytes
		copy(c.members[i].nameDigest[:], buf[off:off+32])
		copy(c.members[i].storeID[:], buf[off+32:off+48])
		copy(c.members[i].journalID[:], buf[off+48:off+64])
		_, _ = membership.Write(buf[off : off+checkpointGroupMemberBytes])
	}
	if !slices.Equal(buf[144:168], membership.Sum(nil)[:24]) {
		return checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	canonical, err := encodeCheckpointGroupCertificate(c)
	if err != nil || !slices.Equal(buf, canonical) {
		return checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	return c, nil
}

func validCheckpointGroupSeedCertificate(c checkpointGroupCertificate) bool {
	// Zero is the legacy and sole canonical representation of span one. The
	// authenticated field records only an actual widening of the format-0
	// applied/transaction sanity envelope.
	if c.maxApplySpan == 1 || c.maxApplySpan > MaxCheckpointGroupUpdateEntries {
		return false
	}
	span := uint64(c.maxApplySpan)
	if span == 0 {
		if c.maxSpanTxn != 0 || c.maxSpanFirst != 0 || c.maxSpanLast != 0 {
			return false
		}
		span = 1
	} else if c.maxSpanTxn == 0 || c.maxSpanTxn > c.txnHighWater ||
		c.maxSpanFirst == 0 || c.maxSpanLast < c.maxSpanFirst ||
		c.maxSpanLast > c.applied ||
		c.maxSpanLast-c.maxSpanFirst+1 != span {
		return false
	}
	zeroState := c.seedState == ([sha256.Size]byte{})
	zeroMember := c.seedMember == ([sha256.Size]byte{})
	if c.seedApplied == 0 {
		if c.maxApplySpan > 1 {
			if c.applied < span ||
				!checkpointGroupAppliedWithin(
					c.maxSpanFirst-1, c.maxSpanTxn-1, span,
				) || !checkpointGroupAppliedWithin(
				c.applied-c.maxSpanLast,
				c.txnHighWater-c.maxSpanTxn, span,
			) {
				return false
			}
		}
		return zeroState && zeroMember && c.applied != math.MaxUint64 &&
			checkpointGroupAppliedWithin(c.applied, c.txnHighWater, span)
	}
	if c.seedApplied <= 1 || c.seedApplied == math.MaxUint64 || zeroState || zeroMember {
		return false
	}
	if c.applied == 0 {
		return c.maxApplySpan == 0 && c.txnHighWater == 0 && c.txnBase == 0
	}
	if c.maxApplySpan > 1 {
		if c.applied-c.seedApplied < span || c.maxSpanTxn <= 2 ||
			c.maxSpanFirst <= c.seedApplied ||
			!checkpointGroupAppliedWithin(
				c.maxSpanFirst-c.seedApplied-1, c.maxSpanTxn-3, span,
			) || !checkpointGroupAppliedWithin(
			c.applied-c.maxSpanLast,
			c.txnHighWater-c.maxSpanTxn, span,
		) {
			return false
		}
	}
	if c.applied == math.MaxUint64 || c.applied < c.seedApplied ||
		c.txnHighWater == 0 {
		return false
	}
	if c.applied == c.seedApplied {
		return true
	}
	return c.txnHighWater >= 3 && checkpointGroupAppliedWithin(
		c.applied-c.seedApplied, c.txnHighWater-2, span,
	)
}

func checkpointGroupAppliedWithin(applied, transactions, span uint64) bool {
	if span == 0 {
		return false
	}
	if transactions > math.MaxUint64/span {
		return true
	}
	return applied <= transactions*span
}

// checkpointGroupCertificateChecksumValid distinguishes an unauthenticated
// torn slot (which may be ignored in favor of the other slot) from a
// checksum-valid format-0 payload that violates the one canonical grammar.
// The latter is a hard corruption: silently falling back would turn an
// authenticated malformed newest certificate into rollback authority.
func checkpointGroupCertificateChecksumValid(buf []byte) bool {
	if len(buf) != checkpointGroupSlotBytes {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(checkpointGroupDigestDomain)
	_, _ = h.Write(buf[:checkpointGroupChecksumOffset])
	return slices.Equal(buf[checkpointGroupChecksumOffset:], h.Sum(nil))
}

func validateCheckpointGroupCertificate(
	c checkpointGroupCertificate,
	header storeio.TxnMarkerHeader,
	members []checkpointGroupMember,
) error {
	if err := validateCheckpointGroupCertificateMembers(c, members); err != nil {
		return err
	}
	if c.markerID != header.MarkerID {
		return fmt.Errorf("%w: marker identity", ErrCheckpointGroupCorrupt)
	}
	if c.markerEpoch != header.Epoch &&
		(c.markerEpoch == math.MaxUint64 || c.markerEpoch+1 != header.Epoch) {
		return fmt.Errorf("%w: marker epoch", ErrCheckpointGroupCorrupt)
	}
	return nil
}

func validateCheckpointGroupCertificateMembers(
	c checkpointGroupCertificate,
	members []checkpointGroupMember,
) error {
	if len(c.members) != len(members) {
		return fmt.Errorf("%w: membership count", ErrCheckpointGroupCorrupt)
	}
	for i := range members {
		if c.members[i].nameDigest != members[i].nameDigest ||
			c.members[i].storeID != members[i].storeID ||
			c.members[i].journalID != members[i].journalID {
			return fmt.Errorf("%w: member %d", ErrCheckpointGroupCorrupt, i)
		}
	}
	if c.seedApplied != 0 {
		found := false
		for i := range members {
			if members[i].nameDigest == c.seedMember {
				if found {
					return fmt.Errorf("%w: duplicate seed member", ErrCheckpointGroupCorrupt)
				}
				found = true
			}
		}
		if !found {
			return fmt.Errorf("%w: seed member", ErrCheckpointGroupCorrupt)
		}
	}
	return nil
}

func openCheckpointGroupCertificate(log *TxnLog) (*os.File, checkpointGroupCertificate, error) {
	if log == nil || log.root == nil {
		return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	info, err := log.root.Lstat(checkpointGroupFilename)
	if err != nil {
		return nil, checkpointGroupCertificate{}, err
	}
	if !info.Mode().IsRegular() || info.Size() != checkpointGroupFileBytes {
		return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	file, err := log.root.OpenFile(checkpointGroupFilename, os.O_RDWR, 0)
	if err != nil {
		return nil, checkpointGroupCertificate{}, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = file.Close()
		if err != nil {
			return nil, checkpointGroupCertificate{}, err
		}
		return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	type slottedCertificate struct {
		slot        int
		certificate checkpointGroupCertificate
	}
	valid := make([]slottedCertificate, 0, checkpointGroupSlots)
	for slot := 0; slot < checkpointGroupSlots; slot++ {
		buf := make([]byte, checkpointGroupSlotBytes)
		if _, err := file.ReadAt(buf, int64(slot*checkpointGroupSlotBytes)); err != nil && !errors.Is(err, io.EOF) {
			_ = file.Close()
			return nil, checkpointGroupCertificate{}, err
		}
		certificate, err := decodeCheckpointGroupCertificate(buf)
		if err != nil {
			if checkpointGroupCertificateChecksumValid(buf) {
				_ = file.Close()
				return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
			}
			continue
		}
		if int(certificate.sequence%checkpointGroupSlots) != slot {
			_ = file.Close()
			return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
		}
		valid = append(valid, slottedCertificate{slot: slot, certificate: certificate})
	}
	if len(valid) == 0 {
		_ = file.Close()
		return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
	}
	slices.SortFunc(valid, func(a, b slottedCertificate) int {
		switch {
		case a.certificate.sequence < b.certificate.sequence:
			return -1
		case a.certificate.sequence > b.certificate.sequence:
			return 1
		default:
			return 0
		}
	})
	selected := valid[len(valid)-1].certificate
	if len(valid) > 1 {
		previous := valid[len(valid)-2].certificate
		if !validCheckpointGroupCertificateSuccessor(previous, selected) {
			_ = file.Close()
			return nil, checkpointGroupCertificate{}, ErrCheckpointGroupCorrupt
		}
	}
	return file, selected, nil
}

func validCheckpointGroupCertificateSuccessor(
	previous, selected checkpointGroupCertificate,
) bool {
	if previous.sequence == math.MaxUint64 ||
		previous.sequence+1 != selected.sequence ||
		previous.markerID != selected.markerID ||
		previous.seedApplied != selected.seedApplied ||
		previous.seedState != selected.seedState ||
		previous.seedMember != selected.seedMember ||
		previous.maxApplySpan > selected.maxApplySpan ||
		len(previous.members) != len(selected.members) {
		return false
	}
	for index := range previous.members {
		left, right := previous.members[index], selected.members[index]
		if left.nameDigest != right.nameDigest || left.storeID != right.storeID ||
			left.journalID != right.journalID {
			return false
		}
	}

	switch {
	case selected.markerEpoch == previous.markerEpoch:
		if selected.txnBase != previous.txnBase {
			return false
		}
	case previous.markerEpoch != math.MaxUint64 &&
		selected.markerEpoch == previous.markerEpoch+1:
		if selected.txnBase != previous.txnHighWater {
			return false
		}
	default:
		return false
	}
	if selected.txnHighWater < previous.txnHighWater ||
		selected.applied < previous.applied {
		return false
	}
	deltaTransactions := selected.txnHighWater - previous.txnHighWater
	deltaApplied := selected.applied - previous.applied
	if deltaTransactions == 0 {
		// The only certificate write without newly certified work is an empty
		// marker rollover. It changes exactly the epoch/base tuple.
		return selected.markerEpoch == previous.markerEpoch+1 &&
			deltaApplied == 0 && selected.maxApplySpan == previous.maxApplySpan &&
			selected.maxSpanTxn == previous.maxSpanTxn &&
			selected.maxSpanFirst == previous.maxSpanFirst &&
			selected.maxSpanLast == previous.maxSpanLast
	}
	if selected.markerEpoch != previous.markerEpoch {
		return false
	}
	// Seed is the one certified transition whose applied index is imported
	// rather than advanced from zero by ordinary Update. Its exact immutable
	// seed profile was compared above. The first transaction must publish that
	// cut without also advertising batching.
	if previous.seedApplied > 1 && previous.applied == 0 &&
		previous.txnHighWater == 0 && selected.txnHighWater == 1 &&
		selected.applied == selected.seedApplied &&
		selected.markerEpoch == previous.markerEpoch &&
		selected.txnBase == 0 && selected.maxApplySpan == 0 {
		return true
	}
	widened := selected.maxApplySpan > previous.maxApplySpan
	if widened {
		if selected.maxSpanTxn <= previous.txnHighWater ||
			selected.maxSpanTxn > selected.txnHighWater ||
			selected.maxSpanFirst <= previous.applied ||
			selected.maxSpanLast > selected.applied {
			return false
		}
	} else if selected.maxSpanTxn != previous.maxSpanTxn ||
		selected.maxSpanFirst != previous.maxSpanFirst ||
		selected.maxSpanLast != previous.maxSpanLast {
		return false
	}
	span := uint64(selected.maxApplySpan)
	if span == 0 {
		span = 1
	}
	if !checkpointGroupAppliedWithin(deltaApplied, deltaTransactions, span) {
		return false
	}
	if widened && (!checkpointGroupAppliedWithin(
		selected.maxSpanFirst-previous.applied-1,
		selected.maxSpanTxn-previous.txnHighWater-1, span,
	) || !checkpointGroupAppliedWithin(
		selected.applied-selected.maxSpanLast,
		selected.txnHighWater-selected.maxSpanTxn, span,
	)) {
		return false
	}
	return true
}

func createCheckpointGroupCertificate(
	log *TxnLog, certificate checkpointGroupCertificate,
) (*os.File, bool, error) {
	encoded, err := encodeCheckpointGroupCertificate(certificate)
	if err != nil {
		return nil, false, err
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, false, err
	}
	temporary := fmt.Sprintf(".%s.%x.tmp", checkpointGroupFilename, nonce)
	file, err := log.root.OpenFile(temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, false, err
	}
	cleanup := func(cause error) (*os.File, bool, error) {
		return nil, false, errors.Join(cause, file.Close(), log.root.Remove(temporary))
	}
	if err := file.Truncate(checkpointGroupFileBytes); err != nil {
		return cleanup(err)
	}
	if _, err := file.WriteAt(encoded, int64((certificate.sequence%checkpointGroupSlots)*checkpointGroupSlotBytes)); err != nil {
		return cleanup(err)
	}
	if err := file.Sync(); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		_ = log.root.Remove(temporary)
		return nil, false, err
	}
	if err := log.root.Rename(temporary, checkpointGroupFilename); err != nil {
		_, statErr := log.root.Lstat(checkpointGroupFilename)
		mayBePublished := statErr == nil || !errors.Is(statErr, os.ErrNotExist)
		removeErr := log.root.Remove(temporary)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		return nil, mayBePublished, errors.Join(err, statErr, removeErr)
	}
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterCertificateRename); err != nil {
			return nil, true, journalCommitOutcomeUnknown(err)
		}
	}
	if err := syncTxnLogDirectory(log.root); err != nil {
		return nil, true, journalCommitOutcomeUnknown(err)
	}
	opened, _, err := openCheckpointGroupCertificate(log)
	return opened, true, err
}
