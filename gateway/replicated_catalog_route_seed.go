package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
)

const (
	replicatedCatalogRouteSeedCandidateFormat   = 1
	replicatedCatalogRouteSeedCandidateSuffix   = ".pending"
	maxReplicatedCatalogRouteSeedCandidateBytes = maxReplicatedCatalogBytes + (64 << 10)
)

var ErrReplicatedCatalogRouteRestartRequired = errors.New(
	"gateway: certified catalog route changed; restart required",
)

type replicatedCatalogRouteSeedTracker struct {
	mu           sync.Mutex
	terminal     atomic.Bool
	immutable    *Snapshot
	path         string
	active       *Snapshot
	activeExists bool
	shutdown     chan struct{}
	terminalErr  error
}

// ReplicatedCatalogRouteSeedControl couples continuous certified catalog reads
// to one local route seed. ShutdownRequired closes before a binding-changing
// head can be used for further topology work. The owner must quiesce every
// authority user, then call CompleteQuiescedHandoff before process restart.
type ReplicatedCatalogRouteSeedControl struct {
	authority *ReplicatedCatalogAuthority
	tracker   *replicatedCatalogRouteSeedTracker
}

// InstallReplicatedCatalogRouteSeed continuously persists certified catalog
// heads. Byte-identical current heads do no disk I/O; same-route newer heads are
// promoted live, while a self-route change is staged and seals the authority.
func (authority *ReplicatedCatalogAuthority) InstallReplicatedCatalogRouteSeed(
	ctx context.Context,
	path string,
	immutableGenesis *Snapshot,
) (*ReplicatedCatalogRouteSeedControl, error) {
	if authority == nil || ctx == nil || path == "" || immutableGenesis == nil ||
		immutableGenesis.Generation() != 1 || authority.routeSeed.Load() != nil {
		return nil, ErrReplicatedCatalog
	}
	state, err := LoadReplicatedCatalogRouteSeed(path)
	if err != nil {
		return nil, err
	}
	if _, pending := state.Pending(); pending {
		return nil, ErrReplicatedCatalogConflict
	}
	active, activeExists := state.Active()
	if !activeExists {
		active = immutableGenesis
	}
	var scratch [ServingReplicaCount]ReplicatedEndpoint
	route, ok := active.ResolveReplicatedRoute(
		ReplicatedCatalogDistribution, ReplicatedCatalogShard, scratch[:0],
	)
	if !ok || !sameReplicatedCatalogRoute(route, authority.route) {
		return nil, ErrReplicatedCatalogConflict
	}
	tracker := &replicatedCatalogRouteSeedTracker{
		immutable: immutableGenesis, path: path, active: active,
		activeExists: activeExists, shutdown: make(chan struct{}),
	}
	if !authority.routeSeed.CompareAndSwap(nil, tracker) {
		return nil, ErrReplicatedCatalog
	}
	_, err = authority.readAttested(ctx, immutableGenesis)
	control := &ReplicatedCatalogRouteSeedControl{authority: authority, tracker: tracker}
	if err != nil {
		if tracker.terminal.Load() {
			return control, nil
		}
		authority.routeSeed.CompareAndSwap(tracker, nil)
		return nil, err
	}
	return control, nil
}

// ShutdownRequired closes exactly once when continued service could use a
// stale catalog route or when local certified-seed durability is uncertain.
func (control *ReplicatedCatalogRouteSeedControl) ShutdownRequired() <-chan struct{} {
	if control == nil || control.tracker == nil {
		return nil
	}
	return control.tracker.shutdown
}

// ReplicatedCatalogRouteSeedControl returns the installed continuous seed
// controller. It is nil until InstallReplicatedCatalogRouteSeed succeeds.
func (authority *ReplicatedCatalogAuthority) ReplicatedCatalogRouteSeedControl() *ReplicatedCatalogRouteSeedControl {
	if authority == nil {
		return nil
	}
	tracker := authority.routeSeed.Load()
	if tracker == nil {
		return nil
	}
	return &ReplicatedCatalogRouteSeedControl{authority: authority, tracker: tracker}
}

// TerminalError reports why ShutdownRequired closed.
func (control *ReplicatedCatalogRouteSeedControl) TerminalError() error {
	if control == nil || control.tracker == nil {
		return ErrReplicatedCatalog
	}
	control.tracker.mu.Lock()
	defer control.tracker.mu.Unlock()
	return control.tracker.terminalErr
}

// CompleteQuiescedHandoff settles and destroys the old durable session before
// promoting a binding-changing candidate. It is safe to retry after every
// outcome-unknown Retire, Release, journal removal, candidate rename, or
// directory fsync; the active seed is never lost or advanced before release.
func (control *ReplicatedCatalogRouteSeedControl) CompleteQuiescedHandoff(
	ctx context.Context,
) error {
	if control == nil || control.authority == nil || control.tracker == nil || ctx == nil ||
		!control.tracker.terminal.Load() {
		return ErrReplicatedCatalog
	}
	authority, tracker := control.authority, control.tracker
	authority.mu.Lock()
	defer authority.mu.Unlock()
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	state, err := LoadReplicatedCatalogRouteSeed(tracker.path)
	if err != nil {
		return err
	}
	pending, pendingExists := state.Pending()
	active, activeExists := state.Active()
	candidate := pending
	if !pendingExists {
		candidate = active
	}
	if candidate == nil || !activeExists && !pendingExists {
		return errors.Join(tracker.terminalErr, ErrReplicatedCatalogConflict)
	}
	var scratch [ServingReplicaCount]ReplicatedEndpoint
	nextRoute, ok := candidate.ResolveReplicatedRoute(
		ReplicatedCatalogDistribution, ReplicatedCatalogShard, scratch[:0],
	)
	if !ok {
		return errors.Join(tracker.terminalErr, ErrReplicatedCatalogMissing)
	}
	changed := !sameReplicatedCatalogRoute(authority.route, nextRoute)
	if changed {
		if authority.session == nil || authority.session.journal == nil {
			return errors.Join(tracker.terminalErr, ErrNativeSession)
		}
		present, presentErr := NativeSessionJournalPresent(authority.session.journal.base)
		if presentErr != nil {
			return errors.Join(tracker.terminalErr, presentErr)
		}
		if present {
			authorized, authorizeErr := serviceauthz.WithAuthority(ctx, authority.authority)
			if authorizeErr != nil {
				return errors.Join(tracker.terminalErr, authorizeErr)
			}
			if err = authority.session.RetireReleaseAndDestroy(authorized); err != nil {
				return errors.Join(tracker.terminalErr, err)
			}
		}
	}
	if pendingExists {
		if err = state.PromotePending(); err != nil {
			return errors.Join(tracker.terminalErr, err)
		}
		tracker.active, tracker.activeExists = pending, true
	}
	return tracker.terminalErr
}

func (tracker *replicatedCatalogRouteSeedTracker) observe(
	receipt ReplicatedCatalogSeedReceipt,
) error {
	if tracker == nil || receipt.authority == nil || receipt.snapshot == nil {
		return ErrReplicatedCatalog
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.terminal.Load() {
		return tracker.terminalErr
	}
	if receipt.authority.routeSeed.Load() != tracker {
		return ErrReplicatedCatalog
	}
	if tracker.activeExists && receipt.snapshot.Generation() == tracker.active.Generation() {
		equal, err := equalCatalogSnapshots(tracker.active, receipt.snapshot)
		if err != nil || !equal {
			return tracker.terminateLocked(errors.Join(err, ErrReplicatedCatalogConflict))
		}
		return nil
	}
	if tracker.activeExists && receipt.snapshot.Generation() < tracker.active.Generation() {
		// Attestation runs outside the publication lock. A read of the prior
		// authenticated head can arrive after a newer receipt was installed.
		// Reject the stale observation without treating that ordering as a
		// durability failure or shutting down the already newer authority.
		return ErrStaleGeneration
	}
	expected := uint64(0)
	if tracker.activeExists {
		expected = tracker.active.Generation()
	}
	if err := receipt.authority.StageReplicatedCatalogRouteSeedAfter(
		tracker.path, expected, receipt,
	); err != nil {
		return tracker.terminateLocked(err)
	}
	state, err := LoadReplicatedCatalogRouteSeed(tracker.path)
	if err != nil {
		return tracker.terminateLocked(err)
	}
	pending, found := state.Pending()
	if !found {
		return tracker.terminateLocked(ErrReplicatedCatalogConflict)
	}
	equal, compareErr := equalCatalogSnapshots(pending, receipt.snapshot)
	if compareErr != nil || !equal {
		return tracker.terminateLocked(errors.Join(compareErr, ErrReplicatedCatalogConflict))
	}
	var scratch [ServingReplicaCount]ReplicatedEndpoint
	nextRoute, ok := pending.ResolveReplicatedRoute(
		ReplicatedCatalogDistribution, ReplicatedCatalogShard, scratch[:0],
	)
	if !ok {
		return tracker.terminateLocked(ErrReplicatedCatalogMissing)
	}
	if !sameReplicatedCatalogRoute(receipt.authority.route, nextRoute) {
		return tracker.terminateLocked(ErrReplicatedCatalogRouteRestartRequired)
	}
	if err = state.PromotePending(); err != nil {
		return tracker.terminateLocked(err)
	}
	tracker.active, tracker.activeExists = pending, true
	return nil
}

func (tracker *replicatedCatalogRouteSeedTracker) terminateLocked(err error) error {
	if err == nil {
		err = ErrReplicatedCatalog
	}
	if !tracker.terminal.Load() {
		tracker.terminalErr = err
		tracker.terminal.Store(true)
		close(tracker.shutdown)
	}
	return tracker.terminalErr
}

func (tracker *replicatedCatalogRouteSeedTracker) fail(err error) error {
	if tracker == nil {
		return errors.Join(err, ErrReplicatedCatalog)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.terminateLocked(err)
}

func (authority *ReplicatedCatalogAuthority) observePublishedCatalog(
	snapshot *Snapshot,
) error {
	tracker := authority.routeSeed.Load()
	if tracker == nil {
		return nil
	}
	head, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil {
		return tracker.fail(err)
	}
	canonical, err := openTypedControlPlaneDocument(
		head, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes,
	)
	if err != nil {
		return tracker.fail(err)
	}
	// Reopen the exact bytes committed to RF3. In-memory proposal snapshots may
	// omit derived catalog-lineage scalars that canonical decoding reconstructs;
	// the sealed route receipt must carry the byte-exact durable head image, not
	// a caller-owned pre-publication representation.
	certified, err := OpenSnapshotDocument(canonical)
	if err != nil {
		return tracker.fail(err)
	}
	return tracker.observe(ReplicatedCatalogSeedReceipt{
		authority: authority, snapshot: certified, canonical: canonical,
		headBytes: uint64(len(head)), headDigest: sha256.Sum256(head),
	})
}

type persistedReplicatedCatalogRouteSeedCandidate struct {
	Format             uint8             `json:"format"`
	ExpectedGeneration uint64            `json:"expected_generation"`
	HeadBytes          uint64            `json:"head_bytes"`
	HeadDigest         [sha256.Size]byte `json:"head_digest"`
	SnapshotBytes      uint64            `json:"snapshot_bytes"`
	SnapshotDigest     [sha256.Size]byte `json:"snapshot_digest"`
	Snapshot           persistedCatalog  `json:"snapshot"`
}

// ReplicatedCatalogRouteSeedState is one validated local reachability cut. A
// pending candidate was written only through an authority-bound certified-read
// receipt and records the active generation it may replace. Private identity
// fields make PromotePending an exact retry rather than a caller-selected CAS.
type ReplicatedCatalogRouteSeedState struct {
	path              string
	active            *Snapshot
	activeExists      bool
	pending           *Snapshot
	pendingExists     bool
	pendingExpected   uint64
	pendingFileDigest [sha256.Size]byte
	pendingHeadBytes  uint64
	pendingHeadDigest [sha256.Size]byte
}

// ValidateReplicatedCatalogRouteSeedSeparation proves that the immutable
// generation-one file cannot alias either the active mutable seed or its
// deterministic pending candidate through path normalization or hard links.
func ValidateReplicatedCatalogRouteSeedSeparation(immutablePath, routeSeedPath string) error {
	immutable, err := canonicalCatalogEntryPath(immutablePath)
	if err != nil {
		return err
	}
	route, err := canonicalCatalogEntryPath(routeSeedPath)
	if err != nil {
		return err
	}
	pending := route + replicatedCatalogRouteSeedCandidateSuffix
	if immutable == route || immutable == pending {
		return ErrReplicatedCatalogConflict
	}
	immutableInfo, immutableErr := os.Lstat(immutable)
	if immutableErr != nil {
		return immutableErr
	}
	if !immutableInfo.Mode().IsRegular() {
		return ErrReplicatedCatalogConflict
	}
	for _, candidate := range []string{route, pending} {
		info, statErr := os.Lstat(candidate)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() || os.SameFile(immutableInfo, info) {
			return ErrReplicatedCatalogConflict
		}
	}
	return nil
}

func canonicalCatalogEntryPath(path string) (string, error) {
	if path == "" {
		return "", ErrReplicatedCatalog
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	directory, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	base := filepath.Base(absolute)
	if base == "" || base == "." || base == string(filepath.Separator) {
		return "", ErrReplicatedCatalog
	}
	return filepath.Join(directory, base), nil
}

// Active returns the validated durable route seed. An absent seed is reported
// separately so the caller can use—but never mutate—the immutable genesis file
// as its first-start reachability coordinate.
func (state ReplicatedCatalogRouteSeedState) Active() (*Snapshot, bool) {
	return state.active, state.activeExists
}

// Pending returns the exact certified candidate staged before a route-binding
// handoff. It is not active routing authority until PromotePending succeeds.
func (state ReplicatedCatalogRouteSeedState) Pending() (*Snapshot, bool) {
	return state.pending, state.pendingExists
}

// PromotePending atomically publishes the exact candidate observed by
// LoadReplicatedCatalogRouteSeed. A completed rename followed by a failed
// cleanup is outcome-unknown but recoverable: reload observes active==pending
// and removes only the byte-identical candidate.
func (state ReplicatedCatalogRouteSeedState) PromotePending() error {
	if state.path == "" || !state.pendingExists || state.pending == nil ||
		state.pendingFileDigest == ([sha256.Size]byte{}) ||
		state.pendingHeadBytes == 0 ||
		state.pendingHeadDigest == ([sha256.Size]byte{}) {
		return ErrReplicatedCatalog
	}
	return promoteReplicatedCatalogRouteSeed(
		state.path, state.pendingExpected, state.pendingFileDigest,
		state.pendingHeadBytes, state.pendingHeadDigest,
	)
}

// PromotePendingAndReload retries the exact sealed promotion and reloads from
// the same private path captured by LoadReplicatedCatalogRouteSeed. Callers
// cannot accidentally promote one seed and inspect a different path.
func (state ReplicatedCatalogRouteSeedState) PromotePendingAndReload() (
	ReplicatedCatalogRouteSeedState, error,
) {
	if err := state.PromotePending(); err != nil {
		return ReplicatedCatalogRouteSeedState{}, err
	}
	return LoadReplicatedCatalogRouteSeed(state.path)
}

// LoadReplicatedCatalogRouteSeed validates the active seed and its deterministic
// private pending candidate under one pinned directory. Symlinks, hard-link
// aliasing, malformed candidates, stale candidates, and divergent completed
// promotions all fail closed.
func LoadReplicatedCatalogRouteSeed(path string) (ReplicatedCatalogRouteSeedState, error) {
	if path == "" {
		return ReplicatedCatalogRouteSeedState{}, ErrReplicatedCatalog
	}
	root, base, err := openCatalogRoot(path)
	if err != nil {
		return ReplicatedCatalogRouteSeedState{}, err
	}
	defer root.Close()
	if err = rejectReplicatedCatalogRouteSeedAliases(root, base, nil); err != nil {
		return ReplicatedCatalogRouteSeedState{}, err
	}
	active, activeEntry, activeFile, activeErr := loadSnapshotAt(root, base)
	if activeFile != nil {
		activeErr = errors.Join(activeErr, activeFile.Close())
	}
	activeExists := activeErr == nil
	if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
		return ReplicatedCatalogRouteSeedState{}, activeErr
	}
	pending, pendingPersisted, pendingRaw, pendingEntry, pendingFile, pendingErr :=
		loadReplicatedCatalogRouteSeedCandidateAt(root, base+replicatedCatalogRouteSeedCandidateSuffix)
	if pendingFile != nil {
		pendingErr = errors.Join(pendingErr, pendingFile.Close())
	}
	pendingExists := pendingErr == nil
	if pendingErr != nil && !errors.Is(pendingErr, os.ErrNotExist) {
		return ReplicatedCatalogRouteSeedState{}, pendingErr
	}
	if activeExists && pendingExists && os.SameFile(activeEntry, pendingEntry) {
		return ReplicatedCatalogRouteSeedState{}, ErrReplicatedCatalogConflict
	}
	state := ReplicatedCatalogRouteSeedState{
		path: path, active: active, activeExists: activeExists,
		pending: pending, pendingExists: pendingExists,
	}
	if !pendingExists {
		return state, nil
	}
	state.pendingExpected = pendingPersisted.ExpectedGeneration
	state.pendingFileDigest = sha256.Sum256(pendingRaw)
	state.pendingHeadBytes = pendingPersisted.HeadBytes
	state.pendingHeadDigest = pendingPersisted.HeadDigest
	if !activeExists {
		if state.pendingExpected != 0 {
			return ReplicatedCatalogRouteSeedState{}, ErrReplicatedCatalogConflict
		}
		return state, nil
	}
	if active.Generation() == state.pendingExpected {
		return state, nil
	}
	equal, compareErr := equalCatalogSnapshots(active, pending)
	if compareErr != nil || !equal || active.Generation() != pending.Generation() {
		return ReplicatedCatalogRouteSeedState{}, errors.Join(compareErr, ErrReplicatedCatalogConflict)
	}
	return state, nil
}

// StageReplicatedCatalogRouteSeedAfter persists one exact authenticated head as
// a pending local candidate. It deliberately does not use SaveSnapshotAfter:
// generic topology validation cannot authorize a certified replica replacement.
func (authority *ReplicatedCatalogAuthority) StageReplicatedCatalogRouteSeedAfter(
	path string,
	expectedGeneration uint64,
	receipt ReplicatedCatalogSeedReceipt,
) error {
	if authority == nil || path == "" || receipt.authority != authority ||
		receipt.snapshot == nil || receipt.snapshot.Generation() == 0 ||
		receipt.headBytes == 0 || receipt.headBytes > maxReplicatedCatalogBytes ||
		receipt.headDigest == ([sha256.Size]byte{}) || len(receipt.canonical) == 0 {
		return ErrReplicatedCatalog
	}
	if receipt.snapshot.Generation() < expectedGeneration {
		return ErrStaleGeneration
	}
	// One valid caller snapshot may omit derived lineage/high-water scalars.
	// Certify it once at this durability boundary and use that exact state for
	// every byte/digest comparison and for the persisted candidate.
	certified, err := initialCatalogState(receipt.snapshot)
	if err != nil {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	canonical, err := AppendSnapshotDocument(nil, certified)
	if err != nil || !bytes.Equal(canonical, receipt.canonical) {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	head, err := appendReplicatedCatalogDocument(
		nil, certified, maxReplicatedCatalogBytes,
	)
	if err != nil || uint64(len(head)) != receipt.headBytes ||
		sha256.Sum256(head) != receipt.headDigest {
		return errors.Join(err, ErrReplicatedCatalogConflict)
	}
	if certified.Generation() == expectedGeneration {
		state, loadErr := LoadReplicatedCatalogRouteSeed(path)
		if loadErr != nil {
			return loadErr
		}
		current, found := state.Active()
		if !found || current.Generation() != expectedGeneration {
			return ErrCatalogGenerationMismatch
		}
		equal, compareErr := equalCatalogSnapshots(current, certified)
		if compareErr != nil || !equal {
			return errors.Join(compareErr, ErrReplicatedCatalogConflict)
		}
		return nil
	}
	persisted := persistedReplicatedCatalogRouteSeedCandidate{
		Format:             replicatedCatalogRouteSeedCandidateFormat,
		ExpectedGeneration: expectedGeneration,
		HeadBytes:          receipt.headBytes, HeadDigest: receipt.headDigest,
		SnapshotBytes: uint64(len(canonical)), SnapshotDigest: sha256.Sum256(canonical),
		Snapshot: toPersisted(certified),
	}
	raw, err := appendReplicatedCatalogRouteSeedCandidate(nil, &persisted)
	if err != nil {
		return err
	}
	return stageReplicatedCatalogRouteSeedAfter(
		path, expectedGeneration, certified, raw,
	)
}

func appendReplicatedCatalogRouteSeedCandidate(
	dst []byte,
	persisted *persistedReplicatedCatalogRouteSeedCandidate,
) ([]byte, error) {
	if persisted == nil || persisted.Format != replicatedCatalogRouteSeedCandidateFormat ||
		persisted.HeadBytes == 0 || persisted.HeadBytes > maxReplicatedCatalogBytes ||
		persisted.HeadDigest == ([sha256.Size]byte{}) || persisted.SnapshotBytes == 0 ||
		persisted.SnapshotDigest == ([sha256.Size]byte{}) ||
		persisted.Snapshot.Generation <= persisted.ExpectedGeneration {
		return dst, ErrReplicatedCatalog
	}
	compact, err := vibejson.Marshal(persisted)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendCanonicalize(dst, compact)
	if err != nil || len(dst)-start > maxReplicatedCatalogRouteSeedCandidateBytes {
		return dst[:start], errors.Join(err, ErrCatalogTooLarge)
	}
	return dst, nil
}

func loadReplicatedCatalogRouteSeedCandidateAt(
	root *os.Root,
	name string,
) (*Snapshot, persistedReplicatedCatalogRouteSeedCandidate, []byte, os.FileInfo, *os.File, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, nil,
			ErrReplicatedCatalogConflict
	}
	file, err := openCatalogRootFile(root, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) || opened.Size() <= 0 ||
		opened.Size() > maxReplicatedCatalogRouteSeedCandidateBytes {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file,
			errors.Join(err, ErrReplicatedCatalog)
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxReplicatedCatalogRouteSeedCandidateBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxReplicatedCatalogRouteSeedCandidateBytes {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file,
			errors.Join(err, ErrReplicatedCatalog)
	}
	var persisted persistedReplicatedCatalogRouteSeedCandidate
	if err = vibejson.Unmarshal(raw, &persisted); err != nil {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file, err
	}
	canonicalCandidate, err := appendReplicatedCatalogRouteSeedCandidate(nil, &persisted)
	if err != nil || !bytes.Equal(raw, canonicalCandidate) {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file,
			errors.Join(err, ErrReplicatedCatalog)
	}
	snapshotRaw, err := vibejson.Marshal(&persisted.Snapshot)
	if err == nil {
		snapshotRaw, err = vibejson.AppendCanonicalize(nil, snapshotRaw)
	}
	if err != nil || uint64(len(snapshotRaw)) != persisted.SnapshotBytes ||
		sha256.Sum256(snapshotRaw) != persisted.SnapshotDigest {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file,
			errors.Join(err, ErrReplicatedCatalog)
	}
	snapshot, err := OpenSnapshotDocument(snapshotRaw)
	if err != nil || snapshot.Generation() <= persisted.ExpectedGeneration {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file,
			errors.Join(err, ErrReplicatedCatalog)
	}
	head, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil || uint64(len(head)) != persisted.HeadBytes ||
		sha256.Sum256(head) != persisted.HeadDigest {
		return nil, persistedReplicatedCatalogRouteSeedCandidate{}, nil, nil, file,
			errors.Join(err, ErrReplicatedCatalog)
	}
	return snapshot, persisted, raw, before, file, nil
}

func stageReplicatedCatalogRouteSeedAfter(
	path string,
	expectedGeneration uint64,
	next *Snapshot,
	candidateRaw []byte,
) (err error) {
	if !catalogDurabilitySupported() {
		return ErrCatalogDurabilityUnsupported
	}
	root, base, err := openCatalogRoot(path)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		closeErr := root.Close()
		if closeErr != nil {
			if published {
				err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, closeErr)
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	lock, lockInfo, err := lockReplicatedCatalogRouteSeed(root, base)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := errors.Join(storeio.UnlockWriter(lock), lock.Close())
		if closeErr != nil {
			if published {
				err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, closeErr)
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	if err = rejectReplicatedCatalogRouteSeedAliases(root, base, lockInfo); err != nil {
		return err
	}
	current, currentEntry, currentFile, currentErr := loadSnapshotAt(root, base)
	if currentFile != nil {
		currentErr = errors.Join(currentErr, currentFile.Close())
	}
	if currentErr != nil && !errors.Is(currentErr, os.ErrNotExist) {
		return currentErr
	}
	pending, _, pendingRaw, pendingEntry, pendingFile, pendingErr :=
		loadReplicatedCatalogRouteSeedCandidateAt(root, base+replicatedCatalogRouteSeedCandidateSuffix)
	if pendingFile != nil {
		pendingErr = errors.Join(pendingErr, pendingFile.Close())
	}
	if pendingErr == nil {
		if currentErr == nil && (os.SameFile(currentEntry, pendingEntry) ||
			os.SameFile(currentEntry, lockInfo)) || os.SameFile(pendingEntry, lockInfo) {
			return ErrReplicatedCatalogConflict
		}
		equal, compareErr := equalCatalogSnapshots(pending, next)
		if compareErr == nil && equal && bytes.Equal(pendingRaw, candidateRaw) {
			return nil
		}
		return errors.Join(compareErr, ErrReplicatedCatalogConflict)
	}
	if !errors.Is(pendingErr, os.ErrNotExist) {
		return pendingErr
	}
	if currentErr == nil {
		if os.SameFile(currentEntry, lockInfo) {
			return ErrReplicatedCatalogConflict
		}
		if current.Generation() != expectedGeneration {
			return fmt.Errorf("%w: expected=%d durable=%d",
				ErrCatalogGenerationMismatch, expectedGeneration, current.Generation())
		}
		switch {
		case next.Generation() < current.Generation():
			return ErrStaleGeneration
		case next.Generation() == current.Generation():
			equal, compareErr := equalCatalogSnapshots(current, next)
			if compareErr != nil || !equal {
				return errors.Join(compareErr, ErrReplicatedCatalogConflict)
			}
			return nil
		}
	} else if expectedGeneration != 0 {
		return fmt.Errorf("%w: expected=%d durable=0",
			ErrCatalogGenerationMismatch, expectedGeneration)
	}
	published, err = replaceReplicatedCatalogRouteSeedEntry(
		root, base+replicatedCatalogRouteSeedCandidateSuffix,
		candidateRaw, maxReplicatedCatalogRouteSeedCandidateBytes,
		base+".lock", lockInfo, nil,
	)
	return err
}

func promoteReplicatedCatalogRouteSeed(
	path string,
	expectedGeneration uint64,
	observedCandidateDigest [sha256.Size]byte,
	observedHeadBytes uint64,
	observedHeadDigest [sha256.Size]byte,
) (err error) {
	if !catalogDurabilitySupported() {
		return ErrCatalogDurabilityUnsupported
	}
	root, base, err := openCatalogRoot(path)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		closeErr := root.Close()
		if closeErr != nil {
			if published {
				err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, closeErr)
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	lock, lockInfo, err := lockReplicatedCatalogRouteSeed(root, base)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := errors.Join(storeio.UnlockWriter(lock), lock.Close())
		if closeErr != nil {
			if published {
				err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, closeErr)
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	if err = rejectReplicatedCatalogRouteSeedAliases(root, base, lockInfo); err != nil {
		return err
	}
	current, currentEntry, currentFile, currentErr := loadSnapshotAt(root, base)
	if currentFile != nil {
		currentErr = errors.Join(currentErr, currentFile.Close())
	}
	if currentErr != nil && !errors.Is(currentErr, os.ErrNotExist) {
		return currentErr
	}
	pending, persisted, pendingRaw, pendingEntry, pendingFile, pendingErr :=
		loadReplicatedCatalogRouteSeedCandidateAt(root, base+replicatedCatalogRouteSeedCandidateSuffix)
	if pendingFile != nil {
		pendingErr = errors.Join(pendingErr, pendingFile.Close())
	}
	if pendingErr != nil {
		return errors.Join(pendingErr, ErrReplicatedCatalogConflict)
	}
	if persisted.ExpectedGeneration != expectedGeneration ||
		sha256.Sum256(pendingRaw) != observedCandidateDigest ||
		persisted.HeadBytes != observedHeadBytes || persisted.HeadDigest != observedHeadDigest ||
		os.SameFile(pendingEntry, lockInfo) ||
		currentErr == nil && (os.SameFile(currentEntry, pendingEntry) || os.SameFile(currentEntry, lockInfo)) {
		return ErrReplicatedCatalogConflict
	}
	if currentErr == nil && current.Generation() != expectedGeneration {
		equal, compareErr := equalCatalogSnapshots(current, pending)
		if compareErr != nil || !equal || current.Generation() != pending.Generation() {
			return errors.Join(compareErr, ErrReplicatedCatalogConflict)
		}
		if err = verifyCatalogEntryUnchanged(root,
			base+replicatedCatalogRouteSeedCandidateSuffix, pendingEntry); err != nil {
			return err
		}
		if err = root.Remove(base + replicatedCatalogRouteSeedCandidateSuffix); err != nil {
			return err
		}
		published = true
		if err = fsyncCatalogRoot(root); err != nil {
			return errors.Join(ErrCatalogPublishOutcomeUnknown, err)
		}
		return nil
	}
	if currentErr != nil && expectedGeneration != 0 {
		return fmt.Errorf("%w: expected=%d durable=0",
			ErrCatalogGenerationMismatch, expectedGeneration)
	}
	if currentErr == nil && current.Generation() != expectedGeneration {
		return fmt.Errorf("%w: expected=%d durable=%d",
			ErrCatalogGenerationMismatch, expectedGeneration, current.Generation())
	}
	raw, err := appendDurableCatalogSnapshot(nil, pending)
	if err != nil {
		return err
	}
	published, err = replaceReplicatedCatalogRouteSeedEntry(
		root, base, raw, maxCatalogBytes, base+".lock", lockInfo, currentEntry,
	)
	if err != nil {
		return err
	}
	if err = verifyCatalogEntryUnchanged(root,
		base+replicatedCatalogRouteSeedCandidateSuffix, pendingEntry); err != nil {
		return errors.Join(ErrCatalogPublishOutcomeUnknown, err)
	}
	if err = root.Remove(base + replicatedCatalogRouteSeedCandidateSuffix); err != nil {
		return errors.Join(ErrCatalogPublishOutcomeUnknown, err)
	}
	if err = fsyncCatalogRoot(root); err != nil {
		return errors.Join(ErrCatalogPublishOutcomeUnknown, err)
	}
	return nil
}

func lockReplicatedCatalogRouteSeed(
	root *os.Root,
	base string,
) (*os.File, os.FileInfo, error) {
	lock, err := openCatalogRootFile(root, base+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, nil, err
	}
	info, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		if errors.Is(err, storeio.ErrWriterLocked) {
			return nil, nil, fmt.Errorf("%w: %v", ErrCatalogWriterLocked, err)
		}
		return nil, nil, err
	}
	if err = verifyCatalogEntryUnchanged(root, base+".lock", info); err != nil {
		_ = storeio.UnlockWriter(lock)
		_ = lock.Close()
		return nil, nil, err
	}
	return lock, info, nil
}

func rejectReplicatedCatalogRouteSeedAliases(
	root *os.Root,
	base string,
	lockInfo os.FileInfo,
) error {
	if root == nil || base == "" {
		return ErrReplicatedCatalog
	}
	active, activeErr := root.Lstat(base)
	if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
		return activeErr
	}
	pending, pendingErr := root.Lstat(base + replicatedCatalogRouteSeedCandidateSuffix)
	if pendingErr != nil && !errors.Is(pendingErr, os.ErrNotExist) {
		return pendingErr
	}
	if activeErr == nil && pendingErr == nil && os.SameFile(active, pending) {
		return ErrReplicatedCatalogConflict
	}
	if lockInfo != nil && (activeErr == nil && os.SameFile(active, lockInfo) ||
		pendingErr == nil && os.SameFile(pending, lockInfo)) {
		return ErrReplicatedCatalogConflict
	}
	return nil
}

func replaceReplicatedCatalogRouteSeedEntry(
	root *os.Root,
	base string,
	raw []byte,
	maximum int,
	lockName string,
	lockInfo os.FileInfo,
	currentEntry os.FileInfo,
) (published bool, err error) {
	if root == nil || base == "" || lockName == "" || maximum <= 0 ||
		len(raw) == 0 || len(raw) > maximum {
		return false, ErrReplicatedCatalog
	}
	tmp, tmpName, err := createCatalogTemp(root, base)
	if err != nil {
		return false, err
	}
	tmpOpen := true
	defer func() {
		var cleanupErr error
		if tmpOpen {
			cleanupErr = tmp.Close()
		}
		if !published {
			if removeErr := root.Remove(tmpName); removeErr != nil &&
				!errors.Is(removeErr, os.ErrNotExist) {
				cleanupErr = errors.Join(cleanupErr, removeErr)
			}
		}
		if cleanupErr != nil {
			if published {
				err = errors.Join(err, ErrCatalogPublishOutcomeUnknown, cleanupErr)
			} else {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return false, err
	}
	if err = writeNativeSessionJournalFull(tmp, raw); err != nil {
		return false, err
	}
	if err = tmp.Sync(); err != nil {
		return false, err
	}
	tmpInfo, err := tmp.Stat()
	if err != nil {
		return false, err
	}
	if err = verifyCatalogEntryUnchanged(root, lockName, lockInfo); err != nil {
		return false, err
	}
	if err = verifyCatalogEntryUnchanged(root, base, currentEntry); err != nil {
		return false, err
	}
	if err = verifyCatalogEntryUnchanged(root, tmpName, tmpInfo); err != nil {
		return false, err
	}
	if err = replaceCatalogEntry(root, tmpName, base); err != nil {
		return false, err
	}
	published = true
	closeErr := tmp.Close()
	tmpOpen = false
	if syncErr := errors.Join(closeErr, fsyncCatalogRoot(root)); syncErr != nil {
		return true, errors.Join(ErrCatalogPublishOutcomeUnknown, syncErr)
	}
	return true, nil
}

func appendDurableCatalogSnapshot(dst []byte, snapshot *Snapshot) ([]byte, error) {
	if snapshot == nil {
		return dst, ErrInvalidCatalog
	}
	state, err := initialCatalogState(snapshot)
	if err != nil {
		return dst, err
	}
	persisted := toPersisted(state)
	compact, err := vibejson.Marshal(&persisted)
	if err != nil {
		return dst, err
	}
	start := len(dst)
	dst, err = vibejson.AppendIndent(dst, compact, "", "  ")
	if err != nil || len(dst)-start > maxCatalogBytes {
		return dst[:start], errors.Join(err, ErrCatalogTooLarge)
	}
	return dst, nil
}

func equalCatalogSnapshots(left, right *Snapshot) (bool, error) {
	leftRaw, leftErr := AppendSnapshotDocument(nil, left)
	rightRaw, rightErr := AppendSnapshotDocument(nil, right)
	if leftErr != nil || rightErr != nil {
		return false, errors.Join(leftErr, rightErr)
	}
	return bytes.Equal(leftRaw, rightRaw), nil
}
