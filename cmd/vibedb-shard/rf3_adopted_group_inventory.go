package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// This is a materialized inventory of certified live groups, not an operation
// journal. Entries survive split operation GC. An entry identifies an exact
// immutable preparation receipt under a manifest-owned root; startup never
// scans historical directories or guesses a SQL/WAL path.
const rf3AdoptedInventoryEntryBytes = 192
const rf3AdoptedInventoryBytes = 64 + maxRF3ManifestGroups*rf3AdoptedInventoryEntryBytes + sha256.Size

type rf3AdoptedGroupEntry struct {
	operation, receipt, plan, certificate, cutover [32]byte
	group, child                                   uint64
}

type rf3AdoptedGroupInventory struct {
	mu       sync.Mutex
	manifest rf3Manifest
	root     *os.Root
	lock     *os.File
	entries  [maxRF3ManifestGroups]rf3AdoptedGroupEntry
	failed   bool
	runtimes map[raftmember.GroupKey]rf3AdoptedRuntime
}

type rf3AdoptedRuntime struct {
	identity raftmember.RuntimeIdentity
	apply    *sqldriver.ReplicatedApply
}

func openRF3AdoptedGroupInventory(manifest rf3Manifest) (*rf3AdoptedGroupInventory, error) {
	if manifest.Digest == ([32]byte{}) || len(manifest.groupBundles()) == 0 || len(manifest.groupBundles()) > maxRF3ManifestGroups {
		return nil, errRF3Serving
	}
	path := manifest.ReplicaControl.SourceDataRoot
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(errRF3Serving, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	result := &rf3AdoptedGroupInventory{manifest: manifest, root: root}
	fail := func(cause error) (*rf3AdoptedGroupInventory, error) { return nil, errors.Join(cause, result.Close()) }
	if info, err := root.Lstat("adopted-groups.lock"); err == nil && !info.Mode().IsRegular() || err != nil && !errors.Is(err, os.ErrNotExist) {
		return fail(errors.Join(errRF3Serving, err))
	}
	result.lock, err = root.OpenFile("adopted-groups.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fail(err)
	}
	if err = storeio.LockWriter(result.lock); err != nil {
		return fail(err)
	}
	info, err = root.Lstat("adopted-groups.state")
	if errors.Is(err, os.ErrNotExist) {
		if err = result.save(result.entries); err != nil {
			return fail(err)
		}
		return result, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() != rf3AdoptedInventoryBytes {
		return fail(errors.Join(errRF3Serving, err))
	}
	file, err := root.Open("adopted-groups.state")
	if err != nil {
		return fail(err)
	}
	var raw [rf3AdoptedInventoryBytes]byte
	_, err = io.ReadFull(file, raw[:])
	if err = errors.Join(err, file.Close()); err != nil {
		return fail(err)
	}
	if !bytes.Equal(raw[:8], []byte("VDBLIVEG")) || !bytes.Equal(raw[8:40], manifest.Digest[:]) ||
		binary.LittleEndian.Uint64(raw[40:48]) != uint64(len(manifest.groupBundles())) ||
		!bytes.Equal(raw[48:64], make([]byte, 16)) || sha256.Sum256(raw[:len(raw)-32]) != [32]byte(raw[len(raw)-32:]) {
		return fail(errRF3Serving)
	}
	for index := range result.entries {
		rawEntry := raw[64+index*rf3AdoptedInventoryEntryBytes : 64+(index+1)*rf3AdoptedInventoryEntryBytes]
		entry := &result.entries[index]
		copy(entry.operation[:], rawEntry[:32])
		copy(entry.receipt[:], rawEntry[32:64])
		copy(entry.plan[:], rawEntry[64:96])
		copy(entry.certificate[:], rawEntry[96:128])
		copy(entry.cutover[:], rawEntry[128:160])
		entry.group, entry.child = binary.LittleEndian.Uint64(rawEntry[160:168]), binary.LittleEndian.Uint64(rawEntry[168:176])
		if !bytes.Equal(rawEntry[176:], make([]byte, 16)) || !result.validEntry(*entry) {
			return fail(errRF3Serving)
		}
		for prior := 0; prior < index; prior++ {
			if entry.operation != ([32]byte{}) && (result.entries[prior].certificate == entry.certificate ||
				result.entries[prior].operation == entry.operation && result.entries[prior].child == entry.child) {
				return fail(errRF3Serving)
			}
		}
	}
	if result.liveCount()+len(manifest.groupBundles()) > maxRF3ManifestGroups {
		return fail(errRF3Serving)
	}
	return result, nil
}

func (inventory *rf3AdoptedGroupInventory) validEntry(entry rf3AdoptedGroupEntry) bool {
	if entry.operation == ([32]byte{}) {
		return entry == (rf3AdoptedGroupEntry{})
	}
	return entry.receipt != ([32]byte{}) && entry.plan != ([32]byte{}) && entry.certificate != ([32]byte{}) &&
		entry.cutover != ([32]byte{}) && entry.group < uint64(len(inventory.manifest.groupBundles())) && entry.child < autosplit.MaxSplitChildren
}

func (inventory *rf3AdoptedGroupInventory) liveCount() int {
	count := 0
	for _, entry := range inventory.entries {
		if entry.operation != ([32]byte{}) {
			count++
		}
	}
	return count
}

func (inventory *rf3AdoptedGroupInventory) save(entries [maxRF3ManifestGroups]rf3AdoptedGroupEntry) error {
	if inventory == nil || inventory.root == nil || inventory.failed {
		return errRF3Serving
	}
	var raw [rf3AdoptedInventoryBytes]byte
	copy(raw[:8], "VDBLIVEG")
	copy(raw[8:40], inventory.manifest.Digest[:])
	binary.LittleEndian.PutUint64(raw[40:48], uint64(len(inventory.manifest.groupBundles())))
	for index, entry := range entries {
		if !inventory.validEntry(entry) {
			return errRF3Serving
		}
		part := raw[64+index*rf3AdoptedInventoryEntryBytes : 64+(index+1)*rf3AdoptedInventoryEntryBytes]
		copy(part[:32], entry.operation[:])
		copy(part[32:64], entry.receipt[:])
		copy(part[64:96], entry.plan[:])
		copy(part[96:128], entry.certificate[:])
		copy(part[128:160], entry.cutover[:])
		binary.LittleEndian.PutUint64(part[160:168], entry.group)
		binary.LittleEndian.PutUint64(part[168:176], entry.child)
	}
	digest := sha256.Sum256(raw[:len(raw)-32])
	copy(raw[len(raw)-32:], digest[:])
	inventory.failed = true
	if err := inventory.root.Remove("adopted-groups.tmp"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := inventory.root.OpenFile("adopted-groups.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	n, err := file.Write(raw[:])
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	if err = errors.Join(err, file.Sync(), file.Close()); err != nil {
		return err
	}
	if err = inventory.root.Rename("adopted-groups.tmp", "adopted-groups.state"); err != nil {
		return err
	}
	directory, err := inventory.root.Open(".")
	if err != nil {
		return err
	}
	if err = errors.Join(directory.Sync(), directory.Close()); err != nil {
		return err
	}
	inventory.entries, inventory.failed = entries, false
	return nil
}

func (inventory *rf3AdoptedGroupInventory) Close() error {
	if inventory == nil {
		return nil
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	var err error
	if inventory.lock != nil {
		err = errors.Join(storeio.UnlockWriter(inventory.lock), inventory.lock.Close())
		inventory.lock = nil
	}
	if inventory.root != nil {
		err = errors.Join(err, inventory.root.Close())
		inventory.root = nil
	}
	return err
}

func (inventory *rf3AdoptedGroupInventory) CheckpointChildAdoption(ctx context.Context, proof splitcontroller.CertifiedChildAdoption, prepared splitcontroller.PreparedChildRuntime) error {
	if inventory == nil || ctx == nil || prepared.Runtime == nil || prepared.Apply == nil {
		return errRF3Serving
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.root == nil || inventory.failed {
		return errRF3Serving
	}
	target := proof.ReplicaTarget()
	index, registry, found := rf3SplitChildRegistryForTarget(inventory.manifest, [32]byte(proof.OperationID()), proof.Child(), target)
	if !found || proof.PlanDigest() == ([32]byte{}) || proof.CutoverDigest() == ([32]byte{}) {
		return errRF3Serving
	}
	raw, err := readRF3AdoptedReceipt(registry, [32]byte(proof.OperationID()), proof.Child())
	if err != nil {
		return err
	}
	receipt, err := splitcontroller.OpenChildPrepareReceipt(raw)
	if err != nil || receipt.Operation != proof.OperationID() || receipt.Child != proof.Child() ||
		receipt.Target.CertificateDigest != target.CertificateDigest || !receipt.Target.SQL.Equal(target.SQL) {
		return errors.Join(errRF3Serving, err)
	}
	identity := prepared.Runtime.Identity()
	if identity.Group != groupFromBinding(target.SQL.Binding) || identity.MemberID != target.Member || identity.StoreID != target.StoreID {
		return errRF3Serving
	}
	entry := rf3AdoptedGroupEntry{operation: [32]byte(proof.OperationID()), receipt: receipt.ReceiptDigest,
		plan: proof.PlanDigest(), certificate: target.CertificateDigest, cutover: proof.CutoverDigest(), group: uint64(index), child: uint64(proof.Child())}
	if err = inventory.record(entry); err != nil {
		return err
	}
	if inventory.runtimes == nil {
		inventory.runtimes = make(map[raftmember.GroupKey]rf3AdoptedRuntime)
	}
	inventory.runtimes[identity.Group] = rf3AdoptedRuntime{identity: identity, apply: prepared.Apply}
	return nil
}

// Caller holds mu. An uncertain publication poisons all retries until reopen.
func (inventory *rf3AdoptedGroupInventory) record(entry rf3AdoptedGroupEntry) error {
	if inventory.root == nil || inventory.failed || !inventory.validEntry(entry) {
		return errRF3Serving
	}
	empty := -1
	for index, prior := range inventory.entries {
		if prior.operation == ([32]byte{}) {
			if empty < 0 {
				empty = index
			}
			continue
		}
		if prior.certificate == entry.certificate || prior.operation == entry.operation && prior.child == entry.child {
			if prior == entry {
				return nil
			}
			return errRF3Serving
		}
	}
	if empty < 0 || inventory.liveCount()+len(inventory.manifest.groupBundles()) >= maxRF3ManifestGroups {
		return errRF3SplitChildRegistryBound
	}
	next := inventory.entries
	next[empty] = entry
	return inventory.save(next)
}

// The preparer's durable slots already reserve not-yet-adopted groups. Count
// their union with live certificates so activation cannot double-count a slot
// or make concurrent preparation exceed the process's serving group ceiling.
func (inventory *rf3AdoptedGroupInventory) checkCapacity(slots [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot) error {
	if inventory == nil {
		return nil
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.root == nil || inventory.failed {
		return errRF3Serving
	}
	count := len(inventory.manifest.groupBundles()) + inventory.liveCount()
	for _, slot := range slots {
		for _, certificate := range slot.certificates {
			if certificate == ([32]byte{}) {
				continue
			}
			live := false
			for _, entry := range inventory.entries {
				if entry.certificate == certificate {
					live = true
					break
				}
			}
			if !live {
				count++
				if count > maxRF3ManifestGroups {
					return errRF3SplitChildRegistryBound
				}
			}
		}
	}
	return nil
}
