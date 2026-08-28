package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/storeio"
)

// One fixed-size checkpoint bounds both recovery work and live admission
// metadata independently of historical split count. Reserve is synced before
// touching child SQL; terminal witnesses authorize reclamation afterwards.
const rf3ChildAdmissionBytes = 64 + maxRF3SplitChildOperations*(40+autosplit.MaxSplitChildren*64) + sha256.Size

type rf3ChildAdmissionStore struct {
	root     *os.Root
	lock     *os.File
	manifest [32]byte
	limit    int
	failed   bool
}

func openRF3ChildAdmissionStore(path string, manifest [32]byte, limit int) (*rf3ChildAdmissionStore, [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot, error) {
	var slots [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot
	if manifest == ([32]byte{}) || limit < 1 || limit > maxRF3SplitChildOperations {
		return nil, slots, errRF3Serving
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, slots, errors.Join(errRF3Serving, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, slots, err
	}
	store := &rf3ChildAdmissionStore{root: root, manifest: manifest, limit: limit}
	closeError := func(err error) (*rf3ChildAdmissionStore, [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot, error) {
		return nil, slots, errors.Join(err, store.Close())
	}
	if info, err := root.Lstat("child-preparations.lock"); err == nil && !info.Mode().IsRegular() || err != nil && !errors.Is(err, os.ErrNotExist) {
		return closeError(errors.Join(errRF3Serving, err))
	}
	store.lock, err = root.OpenFile("child-preparations.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return closeError(err)
	}
	if err = storeio.LockWriter(store.lock); err != nil {
		return closeError(err)
	}
	info, err = root.Lstat("child-preparations.state")
	if errors.Is(err, os.ErrNotExist) {
		if err = store.save(slots); err != nil {
			return closeError(err)
		}
		return store, slots, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() != rf3ChildAdmissionBytes {
		return closeError(errors.Join(errRF3Serving, err))
	}
	file, err := root.Open("child-preparations.state")
	if err != nil {
		return closeError(err)
	}
	var raw [rf3ChildAdmissionBytes]byte
	_, readErr := io.ReadFull(file, raw[:])
	if err = errors.Join(readErr, file.Close()); err != nil {
		return closeError(err)
	}
	if string(raw[:8]) != "VDBCHADM" || binary.LittleEndian.Uint64(raw[8:16]) != uint64(limit) ||
		!bytes.Equal(raw[16:48], manifest[:]) || !bytes.Equal(raw[48:64], make([]byte, 16)) ||
		sha256.Sum256(raw[:len(raw)-32]) != [32]byte(raw[len(raw)-32:]) {
		return closeError(errRF3Serving)
	}
	position := 64
	for index := range slots {
		slot := &slots[index]
		copy(slot.operation[:], raw[position:position+32])
		group := binary.LittleEndian.Uint64(raw[position+32 : position+40])
		if group >= maxRF3ManifestGroups {
			return closeError(errRF3Serving)
		}
		slot.group = int(group)
		position += 40
		for child := range slot.certificates {
			copy(slot.certificates[child][:], raw[position:position+32])
			copy(slot.requests[child][:], raw[position+32:position+64])
			position += 64
		}
		if index >= limit && *slot != (rf3GroupChildPrepareSlot{}) || slot.operation == ([32]byte{}) && *slot != (rf3GroupChildPrepareSlot{}) {
			return closeError(errRF3Serving)
		}
		children := 0
		for child, certificate := range slot.certificates {
			if (certificate == ([32]byte{})) != (slot.requests[child] == ([32]byte{})) {
				return closeError(errRF3Serving)
			}
			if certificate != ([32]byte{}) {
				children++
			}
		}
		if slot.operation != ([32]byte{}) && (children == 0 || slot.group < 0 || slot.group >= maxRF3ManifestGroups) {
			return closeError(errRF3Serving)
		}
		for prior := 0; prior < index; prior++ {
			if slot.operation != ([32]byte{}) && slots[prior].operation == slot.operation {
				return closeError(errRF3Serving)
			}
		}
	}
	return store, slots, nil
}

func (store *rf3ChildAdmissionStore) save(slots [maxRF3SplitChildOperations]rf3GroupChildPrepareSlot) error {
	if store == nil || store.failed || store.root == nil {
		return errRF3Serving
	}
	var raw [rf3ChildAdmissionBytes]byte
	copy(raw[:8], "VDBCHADM")
	binary.LittleEndian.PutUint64(raw[8:16], uint64(store.limit))
	copy(raw[16:48], store.manifest[:])
	position := 64
	for _, slot := range slots {
		copy(raw[position:position+32], slot.operation[:])
		binary.LittleEndian.PutUint64(raw[position+32:position+40], uint64(slot.group))
		position += 40
		for child := range slot.certificates {
			copy(raw[position:position+32], slot.certificates[child][:])
			copy(raw[position+32:position+64], slot.requests[child][:])
			position += 64
		}
	}
	digest := sha256.Sum256(raw[:len(raw)-32])
	copy(raw[len(raw)-32:], digest[:])
	// A failed publication poisons this handle; restart authenticates either
	// old or new complete checkpoint before another admission is possible.
	store.failed = true
	if err := store.root.Remove("child-preparations.tmp"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := store.root.OpenFile("child-preparations.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	n, writeErr := file.Write(raw[:])
	if writeErr == nil && n != len(raw) {
		writeErr = io.ErrShortWrite
	}
	if err = errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	if err = store.root.Rename("child-preparations.tmp", "child-preparations.state"); err != nil {
		return err
	}
	directory, err := store.root.Open(".")
	if err != nil {
		return err
	}
	if err = errors.Join(directory.Sync(), directory.Close()); err != nil {
		return err
	}
	store.failed = false
	return nil
}

func (store *rf3ChildAdmissionStore) Close() error {
	if store == nil {
		return nil
	}
	var err error
	if store.lock != nil {
		err = errors.Join(storeio.UnlockWriter(store.lock), store.lock.Close())
		store.lock = nil
	}
	if store.root != nil {
		err = errors.Join(err, store.root.Close())
		store.root = nil
	}
	return err
}
