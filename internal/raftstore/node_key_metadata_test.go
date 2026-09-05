package raftstore

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestNodeStoreAuthenticatedWrappedKeyMetadata(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 2}
	key := testKey()
	store, err := CreateNodeStore(dir, testNodeIdentity(), key, []NodeBootstrap{
		{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	omittedWrapped := key
	omittedWrapped.Wrapped = nil
	metadata, err := store.AuthenticatedWrappedKeyMetadata(omittedWrapped)
	if err != nil {
		t.Fatalf("create getter: %v", err)
	}
	if !bytes.Equal(metadata, key.Wrapped) {
		t.Fatalf("metadata=%q, want %q", metadata, key.Wrapped)
	}
	metadata[0]++
	retained, err := store.AuthenticatedWrappedKeyMetadata(omittedWrapped)
	if err != nil || !bytes.Equal(retained, key.Wrapped) {
		t.Fatalf("metadata was not detached: %q, %v", retained, err)
	}

	for _, mutate := range []func(*Key){
		func(k *Key) { k.Material[0]++ },
		func(k *Key) { k.ID += "-other" },
		func(k *Key) { k.Wrapped = []byte("wrong locator") },
	} {
		wrong := omittedWrapped
		mutate(&wrong)
		if got, err := store.AuthenticatedWrappedKeyMetadata(wrong); got != nil || !errors.Is(err, ErrKeyMismatch) {
			t.Fatalf("substituted key accepted: metadata=%q err=%v", got, err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := store.AuthenticatedWrappedKeyMetadata(omittedWrapped); got != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed getter: metadata=%q err=%v", got, err)
	}

	reopened, err := OpenNodeStore(dir, testNodeIdentity(), omittedWrapped, options)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.AuthenticatedWrappedKeyMetadata(omittedWrapped)
	if err != nil || !bytes.Equal(got, key.Wrapped) {
		t.Fatalf("reopen getter: metadata=%q err=%v", got, err)
	}
	got[0]++
	got, err = reopened.AuthenticatedWrappedKeyMetadata(omittedWrapped)
	if err != nil || !bytes.Equal(got, key.Wrapped) {
		t.Fatalf("reopen metadata was not detached: %q err=%v", got, err)
	}

	var nilStore *NodeStore
	if got, err := nilStore.AuthenticatedWrappedKeyMetadata(omittedWrapped); got != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("nil getter: metadata=%q err=%v", got, err)
	}

	injected := errors.New("injected node poison")
	reopened.mu.Lock()
	reopened.poisoned = injected
	reopened.mu.Unlock()
	if got, err := reopened.AuthenticatedWrappedKeyMetadata(omittedWrapped); got != nil || !errors.Is(err, ErrPersistenceUnknown) || !errors.Is(err, injected) {
		t.Fatalf("poisoned getter: metadata=%q err=%v", got, err)
	}
}

func TestNodeStoreAuthenticatedWrappedKeyMetadataRacesClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 2}
	key := testKey()
	store, err := CreateNodeStore(dir, testNodeIdentity(), key, []NodeBootstrap{
		{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	var calls sync.WaitGroup
	calls.Add(1)
	go func() {
		defer calls.Done()
		for {
			_, callErr := store.AuthenticatedWrappedKeyMetadata(Key{ID: key.ID, Material: key.Material})
			if errors.Is(callErr, ErrClosed) {
				return
			}
			if callErr != nil {
				t.Errorf("concurrent getter: %v", callErr)
				return
			}
		}
	}()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	calls.Wait()
}
