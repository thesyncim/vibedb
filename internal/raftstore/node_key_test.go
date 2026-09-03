package raftstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNodeStoreRecoversAuthenticatedWrappedKeyMetadata(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "node")
	options := NodeStoreOptions{MaxWaveBytes: 1 << 20, MaxSegmentEvents: 64, RecentWaves: 16, MaxEntriesPerGroup: 16, ReaderSlots: 1, MaxGroups: 2}
	key := testKey()
	store, err := CreateNodeStore(dir, testNodeIdentity(), key, []NodeBootstrap{
		{Descriptor: testGroupDescriptor(1), Snapshot: nodeSnapshot(1, 1, 1)},
	}, options)
	if err != nil {
		t.Fatal(err)
	}
	created := store
	t.Cleanup(func() { _ = created.Close() })
	clear(key.Wrapped)
	if !bytes.Equal(store.key.Wrapped, testKey().Wrapped) {
		t.Fatal("CreateNodeStore retained the caller's mutable wrapped-key slice")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, supplyWrapped := range []bool{false, true} {
		key = testKey()
		if !supplyWrapped {
			key.Wrapped = nil // a restart key provider need not repeat locator metadata
		}
		store, err = OpenNodeStore(dir, testNodeIdentity(), key, options)
		if err != nil {
			t.Fatalf("reopen suppliedWrapped=%t: %v", supplyWrapped, err)
		}
		clear(key.Wrapped)
		if !bytes.Equal(store.key.Wrapped, testKey().Wrapped) {
			t.Fatal("reopen lost or aliased authenticated locator metadata")
		}
		if _, err := store.Group(1).Snapshot(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	key = testKey()
	key.Wrapped = []byte("wrong locator")
	if got, err := OpenNodeStore(dir, testNodeIdentity(), key, options); err == nil {
		_ = got.Close()
		t.Fatal("explicitly mismatched wrapped key was accepted")
	}
	// Omitting the expected locator must not omit its authentication. The
	// complete header is AEAD associated data, including the wrapped key.
	metaPath := filepath.Join(dir, nodeMetaName)
	meta, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	meta[nodeMetaHeaderBytes+len(testKey().ID)] ^= 1
	if err := os.WriteFile(metaPath, meta, 0o600); err != nil {
		t.Fatal(err)
	}
	key = testKey()
	key.Wrapped = nil
	if got, err := OpenNodeStore(dir, testNodeIdentity(), key, options); !errors.Is(err, ErrCorrupt) {
		if got != nil {
			_ = got.Close()
		}
		t.Fatalf("tampered wrapped key accepted without expected locator: %v", err)
	}
}
