package main

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func restoreRF3PrivateProcessRoot(t *testing.T) string {
	t.Helper()
	// testing.TempDir's numbered child inherits the ambient umask (0777 at
	// creation), but activation authority/session directories require 0700.
	// MkdirTemp supplies that exact private contract without changing umask.
	root, err := os.MkdirTemp(t.TempDir(), "restore-authority-")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestRestoreRF3FixtureRootIsPrivate(t *testing.T) {
	root := restoreRF3PrivateProcessRoot(t)
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		t.Fatalf("restore fixture authority root must be a private real directory: info=%v err=%v", info, err)
	}
}

func restoreRF3PointReadRequest(state shardservice.ReplicatedMemberState,
	authority serviceauthz.Authority, relation replication.RelationID, key []byte,
) *shardservice.ReplicatedRequest {
	return &shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedReadLeader,
		Authority: authority, Capability: serviceauthz.CapabilityDataRead, Fence: state.Fence,
		Relation: relation, Key: key, MinimumApplied: state.Applied,
		MaxValueBytes: replication.MaxMutationValueBytes}
}

func TestRestoreRF3PointReadCarriesObservedAppliedFloor(t *testing.T) {
	store := rf3CommandStoreIdentity(1)
	identity := raftmember.RuntimeIdentity{Group: rf3CommandGroup(), AllocationGeneration: store.AllocationGeneration,
		MemberID: store.MemberID, StoreID: store.StoreID, NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
	state := shardservice.ReplicatedMemberState{Applied: 17, Fence: shardservice.ReplicatedFence{
		Group: identity.Group, AllocationGeneration: identity.AllocationGeneration,
		MemberID: identity.MemberID, StoreID: identity.StoreID, NodeIncarnation: identity.NodeIncarnation,
		Term: 2, Command: commandFenceFromPublication(rf3CommandAuthority(), identity, 1)}}
	authority := serviceauthz.Authority{Node: [16]byte{9}, Generation: state.Fence.Command.ActivePolicyGeneration}
	for _, relation := range []replication.RelationID{1, 2} {
		request := restoreRF3PointReadRequest(state, authority, relation, []byte("exact-key"))
		var frame bytes.Buffer
		if err := shardservice.EncodeReplicatedRequest(&frame, request); err != nil {
			t.Fatalf("restored relation %d read cannot encode: %v", relation, err)
		}
		decoded, err := shardservice.DecodeReplicatedRequest(&frame)
		if err != nil || decoded.MinimumApplied != state.Applied || decoded.Fence != state.Fence ||
			decoded.Authority != authority || decoded.Capability != serviceauthz.CapabilityDataRead ||
			decoded.Relation != relation || decoded.Operation != shardservice.ReplicatedReadLeader ||
			decoded.MaxValueBytes != replication.MaxMutationValueBytes || !bytes.Equal(decoded.Key, request.Key) {
			t.Fatalf("restored read lost its exact observed contract: request=%+v err=%v", decoded, err)
		}
	}
	state.Applied = 0
	invalid := restoreRF3PointReadRequest(state, authority, 1, []byte("exact-key"))
	var frame bytes.Buffer
	if err := shardservice.EncodeReplicatedRequest(&frame, invalid); !errors.Is(err, shardservice.ErrReplicatedWire) || frame.Len() != 0 {
		t.Fatalf("zero observed floor must fail before frame emission: bytes=%d err=%v", frame.Len(), err)
	}
}
