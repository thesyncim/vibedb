package gateway

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"testing"
)

func TestCommandFenceDigestStartsWithInteger(t *testing.T) {
	fence := raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 2, ProtectionEpoch: 3, OwnershipEpoch: 4, SchemaGeneration: 5, RelationManifestDigest: [32]byte{6}, RoutingVersion: 7, RouteGeneration: 8}
	var encoded []byte
	for _, value := range []uint64{1, 2, 3, 4, 5, 32} {
		encoded = binary.BigEndian.AppendUint64(encoded, value)
	}
	encoded = append(encoded, fence.RelationManifestDigest[:]...)
	encoded = binary.BigEndian.AppendUint64(encoded, 7)
	encoded = binary.BigEndian.AppendUint64(encoded, 8)
	if got, want := DigestCommandFence(fence), sha256.Sum256(encoded); got != want {
		t.Fatalf("digest=%x want=%x", got, want)
	}
	fence.RouteGeneration++
	if DigestCommandFence(fence) == sha256.Sum256(encoded) {
		t.Fatal("route generation not fenced")
	}
}
