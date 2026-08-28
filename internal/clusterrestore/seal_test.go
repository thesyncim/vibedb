package clusterrestore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

func TestPlanFreshTargetsPrecedesExactCatalogProjectionSeal(t *testing.T) {
	fixture := restoreOperationFixture(t, 2)
	targets, err := planFreshTargets(bytes.NewReader(identityEntropy(2)), fixture.Permit, fixture.Certificate, 9)
	if err != nil || len(targets) != 2 {
		t.Fatalf("targets=%d err=%v", len(targets), err)
	}
	// The canonical projection can now bind the actual fresh identities before
	// NewOperation seals its digest; no operation bytes need to be rewritten.
	projection := sha256.New()
	for _, target := range targets {
		_, _ = projection.Write(target.Group.GroupID[:])
		for _, replica := range target.Replicas {
			_, _ = projection.Write(replica.Node[:])
		}
	}
	var digest [32]byte
	copy(digest[:], projection.Sum(nil))
	operation, err := NewOperation(fixture.Permit, fixture.Certificate, 0, 1,
		filled32(1), filled32(2), digest, targets)
	if err != nil || operation.TargetCatalogDigest != digest || operation.Digest == ([32]byte{}) {
		t.Fatalf("sealed projection err=%v", err)
	}
	targets[0].Replicas[0].Node[0] ^= 1
	if operation.Targets[0].Replicas[0].Node == targets[0].Replicas[0].Node {
		t.Fatal("sealed operation aliases mutable target plan")
	}
	if _, err = planFreshTargets(bytes.NewReader(make([]byte, len(identityEntropy(2)))),
		fixture.Permit, fixture.Certificate, 9); err == nil {
		t.Fatal("zero or colliding identity plan accepted")
	}
}

func TestSealFreshOperationMintsCanonicalDistinctRF3Identity(t *testing.T) {
	fixture := restoreOperationFixture(t, 3)
	entropy := identityEntropy(3)
	spec := TargetSpec{CatalogOrdinal: 0, PolicyGeneration: fixture.PolicyGeneration,
		TopologyEpoch: 9, BuildGrammarDigest: fixture.BuildGrammarDigest,
		TargetPolicyDigest: fixture.TargetPolicyDigest, TargetCatalogDigest: fixture.TargetCatalogDigest}
	operation, err := sealFreshOperation(bytes.NewReader(entropy), fixture.Permit, fixture.Certificate, spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(operation.Targets) != 3 || operation.Targets[0].Group.TopologyRecoveryEpoch != 9 {
		t.Fatalf("targets=%d first=%+v", len(operation.Targets), operation.Targets[0].Group)
	}
	for _, group := range operation.Targets {
		for ordinal, replica := range group.Replicas {
			if replica.Member != uint64(ordinal+1) || replica.NodeIncarnation != 1 {
				t.Fatalf("replica=%+v", replica)
			}
		}
	}
	replay, err := sealFreshOperation(bytes.NewReader(entropy), fixture.Permit, fixture.Certificate, spec)
	if err != nil || replay.Digest != operation.Digest {
		t.Fatalf("replay=%x operation=%x err=%v", replay.Digest, operation.Digest, err)
	}
}

func TestSealFreshOperationPropagatesEntropyFailureWithoutPartialOperation(t *testing.T) {
	fixture := restoreOperationFixture(t, 2)
	cause := errors.New("entropy unavailable")
	operation, err := sealFreshOperation(failingIdentityReader{cause: cause}, fixture.Permit,
		fixture.Certificate, TargetSpec{CatalogOrdinal: 0, PolicyGeneration: 1, TopologyEpoch: 1,
			BuildGrammarDigest: filled32(1), TargetPolicyDigest: filled32(2),
			TargetCatalogDigest: filled32(3)})
	if !errors.Is(err, cause) || operation.Digest != ([32]byte{}) || len(operation.Targets) != 0 {
		t.Fatalf("operation=%+v err=%v", operation, err)
	}
}

func identityEntropy(groups int) []byte {
	identities := groups * (2 + 3*2)
	raw := make([]byte, identities*16)
	for identity := 0; identity < identities; identity++ {
		raw[identity*16] = byte(identity + 1)
		raw[identity*16+15] = byte(255 - identity)
	}
	return raw
}

type failingIdentityReader struct{ cause error }

func (reader failingIdentityReader) Read([]byte) (int, error) { return 0, reader.cause }

var _ io.Reader = failingIdentityReader{}
