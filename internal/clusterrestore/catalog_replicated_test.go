package clusterrestore

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

type catalogProposerFunc func(context.Context, []byte) ([]byte, error)

func (proposal catalogProposerFunc) ProposeRestoreActivation(ctx context.Context, raw []byte) ([]byte, error) {
	return proposal(ctx, raw)
}

func TestReplicatedCatalogPublisherSettlesExactCanonicalWitness(t *testing.T) {
	operation := restoreOperationFixture(t, 2)
	roots := servingRootsFixture(operation)
	witness := makeCatalogWitness(operation, roots)
	publisher := ReplicatedCatalogPublisher{Proposer: catalogProposerFunc(func(_ context.Context, raw []byte) ([]byte, error) {
		opened, err := OpenCatalogActivation(raw)
		if err != nil || opened != witness {
			t.Fatalf("opened=%+v err=%v", opened, err)
		}
		canonical, err := AppendCatalogActivation(nil, opened)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("canonical=%t err=%v", bytes.Equal(canonical, raw), err)
		}
		return append([]byte(nil), opened.CatalogDigest[:]...), nil
	})}
	if err := publisher.Publish(t.Context(), witness); err != nil {
		t.Fatal(err)
	}
	raw, _ := AppendCatalogActivation(nil, witness)
	for _, malformed := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenCatalogActivation(malformed); err == nil {
			t.Fatal("accepted malformed catalog activation")
		}
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)/2] ^= 1
	if _, err := OpenCatalogActivation(corrupt); err == nil {
		t.Fatal("accepted corrupt catalog activation")
	}
}

func TestReplicatedCatalogPublisherRejectsOutcomeUnknownAndWrongSettlement(t *testing.T) {
	operation := restoreOperationFixture(t, 1)
	witness := makeCatalogWitness(operation, servingRootsFixture(operation))
	cause := errors.New("outcome unknown")
	for name, proposer := range map[string]catalogProposerFunc{
		"unknown": func(context.Context, []byte) ([]byte, error) { return nil, cause },
		"wrong":   func(context.Context, []byte) ([]byte, error) { return make([]byte, 32), nil },
	} {
		t.Run(name, func(t *testing.T) {
			err := (ReplicatedCatalogPublisher{Proposer: proposer}).Publish(t.Context(), witness)
			if err == nil || name == "unknown" && !errors.Is(err, cause) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func servingRootsFixture(operation Operation) []RootWitness {
	roots := make([]RootWitness, len(operation.Targets))
	for ordinal := range roots {
		cut := operation.Certificate.Groups[ordinal]
		appendGroupKey(roots[ordinal].TargetGroup[:], operation.Targets[ordinal].Group)
		roots[ordinal].ArtifactManifest = cut.ArtifactManifestDigest
		roots[ordinal].SanitizedImageDigest = filled32(byte(150 + ordinal))
		roots[ordinal].GenesisProof = filled32(byte(160 + ordinal))
		roots[ordinal].SnapshotIndex, roots[ordinal].SnapshotTerm = cut.SnapshotIndex, cut.SnapshotTerm
		for replica := range roots[ordinal].ReplicaRoots {
			roots[ordinal].ReplicaRoots[replica] = filled32(byte(170 + ordinal*3 + replica))
		}
	}
	return roots
}
