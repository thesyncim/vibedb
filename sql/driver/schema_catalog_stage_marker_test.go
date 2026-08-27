package driver

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedSchemaStageMarkerCanonicalAndCorruptionClosed(t *testing.T) {
	marker := replicatedSchemaStageMarker{
		schemaGeneration: 7, sourceApplied: 41,
		membership: durable.CheckpointMembershipWitness{
			Sequence: 3, Source: [32]byte{1}, Target: [32]byte{2},
		},
		catalogDigest: [32]byte{3}, relationWitness: [32]byte{4},
		applyContract: [32]byte{5}, authorization: [32]byte{6}, targetWitness: [32]byte{9},
		storages: [][32]byte{{7}, {8}},
	}
	raw, err := encodeReplicatedSchemaStageMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := decodeReplicatedSchemaStageMarker(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeReplicatedSchemaStageMarker(opened)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("canonical marker = %v", err)
	}
	damaged := bytes.Clone(raw)
	damaged[72] ^= 1
	if _, err = decodeReplicatedSchemaStageMarker(damaged); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
		t.Fatalf("corrupt marker error = %v", err)
	}
}
