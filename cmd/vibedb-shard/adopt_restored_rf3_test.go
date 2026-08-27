package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/kubeoperator"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestWriteRestoredRF3ArtifactIsExactAndIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restore_preparing")
	if err := writeRestoredRF3Artifact(path, []byte("exact")); err != nil {
		t.Fatal(err)
	}
	if err := writeRestoredRF3Artifact(path, []byte("exact")); err != nil {
		t.Fatal(err)
	}
	if err := writeRestoredRF3Artifact(path, []byte("different")); err == nil {
		t.Fatal("different retained artifact accepted")
	}
	raw, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(raw, []byte("exact")) {
		t.Fatalf("raw=%q err=%v", raw, err)
	}
}

func TestRestoredRosterMatchesOnlyAuthenticatedTargets(t *testing.T) {
	var nodes [3]rafttransport.NodeID
	state := kubeoperator.RestoredReplicaState{}
	input := prepareRF3Manifest{Members: make([]prepareRF3Member, 3)}
	for index := range input.Members {
		nodes[index][0] = byte(index + 1)
		state.Targets[index] = kubeoperator.RestoredReplicaTarget{
			Member: uint64(index + 1), Node: [16]byte(nodes[index]), NodeIncarnation: 1,
		}
		input.Members[index] = prepareRF3Member{MemberID: uint64(index + 1)}
	}
	if !restoredRosterMatchesTargets(input, nodes, state) {
		t.Fatal("exact target roster rejected")
	}
	nodes[1][0] ^= 0xff
	if restoredRosterMatchesTargets(input, nodes, state) {
		t.Fatal("unauthenticated target node accepted")
	}
}
