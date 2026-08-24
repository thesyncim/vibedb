package raftmodel

import (
	"crypto/sha256"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestValidateNormalBatchDataChainWitnessesRejectsHiddenNoopChange(t *testing.T) {
	before := sha256.Sum256([]byte("before"))
	afterCommand := sha256.Sum256([]byte("after-command"))
	entries := []NormalApply{
		{
			Meta: ApplyMeta{Index: 2, Term: 1, Type: pb.EntryNormal},
			Data: []byte("command"),
		},
		{Meta: ApplyMeta{Index: 3, Term: 1, Type: pb.EntryNormal}},
	}
	witnesses := [][32]byte{afterCommand, afterCommand}
	final := Publication{Applied: 3, DataChainDigest: afterCommand}
	if err := validateNormalBatchDataChainWitnesses(
		before, entries, len(entries), witnesses, final,
	); err != nil {
		t.Fatalf("valid mixed witnesses = %v", err)
	}

	hiddenChange := sha256.Sum256([]byte("invalid-after-noop"))
	witnesses[1] = hiddenChange
	final.DataChainDigest = hiddenChange
	if err := validateNormalBatchDataChainWitnesses(
		before, entries, len(entries), witnesses, final,
	); err == nil {
		t.Fatal("mixed witnesses accepted a hidden no-op digest change")
	}
}
