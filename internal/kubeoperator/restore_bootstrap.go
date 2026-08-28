package kubeoperator

import (
	"bytes"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	pb "go.etcd.io/raft/v3/raftpb"
)

var restoreBootstrapMagic = [8]byte{'V', 'B', 'R', 'S', 'T', 'R', 'O', 'P'}

// RestoreBootstrapOperation reads restore identity from the authenticated
// immutable static bootstrap, including when a newer snapshot retains it.
// A removable local marker is never sufficient to classify a restored root.
func RestoreBootstrapOperation(snapshot *pb.Snapshot) (
	operation [32]byte, ordinal uint32, schema [32]byte, restored bool, err error,
) {
	bootstrap, err := replicatedstate.StaticBootstrapForSnapshot(snapshot)
	if err != nil {
		return operation, 0, schema, false, err
	}
	raw := bootstrap.GetData()
	if len(raw) < len(restoreBootstrapMagic) || !bytes.Equal(raw[:8], restoreBootstrapMagic[:]) {
		return operation, 0, schema, false, nil
	}
	if len(raw) != 76 {
		return operation, 0, schema, false, ErrBootstrap
	}
	copy(operation[:], raw[8:40])
	ordinal = binary.BigEndian.Uint32(raw[40:44])
	copy(schema[:], raw[44:76])
	if operation == ([32]byte{}) || schema == ([32]byte{}) {
		return [32]byte{}, 0, [32]byte{}, false, ErrBootstrap
	}
	return operation, ordinal, schema, true, nil
}
