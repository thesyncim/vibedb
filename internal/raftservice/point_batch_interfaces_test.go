package raftservice_test

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type shippedPointBatchOwner interface {
	ReadPointBatch(context.Context, raftservice.PointReadBatchRequest) (raftservice.PointReadBatchResult, raftservice.PointReadLease, error)
}

var (
	_ shippedPointBatchOwner      = (*raftservice.ExecutionOwners)(nil)
	_ raftservice.BatchReadSource = (*sqldriver.ReplicatedApply)(nil)
)

func TestShippedPointBatchReadInterfaces(t *testing.T) {
	if _, ok := any((*raftservice.ExecutionOwners)(nil)).(shippedPointBatchOwner); !ok {
		t.Error("shipped execution owners omit native point batch dispatch")
	}
	if _, ok := any((*sqldriver.ReplicatedApply)(nil)).(raftservice.BatchReadSource); !ok {
		t.Error("shipped SQL apply omits coherent point batch source")
	}
}
