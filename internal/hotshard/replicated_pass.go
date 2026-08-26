package hotshard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
)

type ReplicatedDirectory interface {
	ReadPressureRecord(context.Context) (gateway.ReplicatedPressureRecord, error)
}

type Pass struct {
	CatalogGeneration uint64
	AuthorityRevision uint64
	AlreadyApplied    bool
	Admission         Admission
}

// RunReplicatedPass consumes exactly one catalog-Raft pressure record. The
// opaque record digest, embedded generation/revision, and controller cursor
// must all agree before any tracker or scheduler state advances.
func RunReplicatedPass(
	ctx context.Context, catalog *gateway.Snapshot, directory ReplicatedDirectory,
	controller *Controller, sink Sink,
) (Pass, error) {
	if ctx == nil || catalog == nil || directory == nil || controller == nil || sink == nil {
		return Pass{}, ErrInvalidPressureCut
	}
	record, err := directory.ReadPressureRecord(ctx)
	if err != nil {
		return Pass{}, err
	}
	pass := Pass{CatalogGeneration: record.CatalogGeneration,
		AuthorityRevision: record.AuthorityRevision}
	if record.CatalogGeneration != catalog.Generation() ||
		record.AuthorityRevision == 0 || record.PayloadDigest == ([32]byte{}) ||
		sha256.Sum256(record.Payload) != record.PayloadDigest {
		return pass, ErrInvalidPressureCut
	}
	checkpoint := controller.Checkpoint()
	if record.AuthorityRevision <= checkpoint.AuthorityRevision {
		if record.AuthorityRevision == checkpoint.AuthorityRevision &&
			record.CatalogGeneration == checkpoint.CatalogGeneration {
			pass.AlreadyApplied = true
			return pass, nil
		}
		return pass, ErrInvalidPressureCut
	}
	view, err := OpenView(record.Payload)
	if err != nil || view.CatalogGeneration != record.CatalogGeneration ||
		view.AuthorityRevision != record.AuthorityRevision {
		return pass, errors.Join(err, ErrInvalidPressureCut)
	}
	canonical, err := AppendView(nil, view)
	if err != nil || !bytes.Equal(canonical, record.Payload) {
		return pass, errors.Join(err, ErrInvalidPressureCut)
	}
	pass.Admission, err = controller.Process(ctx, catalog, view, sink)
	return pass, err
}
