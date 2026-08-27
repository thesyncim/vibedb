package splitcontroller

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rangesplit"
)

// CertifiedChildAdoption is minted only after the exact activated SQL/WAL
// pair and the plan's cutover certificate have been checked. Its compact
// digests let a local serving inventory outlive operation-state collection
// without becoming a second topology decision log.
type CertifiedChildAdoption struct {
	operation     OperationID
	child         uint8
	plan, cutover [32]byte
	replica       ChildReplicaTarget
}

func (proof CertifiedChildAdoption) OperationID() OperationID { return proof.operation }
func (proof CertifiedChildAdoption) Child() uint8             { return proof.child }
func (proof CertifiedChildAdoption) PlanDigest() [32]byte     { return proof.plan }
func (proof CertifiedChildAdoption) CutoverDigest() [32]byte  { return proof.cutover }
func (proof CertifiedChildAdoption) ReplicaTarget() ChildReplicaTarget {
	copy := proof.replica
	copy.SQL = proof.replica.SQL.Clone()
	return copy
}

// ChildAdoptionCheckpoint durably records the already-certified group before
// its runtime is exposed to Multi-Raft. Failure must prevent registration.
type ChildAdoptionCheckpoint interface {
	CheckpointChildAdoption(context.Context, CertifiedChildAdoption, PreparedChildRuntime) error
}

type childAdoptionCheckpointBinding struct {
	plan       *Plan
	digest     [32]byte
	child      uint8
	replica    ChildReplicaTarget
	lease      *RuntimeStoreLease
	checkpoint ChildAdoptionCheckpoint
}

func (binding *childAdoptionCheckpointBinding) record(ctx context.Context, prepared PreparedChildRuntime) error {
	if binding == nil {
		return nil
	}
	if binding.plan == nil || binding.digest == ([32]byte{}) || binding.lease == nil || binding.checkpoint == nil {
		return ErrRuntimeStore
	}
	stored, found, err := binding.lease.Load(RuntimeStateCertificate, 0)
	if err != nil || !found {
		return errors.Join(ErrTopologyConflict, err)
	}
	certificate, err := rangesplit.OpenCutoverCertificate(stored.Payload)
	if err != nil || certificate == nil || binding.plan.partitioner.VerifyCutoverCertificate(*certificate) != nil {
		return errors.Join(ErrTopologyConflict, err)
	}
	proof := CertifiedChildAdoption{operation: binding.plan.operation,
		child: binding.child, plan: binding.digest, cutover: certificate.Digest(), replica: binding.replica}
	return binding.checkpoint.CheckpointChildAdoption(ctx, proof, prepared)
}
