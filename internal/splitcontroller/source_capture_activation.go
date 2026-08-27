package splitcontroller

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

var ErrSourceCaptureActivation = errors.New("splitcontroller: invalid source capture activation")

// AppendSourceCaptureActivation builds the canonical nested Raft command from
// an observed serving cut. The caller wraps it in a topology-authority
// replication.Command using the same route/session identity.
func (p *Plan) AppendSourceCaptureActivation(dst []byte, state replicatedstate.State) ([]byte, error) {
	if p == nil || p.partitioner == nil || state.Applied == 0 || state.LastTerm == 0 || state.Binding.Distribution != string(p.source.Distribution) || state.Binding.Shard != string(p.source.Shard) || state.Binding.AllocationGeneration != uint64(p.source.AllocationGeneration) || state.Binding.OwnershipEpoch != uint64(p.source.OwnershipEpoch) || state.Binding.SchemaGeneration == 0 || state.Binding.OwnedRange != p.source.Range {
		return dst, ErrSourceCaptureActivation
	}
	spec, err := rangesplit.AppendPortablePartitioner(nil, p.partitioner)
	if err != nil {
		return dst, err
	}
	planHash := sha256.New()
	_, _ = planHash.Write([]byte("vibedb/splitcontroller/activation-plan\x00"))
	_, _ = planHash.Write(p.operation[:])
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], p.current)
	_, _ = planHash.Write(fixed[:])
	_, _ = planHash.Write(p.relationDigest[:])
	var planDigest [32]byte
	_ = planHash.Sum(planDigest[:0])
	lineageHash := sha256.New()
	_, _ = lineageHash.Write([]byte("vibedb/splitcontroller/activation-lineage\x00"))
	_, _ = lineageHash.Write(p.operation[:])
	_, _ = lineageHash.Write(state.Binding.ShardIncarnation[:])
	_, _ = lineageHash.Write(state.Binding.GroupID[:])
	binary.LittleEndian.PutUint64(fixed[:], state.Binding.AllocationGeneration)
	_, _ = lineageHash.Write(fixed[:])
	_, _ = lineageHash.Write(state.Binding.OwnedRange.Start[:])
	_, _ = lineageHash.Write(state.Binding.OwnedRange.End.Point[:])
	var lineage [32]byte
	_ = lineageHash.Sum(lineage[:0])
	return splitcapture.AppendCommand(dst, splitcapture.Command{Operation: [32]byte(p.operation), PlanDigest: planDigest, PartitionerDigest: p.partitioner.Digest(), RelationManifestDigest: p.relationDigest, LineageDigest: lineage, BindingDigest: replicatedstate.SplitCaptureBindingDigest(state.Binding), PriorEntryDigest: state.LastEntryDigest, PriorDataChainDigest: state.DataChainDigest, PriorApplied: state.Applied, PriorTerm: state.LastTerm, SourceGeneration: state.Binding.RouteGeneration, SchemaGeneration: state.Binding.SchemaGeneration, Spec: spec})
}
