package raftmember

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
)

var ErrNodePersistenceBinding = errors.New("raftmember: node persistence group binding mismatch")

// NodeRuntimePersistence is the worker-free append lane for one Runtime group.
// All instances for a node share one NodeSubmissionSequencer; this adapter owns
// only one caller-reserved completion cell and cannot bypass global ticket
// order. Apply/outbound processing must wait for Wait/Poll completion.
type NodeRuntimePersistence struct {
	sequencer   *raftstore.NodeSubmissionSequencer
	cell        raftstore.Submission
	group       uint64
	incarnation uint64
}

func BindNodeRuntimePersistence(store *raftstore.NodeStore, sequencer *raftstore.NodeSubmissionSequencer, identity RuntimeIdentity) (*NodeRuntimePersistence, error) {
	if store == nil || !sequencer.Owns(store) {
		return nil, ErrNodePersistenceBinding
	}
	node := store.NodeIdentity()
	if node.ClusterID != identity.Group.ClusterID || node.ClusterIncarnation != identity.Group.ClusterIncarnation {
		return nil, ErrNodePersistenceBinding
	}
	view, ok := store.GroupByID(identity.Group.GroupID)
	if !ok {
		return nil, ErrNodePersistenceBinding
	}
	descriptor, err := view.Descriptor()
	if err != nil || descriptor.TopologyRecoveryEpoch != identity.Group.TopologyRecoveryEpoch || descriptor.ShardIncarnation != identity.Group.ShardIncarnation || descriptor.AllocationGeneration != identity.AllocationGeneration || descriptor.MemberID != identity.MemberID || descriptor.StoreID != identity.StoreID || descriptor.Distribution != identity.Distribution || descriptor.Shard != identity.Shard || !bytes.Equal(descriptor.GroupID[:], identity.Group.GroupID[:]) {
		return nil, ErrNodePersistenceBinding
	}
	incarnation, err := view.NodeIncarnation()
	if err != nil || incarnation != identity.NodeIncarnation {
		return nil, ErrNodePersistenceBinding
	}
	adapter := &NodeRuntimePersistence{sequencer: sequencer, group: descriptor.LogKey, incarnation: incarnation}
	if err = adapter.cell.Initialize(); err != nil {
		return nil, err
	}
	return adapter, nil
}

func (p *NodeRuntimePersistence) Submit(batch raftmodel.PersistBatch) (uint64, error) {
	if p == nil || p.sequencer == nil {
		return 0, ErrRuntimeClosed
	}
	if batch.NodeIncarnation != p.incarnation {
		return 0, ErrNodePersistenceBinding
	}
	if err := p.cell.Prepare(raftstore.NodeReady{GroupID: p.group, Batch: batch}); err != nil {
		return 0, err
	}
	return p.sequencer.TrySubmit(&p.cell)
}

func (p *NodeRuntimePersistence) Poll() (uint64, bool, error) {
	if p == nil {
		return 0, false, ErrRuntimeClosed
	}
	return p.cell.Poll()
}

func (p *NodeRuntimePersistence) Wait() (uint64, error) {
	if p == nil {
		return 0, ErrRuntimeClosed
	}
	return p.cell.Wait()
}
