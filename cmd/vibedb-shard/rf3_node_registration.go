package main

import (
	"bytes"
	"errors"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ensurePreparedGroup is used at startup and hot reload. A crash after the
// registration barrier is recovered by group identity, never by replaying a
// bootstrap over an existing group's newer log or resetting its incarnation.
func (owner *rf3NodeOwner) ensurePreparedGroup(manifest rf3Manifest, base sqldriver.ReplicatedShardStoreIdentity, applyID sqldriver.ReplicatedApplyIdentity, opening sqldriver.ReplicatedOpenOptions) (*raftstore.GroupView, error) {
	if owner == nil {
		return nil, raftmember.ErrWALUnavailable
	}
	if _, found := owner.store.GroupByID(base.Binding.GroupID); found {
		return owner.group(base.Binding)
	}
	binding := base.Binding
	node := owner.store.NodeIdentity()
	if binding.ClusterID != node.ClusterID || binding.ClusterIncarnation != node.ClusterIncarnation || !rf3RouteMatchesBinding(manifest.Route, binding) {
		return nil, raftmember.ErrBindingMismatch
	}
	roster := manifest.memberRoster()
	voters := make([]uint64, len(roster))
	local := false
	for i, member := range roster {
		voters[i] = member.MemberID
		if member.MemberID == binding.MemberID && [16]byte(member.NodeID) == node.NodeID {
			local = true
		}
	}
	if !local || len(roster) != rf3ManifestMembers || manifest.EnrolledTarget != nil {
		return nil, raftmember.ErrBindingMismatch
	}
	raw, err := readRF3BoundedFile(filepath.Join(manifest.Route.MemberRoot, "node-bootstrap.pb"), replicatedstate.MaxStaticBootstrapEnvelopeBytes)
	if err != nil {
		return nil, err
	}
	snapshot := new(pb.Snapshot)
	if err := proto.Unmarshal(raw, snapshot); err != nil {
		return nil, err
	}
	index, term := uint64(1), uint64(1)
	expected := &pb.Snapshot{Data: []byte("vibedb-rf3-bootstrap"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: voters}}}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil || !bytes.Equal(raw, canonical) || !proto.Equal(snapshot, expected) {
		return nil, errors.Join(errPrepareRF3, err)
	}
	// Keep the SQL writer claim through registration. Partial preparation,
	// foreign SQL identities and non-bootstrap publications cannot create a
	// durability identity, and an accepted registration never grants SQL access.
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(manifest.SQL.Path, base, applyID, opening)
	if err != nil {
		return nil, err
	}
	claim, actual, err := database.OpenReplicatedApply(base, snapshot, replicatedApplyOptions(applyID))
	if err != nil {
		return nil, errors.Join(err, database.Close())
	}
	if actual != applyID || claim.Applied() != index || !proto.Equal(claim.Published().ConfState, expected.GetMetadata().GetConfState()) {
		return nil, errors.Join(errPrepareRF3, claim.Close(), database.Close())
	}
	descriptor := raftstore.GroupDescriptor{TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, AllocationGeneration: binding.AllocationGeneration, MemberID: binding.MemberID, GroupID: binding.GroupID, ShardIncarnation: binding.ShardIncarnation, StoreID: binding.StoreID, Distribution: binding.Distribution, Shard: binding.Shard}
	owner.controlMu.Lock()
	var submission raftstore.Submission
	err = submission.Initialize()
	if err == nil {
		err = submission.PrepareRegisterGroupWithSnapshot(descriptor, snapshot)
	}
	if err == nil {
		_, err = owner.sequencer.TrySubmit(&submission)
	}
	if err == nil {
		_, err = submission.Wait()
	}
	owner.controlMu.Unlock()
	if err = errors.Join(err, claim.Close(), database.Close()); err != nil {
		return nil, err
	}
	return owner.group(binding)
}
