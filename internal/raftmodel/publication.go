package raftmodel

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func (n *Node) acceptNormalPublication(meta ApplyMeta, noop bool, returned Publication) error {
	previous := n.published
	if err := n.validateObservedPublication(meta.Index, returned); err != nil {
		return err
	}
	if !proto.Equal(returned.ConfState, previous.ConfState) {
		return errors.New("normal entry changed ConfState")
	}
	if returned.ReplicaSetVersion != previous.ReplicaSetVersion {
		return errors.New("normal entry changed ReplicaSetVersion")
	}
	if noop && returned.LogicalDigest != previous.LogicalDigest {
		return errors.New("no-op entry changed logical digest")
	}
	n.published = clonePublication(returned)
	return nil
}

func (n *Node) acceptConfigurationPublication(meta ApplyMeta, state *pb.ConfState, returned Publication) error {
	previous := n.published
	if err := n.validateObservedPublication(meta.Index, returned); err != nil {
		return err
	}
	if !proto.Equal(returned.ConfState, state) {
		return errors.New("configuration publication differs from core ConfState")
	}
	if returned.ConfState.GetAutoLeave() {
		return &UnsupportedError{Feature: "automatic joint consensus publication"}
	}
	if returned.ReplicaSetVersion != meta.Index {
		return fmt.Errorf("ReplicaSetVersion %d differs from configuration index %d", returned.ReplicaSetVersion, meta.Index)
	}
	if returned.LogicalDigest != previous.LogicalDigest {
		return errors.New("configuration entry changed logical digest")
	}
	n.published = clonePublication(returned)
	return nil
}

func (n *Node) acceptSnapshotPublication(index uint64, state *pb.ConfState, returned Publication) error {
	previous := n.published
	if err := n.validateObservedPublication(index, returned); err != nil {
		return err
	}
	if !proto.Equal(returned.ConfState, state) {
		return errors.New("snapshot publication differs from snapshot ConfState")
	}
	if returned.ReplicaSetVersion < previous.ReplicaSetVersion {
		return errors.New("snapshot regressed ReplicaSetVersion")
	}
	if returned.ReplicaSetVersion == 0 && confStateHasMembers(returned.ConfState) {
		return errors.New("snapshot published a nonempty ConfState with zero ReplicaSetVersion")
	}
	if returned.ReplicaSetVersion > index {
		return errors.New("snapshot ReplicaSetVersion exceeds snapshot index")
	}
	n.published = clonePublication(returned)
	return nil
}

func (n *Node) validateObservedPublication(index uint64, returned Publication) error {
	if returned.Applied != index {
		return fmt.Errorf("returned publication index %d, want %d", returned.Applied, index)
	}
	if applied := n.machine.Applied(); applied != index {
		return fmt.Errorf("state machine applied index %d, want %d", applied, index)
	}
	observed := n.machine.Published()
	if !equalPublication(returned, observed) {
		return errors.New("returned publication differs from reader-visible publication")
	}
	return nil
}

func clonePublication(publication Publication) Publication {
	publication.ConfState = cloneConfState(publication.ConfState)
	return publication
}

func cloneConfState(state *pb.ConfState) *pb.ConfState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*pb.ConfState)
}

func confStateHasMembers(state *pb.ConfState) bool {
	return state != nil && (len(state.GetVoters()) != 0 || len(state.GetVotersOutgoing()) != 0 ||
		len(state.GetLearners()) != 0 || len(state.GetLearnersNext()) != 0)
}

func equalPublication(left, right Publication) bool {
	return left.Applied == right.Applied &&
		left.LogicalDigest == right.LogicalDigest &&
		left.ReplicaSetVersion == right.ReplicaSetVersion &&
		proto.Equal(left.ConfState, right.ConfState)
}
