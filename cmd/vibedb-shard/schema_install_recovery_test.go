package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
)

func testRF3SchemaRecoveryCommand(t *testing.T) (schemainstall.Request, schemainstall.Authorization, replicatedstate.SchemaTransitionView) {
	t.Helper()
	from := replicatedstate.Binding{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, Distribution: "data", Shard: "source", AllocationGeneration: 4,
		ShardIncarnation: [16]byte{5}, GroupID: [16]byte{6}, ActivePolicyGeneration: 7,
		ProtectionEpoch: 8, OwnershipEpoch: 9, SchemaGeneration: 10, RoutingVersion: 11, RouteGeneration: 12,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	request := schemainstall.Request{Operation: [32]byte{13}, Group: raftmember.GroupKey{
		ClusterID: from.ClusterID, ClusterIncarnation: from.ClusterIncarnation,
		TopologyRecoveryEpoch: from.TopologyRecoveryEpoch, ShardIncarnation: from.ShardIncarnation, GroupID: from.GroupID},
		AllocationGeneration: 4, FromSchemaGeneration: 10, FromRelationManifestDigest: [32]byte{14},
		ToSchemaGeneration: 11, ToRelationManifestDigest: [32]byte{15}, ApplyContractDigest: [32]byte{16}}
	authorization := schemainstall.Authorization{Operation: request.Operation, TargetCatalogGeneration: 17,
		TargetCatalogDigest: [32]byte{18}, PreparedGroupCount: 1, PreparedGroupRoot: [32]byte{19}, ContractDigest: schemainstall.ContractDigest()}
	command, err := replicatedstate.AppendSchemaTransition(nil, replicatedstate.SchemaTransition{
		From: from, ToSchemaGeneration: request.ToSchemaGeneration, ExpectedReplicaSetVersion: 2,
		MembershipSequence: 21, MembershipSource: [32]byte{22}, MembershipTarget: [32]byte{23},
		FromManifest: request.FromRelationManifestDigest, FromApplyContract: [32]byte{24},
		ToManifest: request.ToRelationManifestDigest, ToApplyContract: request.ApplyContractDigest,
		FromPlacementDigest: [32]byte{25}, ToPlacementDigest: [32]byte{26}, RequestDigest: request.Operation,
		AuthorizationDigest: schemainstall.AuthorizationDigest(authorization), CatalogCASDigest: [32]byte{27},
	})
	if err != nil {
		t.Fatal(err)
	}
	transition, err := replicatedstate.OpenSchemaTransition(command)
	if err != nil {
		t.Fatal(err)
	}
	return request, authorization, transition
}

func TestRF3SchemaActivationRecoveryKeepsOriginalCommittedCommand(t *testing.T) {
	request, authorization, transition := testRF3SchemaRecoveryCommand(t)
	builds := 0
	// The source machine has already committed N+1. Its append API must still
	// reject the N staging proof; recovery must never call that API again.
	buildAtAdvancedSource := func() ([]byte, error) {
		builds++
		return nil, errors.New("staging source applied N differs from committed N+1")
	}
	command, err := rf3SchemaActivationCommand(request, authorization, transition, true, buildAtAdvancedSource)
	if err != nil || builds != 0 || !bytes.Equal(command, transition.Bytes()) {
		t.Fatalf("recovery rebuilt or changed committed command: builds=%d err=%v", builds, err)
	}
	owner := &schemaRecoveryOwner{command: command, committed: true}
	if err := settleRF3SchemaCommit(context.Background(), owner, request.Group, command); err != nil ||
		owner.observations != 1 || owner.proposals != 0 || owner.probes != 0 {
		t.Fatalf("already committed command was reproposed: %+v err=%v", owner, err)
	}
	// Return ownership is detached; the retained transition witness is intact.
	command[0] ^= 1
	if bytes.Equal(command, transition.Bytes()) {
		t.Fatal("recovered command aliases persisted witness")
	}
}

func TestRF3SchemaActivationRecoveryRejectsForeignAuthorityBeforeBuild(t *testing.T) {
	request, authorization, transition := testRF3SchemaRecoveryCommand(t)
	tests := []struct {
		name   string
		change func(*schemainstall.Request, *schemainstall.Authorization)
	}{
		{"group", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.Group.GroupID[0]++ }},
		{"allocation", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.AllocationGeneration++ }},
		{"source-schema", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.FromSchemaGeneration++ }},
		{"source-manifest", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.FromRelationManifestDigest[0]++ }},
		{"target-schema", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.ToSchemaGeneration++ }},
		{"target-manifest", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.ToRelationManifestDigest[0]++ }},
		{"apply-contract", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.ApplyContractDigest[0]++ }},
		{"operation", func(r *schemainstall.Request, _ *schemainstall.Authorization) { r.Operation[0]++ }},
		{"authorization", func(_ *schemainstall.Request, a *schemainstall.Authorization) { a.TargetCatalogDigest[0]++ }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, a := request, authorization
			tc.change(&r, &a)
			if _, err := rf3SchemaActivationCommand(r, a, transition, true, func() ([]byte, error) {
				t.Fatal("mismatched retained authority attempted to rebuild")
				return nil, nil
			}); !errors.Is(err, schemainstall.ErrConflict) {
				t.Fatalf("foreign retained authority accepted: %v", err)
			}
		})
	}
}

func TestRF3SchemaActivationBuildsOnlyAbsentValidatedCommand(t *testing.T) {
	request, authorization, transition := testRF3SchemaRecoveryCommand(t)
	builds := 0
	build := func() ([]byte, error) {
		builds++
		return bytes.Clone(transition.Bytes()), nil
	}
	command, err := rf3SchemaActivationCommand(request, authorization, replicatedstate.SchemaTransitionView{}, false, build)
	if err != nil || builds != 1 || !bytes.Equal(command, transition.Bytes()) {
		t.Fatalf("absent command did not build exactly once: builds=%d err=%v", builds, err)
	}
	request.Group.GroupID[0]++
	if _, err := rf3SchemaActivationCommand(request, authorization, replicatedstate.SchemaTransitionView{}, false, build); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("fresh foreign-group command permitted for persistence: %v", err)
	}
}

type schemaRecoveryOwner struct {
	rf3SchemaOwner
	command                         []byte
	committed                       bool
	observations, proposals, probes int
	observeErr, proposeErr          error
}

func (o *schemaRecoveryOwner) ObserveSchemaTransition(_ context.Context, _ raftmember.GroupKey, command []byte) (bool, error) {
	o.observations++
	if !bytes.Equal(command, o.command) {
		return false, schemainstall.ErrConflict
	}
	return o.committed, o.observeErr
}

func (o *schemaRecoveryOwner) Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
	o.probes++
	return raftservice.ServingState{}, nil
}

func (o *schemaRecoveryOwner) ProposeSchemaTransition(_ context.Context, _ raftservice.ServingFence, command []byte) error {
	o.proposals++
	if !bytes.Equal(command, o.command) {
		return schemainstall.ErrConflict
	}
	o.committed = true
	return o.proposeErr
}

func TestRF3SchemaActivationRecoverySettlesOriginalUncertainProposal(t *testing.T) {
	request, authorization, transition := testRF3SchemaRecoveryCommand(t)
	command, err := rf3SchemaActivationCommand(request, authorization, transition, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner := &schemaRecoveryOwner{command: command, proposeErr: raftservice.ErrOutcomeUnknown}
	if err := settleRF3SchemaCommit(context.Background(), owner, request.Group, command); err != nil ||
		owner.observations != 2 || owner.proposals != 1 || owner.probes != 1 {
		t.Fatalf("uncertain exact proposal not settled: %+v err=%v", owner, err)
	}
	owner = &schemaRecoveryOwner{command: command, observeErr: schemainstall.ErrConflict}
	if err := settleRF3SchemaCommit(context.Background(), owner, request.Group, command); !errors.Is(err, schemainstall.ErrConflict) || owner.proposals != 0 || owner.probes != 0 {
		t.Fatalf("failed observation caused reproposal: %+v err=%v", owner, err)
	}
}
