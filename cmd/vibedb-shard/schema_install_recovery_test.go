package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type schemaRecoveryWAL struct {
	entries []*pb.Entry
	err     error
}

func TestRF3SchemaReplayNeutralSuffix(t *testing.T) {
	operation := sha256.Sum256([]byte("schema-operation"))
	group := raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}}
	identity, binding := schemainstall.SchemaDDLRouteGateIdentity(operation, group)
	client := replication.ID128{6}
	base := replication.Command{AuthorityClass: replication.CommandAuthorityTopology,
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, Distribution: "data", Shard: "all",
		AllocationGeneration: 1, ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
		SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1, Tenant: []byte("public"),
		ClientID: client, Fingerprint: sha256.Sum256([]byte("fingerprint")), RetryHome: replication.RetryHome{7}}
	entry := func(index uint64, command replication.Command) *pb.Entry {
		data, err := replication.AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		entryType, term := pb.EntryNormal, uint64(1)
		return &pb.Entry{Index: &index, Term: &term, Type: &entryType, Data: data}
	}
	open := base
	open.Kind, open.ClientSequence, open.NextDeadlineUnixNano = replication.CommandSessionOpen, 1, 10
	gate := base
	gate.Kind, gate.ClientEpoch, gate.ClientSequence = replication.CommandRouteGate, 8, 2
	gate.RouteGate, _ = routegate.AppendCommand(nil, routegate.Command{Operation: routegate.OperationBeginExclusive,
		Epoch: 9, Identity: identity, Binding: binding})
	retire := base
	retire.Kind, retire.ClientEpoch, retire.ClientSequence = replication.CommandSessionRetire, 8, 3
	release := retire
	release.Kind = replication.CommandSessionRelease
	wal := schemaRecoveryWAL{entries: []*pb.Entry{entry(8, open), entry(9, gate), entry(10, retire), entry(11, release)}}
	if err := rf3SchemaReplayNeutralSuffix(wal, 7, 11, operation, group); err != nil {
		t.Fatal(err)
	}
	if err := rf3SchemaReplayNeutralSuffix(schemaRecoveryWAL{entries: wal.entries[:3]}, 7, 10, operation, group); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("partial gate lifecycle accepted: %v", err)
	}
	foreign := operation
	foreign[0] ^= 1
	if err := rf3SchemaReplayNeutralSuffix(wal, 7, 11, foreign, group); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("foreign gate identity accepted: %v", err)
	}
}

func (w schemaRecoveryWAL) Entries(lo, hi, _ uint64) ([]*pb.Entry, error) {
	if w.err != nil {
		return nil, w.err
	}
	var result []*pb.Entry
	for _, entry := range w.entries {
		if entry.GetIndex() >= lo && entry.GetIndex() < hi {
			result = append(result, entry)
		}
	}
	return result, nil
}

func TestRF3SchemaEmptyNormalSuffix(t *testing.T) {
	empty := func(index uint64) *pb.Entry {
		entryType, term := pb.EntryNormal, uint64(1)
		return &pb.Entry{Index: &index, Term: &term, Type: &entryType}
	}
	wal := schemaRecoveryWAL{entries: []*pb.Entry{empty(8), empty(9), empty(10)}}
	if err := rf3SchemaEmptyNormalSuffix(wal, 7, 10); err != nil {
		t.Fatal(err)
	}
	if err := rf3SchemaEmptyNormalSuffix(wal, 10, 10); err != nil {
		t.Fatal(err)
	}
	command := empty(9)
	command.Data = []byte{1}
	for name, candidate := range map[string]schemaRecoveryWAL{
		"command": {entries: []*pb.Entry{empty(8), command, empty(10)}},
		"gap":     {entries: []*pb.Entry{empty(8), empty(10)}},
		"read":    {err: errors.New("wal unavailable")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := rf3SchemaEmptyNormalSuffix(candidate, 7, 10); !errors.Is(err, schemainstall.ErrConflict) {
				t.Fatalf("unsafe suffix accepted: %v", err)
			}
		})
	}
}

func TestRF3SchemaCommittedTransitionAlias(t *testing.T) {
	index, term, entryType := uint64(11), uint64(3), pb.EntryNormal
	command := []byte("committed schema command")
	entry := &pb.Entry{Index: &index, Term: &term, Type: &entryType, Data: command}
	alias, err := rf3SchemaCommittedTransitionAlias(schemaRecoveryWAL{entries: []*pb.Entry{entry}}, index)
	if err != nil || !bytes.Equal(alias, command) {
		t.Fatalf("retained committed command = %q, %v", alias, err)
	}
	alias[0] ^= 1
	if bytes.Equal(alias, entry.GetData()) {
		t.Fatal("returned committed command aliases WAL storage")
	}
	if alias, err = rf3SchemaCommittedTransitionAlias(schemaRecoveryWAL{err: raft.ErrCompacted}, index); err != nil || alias != nil {
		t.Fatalf("compacted committed command = %q, %v", alias, err)
	}
	for name, wal := range map[string]schemaRecoveryWAL{
		"unavailable": {err: raft.ErrUnavailable},
		"missing":     {},
		"empty":       {entries: []*pb.Entry{{Index: &index, Term: &term, Type: &entryType}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := rf3SchemaCommittedTransitionAlias(wal, index); !errors.Is(err, schemainstall.ErrConflict) {
				t.Fatalf("unsafe committed alias accepted: %v", err)
			}
		})
	}
}

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

func TestRF3SchemaActivationRecognizesExactRetiredPredecessor(t *testing.T) {
	prior, _, transition := testRF3SchemaRecoveryCommand(t)
	next := prior
	next.Operation = [32]byte{41}
	next.FromSchemaGeneration = prior.ToSchemaGeneration
	next.FromRelationManifestDigest = prior.ToRelationManifestDigest
	next.ToSchemaGeneration++
	next.ToRelationManifestDigest = [32]byte{42}
	if !rf3SchemaTransitionIsPredecessor(next, transition) {
		t.Fatal("exact prior target was not recognized as the next rollout source")
	}
	for _, mutate := range []func(*schemainstall.Request){
		func(r *schemainstall.Request) { r.Group.GroupID[0]++ },
		func(r *schemainstall.Request) { r.AllocationGeneration++ },
		func(r *schemainstall.Request) { r.FromSchemaGeneration++ },
		func(r *schemainstall.Request) { r.FromRelationManifestDigest[0]++ },
	} {
		candidate := next
		mutate(&candidate)
		if rf3SchemaTransitionIsPredecessor(candidate, transition) {
			t.Fatal("foreign predecessor was accepted")
		}
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
