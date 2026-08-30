package main

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type schemaDDLResumeResult struct {
	request schemainstall.BuildRequest
	sql     string
	err     error
}

type schemaDDLResumeFake map[rafttransport.NodeID]schemaDDLResumeResult

func (f schemaDDLResumeFake) ResumeBuild(_ context.Context, node rafttransport.NodeID,
	_ [32]byte, _ raftmember.GroupKey,
) (schemainstall.BuildRequest, string, sqldriver.ReplicatedSchemaDDLTarget, bool, error) {
	result, found := f[node]
	if !found {
		return schemainstall.BuildRequest{}, "", sqldriver.ReplicatedSchemaDDLTarget{}, false, schemainstall.ErrMissing
	}
	return result.request, result.sql, sqldriver.ReplicatedSchemaDDLTarget{}, true, result.err
}

func testSchemaDDLRecoveryDescriptor() ([32]byte, gateway.ReplicatedShardDescriptor) {
	operation := [32]byte{1}
	group := raftmember.GroupKey{ClusterID: [16]byte{2}, ClusterIncarnation: [16]byte{3},
		TopologyRecoveryEpoch: 4, ShardIncarnation: [16]byte{5}, GroupID: [16]byte{6}}
	descriptor := gateway.ReplicatedShardDescriptor{Distribution: "table-docs", Shard: "all",
		Group: group, AllocationGeneration: distribution.ShardAllocationGeneration(7),
		Command: raftservice.CommandFence{SchemaGeneration: 8, RelationManifestDigest: [32]byte{9}},
		Replicas: []gateway.ReplicatedReplicaDescriptor{
			{Member: 1, Node: rafttransport.NodeID{11}},
			{Member: 2, Node: rafttransport.NodeID{12}},
			{Member: 3, Node: rafttransport.NodeID{13}},
		}}
	return operation, descriptor
}

func TestGatewaySchemaDDLRecoverySQLUsesAuthenticatedRetainedReceipt(t *testing.T) {
	operation, descriptor := testSchemaDDLRecoveryDescriptor()
	request := schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
		AllocationGeneration:       descriptor.AllocationGeneration,
		FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
		FromRelationManifestDigest: descriptor.Command.RelationManifestDigest}
	runtime := &gatewaySchemaDDLRuntime{resumer: schemaDDLResumeFake{
		descriptor.Replicas[0].Node: {request: request, sql: "DROP INDEX docs_by_city"},
		descriptor.Replicas[1].Node: {request: request, sql: "DROP INDEX docs_by_city"},
		descriptor.Replicas[2].Node: {request: request, sql: "DROP INDEX docs_by_city"},
	}}
	sql, err := runtime.recoverySQL(t.Context(), []gateway.ReplicatedShardDescriptor{descriptor}, operation)
	if err != nil || sql != "DROP INDEX docs_by_city" {
		t.Fatalf("recovery SQL = %q, %v", sql, err)
	}
	foreign := runtime.resumer.(schemaDDLResumeFake)
	changed := foreign[descriptor.Replicas[2].Node]
	changed.sql = "TRUNCATE docs"
	foreign[descriptor.Replicas[2].Node] = changed
	if _, err := runtime.recoverySQL(t.Context(), []gateway.ReplicatedShardDescriptor{descriptor}, operation); !errors.Is(err, gateway.ErrSchemaRolloutConflict) {
		t.Fatalf("inconsistent retained SQL accepted: %v", err)
	}
}

func TestGatewaySchemaDDLRecoverySQLFailsClosed(t *testing.T) {
	operation, descriptor := testSchemaDDLRecoveryDescriptor()
	runtime := &gatewaySchemaDDLRuntime{resumer: schemaDDLResumeFake{}}
	if _, err := runtime.recoverySQL(t.Context(), []gateway.ReplicatedShardDescriptor{descriptor}, operation); !errors.Is(err, schemainstall.ErrMissing) || !errors.Is(err, gateway.ErrSchemaRolloutConflict) {
		t.Fatalf("missing recovery receipt accepted: %v", err)
	}
	request := schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
		AllocationGeneration:       descriptor.AllocationGeneration,
		FromSchemaGeneration:       descriptor.Command.SchemaGeneration + 1,
		FromRelationManifestDigest: descriptor.Command.RelationManifestDigest}
	runtime.resumer = schemaDDLResumeFake{descriptor.Replicas[0].Node: {request: request, sql: "DROP INDEX docs_by_city"}}
	if _, err := runtime.recoverySQL(t.Context(), []gateway.ReplicatedShardDescriptor{descriptor}, operation); !errors.Is(err, gateway.ErrSchemaRolloutConflict) {
		t.Fatalf("foreign source receipt accepted: %v", err)
	}
}
