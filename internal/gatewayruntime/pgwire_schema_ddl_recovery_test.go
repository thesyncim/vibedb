package gatewayruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
	target  sqldriver.ReplicatedSchemaDDLTarget
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
	return result.request, result.sql, result.target, true, result.err
}

type schemaDDLBuildFake struct {
	target sqldriver.ReplicatedSchemaDDLTarget
	err    error
	calls  int
}

func (f *schemaDDLBuildFake) Build(_ context.Context, _ rafttransport.NodeID,
	_ schemainstall.BuildRequest, _ string,
) (sqldriver.ReplicatedSchemaDDLTarget, error) {
	f.calls++
	return f.target, f.err
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

func TestGatewaySchemaDDLBuildResumesRetainedReceiptAfterUnknownOutcome(t *testing.T) {
	operation, descriptor := testSchemaDDLRecoveryDescriptor()
	const sql = "CREATE INDEX docs_by_city ON documents (city)"
	request := schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
		AllocationGeneration:       descriptor.AllocationGeneration,
		FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
		FromRelationManifestDigest: descriptor.Command.RelationManifestDigest,
		SourceApplied:              19, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	retained := request
	retained.SourceApplied = 17
	want := sqldriver.ReplicatedSchemaDDLTarget{Catalog: []byte("retained")}
	builder := &schemaDDLBuildFake{err: errors.Join(schemainstall.ErrOutcomeUnknown, errors.New("lost response"))}
	runtime := &gatewaySchemaDDLRuntime{builder: builder, resumer: schemaDDLResumeFake{
		descriptor.Replicas[0].Node: {request: retained, sql: sql, target: want},
	}}
	gotRequest, got, err := runtime.buildOrResume(t.Context(), descriptor.Replicas[0].Node, request, sql)
	if err != nil || gotRequest != retained || !reflect.DeepEqual(got, want) || builder.calls != 1 {
		t.Fatalf("resume: request=%+v target=%+v err=%v calls=%d", gotRequest, got, err, builder.calls)
	}
}

func TestGatewaySchemaDDLBuildResumeFailsClosed(t *testing.T) {
	operation, descriptor := testSchemaDDLRecoveryDescriptor()
	const sql = "CREATE INDEX docs_by_city ON documents (city)"
	request := schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
		AllocationGeneration:       descriptor.AllocationGeneration,
		FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
		FromRelationManifestDigest: descriptor.Command.RelationManifestDigest,
		SourceApplied:              19, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	for _, test := range []struct {
		name     string
		retained schemainstall.BuildRequest
		text     string
	}{
		{name: "future-cut", retained: func() schemainstall.BuildRequest { changed := request; changed.SourceApplied++; return changed }(), text: sql},
		{name: "different-sql", retained: request, text: "TRUNCATE documents"},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := &schemaDDLBuildFake{err: schemainstall.ErrConflict}
			runtime := &gatewaySchemaDDLRuntime{builder: builder, resumer: schemaDDLResumeFake{
				descriptor.Replicas[0].Node: {request: test.retained, sql: test.text},
			}}
			if _, _, err := runtime.buildOrResume(t.Context(), descriptor.Replicas[0].Node, request, sql); !errors.Is(err, gateway.ErrSchemaRolloutConflict) {
				t.Fatalf("foreign retained receipt accepted: %v", err)
			}
		})
	}
}

func TestGatewaySchemaDDLSelectsAuthenticatedRetainedOperation(t *testing.T) {
	operation, descriptor := testSchemaDDLRecoveryDescriptor()
	const sql = "CREATE INDEX docs_by_city ON documents (city)"
	request := schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
		AllocationGeneration:       descriptor.AllocationGeneration,
		FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
		FromRelationManifestDigest: descriptor.Command.RelationManifestDigest,
		SourceApplied:              17, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	journal := t.TempDir()
	if err := os.Mkdir(filepath.Join(journal, hex.EncodeToString(operation[:])), 0o700); err != nil {
		t.Fatal(err)
	}
	resume := make(schemaDDLResumeFake, len(descriptor.Replicas))
	for _, replica := range descriptor.Replicas {
		resume[replica.Node] = schemaDDLResumeResult{request: request, sql: sql}
	}
	runtime := &gatewaySchemaDDLRuntime{journal: journal, resumer: resume}
	current := [32]byte{99}
	selected, err := runtime.retainedOperation(t.Context(), []gateway.ReplicatedShardDescriptor{descriptor}, sql, current)
	if err != nil || selected != operation {
		t.Fatalf("selected=%x want=%x err=%v", selected, operation, err)
	}
	selected, err = runtime.retainedOperation(t.Context(), []gateway.ReplicatedShardDescriptor{descriptor}, "TRUNCATE documents", current)
	if err != nil || selected != current {
		t.Fatalf("foreign SQL selected retained operation: %x %v", selected, err)
	}
}
