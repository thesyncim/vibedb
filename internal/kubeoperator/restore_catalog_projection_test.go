package kubeoperator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

func restoreTestSchemaProjection(t *testing.T, templates []restoreSchemaTemplate, operation clusterrestore.Operation) ([]byte, []byte) {
	t.Helper()
	policy := []byte(`{"generation":1,"principals":[{"node":"01000000000000000000000000000000","capabilities":["topology","restore_activate"]}]}`)
	var config distribution.ClusterConfig
	endpoints := make(map[distribution.EndpointID]string)
	var descriptors []gateway.ReplicatedShardDescriptor
	set := restoreSchemaSet{Format: 1, Policy: policy}
	for i, template := range templates {
		set.Groups = append(set.Groups, restoreSchemaSlot{Ordinal: uint32(i), Schema: template})
		dist, shard := distribution.DistributionName(template.Distribution), distribution.ShardID(template.Shard)
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{Name: dist, Arity: 1, MapperVersion: distribution.NativeMapperVersion})
		config.Placements = append(config.Placements, distribution.TablePlacement{Table: template.BaseTable, Distribution: dist, Columns: []string{"/id"}})
		var leaders []distribution.EndpointID
		var replicas []gateway.ReplicatedReplicaDescriptor
		for r, target := range operation.Targets[i].Replicas {
			id := distribution.EndpointID(fmt.Sprintf("g%d-r%d", i, r))
			leaders = append(leaders, id)
			endpoints[id] = fmt.Sprintf("127.0.0.1:%d", 8000+i*3+r)
			native, control := id+"-native", id+"-control"
			endpoints[native] = fmt.Sprintf("127.0.0.1:%d", 9000+i*3+r)
			endpoints[control] = fmt.Sprintf("127.0.0.1:%d", 10000+i*3+r)
			replicas = append(replicas, gateway.ReplicatedReplicaDescriptor{Member: target.Member, Node: target.Node, StoreID: target.Store, NodeIncarnation: target.NodeIncarnation, Endpoint: id, NativeEndpoint: native, ControlEndpoint: control})
		}
		m, err := distribution.NewManifest(dist, 1, []distribution.Shard{{ID: shard, AllocationGeneration: distribution.ShardAllocationGeneration(template.AllocationGeneration), Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}, Leaders: leaders, Epoch: 1}})
		if err != nil {
			t.Fatal(err)
		}
		config.Manifests = append(config.Manifests, m)
		digest := replication.Digest(operation.Certificate.Groups[i].RelationManifestDigest)
		if digest == (replication.Digest{}) {
			digest[0] = 1
		}
		descriptors = append(descriptors, gateway.ReplicatedShardDescriptor{Distribution: dist, Shard: shard, AllocationGeneration: distribution.ShardAllocationGeneration(template.AllocationGeneration), Group: operation.Targets[i].Group,
			RangeIdentity: replication.Digest{byte(40 + i)}, LineageDigest: replication.Digest{byte(50 + i)}, ForwardingRuleDigest: replication.Digest{byte(60 + i)},
			Command: raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: operation.Certificate.Groups[i].SchemaGeneration, RelationManifestDigest: digest, RoutingVersion: 1, RouteGeneration: 1}, Replicas: replicas})
		if i == 0 {
			descriptors[i].RequestLedgerRanges = []gateway.DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{77}}}
		}
	}
	snapshot, err := gateway.NewSnapshotWithReplicatedMetadata(config, endpoints, 1, nil, nil, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	set.Catalog, err = gateway.AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := vibejson.Marshal(&set)
	if err != nil {
		t.Fatal(err)
	}
	return raw, policy
}

func TestRestoreCatalogProjectionIsOperationBoundAndExcludesSourceAuthority(t *testing.T) {
	template := validRestoreTestTemplate()
	template.Distribution = "catalog"
	template.Shard = "controlplane"
	template.BaseTable = "controlplane"
	template.DDL = []string{"CREATE TABLE controlplane (PRIMARY KEY (id))"}
	target := clusterrestore.TargetGroup{Group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 2, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}}
	for i := range target.Replicas {
		target.Replicas[i] = clusterrestore.ReplicaIdentity{Member: uint64(i + 1), Node: rafttransport.NodeID{byte(10 + i)}, Store: [16]byte{byte(20 + i)}, NodeIncarnation: 1}
	}
	operation := clusterrestore.Operation{Targets: []clusterrestore.TargetGroup{target}, PolicyGeneration: 1, Digest: [32]byte{99}, Certificate: clusterbackup.Certificate{Groups: []clusterbackup.GroupCut{{SchemaGeneration: 1}}}}
	raw, policy := restoreTestSchemaProjection(t, []restoreSchemaTemplate{template}, operation)
	operation.TargetCatalogDigest = sha256.Sum256(raw)
	operation.TargetPolicyDigest = sha256.Sum256(policy)
	rows, err := openRestoreCatalogProjection(raw, operation)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("unexpected authority rows %d", len(rows))
	}
	for _, row := range rows {
		if bytes.Contains(row.Value, []byte("source-operation")) {
			t.Fatal("source authority retained")
		}
	}
	again, err := openRestoreCatalogProjection(raw, operation)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if !bytes.Equal(rows[i].Key, again[i].Key) || !bytes.Equal(rows[i].Value, again[i].Value) {
			t.Fatal("retry changed projection")
		}
	}
	operation.TargetPolicyDigest[0] ^= 1
	if _, err := openRestoreCatalogProjection(raw, operation); err == nil {
		t.Fatal("changed policy accepted")
	}
	operation.TargetPolicyDigest[0] ^= 1
	operation.Targets[0].Replicas[0].Store[0] ^= 1
	if _, err := openRestoreCatalogProjection(raw, operation); err == nil {
		t.Fatal("changed target identity accepted")
	}
}
