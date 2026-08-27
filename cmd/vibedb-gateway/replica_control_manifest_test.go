package main

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rebalance"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

func gatewaySplitTemplateFixture() persistedGatewaySplitTemplate {
	return persistedGatewaySplitTemplate{
		MaxSessions: 32, RetryWindow: 8,
		TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
		ShardKey:  "/id", MaxBatchDocuments: 64, MaxBatchBytes: 16<<20 + 64*256,
		TupleVersion: uint16(distribution.CurrentTupleVersion), MapperVersion: uint16(distribution.NativeMapperVersion),
	}
}

func gatewayReplicaManifestFixture(t testing.TB) ([]byte, persistedGatewayReplicaControlManifest) {
	t.Helper()
	manifest := persistedGatewayReplicaControlManifest{Generation: 7,
		LocalGateway: persistedGatewayControlEndpoint{Node: "01000000000000000000000000000000",
			Incarnation: 11, ControlAddress: "127.0.0.1:7101"},
		TLS: persistedGatewayReplicaTLS{Certificate: "/tls/cert", Key: "/tls/key",
			Roots: "/tls/roots", IdentityOID: "1.2.3.4", AuthorizationPolicy: "/tls/policy"},
		Bounds: persistedGatewayReplicaBounds{MaxConnections: 32, MaxHandshakes: 8,
			MaxConcurrentDrains: 4, ControllerInterval: 100, ReadTimeout: 1000, WriteTimeout: 1000},
		ShardEndpoints: []persistedGatewayShardControlEndpoint{
			{Node: "0a000000000000000000000000000000", ControlAddress: "127.0.0.1:7201", SplitSnapshotAddress: "127.0.0.1:7301", SplitChildRoot: "/srv/vibedb/a/split-children"},
			{Node: "0b000000000000000000000000000000", ControlAddress: "127.0.0.1:7202", SplitSnapshotAddress: "127.0.0.1:7302", SplitChildRoot: "/srv/vibedb/b/split-children"},
			{Node: "1f000000000000000000000000000000", ControlAddress: "127.0.0.1:7203", SplitSnapshotAddress: "127.0.0.1:7303", SplitChildRoot: "/srv/vibedb/c/split-children"},
			{Node: "20000000000000000000000000000000", ControlAddress: "127.0.0.1:7204", SplitSnapshotAddress: "127.0.0.1:7304", SplitChildRoot: "/srv/vibedb/d/split-children"}},
		GatewayEndpoints: []persistedGatewayControlEndpoint{
			{Node: "01000000000000000000000000000000", Incarnation: 11, ControlAddress: "127.0.0.1:7101"},
			{Node: "02000000000000000000000000000000", Incarnation: 12, ControlAddress: "127.0.0.1:7102"}},
		Candidates: []persistedGatewayReplacementCandidate{
			{Member: 31, Node: "1f000000000000000000000000000000", Store: "29000000000000000000000000000000",
				NodeIncarnation: 41, Endpoint: "candidate-a", Load: 2},
			{Member: 32, Node: "20000000000000000000000000000000", Store: "2a000000000000000000000000000000",
				NodeIncarnation: 42, Endpoint: "candidate-b", Load: 3}},
		SplitTemplate: gatewaySplitTemplateFixture(),
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	return raw, manifest
}

func TestGatewayReplicaControlManifestCanonicalExactAndInventoryBound(t *testing.T) {
	raw, _ := gatewayReplicaManifestFixture(t)
	var local [16]byte
	local[0] = 1
	manifest, err := openGatewayReplicaControlManifest(raw, local)
	if err != nil || manifest.Generation != 7 || len(manifest.Shards) != 4 ||
		len(manifest.Gateways) != 2 || manifest.Local.Member.Incarnation != 11 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	catalog := &gateway.Snapshot{}
	if _, err = manifest.ReplacementCandidates(t.Context(), catalog,
		rebalance.FailureQuorumCertificate{}); err == nil {
		t.Fatal("unfenced candidate inventory accepted")
	}
	// The inventory binds dynamic authority epochs at read time rather than
	// persisting a stale epoch in static placement configuration.
	certificate := rebalance.FailureQuorumCertificate{CatalogGeneration: catalog.Generation(),
		ConfirmedEpoch: 9, Group: raftmember.GroupKey{TopologyRecoveryEpoch: 13}}
	candidates, err := manifest.ReplacementCandidates(t.Context(), catalog, certificate)
	if err != nil || len(candidates) != 2 || candidates[0].HealthEpoch != 9 ||
		candidates[0].TopologyRecoveryEpoch != 13 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	for _, invalid := range [][]byte{
		append(append([]byte(nil), raw...), '\n'),
		bytes.Replace(raw, []byte(`"generation":7`), []byte(`"unknown":1,"generation":7`), 1),
		bytes.Replace(raw, []byte(`"node":"01000000000000000000000000000000"`),
			[]byte(`"node":"03000000000000000000000000000000"`), 1),
	} {
		if _, err = openGatewayReplicaControlManifest(invalid, local); err == nil {
			t.Fatalf("invalid canonical manifest accepted: %s", invalid)
		}
	}
}
