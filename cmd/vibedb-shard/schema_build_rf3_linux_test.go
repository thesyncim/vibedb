//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type schemaBuildTestArtifact string

func (path schemaBuildTestArtifact) OpenArtifact(schemainstall.Request) (*os.File, error) {
	return os.Open(string(path))
}

type schemaBuildPeer struct {
	net.Conn
	peer rafttransport.PeerIdentity
}

func (c *schemaBuildPeer) PeerIdentity() rafttransport.PeerIdentity { return c.peer }
func (*schemaBuildPeer) PeerKeyDigest() [32]byte                    { return [32]byte{} }
func (c *schemaBuildPeer) TrafficClass() rafttransport.TrafficClass {
	return rafttransport.TrafficShardControl
}

type schemaBuildOpener func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (f schemaBuildOpener) OpenShardControl(ctx context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
	return f(ctx, node)
}

func buildSchemaThroughControl(t *testing.T, activator *rf3SchemaActivator, request schemainstall.BuildRequest, sql string, replica byte) sqldriver.ReplicatedSchemaDDLTarget {
	t.Helper()
	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }
	peer := rafttransport.PeerIdentity{Node: [16]byte{replica}, TrustDomain: rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}}
	service, err := schemainstall.NewBuildControlService(schemainstall.BuildControlOptions{
		Builder: activator, ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 2, BuildTimeout: time.Minute,
		Authorize: func(identity rafttransport.PeerIdentity, r schemainstall.BuildRequest) bool {
			return identity == peer && r.Group == request.Group
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := rf3ControlTestHandler{}
	mux, err := newRF3ControlMux(handler, handler, handler, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	client, err := schemainstall.NewClient(schemainstall.ClientOptions{ReadDeadline: deadline, WriteDeadline: deadline,
		Opener: schemaBuildOpener(func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error) {
			local, remote := net.Pipe()
			go func() { done <- mux.Serve(t.Context(), &schemaBuildPeer{Conn: remote, peer: peer}) }()
			return &schemaBuildPeer{Conn: local, peer: peer}, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	target, err := client.Build(t.Context(), peer.Node, request, sql)
	serveErr := <-done
	if err != nil || serveErr != nil {
		t.Fatalf("build over control: %v / %v", err, serveErr)
	}
	return target
}

func seedSchemaBuildReplica(t *testing.T, member *rf3testfixture.PreparedMember, persist ...func(uint64, []byte)) {
	t.Helper()
	b := member.Base.Binding
	command := replication.Command{ClusterID: replication.ID128(b.ClusterID), ClusterIncarnation: replication.ID128(b.ClusterIncarnation),
		TopologyRecoveryEpoch: b.TopologyRecoveryEpoch, Distribution: b.Distribution, Shard: b.Shard,
		AllocationGeneration: b.AllocationGeneration, ShardIncarnation: replication.ID128(b.ShardIncarnation), GroupID: replication.ID128(b.GroupID),
		ReplicaSetVersion: 1, ActivePolicyGeneration: b.Authority.ActivePolicyGeneration, ProtectionEpoch: b.Authority.ProtectionEpoch,
		OwnershipEpoch: b.Authority.OwnershipEpoch, SchemaGeneration: b.Authority.SchemaGeneration, RoutingVersion: b.Authority.RoutingVersion,
		RouteGeneration: b.Authority.RouteGeneration, Tenant: []byte("test"), ClientID: replication.ID128{9}, ClientSequence: 1,
		Kind: replication.CommandSessionOpen, NextDeadlineUnixNano: 2_000_000_000_000_000_000, Fingerprint: sha256.Sum256([]byte("schema-build-seed"))}
	apply := func(want uint32) replication.CompletionView {
		raw, err := replication.AppendCommand(nil, command)
		if err != nil {
			t.Fatal(err)
		}
		if err := member.Apply.AdmitCommand(raw); err != nil {
			t.Fatal(err)
		}
		for _, beforeApply := range persist {
			beforeApply(member.Apply.Applied()+1, raw)
		}
		if _, err := member.Apply.ApplyNormal(raftmodel.ApplyMeta{Index: member.Apply.Applied() + 1, Term: 2}, raw); err != nil {
			t.Fatal(err)
		}
		lookup, err := member.Apply.LookupCompletion(raw)
		if err != nil {
			t.Fatal(err)
		}
		completion, err := replication.OpenCompletion(lookup.Bytes)
		if err != nil || uint32(completion.ResultCode) != want {
			t.Fatalf("seed completion: %+v %v", completion, err)
		}
		return completion
	}
	opened := apply(uint32(replicatedstate.ResultSessionOpened))
	command.Kind, command.NextDeadlineUnixNano, command.ClientEpoch = replication.CommandMutationBatch, 0, opened.ClientEpoch
	for first := 0; first < 1000; first += 64 {
		command.ClientSequence++
		command.AckThrough = command.ClientSequence - 1
		command.Fingerprint = sha256.Sum256([]byte(fmt.Sprintf("schema-build-seed-%d", first)))
		mutations := make([]replication.Mutation, 0, 64)
		for i := first; i < min(first+64, 1000); i++ {
			id := fmt.Sprintf("employee-%04d", i)
			document := []byte(fmt.Sprintf(`{"id":%q,"name":"Alex","team":"Engineering","city":"Lisbon","score":%d,"active":true}`, id, i))
			mutations = append(mutations, replication.Mutation{Kind: replication.MutationPut, Key: rf3FaultKey(t, id), Value: document})
		}
		command.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}
		apply(uint32(replicatedstate.ResultApplied))
	}
}

// Real durable stores for three replica identities exercise the actual shard
// builder and installer adapter. This is not a simulated quorum/activation;
// those remain the responsibility of the schema rollout process tests.
func TestRF3SchemaBuildReplicasAndExactPrepareCut(t *testing.T) {
	for _, advance := range []bool{false, true} {
		t.Run(fmt.Sprintf("advance=%t", advance), func(t *testing.T) {
			var first sqldriver.ReplicatedSchemaDDLTarget
			for replica := 1; replica <= 3; replica++ {
				options := rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayDataAGroup]
				options.SchemaStatements, options.GlobalIndexes = nil, nil
				options.Table = "employees"
				options.CreateTable = "CREATE TABLE employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, team TEXT NOT NULL, city TEXT, score INTEGER NOT NULL, active BOOLEAN NOT NULL)"
				options.Root = t.TempDir()
				options.Identity = raftstore.Identity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
					ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}, Distribution: "employees", Shard: "source",
					AllocationGeneration: 1, MemberID: uint64(replica), StoreID: [16]byte{byte(replica + 4)}}
				options.Bootstrap = rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
				options.WAL, options.Key = rf3testfixture.DurableGatewayWALOptions(), raftstore.Key{ID: "schema-build-test", Wrapped: []byte("wrapped")}
				options.Key.Material[0] = 9
				member, err := rf3testfixture.PrepareMember(options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = member.Close() })
				seedSchemaBuildReplica(t, member)
				group := groupFromBinding(member.Base.Binding)
				state := &rf3SchemaGeneration{path: member.SQLPath, wal: member.WAL, base: member.Base, applyID: member.ApplyIdentity, apply: member.Apply}
				activator := &rf3SchemaActivator{groups: map[raftmember.GroupKey]*rf3SchemaGeneration{group: state}}
				manifest, err := member.Apply.RangeSplitRelationManifestDigest()
				if err != nil {
					t.Fatal(err)
				}
				const sql = "CREATE INDEX by_city ON employees (city)"
				request := schemainstall.BuildRequest{Operation: [32]byte{31}, Group: group,
					AllocationGeneration: distribution.ShardAllocationGeneration(member.Base.Binding.AllocationGeneration),
					FromSchemaGeneration: member.Base.Binding.Authority.SchemaGeneration, FromRelationManifestDigest: manifest,
					SourceApplied: member.Apply.Applied(), SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
				target := buildSchemaThroughControl(t, activator, request, sql, byte(replica))
				if target.Proof.Relations.TotalRows != 1000 {
					t.Fatalf("replica=%d build: %+v", replica, target.Proof)
				}
				if replica == 1 {
					first = target
				} else if first.Proof.Catalog.RelationManifestDigest != target.Proof.Catalog.RelationManifestDigest ||
					first.Proof.ApplyContract != target.Proof.ApplyContract || bytes.Equal(first.Catalog, target.Catalog) {
					t.Fatal("replicas disagree on shared contract or reused a physical image identity")
				}
				replay, err := activator.BuildSchema(t.Context(), request, sql)
				if err != nil || !bytes.Equal(target.Catalog, replay.Catalog) || target.Proof != replay.Proof {
					t.Fatalf("exact replay: %v", err)
				}
				bad := request
				bad.FromSchemaGeneration++
				if _, err := activator.BuildSchema(t.Context(), bad, sql); !errors.Is(err, schemainstall.ErrConflict) {
					t.Fatalf("stale schema accepted: %v", err)
				}
				bad = request
				bad.AllocationGeneration++
				if _, err := activator.BuildSchema(t.Context(), bad, sql); !errors.Is(err, schemainstall.ErrConflict) {
					t.Fatalf("stale allocation accepted: %v", err)
				}
				if advance {
					if _, err := member.Apply.ApplyNormal(raftmodel.ApplyMeta{Index: request.SourceApplied + 1, Term: 2}, nil); err != nil {
						t.Fatal(err)
					}
				}
				artifact := filepath.Join(t.TempDir(), "catalog")
				if err := os.WriteFile(artifact, target.Catalog, 0600); err != nil {
					t.Fatal(err)
				}
				activator.files = schemaBuildTestArtifact(artifact)
				install := schemainstall.Request{Operation: request.Operation, Group: group, AllocationGeneration: request.AllocationGeneration,
					FromSchemaGeneration: request.FromSchemaGeneration, FromRelationManifestDigest: manifest,
					ToSchemaGeneration: target.Proof.Catalog.SchemaGeneration, ToRelationManifestDigest: target.Proof.Catalog.RelationManifestDigest,
					ApplyContractDigest: target.Proof.ApplyContract, BundleDigest: target.Proof.Catalog.Digest, BundleBytes: uint64(len(target.Catalog))}
				witness, err := activator.Stage(t.Context(), install, install.BundleDigest, "")
				if advance {
					if !errors.Is(err, schemainstall.ErrConflict) {
						t.Fatalf("silently prepared stale target: %v", err)
					}
				} else if err != nil || witness == ([32]byte{}) {
					t.Fatalf("prepare exact source: %v", err)
				} else {
					if state.verified == nil {
						t.Fatal("stage did not retain its activation audit")
					}
					if _, err := state.verified.Prepare(t.Context(), install.Operation); err == nil {
						t.Fatal("stage retained open target files")
					}
				}
			}
		})
	}
}
