//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

// This fixture transports opaque canonical table metadata unchanged. The
// target relation identity and collection are built by the real SQL binder;
// the source's hidden apply targets stay in the same checkpoint group.
type schemaStartupRaw []byte

func (raw *schemaStartupRaw) UnmarshalJSON(value []byte) error { *raw = bytes.Clone(value); return nil }
func (raw schemaStartupRaw) MarshalJSON() ([]byte, error)      { return raw, nil }

type schemaStartupCatalog struct {
	Version              int                         `json:"version"`
	Tables               map[string]schemaStartupRaw `json:"tables"`
	Views                schemaStartupRaw            `json:"views,omitempty"`
	ShardStore           schemaStartupRaw            `json:"shard_store,omitempty"`
	ShardStoreFence      schemaStartupRaw            `json:"shard_store_fence,omitempty"`
	ReplicatedShardStore schemaStartupRaw            `json:"replicated_shard_store,omitempty"`
	ReplicatedChildApply schemaStartupRaw            `json:"replicated_child_apply,omitempty"`
	ReplicatedApply      schemaStartupRaw            `json:"replicated_apply,omitempty"`
}

func prepareSchemaStartupTarget(t *testing.T, member *rf3testfixture.PreparedMember, options rf3testfixture.MemberOptions) []byte {
	t.Helper()
	local := sqldriver.ShardStoreIdentity{Distribution: distribution.DistributionName(member.Base.Binding.Distribution),
		Shard: distribution.ShardID(member.Base.Binding.Shard), AllocationGeneration: distribution.ShardAllocationGeneration(member.Base.Binding.AllocationGeneration), LogID: member.Base.LogID}
	targetPath := filepath.Join(t.TempDir(), "target.vdb")
	target, err := sqldriver.InitializeShardStoreIdentity(targetPath, local)
	if err != nil {
		t.Fatal(err)
	}
	session, err := target.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	statement, err := session.Prepare(context.Background(), options.CreateTable)
	if err != nil {
		t.Fatal(err)
	}
	_, err = statement.Exec(context.Background(), nil)
	if err = errors.Join(err, statement.Close(), session.Close()); err != nil {
		t.Fatal(err)
	}
	binding := member.Base.Binding
	binding.Authority.SchemaGeneration++
	base, err := target.BindReplicatedShardStore(binding, options.Table)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	applyID, err := sqldriver.NewReplicatedChildApplyIdentity(base, member.ApplyIdentity.Storage, member.ApplyIdentity.CaptureStorage, options.Apply)
	if err != nil {
		t.Fatal(err)
	}
	read := func(path string) schemaStartupCatalog {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var catalog schemaStartupCatalog
		if err := vibejson.Unmarshal(raw, &catalog); err != nil {
			t.Fatal(err)
		}
		return catalog
	}
	catalog, targetCatalog := read(member.SQLPath), read(targetPath)
	catalog.Tables[options.Table] = targetCatalog.Tables[options.Table]
	catalog.ReplicatedShardStore, err = base.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	catalog.ReplicatedApply, err = applyID.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := vibejson.Marshal(&catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqldriver.ValidateReplicatedSchemaCatalogImage(raw); err != nil {
		t.Fatal(err)
	}
	directory, err := member.Apply.ReplicatedSchemaTargetDirectory()
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".vjc", ".vjc.rjournal"} {
		input, err := os.Open(filepath.Join(targetPath+".tables", base.UserStorage+suffix))
		if os.IsNotExist(err) && suffix == ".vjc.rjournal" {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		output, err := os.OpenFile(filepath.Join(directory, base.UserStorage+suffix), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			_ = input.Close()
			t.Fatal(err)
		}
		_, err = io.Copy(output, input)
		err = errors.Join(err, output.Sync(), output.Close(), input.Close())
		if err != nil {
			t.Fatal(err)
		}
	}
	return raw
}

// Mandatory Linux real-storage composition: drop every WAL/SQL/apply handle
// at the precommit and committed-before-publication cuts, then use the exact
// opener called by prepareRF3GroupSet, not a mocked recovery interface.
func TestRF3SchemaStartupSettlesCommittedSourceBeforeRuntimeAdoption(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "prepared-before-commit"
		if committed {
			name = "committed-before-publication"
		}
		t.Run(name, func(t *testing.T) {
			options := rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayDataAGroup]
			options.SchemaStatements, options.GlobalIndexes = nil, nil
			options.Root = t.TempDir()
			options.Identity = raftstore.Identity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
				ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}, Distribution: "orders", Shard: "source", AllocationGeneration: 1, MemberID: 1, StoreID: [16]byte{5}}
			options.Bootstrap = rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
			options.WAL, options.Key = rf3testfixture.DurableGatewayWALOptions(), raftstore.Key{ID: "schema-test", Wrapped: []byte("wrapped")}
			options.Key.Material[0] = 9
			member, err := rf3testfixture.PrepareMember(options)
			if err != nil {
				t.Fatal(err)
			}
			raw := prepareSchemaStartupTarget(t, member, options)
			proof, err := member.Apply.PrepareReplicatedSchemaTarget(raw, member.Apply.Applied(), [32]byte{41})
			if err != nil {
				t.Fatal(err)
			}
			cas, err := member.Apply.ReplicatedSchemaCatalogCASDigest(proof, [32]byte{41}, [32]byte{42})
			if err != nil {
				t.Fatal(err)
			}
			command, err := member.Apply.AppendReplicatedSchemaTransition(nil, proof, sqldriver.ReplicatedSchemaTransitionAuthority{
				RequestDigest: [32]byte{41}, AuthorizationDigest: [32]byte{42}, CatalogCASDigest: cas})
			if err != nil {
				t.Fatal(err)
			}
			if err := member.Apply.PersistReplicatedSchemaTransition(command); err != nil {
				t.Fatal(err)
			}
			if committed {
				incarnation, err := member.WAL.BeginIncarnation()
				if err != nil {
					t.Fatal(err)
				}
				index, term, entryType := proof.SourceApplied+1, uint64(2), pb.EntryNormal
				if err := member.WAL.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: 1, MustSync: true,
					HardState: &pb.HardState{Term: &term, Commit: &index}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &entryType, Data: command}}}); err != nil {
					t.Fatal(err)
				}
				if _, err := member.Apply.ApplyNormal(raftmodel.ApplyMeta{Index: index, Term: term, Type: pb.EntryNormal}, command); err != nil {
					t.Fatal(err)
				}
			}
			if err := member.Close(); err != nil {
				t.Fatal(err)
			}
			wal, err := raftstore.Open(member.WALPath, options.Identity, member.Base.Binding.TopologyRecoveryEpoch, options.Key, options.WAL)
			if err != nil {
				t.Fatal(err)
			}
			base, applyID, database, apply, err := openRF3RetainedApply(member.SQLPath, wal, member.Base, member.ApplyIdentity)
			if err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			wantSchema, wantApplied := member.Base.Binding.Authority.SchemaGeneration, proof.SourceApplied
			if committed {
				wantSchema++
				wantApplied++
			}
			if base.Binding.Authority.SchemaGeneration != wantSchema || apply.Applied() != wantApplied {
				t.Fatalf("startup selected schema/applied %d/%d want %d/%d", base.Binding.Authority.SchemaGeneration, apply.Applied(), wantSchema, wantApplied)
			}
			persisted, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(member.SQLPath)
			if err != nil || !found || !bytes.Equal(persisted.Bytes(), command) {
				t.Fatalf("startup changed original witness: %v", err)
			}
			if committed {
				if _, err := apply.SchemaApplyContractDigest(); err != nil {
					t.Fatal("startup returned fenced source instead of target", err)
				}
				if applyID.ValidationDigest == member.ApplyIdentity.ValidationDigest {
					t.Fatal("target retained source validation profile")
				}
			} else {
				if !base.Equal(member.Base) || applyID != member.ApplyIdentity {
					t.Fatal("uncommitted source identity changed")
				}
				if _, published, err := sqldriver.ObservePublishedReplicatedSchemaTransition(member.SQLPath); err != nil || published {
					t.Fatalf("uncommitted source published: %v", err)
				}
			}
			runtime, err := raftmember.AdoptRuntime(wal, database, apply)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Close(); err != nil {
				t.Fatal(err)
			}
			// The retained CLI identity may still name the source after local
			// publication. Another restart must select the same target, never
			// reconstruct or advance the original schema transition again.
			wal, err = raftstore.Open(member.WALPath, options.Identity, member.Base.Binding.TopologyRecoveryEpoch, options.Key, options.WAL)
			if err != nil {
				t.Fatal(err)
			}
			againBase, againID, database, apply, err := openRF3RetainedApply(member.SQLPath, wal, member.Base, member.ApplyIdentity)
			if err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			if !againBase.Equal(base) || againID != applyID || apply.Applied() != wantApplied {
				t.Fatal("second restart changed selected schema identity or applied cut")
			}
			if err := errors.Join(apply.Close(), database.Close(), wal.Close()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
