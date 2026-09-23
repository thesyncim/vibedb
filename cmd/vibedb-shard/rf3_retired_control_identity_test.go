package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func rf3RetiredManifestFixture(t *testing.T, count int) (rf3Manifest, []replicaaction.Record) {
	t.Helper()
	root := t.TempDir()
	manifest := rf3Manifest{NodeLog: &rf3NodeLogManifest{}, NodeIncarnation: 99}
	records := make([]replicaaction.Record, 0, count)
	for index := 0; index < count; index++ {
		intent := rf3RecoveryEnrollmentIntent()
		intent.Group.GroupID[0] += byte(index)
		intent.Target.StoreID[0] += byte(index)
		binding := sqldriver.ReplicatedShardStoreBinding{ClusterID: intent.Group.ClusterID,
			ClusterIncarnation: intent.Group.ClusterIncarnation, TopologyRecoveryEpoch: intent.Group.TopologyRecoveryEpoch,
			ShardIncarnation: intent.Group.ShardIncarnation, GroupID: intent.Group.GroupID,
			MemberID: intent.Target.Member, StoreID: intent.Target.StoreID, AllocationGeneration: uint64(intent.AllocationGeneration),
			Distribution: string(intent.Distribution), Shard: string(intent.Shard),
			Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: 1, ProtectionEpoch: 1,
				OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1},
		}
		base, err := sqldriver.NewReplicatedChildShardStoreIdentity(sqldriver.ShardStoreIdentity{
			Distribution: intent.Distribution, Shard: intent.Shard, AllocationGeneration: intent.AllocationGeneration,
			LogID: [16]byte{byte(index + 1)},
		}, binding, "docs", strings.Repeat("a", 64), "/id", sqldriver.ReplicatedShardStoreLimits{
			MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20, MaxBatchDocuments: 64,
			MaxBatchBytes: replication.MaxCommandBytes + 64*256,
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, string(rune('a'+index))+".identity")
		raw, err := base.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		manifest.Groups = append(manifest.Groups, rf3ManifestGroup{
			// Neither storage path exists. A positive result proves that no
			// SQL catalog, lineage file, or WAL was needed to identify retirement.
			WAL: rf3ManifestWAL{Path: filepath.Join(root, "removed-wal")},
			SQL: rf3ManifestSQL{IdentityPath: path, Path: filepath.Join(root, "removed-sql"),
				ApplyIdentityPath: filepath.Join(root, "removed-apply-identity")},
			Route: rf3ManifestGroupRoute{Group: intent.Group, Distribution: string(intent.Distribution), Shard: string(intent.Shard),
				MemberID: intent.Target.Member, StoreID: intent.Target.StoreID, AllocationGeneration: uint64(intent.AllocationGeneration)},
		})
		record := rf3RetirementRecoveryRecord(intent)
		record.Revision, record.State = 2, replicaaction.RetirementAuthorized
		record.Request.Operation[0] += byte(index)
		records = append(records, record)
	}
	return manifest, records
}

func TestRF3AllManifestSourcesRetiredUsesOnlyExactIdentityProofs(t *testing.T) {
	for _, state := range []replicaaction.State{replicaaction.RetirementAuthorized, replicaaction.Complete} {
		manifest, records := rf3RetiredManifestFixture(t, 2)
		for index := range records {
			records[index].State = state
		}
		if retired, err := rf3AllManifestSourcesRetired(manifest, records); err != nil || !retired {
			t.Fatalf("state %d did not prove all original sources retired: retired=%t err=%v", state, retired, err)
		}
		// Restart incarnation and schema fences are intentionally absent from
		// this comparison: the immutable retired storage identity cannot revive.
		manifest.NodeIncarnation++
		records[0].Request.Fence.Command.SchemaGeneration++
		if retired, err := rf3AllManifestSourcesRetired(manifest, records); err != nil || !retired {
			t.Fatalf("restart revived retired storage: retired=%t err=%v", retired, err)
		}
	}
	manifest, records := rf3RetiredManifestFixture(t, 1)
	legacy := manifest.withGroup(manifest.Groups[0])
	legacy.NodeLog, legacy.Groups = nil, nil
	if retired, err := rf3AllManifestSourcesRetired(legacy, records); err != nil || !retired {
		t.Fatalf("legacy single group: retired=%t err=%v", retired, err)
	}
}

func TestRF3AllManifestSourcesRetiredRequiresEveryOriginalSource(t *testing.T) {
	for _, mutate := range []func(*rf3Manifest, *[]replicaaction.Record){
		func(_ *rf3Manifest, records *[]replicaaction.Record) { *records = nil },
		func(_ *rf3Manifest, records *[]replicaaction.Record) { *records = (*records)[:1] },
		func(_ *rf3Manifest, records *[]replicaaction.Record) { (*records)[1].State = replicaaction.Running },
		func(_ *rf3Manifest, records *[]replicaaction.Record) { (*records)[1].Request.Fence.Group.GroupID[0]++ },
		func(_ *rf3Manifest, records *[]replicaaction.Record) { (*records)[1].Request.SourceMember++ },
		func(_ *rf3Manifest, records *[]replicaaction.Record) { (*records)[1].Request.Fence.MemberID++ },
		func(_ *rf3Manifest, records *[]replicaaction.Record) { (*records)[1].Request.Fence.StoreID[0]++ },
		func(_ *rf3Manifest, records *[]replicaaction.Record) {
			(*records)[1].Request.Fence.AllocationGeneration++
		},
		func(manifest *rf3Manifest, _ *[]replicaaction.Record) { manifest.Groups = nil },
	} {
		manifest, records := rf3RetiredManifestFixture(t, 2)
		mutate(&manifest, &records)
		if retired, _ := rf3AllManifestSourcesRetired(manifest, records); retired {
			t.Fatal("incomplete or foreign proof selected control-only startup")
		}
	}
}

func TestRF3AllManifestSourcesRetiredRejectsMissingOrConflictingIdentity(t *testing.T) {
	for _, mutate := range []func(*rf3Manifest, *[]replicaaction.Record){
		func(manifest *rf3Manifest, _ *[]replicaaction.Record) {
			manifest.Groups[0].SQL.IdentityPath += ".missing"
		},
		func(manifest *rf3Manifest, _ *[]replicaaction.Record) { manifest.Groups[0].Route.StoreID[0]++ },
		func(manifest *rf3Manifest, _ *[]replicaaction.Record) {
			manifest.Groups = append(manifest.Groups, manifest.Groups[0])
		},
		func(manifest *rf3Manifest, _ *[]replicaaction.Record) {
			manifest.Groups = make([]rf3ManifestGroup, maxRF3ManifestGroups+1)
		},
		func(_ *rf3Manifest, records *[]replicaaction.Record) {
			*records = make([]replicaaction.Record, replicaaction.AbsoluteMaxReplicaActionRecords+1)
		},
		func(_ *rf3Manifest, records *[]replicaaction.Record) { (*records)[0].Revision = 1 },
	} {
		manifest, records := rf3RetiredManifestFixture(t, 1)
		mutate(&manifest, &records)
		if retired, err := rf3AllManifestSourcesRetired(manifest, records); retired || !errors.Is(err, errRF3Serving) {
			t.Fatalf("invalid identity/proof: retired=%t err=%v", retired, err)
		}
	}
}
