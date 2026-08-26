package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
)

func testSchemaRolloutTarget(
	t *testing.T, current *Snapshot,
) (*Snapshot, []SchemaRolloutPreparedGroup) {
	t.Helper()
	if current == nil {
		t.Fatal("nil current catalog")
	}
	descriptors := current.ReplicatedShardDescriptors()
	if len(descriptors) == 0 {
		t.Fatal("current catalog has no replicated shards")
	}
	contract := SchemaRolloutContractDigest()
	receipts := make([]SchemaRolloutPreparedGroup, len(descriptors))
	for index := range descriptors {
		before := descriptors[index].Command
		descriptors[index].Command.SchemaGeneration++
		descriptors[index].Command.RelationManifestDigest = sha256.Sum256(
			[]byte{0x91, byte(index), byte(descriptors[index].Command.SchemaGeneration)},
		)
		receipts[index] = SchemaRolloutPreparedGroup{
			Group:                descriptors[index].Group,
			AllocationGeneration: descriptors[index].AllocationGeneration,
			FromSchemaGeneration: before.SchemaGeneration,
			FromRelationManifestDigest: replication.Digest(
				before.RelationManifestDigest,
			),
			ToSchemaGeneration: descriptors[index].Command.SchemaGeneration,
			ToRelationManifestDigest: replication.Digest(
				descriptors[index].Command.RelationManifestDigest,
			),
			InstallationDigest: sha256.Sum256([]byte{0xa1, byte(index)}),
			ContractDigest:     contract,
		}
	}
	target, err := NewSnapshotWithReplicatedMetadata(
		current.config, current.endpoints, current.Generation()+1,
		nil, nil, descriptors,
	)
	if err != nil {
		t.Fatal(err)
	}
	return target, receipts
}

func TestSchemaRolloutPrepareActivateExactCatalog(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, receipts := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte{0x11})

	planned, err := authority.PrepareSchemaRollout(
		context.Background(), id, target, receipts,
	)
	if err != nil || planned.State != ReplicatedOperationPlanned ||
		planned.Kind != ReplicatedOperationSchema || planned.Revision != 1 {
		t.Fatalf("planned=%+v err=%v", planned, err)
	}
	loaded, err := authority.ReadOperation(context.Background(), id)
	if err != nil || !loaded.Equal(planned) {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	complete, err := authority.ActivateSchemaRollout(context.Background(), id, target)
	if err != nil || complete.State != ReplicatedOperationComplete || complete.Revision != 3 {
		t.Fatalf("complete=%+v err=%v", complete, err)
	}
	installed, err := authority.Read(context.Background())
	if err != nil || installed.Generation() != target.Generation() {
		t.Fatalf("installed=%v err=%v", installed, err)
	}
	if again, againErr := authority.ActivateSchemaRollout(
		context.Background(), id, target,
	); againErr != nil || !again.Equal(complete) {
		t.Fatalf("idempotent activation=%+v err=%v", again, againErr)
	}
}

func TestSchemaRolloutRestartCompletesPublishedRunningCut(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, receipts := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte{0x21})
	planned, err := authority.PrepareSchemaRollout(
		context.Background(), id, target, receipts,
	)
	if err != nil {
		t.Fatal(err)
	}
	running := planned
	running.State, running.Revision = ReplicatedOperationRunning, 2
	if err = authority.PublishOperation(context.Background(), planned.Revision, running); err != nil {
		t.Fatal(err)
	}
	if err = authority.Publish(context.Background(), current.Generation(), target); err != nil {
		t.Fatal(err)
	}

	// A new controller has only the replicated operation and catalog rows. Its
	// stale local holder is advanced from the certified target before completion.
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x72)
	complete, err := peer.ActivateSchemaRollout(context.Background(), id, target)
	if err != nil || complete.State != ReplicatedOperationComplete || complete.Revision != 3 {
		t.Fatalf("resumed=%+v err=%v", complete, err)
	}
	if peer.holder.Current().Generation() != target.Generation() {
		t.Fatalf("peer holder generation=%d", peer.holder.Current().Generation())
	}
}

func TestSchemaRolloutAbortIsSafeOnlyBeforeActivationBoundary(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, receipts := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte{0x31})
	planned, err := authority.PrepareSchemaRollout(
		context.Background(), id, target, receipts,
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := authority.AbortSchemaRollout(context.Background(), id)
	if err != nil || cancelled.State != ReplicatedOperationCancelled || cancelled.Revision != 2 {
		t.Fatalf("cancelled=%+v err=%v", cancelled, err)
	}
	if again, againErr := authority.AbortSchemaRollout(
		context.Background(), id,
	); againErr != nil || !again.Equal(cancelled) {
		t.Fatalf("idempotent abort=%+v err=%v", again, againErr)
	}
	if _, err = authority.ActivateSchemaRollout(
		context.Background(), id, target,
	); !errors.Is(err, ErrSchemaRolloutConflict) {
		t.Fatalf("cancelled activation err=%v", err)
	}

	runningID := sha256.Sum256([]byte{0x32})
	planned, err = authority.PrepareSchemaRollout(
		context.Background(), runningID, target, receipts,
	)
	if err != nil {
		t.Fatal(err)
	}
	running := planned
	running.State, running.Revision = ReplicatedOperationRunning, 2
	if err = authority.PublishOperation(context.Background(), 1, running); err != nil {
		t.Fatal(err)
	}
	if _, err = authority.AbortSchemaRollout(
		context.Background(), runningID,
	); !errors.Is(err, ErrSchemaRolloutConflict) {
		t.Fatalf("running abort err=%v", err)
	}
}

func TestSchemaRolloutRejectsWrongManifestReceiptAndRollingContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]SchemaRolloutPreparedGroup) []SchemaRolloutPreparedGroup
	}{
		{"missing", func(receipts []SchemaRolloutPreparedGroup) []SchemaRolloutPreparedGroup {
			return receipts[:0]
		}},
		{"wrong-manifest", func(receipts []SchemaRolloutPreparedGroup) []SchemaRolloutPreparedGroup {
			receipts[0].ToRelationManifestDigest[0] ^= 0x80
			return receipts
		}},
		{"mixed-build-contract", func(receipts []SchemaRolloutPreparedGroup) []SchemaRolloutPreparedGroup {
			receipts[0].ContractDigest[0] ^= 0x80
			return receipts
		}},
		{"duplicate", func(receipts []SchemaRolloutPreparedGroup) []SchemaRolloutPreparedGroup {
			return append(receipts, receipts[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			authority, _, current := newCatalogAuthorityFixture(t)
			target, receipts := testSchemaRolloutTarget(t, current)
			receipts = test.mutate(receipts)
			id := sha256.Sum256([]byte{0x41, byte(len(test.name))})
			if _, err := authority.PrepareSchemaRollout(
				context.Background(), id, target, receipts,
			); !errors.Is(err, ErrSchemaRollout) {
				t.Fatalf("prepare err=%v", err)
			}
			if _, err := authority.ReadOperation(
				context.Background(), id,
			); !errors.Is(err, ErrReplicatedOperationMissing) {
				t.Fatalf("invalid plan was published: %v", err)
			}
		})
	}
}

func TestSchemaRolloutRejectsDifferentTargetAtActivation(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, receipts := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte{0x51})
	if _, err := authority.PrepareSchemaRollout(
		context.Background(), id, target, receipts,
	); err != nil {
		t.Fatal(err)
	}
	descriptors := target.ReplicatedShardDescriptors()
	descriptors[0].Command.RelationManifestDigest[0] ^= 0x01
	different, err := NewSnapshotWithReplicatedMetadata(
		target.config, target.endpoints, target.Generation(),
		nil, nil, descriptors,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = authority.ActivateSchemaRollout(
		context.Background(), id, different,
	); !errors.Is(err, ErrSchemaRolloutConflict) {
		t.Fatalf("different target activation err=%v", err)
	}
	if cancelled, abortErr := authority.AbortSchemaRollout(
		context.Background(), id,
	); abortErr != nil || cancelled.State != ReplicatedOperationCancelled {
		t.Fatalf("abort after rejected target=%+v err=%v", cancelled, abortErr)
	}
}

func TestSchemaRolloutRejectsMixedOldAndNewShards(t *testing.T) {
	config, endpoints, firstDescriptor := testReplicatedCatalogInput(t)
	first, _ := config.Manifests[0].ShardInfo(0)
	second, _ := config.Manifests[0].ShardInfo(1)
	second.Leaders = []distribution.EndpointID{"ep-b", "ep-c", "ep-d"}
	manifest, err := distribution.NewManifest(
		config.Manifests[0].Distribution(), config.Manifests[0].Version(),
		[]distribution.Shard{first, second},
	)
	if err != nil {
		t.Fatal(err)
	}
	config.Manifests[0] = manifest
	secondDescriptor := firstDescriptor
	secondDescriptor.Shard = second.ID
	secondDescriptor.AllocationGeneration = second.AllocationGeneration
	secondDescriptor.Command.OwnershipEpoch = uint64(second.Epoch)
	secondDescriptor.Group.GroupID[0] ^= 0x80
	secondDescriptor.Replicas = append(
		[]ReplicatedReplicaDescriptor(nil), firstDescriptor.Replicas...,
	)
	secondDescriptor.Replicas[0].Endpoint = "ep-b"
	secondDescriptor.Replicas[0].NativeEndpoint = "ep-b-native"
	secondDescriptor.Replicas[0].ControlEndpoint = "ep-b-control"
	current, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil,
		[]ReplicatedShardDescriptor{firstDescriptor, secondDescriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}
	targetDescriptors := current.ReplicatedShardDescriptors()
	targetDescriptors[0].Command.SchemaGeneration++
	targetDescriptors[0].Command.RelationManifestDigest = sha256.Sum256([]byte{0x61})
	target, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, targetDescriptors,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = schemaRolloutChanges(current, target); !errors.Is(err, ErrSchemaRollout) {
		t.Fatalf("mixed old/new target err=%v", err)
	}
}

func TestSchemaRolloutIntentIsCanonicalCompactAndExact(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, receipts := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte{0x71})
	record, err := authority.PrepareSchemaRollout(
		context.Background(), id, target, receipts,
	)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := openSchemaRolloutIntent(record.Intent)
	if err != nil || intent.PreparedGroupCount != uint64(len(receipts)) ||
		intent.ContractDigest != SchemaRolloutContractDigest() {
		t.Fatalf("intent=%+v err=%v", intent, err)
	}
	again, err := appendSchemaRolloutIntent(nil, intent)
	if err != nil || !bytes.Equal(again, record.Intent) {
		t.Fatal("schema rollout intent is not canonical")
	}
	if len(record.Intent) >= 1024 {
		t.Fatalf("constant-size intent bytes=%d", len(record.Intent))
	}
	damaged := append(append([]byte(nil), record.Intent...), ' ')
	if _, err = openSchemaRolloutIntent(damaged); !errors.Is(err, ErrSchemaRollout) {
		t.Fatalf("noncanonical intent err=%v", err)
	}
}
