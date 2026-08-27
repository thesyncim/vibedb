package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
)

type schemaControllerClient struct {
	authority    *ReplicatedCatalogAuthority
	base         uint64
	prepared     atomic.Uint64
	authorized   atomic.Uint64
	activated    atomic.Uint64
	failActivate atomic.Bool
	activateErr  error
}

func (client *schemaControllerClient) Prepare(
	_ context.Context, node rafttransport.NodeID, request schemainstall.Request, bundle []byte,
) (schemainstall.Receipt, error) {
	client.prepared.Add(1)
	h := sha256.New()
	_, _ = h.Write(node[:])
	_, _ = h.Write(bundle)
	var installation [32]byte
	h.Sum(installation[:0])
	return schemainstall.Receipt{Group: request.Group,
		AllocationGeneration:       request.AllocationGeneration,
		FromSchemaGeneration:       request.FromSchemaGeneration,
		FromRelationManifestDigest: request.FromRelationManifestDigest,
		ToSchemaGeneration:         request.ToSchemaGeneration,
		ToRelationManifestDigest:   request.ToRelationManifestDigest,
		InstallationDigest:         installation, ContractDigest: schemainstall.ContractDigest()}, nil
}

func (client *schemaControllerClient) Authorize(
	context.Context, rafttransport.NodeID, schemainstall.Request, schemainstall.Authorization,
) (schemainstall.Record, error) {
	client.authorized.Add(1)
	return schemainstall.Record{State: schemainstall.StateAuthorized}, nil
}

func (client *schemaControllerClient) Activate(
	ctx context.Context, _ rafttransport.NodeID, _ schemainstall.Request,
	_ schemainstall.Authorization,
) (schemainstall.Record, error) {
	current, err := client.authority.Read(ctx)
	if err != nil || current.Generation() != client.base {
		return schemainstall.Record{}, errors.Join(err, ErrSchemaRolloutConflict)
	}
	client.activated.Add(1)
	if client.failActivate.CompareAndSwap(true, false) {
		if client.activateErr != nil {
			return schemainstall.Record{}, client.activateErr
		}
		return schemainstall.Record{}, errors.New("injected shard activation failure")
	}
	return schemainstall.Record{State: schemainstall.StateActive}, nil
}

func (*schemaControllerClient) Drain(
	context.Context, rafttransport.NodeID, schemainstall.Request,
	schemainstall.Authorization, schemainstall.DrainProof,
) (schemainstall.Record, error) {
	return schemainstall.Record{State: schemainstall.StateDrained}, nil
}

func schemaControllerPlans(
	t testing.TB, id [32]byte, base, target *Snapshot,
) []SchemaRolloutReplicaPlan {
	t.Helper()
	changes, err := schemaRolloutChanges(base, target)
	if err != nil {
		t.Fatal(err)
	}
	descriptors := target.replicatedDescriptors()
	plans := make([]SchemaRolloutReplicaPlan, 0, len(changes)*ServingReplicaCount)
	for _, change := range changes {
		var descriptor ReplicatedShardDescriptor
		for _, candidate := range descriptors {
			if candidate.Group == change.group {
				descriptor = candidate
				break
			}
		}
		if len(descriptor.Replicas) != ServingReplicaCount {
			t.Fatalf("group %+v replicas=%d", change.group, len(descriptor.Replicas))
		}
		for _, replica := range descriptor.Replicas {
			bundle := []byte{byte(replica.Member), replica.Node[0], byte(change.toSchemaGeneration)}
			plans = append(plans, SchemaRolloutReplicaPlan{Member: replica.Member,
				Node: replica.Node, Bundle: bundle,
				Request: schemainstall.Request{Operation: id, Group: change.group,
					AllocationGeneration:       change.allocation,
					FromSchemaGeneration:       change.fromSchemaGeneration,
					FromRelationManifestDigest: change.fromRelationManifestDigest,
					ToSchemaGeneration:         change.toSchemaGeneration,
					ToRelationManifestDigest:   change.toRelationManifestDigest,
					ApplyContractDigest:        [32]byte{0x91}, BundleDigest: sha256.Sum256(bundle),
					BundleBytes: uint64(len(bundle))}})
		}
	}
	return plans
}

func TestSchemaRolloutControllerActivatesEveryReplicaBeforeCatalogCAS(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, _ := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte("schema-controller"))
	client := &schemaControllerClient{authority: authority, base: current.Generation()}
	controller, err := NewSchemaRolloutController(SchemaRolloutControllerOptions{
		Authority: authority, Client: client, MaxConcurrent: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	plans := schemaControllerPlans(t, id, current, target)
	result, err := controller.Execute(context.Background(), id, target, plans)
	if err != nil || result.Record.State != ReplicatedOperationComplete ||
		result.Authorization.PreparedGroupCount == 0 ||
		client.prepared.Load() != uint64(len(plans)) ||
		client.authorized.Load() != uint64(len(plans)) ||
		client.activated.Load() != uint64(len(plans)) {
		t.Fatalf("result=%+v counts=%d/%d/%d err=%v", result,
			client.prepared.Load(), client.authorized.Load(), client.activated.Load(), err)
	}
	installed, err := authority.Read(context.Background())
	if err != nil || installed.Generation() != target.Generation() {
		t.Fatalf("installed generation=%d err=%v", installed.Generation(), err)
	}
}

func TestSchemaRolloutControllerResumesRunningCutAfterShardFailure(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	target, _ := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte("schema-controller-resume"))
	client := &schemaControllerClient{authority: authority, base: current.Generation()}
	client.failActivate.Store(true)
	controller, err := NewSchemaRolloutController(SchemaRolloutControllerOptions{
		Authority: authority, Client: client, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	plans := schemaControllerPlans(t, id, current, target)
	result, err := controller.Execute(context.Background(), id, target, plans)
	if err == nil || result.Record.State != ReplicatedOperationRunning {
		t.Fatalf("failed activation result=%+v err=%v", result, err)
	}
	cut, readErr := authority.Read(context.Background())
	if readErr != nil || cut.Generation() != current.Generation() {
		t.Fatalf("failed activation published generation=%d err=%v", cut.Generation(), readErr)
	}
	result, err = controller.Execute(context.Background(), id, target, plans)
	if err != nil || result.Record.State != ReplicatedOperationComplete {
		t.Fatalf("resumed result=%+v err=%v", result, err)
	}
}
