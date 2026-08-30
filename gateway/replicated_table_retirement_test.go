package gateway

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func tableRetirementFixture(t *testing.T) (*Snapshot, replicatedTableRetirement) {
	t.Helper()
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	keep, err := distribution.NewManifest("keep", 1, []distribution.Shard{{
		ID: "all", AllocationGeneration: 1,
		Range:   distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		Leaders: []distribution.EndpointID{"peer-a"}, Epoch: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config.Distributions = append(config.Distributions, distribution.DistributionSpec{
		Name: "keep", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
	})
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "keep", Distribution: "keep", Columns: []string{"/id"},
	})
	config.Manifests = append(config.Manifests, keep)
	current, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
		[]ReplicatedTableProfile{profile},
		[]ReplicatedTableDeclaration{{Table: "messages", CreateTable: "CREATE TABLE messages (id STRING PRIMARY KEY)"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}
	return current, replicatedTableRetirement{
		Operation: [32]byte{1}, Proof: [32]byte{2}, SourceGeneration: current.Generation(),
		Table: "messages", Distribution: "data",
	}
}

func TestReplicatedTableRetirementIsExactBoundedCatalogTransition(t *testing.T) {
	current, retirement := tableRetirementFixture(t)
	target, err := buildReplicatedTableRetirementTarget(current, retirement)
	if err != nil {
		t.Fatal(err)
	}
	certified, err := advanceCatalogState(current, target)
	if err != nil {
		t.Fatal(err)
	}
	if certified.Generation() != current.Generation()+1 || certified.tableRetirement == nil ||
		*certified.tableRetirement != retirement {
		t.Fatalf("retirement witness=%+v generation=%d", certified.tableRetirement, certified.Generation())
	}
	if _, found := certified.Placement("messages"); found {
		t.Fatal("retired table remains placed")
	}
	if _, found := certified.Spec("data"); found {
		t.Fatal("retired distribution remains active")
	}
	if _, found := certified.Placement("keep"); !found {
		t.Fatal("unrelated table was removed")
	}
	if len(certified.ReplicatedShardDescriptors()) != 0 ||
		len(certified.ReplicatedTableProfiles()) != 0 ||
		len(certified.ReplicatedTableDeclarations()) != 0 {
		t.Fatal("retired RF3 metadata remains active")
	}
	raw, err := AppendSnapshotDocument(nil, certified)
	if err != nil || !bytes.Contains(raw, []byte(`"table_retirement"`)) {
		t.Fatalf("persist retirement err=%v raw=%s", err, raw)
	}
	opened, err := OpenSnapshotDocument(raw)
	if err != nil || opened.tableRetirement == nil || *opened.tableRetirement != retirement {
		t.Fatalf("open retirement=%+v err=%v", opened, err)
	}
	cleanup, err := buildReplicatedTableRetirementCleanupTarget(opened)
	if err != nil {
		t.Fatal(err)
	}
	clean, err := advanceCatalogState(opened, cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if clean.tableRetirement != nil || clean.Generation() != opened.Generation()+1 {
		t.Fatalf("cleanup generation=%d witness=%+v err=%v", clean.Generation(), clean.tableRetirement, err)
	}
}

func TestReplicatedTableRetirementRejectsUncertifiedOrDivergentRemoval(t *testing.T) {
	current, retirement := tableRetirementFixture(t)
	target, err := buildReplicatedTableRetirementTarget(current, retirement)
	if err != nil {
		t.Fatal(err)
	}
	target.tableRetirement = nil
	if _, err := advanceCatalogState(current, target); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("uncertified removal err=%v", err)
	}
	target, err = buildReplicatedTableRetirementTarget(current, retirement)
	if err != nil {
		t.Fatal(err)
	}
	target.tableRetirement.SourceGeneration++
	if _, err := advanceCatalogState(current, target); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("divergent proof err=%v", err)
	}
}

func TestReplicatedTableRetirementBlocksUnrelatedCatalogChangeUntilCleanup(t *testing.T) {
	current, retirement := tableRetirementFixture(t)
	retired, err := buildReplicatedTableRetirementTarget(current, retirement)
	if err != nil {
		t.Fatal(err)
	}
	retired, err = advanceCatalogState(current, retired)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := NewSnapshotWithReplicatedTableMetadata(
		cloneConfig(retired.config), retired.endpoints, retired.Generation()+1,
		retired.indexDescriptors(), retired.statistics.Descriptors(), retired.replicatedDescriptors(),
		retired.replicatedTableProfiles(), retired.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatal(err)
	}
	unrelated.config.Distributions[0].Arity++
	if _, err = advanceCatalogState(retired, unrelated); err == nil {
		t.Fatal("pending retirement admitted unrelated catalog mutation")
	}
}

func TestReplicatedTableRetirementTreatsStaleProvisionRegistrationAsRetired(t *testing.T) {
	current, retirement := tableRetirementFixture(t)
	retired, err := buildReplicatedTableRetirementTarget(current, retirement)
	if err != nil {
		t.Fatal(err)
	}
	retired, err = advanceCatalogState(current, retired)
	if err != nil {
		t.Fatal(err)
	}
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	addition, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 1, nil, nil, []ReplicatedShardDescriptor{descriptor},
		[]ReplicatedTableProfile{profile}, []ReplicatedTableDeclaration{{
			Table: "messages", CreateTable: "CREATE TABLE messages (id STRING PRIMARY KEY)",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildReplicatedTableAddition(retired, addition)
	if err != nil || got != retired {
		t.Fatalf("stale provision registration got=%p want=%p err=%v", got, retired, err)
	}
}
