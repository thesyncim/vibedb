package replicatedstate

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

type schemaPlacementValidator struct{}

func (schemaPlacementValidator) GlobalIndexPlacementPoint(key []byte) (distribution.KeyspacePoint, bool) {
	return globalIndexProfilePoint(testGlobalIndexProfile(91, 7, 1, true), key)
}
func (schemaPlacementValidator) GlobalIndexPlacementRange() distribution.KeyRange {
	return testBinding().OwnedRange
}
func (v schemaPlacementValidator) ValidatePut(key, _ []byte) MutationValidation {
	if _, ok := v.GlobalIndexPlacementPoint(key); !ok {
		return MutationValidationInvalid
	}
	return MutationValidationAccept
}
func (v schemaPlacementValidator) ValidateDelete(key, value []byte, _ bool) MutationValidation {
	return v.ValidatePut(key, value)
}

func newSchemaPlacementFixture(t testing.TB) relationBundleFixture {
	return newRelationBundleFixtureWithSecondKind(t, false, false, durable.Options{}, durable.Options{},
		RelationGlobalIndex, schemaPlacementValidator{})
}

func TestSchemaTransitionPreservesCertifiedBaseLocalGlobalPlacement(t *testing.T) {
	fixture := newSchemaPlacementFixture(t)
	key, err := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("a")})
	if err != nil {
		t.Fatal(err)
	}
	command := fixture.command(t, 1,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("doc"), Value: []byte(`{"email":"a"}`)}}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: []byte(`["doc"]`)}}})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if result := bundleCompletionResult(t, fixture.machine, command); result != ResultApplied {
		t.Fatalf("bundle seed result=%d", result)
	}
	toBinding := fixture.binding
	toBinding.SchemaGeneration++
	specs := relationBundleCollections(fixture.base, fixture.global, fixture.index, RelationGlobalIndex)
	proof, err := CertifyRelationImages(toBinding, specs)
	if err != nil || !proof.Valid() || proof.TotalRows != 2 || proof.PlacementDigest == ([32]byte{}) {
		t.Fatalf("target certificate=%+v err=%v", proof, err)
	}
	contract, err := RelationBundleApplyContractDigest(toBinding, specs, BundleApplyContractOptions{
		MaxSessions: fixture.options.MaxSessions, RetryWindow: fixture.options.RetryWindow})
	if err != nil {
		t.Fatal(err)
	}
	transition := testSchemaTransition(fixture.binding, fixture.machine.manifestDigest, fixture.machine.applyContract,
		proof.ManifestDigest, contract, fixture.machine.state.ReplicaSetVersion)
	transition.FromPlacementDigest = fixture.machine.state.RelationPlacementDigest
	transition.ToPlacementDigest = proof.PlacementDigest
	if transition.FromPlacementDigest == ([32]byte{}) || transition.FromPlacementDigest == transition.ToPlacementDigest {
		t.Fatal("global placement commitment did not bind schema generation")
	}
	foreign := transition
	foreign.FromPlacementDigest[0]++
	wrong, err := AppendSchemaTransition(nil, foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.AdmitCommand(wrong); err == nil {
		t.Fatal("foreign source placement admitted")
	}
	encoded, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.AdmitCommand(encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), encoded); err != nil {
		t.Fatal(err)
	}
	if fixture.machine.state.RelationPlacementDigest != proof.PlacementDigest {
		t.Fatal("committed transition retained old placement commitment")
	}
	for _, change := range []func(*State){
		func(state *State) { state.LastTerm++ },
		func(state *State) { state.Applied++ },
	} {
		observed := &Machine{state: cloneState(fixture.machine.state), schemaTransitioned: true}
		change(&observed.state)
		if _, committed, err := observed.ObserveSchemaTransition(encoded); err != nil || committed {
			t.Fatalf("entry term/index substitution authorized publication: committed=%t err=%v", committed, err)
		}
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), encoded); err != nil {
		t.Fatalf("exact transition replay: %v", err)
	}
	options := fixture.options
	options.SchemaTransition = encoded
	options.SchemaMembershipWitness = durable.CheckpointMembershipWitness{Sequence: transition.MembershipSequence,
		Source: transition.MembershipSource, Target: transition.MembershipTarget}
	options.SchemaAuthorizationDigest, options.SchemaCatalogCASDigest = transition.AuthorizationDigest, transition.CatalogCASDigest
	target, err := OpenBundle(toBinding, testBootstrap(), fixture.system, specs, fixture.log, options)
	if err != nil {
		t.Fatalf("reopen certified indexed target: %v", err)
	}
	if target.state.RelationPlacementDigest != proof.PlacementDigest || target.state.Binding != toBinding {
		t.Fatal("reopened target changed placement authority")
	}
	result, err := target.PointReadInto(2, key, 4, fixture.global.Limits.MaxDocumentBytes, nil)
	if err != nil || !bytes.Equal(result.Value, []byte(`["doc"]`)) {
		t.Fatalf("global index lost after schema reopen: %+v %v", result, err)
	}
	if keys := exactIndexKeys(t, fixture.base.Collection, fixture.index.Name, []byte(`"a"`)); len(keys) != 1 || !bytes.Equal(keys[0], []byte("doc")) {
		t.Fatalf("local exact index lost after schema reopen: %q", keys)
	}
	if _, err := target.ApplyNormal(normalMeta(5), nil); err != nil {
		t.Fatalf("new schema cannot continue apply: %v", err)
	}
}

func TestRelationImageCertificateRejectsPlacementSplicing(t *testing.T) {
	fixture := newSchemaPlacementFixture(t)
	proof, err := CertifyRelationImages(fixture.binding,
		relationBundleCollections(fixture.base, fixture.global, fixture.index, RelationGlobalIndex))
	if err != nil || !proof.Valid() {
		t.Fatalf("certificate invalid: %v", err)
	}
	for name, mutate := range map[string]func(*RelationImageCertificate){
		"placement":  func(p *RelationImageCertificate) { p.PlacementDigest[0]++ },
		"generation": func(p *RelationImageCertificate) { p.SchemaGeneration++ },
		"manifest":   func(p *RelationImageCertificate) { p.ManifestDigest[0]++ },
		"rows":       func(p *RelationImageCertificate) { p.TotalRows++ },
		"witness":    func(p *RelationImageCertificate) { p.Witness[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			foreign := proof
			mutate(&foreign)
			if foreign.Valid() {
				t.Fatal("certificate splice accepted")
			}
		})
	}
}
