package rangesplit

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcapture"
	vibejson "github.com/thesyncim/vibejson"
)

func testRelationPartitioner(t testing.TB) (*Partitioner, BundleProfile) {
	t.Helper()
	p, err := NewPartitioner(testSplitPlan(t, "node-b"), "docs", []string{"/tenant"}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	profile := BundleProfile{SchemaGeneration: 7, SourceManifest: [32]byte{1}, ChildManifests: [][32]byte{{2}, {3}},
		Relations: []RelationProfile{{Relation: 1, Kind: replicatedstate.RelationJSON, Collection: "docs"},
			{Relation: 2, Kind: replicatedstate.RelationGlobalIndex, Collection: "email_index", GlobalIndex: replicatedstate.GlobalIndexProfile{
				IndexID: 41, Incarnation: 3, LocatorCount: 1, KeyEncoding: replicatedstate.GlobalIndexKeyCanonicalTuple,
				KeyArity: 1, TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion, BucketBits: 17,
			}}}}
	return p, profile
}

func cloneTestBundle(profile BundleProfile) BundleProfile {
	profile.ChildManifests = append([][32]byte(nil), profile.ChildManifests...)
	profile.Relations = append([]RelationProfile(nil), profile.Relations...)
	return profile
}

func TestRelationProfileBindsBothOrdersAndOwnsMetadata(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	before := TailSourceCoordinates{OwnershipEpoch: 5, RoutingVersion: 10, RouteGeneration: 17}
	left, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	left, err = left.BindSourceFence(before, 20)
	if err != nil {
		t.Fatal(err)
	}
	right, err := p.BindSourceFence(before, 20)
	if err != nil {
		t.Fatal(err)
	}
	right, err = right.BindRelations(profile)
	if err != nil || right.Digest() != left.Digest() || left.GeometryDigest() != p.GeometryDigest() || left.Digest() == p.Digest() {
		t.Fatalf("binding order changed authority: %v", err)
	}
	if again, err := left.BindRelations(profile); err != nil || again != left {
		t.Fatal("identical profile is not idempotent", err)
	}
	if !left.MatchesSourceSchema(7, [32]byte{1}) || left.MatchesSourceSchema(8, [32]byte{1}) ||
		left.MatchesSourceSchema(7, [32]byte{9}) || p.MatchesSourceSchema(0, [32]byte{}) {
		t.Fatal("source schema comparison was not exact")
	}
	profile.ChildManifests[0][0]++
	profile.Relations[1].Collection = "changed"
	if manifest, ok := left.ChildManifest(0); !ok || manifest != ([32]byte{2}) {
		t.Fatal("child manifest aliases input")
	}
	if relation, _ := left.Relation(2); relation.Collection != "email_index" {
		t.Fatal("relation slice aliases input")
	}
	if _, err := left.BindRelations(profile); err == nil {
		t.Fatal("different profile rebound")
	}
	raw, err := AppendPortablePartitioner(nil, left)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenPortablePartitioner(raw)
	if err != nil || reopened.Digest() != left.Digest() || reopened.RelationCount() != 2 || reopened.SchemaGeneration() != 7 {
		t.Fatal("profile portable reopen", err)
	}
	again, err := AppendPortablePartitioner(nil, reopened)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("noncanonical profile round trip", err)
	}
}

func TestRelationProfileEveryDescriptorFieldCommits(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	baseline, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*BundleProfile){
		"generation":  func(b *BundleProfile) { b.SchemaGeneration++ },
		"source":      func(b *BundleProfile) { b.SourceManifest[0]++ },
		"child":       func(b *BundleProfile) { b.ChildManifests[1][0]++ },
		"collection":  func(b *BundleProfile) { b.Relations[1].Collection += "x" },
		"index":       func(b *BundleProfile) { b.Relations[1].GlobalIndex.IndexID++ },
		"incarnation": func(b *BundleProfile) { b.Relations[1].GlobalIndex.Incarnation++ },
		"locator":     func(b *BundleProfile) { b.Relations[1].GlobalIndex.LocatorCount++ },
		"unique":      func(b *BundleProfile) { b.Relations[1].GlobalIndex.Unique = true },
		"arity":       func(b *BundleProfile) { b.Relations[1].GlobalIndex.KeyArity++ },
		"buckets":     func(b *BundleProfile) { b.Relations[1].GlobalIndex.BucketBits++ },
		"kind": func(b *BundleProfile) {
			b.Relations[1].Kind = replicatedstate.RelationJSON
			b.Relations[1].GlobalIndex = replicatedstate.GlobalIndexProfile{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := cloneTestBundle(profile)
			mutate(&changed)
			bound, err := p.BindRelations(changed)
			if err != nil || bound.Digest() == baseline.Digest() || bound.GeometryDigest() != baseline.GeometryDigest() {
				t.Fatalf("uncommitted field %s: %v", name, err)
			}
		})
	}
	other, err := NewPartitioner(testSplitPlan(t, "node-b"), "docs", []string{"/other"}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	other, err = other.BindRelations(profile)
	if err != nil || other.Digest() == baseline.Digest() || other.GeometryDigest() != baseline.GeometryDigest() {
		t.Fatal("JSON program not bound independently of geometry", err)
	}
}

func TestRelationProfileRejectsMalformedAndForeignSlots(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	for name, mutate := range map[string]func(*BundleProfile){
		"zero generation": func(b *BundleProfile) { b.SchemaGeneration = 0 },
		"zero source":     func(b *BundleProfile) { b.SourceManifest = [32]byte{} },
		"zero child":      func(b *BundleProfile) { b.ChildManifests[0] = [32]byte{} },
		"missing child":   func(b *BundleProfile) { b.ChildManifests = b.ChildManifests[:1] },
		"empty":           func(b *BundleProfile) { b.Relations = nil },
		"sparse":          func(b *BundleProfile) { b.Relations[1].Relation = 3 },
		"duplicate":       func(b *BundleProfile) { b.Relations[1].Relation = 1 },
		"reordered":       func(b *BundleProfile) { b.Relations[0], b.Relations[1] = b.Relations[1], b.Relations[0] },
		"duplicate name":  func(b *BundleProfile) { b.Relations[1].Collection = b.Relations[0].Collection },
		"foreign base":    func(b *BundleProfile) { b.Relations[0].Collection = "other" },
		"long name": func(b *BundleProfile) {
			b.Relations[1].Collection = strings.Repeat("x", replication.MaxIdentityBytes+1)
		},
		"nul name":       func(b *BundleProfile) { b.Relations[1].Collection = "a\x00b" },
		"utf8 name":      func(b *BundleProfile) { b.Relations[1].Collection = "\xff" },
		"unknown kind":   func(b *BundleProfile) { b.Relations[1].Kind++ },
		"json metadata":  func(b *BundleProfile) { b.Relations[0].GlobalIndex = b.Relations[1].GlobalIndex },
		"index identity": func(b *BundleProfile) { b.Relations[1].GlobalIndex.IndexID = 0 },
		"encoding":       func(b *BundleProfile) { b.Relations[1].GlobalIndex.KeyEncoding++ },
		"tuple":          func(b *BundleProfile) { b.Relations[1].GlobalIndex.TupleVersion++ },
		"mapper":         func(b *BundleProfile) { b.Relations[1].GlobalIndex.MapperVersion++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := cloneTestBundle(profile)
			mutate(&changed)
			if _, err := p.BindRelations(changed); err == nil {
				t.Fatal("malformed profile accepted")
			}
		})
	}
	bound, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []replication.RelationID{0, 3, 65535} {
		if _, ok := bound.Relation(id); ok {
			t.Fatal("foreign relation alias")
		}
		if _, err := bound.RelationPoint(id, nil, nil, nil); err == nil {
			t.Fatal("foreign mapper alias")
		}
	}
	if p.RelationCount() != 1 {
		t.Fatal("singleton changed")
	}
	if r, ok := p.Relation(1); !ok || r.Kind != replicatedstate.RelationJSON || r.Collection != "docs" {
		t.Fatal("singleton descriptor")
	}
}

func TestRelationProfileGlobalMapperIgnoresValueAndLocatorPlacement(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	p, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	keyTuple, _ := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("index-value")})
	locator, _ := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("row-one")})
	otherLocator, _ := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("row-two")})
	key := append(bytes.Clone(keyTuple), locator...)
	other := append(bytes.Clone(keyTuple), otherLocator...)
	want, ok := profile.Relations[1].GlobalIndex.GlobalIndexStorageKeyPoint(key)
	if !ok {
		t.Fatal("invalid fixture")
	}
	for _, stored := range [][]byte{key, other} {
		point, err := p.RelationPoint(2, stored, []byte("not JSON"), nil)
		if err != nil || point != want {
			t.Fatal("global mapper used document/locator", err)
		}
	}
	for _, stored := range [][]byte{nil, keyTuple, key[:len(key)-1], append(bytes.Clone(key), 0)} {
		if _, err := p.RelationPoint(2, stored, nil, nil); err == nil {
			t.Fatal("malformed global key accepted")
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() { _, _ = p.RelationPoint(2, key, nil, nil) }); allocations != 0 {
		t.Fatalf("global mapper allocations %v", allocations)
	}
	var workspace distribution.DocumentPointWorkspace
	document := []byte(`{"tenant":"example"}`)
	if _, err := p.RelationPoint(1, nil, document, &workspace); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(1000, func() { _, _ = p.RelationPoint(1, nil, document, &workspace) }); allocations != 0 {
		t.Fatalf("JSON mapper allocations %v", allocations)
	}
}

func TestPortableRelationProfileMaximumShapeAndBounds(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	profile.SchemaGeneration = math.MaxUint64
	profile.Relations = make([]RelationProfile, replication.MaxRelationsPerBundle)
	for i := range profile.Relations {
		name := fmt.Sprintf("%02d", i) + strings.Repeat("\x01", replication.MaxIdentityBytes-2)
		profile.Relations[i] = RelationProfile{Relation: replication.RelationID(i + 1), Kind: replicatedstate.RelationGlobalIndex, Collection: name,
			GlobalIndex: replicatedstate.GlobalIndexProfile{IndexID: math.MaxUint64, Incarnation: math.MaxUint64, LocatorCount: 8,
				KeyEncoding: replicatedstate.GlobalIndexKeyCanonicalTuple, KeyArity: distribution.KeyspaceWidth,
				TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion, BucketBits: 20}}
	}
	profile.Relations[0].Kind, profile.Relations[0].GlobalIndex = replicatedstate.RelationJSON, replicatedstate.GlobalIndexProfile{}
	p, err := NewPartitioner(testSplitPlan(t, "node-b"), profile.Relations[0].Collection, []string{"/tenant"}, distribution.DefaultVirtualBucketBits)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendPortablePartitioner(nil, bound)
	if err != nil || len(raw) <= portableGeometryBytes || len(raw) > MaxPortablePartitionerBytes {
		t.Fatalf("max profile bytes=%d err=%v", len(raw), err)
	}
	if _, err := OpenPortablePartitioner(raw); err != nil {
		t.Fatal("legal max shape rejected", err)
	}
	activation := splitcapture.Command{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, PartitionerDigest: bound.Digest(),
		RelationManifestDigest: profile.SourceManifest, LineageDigest: [32]byte{3}, BindingDigest: [32]byte{4},
		PriorEntryDigest: [32]byte{5}, PriorDataChainDigest: [32]byte{6}, PriorApplied: 1, PriorTerm: 1,
		SourceGeneration: 1, SchemaGeneration: profile.SchemaGeneration, Spec: raw}
	encoded, err := splitcapture.AppendCommand(nil, activation)
	if err != nil {
		t.Fatal("max profile cannot enter capture envelope", err)
	}
	if opened, err := splitcapture.OpenCommand(encoded); err != nil || !bytes.Equal(opened.Spec, raw) {
		t.Fatal("max capture profile round trip", err)
	}
	var spec portablePartitioner
	if err := vibejson.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	spec.Bundle.Relations = append(spec.Bundle.Relations, spec.Bundle.Relations[0])
	tooMany, err := vibejson.Marshal(&spec)
	if err != nil {
		t.Fatal(err)
	}
	if validatePortableBounds(tooMany) == nil {
		t.Fatal("predecode count bound not enforced")
	}
	if _, err := OpenPortablePartitioner(tooMany); err == nil {
		t.Fatal("oversized relation array accepted")
	}
	profile.Relations = append(profile.Relations, profile.Relations[0])
	if _, err := p.BindRelations(profile); err == nil {
		t.Fatal("oversized direct profile accepted")
	}
	if _, err := OpenPortablePartitioner(make([]byte, MaxPortablePartitionerBytes+1)); err == nil {
		t.Fatal("byte bound not enforced")
	}
}

func TestRelationProfileRefusesPartialBaseOnlyArtifacts(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	p, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	var reads, writes int
	rows := func(func([]byte, []byte) error) error { reads++; return nil }
	sink := func([]byte, []byte) error { writes++; return nil }
	state := testSourceState(testSplitPlan(t, "node-b"))
	if _, err := p.partitionRows(state, rows, []RowSink{nil, sink}, &PartitionWorkspace{}); err == nil {
		t.Fatal("multi-relation recipe produced base-only partition evidence")
	}
	if _, err := p.writeChildArtifacts(state, rows, ChildArtifactOptions{}, &ChildArtifactWorkspace{}); err == nil {
		t.Fatal("multi-relation recipe produced base-only artifact evidence")
	}
	if _, err := p.VerifyChildArtifact(bytes.NewReader(nil), 1, ChildArtifactCallbacks{}, nil); err == nil {
		t.Fatal("multi-relation recipe admitted base-only artifact verifier")
	}
	if reads != 0 || writes != 0 {
		t.Fatal("refused bundle touched rows")
	}
}

func TestPortableRelationProfileRejectsCanonicalTampering(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	p, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendPortablePartitioner(nil, p)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*BundleProfile){
		func(b *BundleProfile) { b.SchemaGeneration++ },
		func(b *BundleProfile) { b.SourceManifest[0]++ },
		func(b *BundleProfile) { b.ChildManifests[0][0]++ },
		func(b *BundleProfile) { b.Relations[1].GlobalIndex.Incarnation++ },
		func(b *BundleProfile) { b.Relations[1].Collection += "other" },
	} {
		var spec portablePartitioner
		if err := vibejson.Unmarshal(raw, &spec); err != nil {
			t.Fatal(err)
		}
		mutate(spec.Bundle)
		changed, err := vibejson.Marshal(&spec)
		if err == nil {
			changed, err = vibejson.AppendCanonicalize(nil, changed)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenPortablePartitioner(changed); err == nil {
			t.Fatal("different profile retained original commitment")
		}
	}
	for _, changed := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), '\n'),
		bytes.Replace(raw, []byte(`"schema_generation":7`), []byte(`"schema_generation":7,"schema_generation":7`), 1)} {
		if _, err := OpenPortablePartitioner(changed); err == nil {
			t.Fatal("noncanonical profile accepted")
		}
	}
}

func TestRelationProfileChecksActualSourceCutBeforeRows(t *testing.T) {
	p, profile := testRelationPartitioner(t)
	fixture := newSourceCaptureFixture(t, p)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	cut, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	fence := cut.Fence()
	profile.SchemaGeneration, profile.SourceManifest = fence.Binding.SchemaGeneration, fence.RelationManifestDigest
	profile.Relations = profile.Relations[:1]
	bound, err := p.BindRelations(profile)
	if err != nil {
		t.Fatal(err)
	}
	sinks := []RowSink{nil, func([]byte, []byte) error { t.Fatal("empty source has rows"); return nil }}
	if _, err := bound.PartitionSnapshot(cut, sinks, &PartitionWorkspace{}); err != nil {
		t.Fatal("exact source cut refused", err)
	}
	var output bytes.Buffer
	options := ChildArtifactOptions{}
	options.Writers[1] = &output
	if _, err := bound.WriteChildArtifacts(cut, options, &ChildArtifactWorkspace{}); err != nil {
		t.Fatal("exact source artifact refused", err)
	}
	for _, mutate := range []func(*BundleProfile){func(b *BundleProfile) { b.SchemaGeneration++ }, func(b *BundleProfile) { b.SourceManifest[0]++ }} {
		wrong := cloneTestBundle(profile)
		mutate(&wrong)
		bound, err := p.BindRelations(wrong)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bound.PartitionSnapshot(cut, sinks, &PartitionWorkspace{}); err != ErrSourceFence {
			t.Fatal("foreign source cut accepted", err)
		}
		output.Reset()
		if _, err := bound.WriteChildArtifacts(cut, options, &ChildArtifactWorkspace{}); err != ErrSourceFence || output.Len() != 0 {
			t.Fatal("foreign source emitted bytes", err)
		}
	}
}

func TestPortablePartitionerRejectsTooFewTargetShardsBeforeAllocation(t *testing.T) {
	p, _ := testRelationPartitioner(t)
	raw, err := AppendPortablePartitioner(nil, p)
	if err != nil {
		t.Fatal(err)
	}
	var spec portablePartitioner
	if err := vibejson.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	third := spec.Children[1]
	third.Shard = "third"
	spec.Children = append(spec.Children, third)
	spec.ChildCount = 3
	spec.Manifest = spec.Manifest[:1]
	spec.Manifest[0].Range = spec.Source.Range
	raw, err = vibejson.Marshal(&spec)
	if err == nil {
		raw, err = vibejson.AppendCanonicalize(nil, raw)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPortablePartitioner(raw); err == nil {
		t.Fatal("target missing split children accepted")
	}
}

func FuzzPortableRelationProfile(f *testing.F) {
	p, profile := testRelationPartitioner(f)
	p, err := p.BindRelations(profile)
	if err != nil {
		f.Fatal(err)
	}
	raw, err := AppendPortablePartitioner(nil, p)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"bundle":{"relations":[{}]}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		opened, err := OpenPortablePartitioner(raw)
		if err != nil {
			return
		}
		encoded, err := AppendPortablePartitioner(nil, opened)
		if err != nil || !bytes.Equal(raw, encoded) {
			t.Fatal("accepted recipe lacks unique canonical encoding", err)
		}
		if opened.RelationCount() < 1 || opened.RelationCount() > replication.MaxRelationsPerBundle {
			t.Fatal("relation bound escaped")
		}
	})
}

func BenchmarkRelationProfileGlobalPoint(b *testing.B) {
	p, profile := testRelationPartitioner(b)
	p, err := p.BindRelations(profile)
	if err != nil {
		b.Fatal(err)
	}
	key, _ := distribution.CurrentTupleCodec.AppendTuple(nil, []distribution.Scalar{distribution.NewString("email@example.test"), distribution.NewString("row-7")})
	b.ReportAllocs()
	b.SetBytes(int64(len(key)))
	b.ResetTimer()
	for b.Loop() {
		_, _ = p.RelationPoint(2, key, nil, nil)
	}
}
