package replicatedstate

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestSchemaSourceRecoveryProofCannotBypassMembership(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	transition := testSchemaTransition(f.binding, f.machine.manifestDigest,
		f.machine.applyContract, sha256.Sum256([]byte("target-manifest")), sha256.Sum256([]byte("target-contract")),
		f.machine.state.ReplicaSetVersion)
	command, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	options := f.machine.options
	options.SchemaSourceRecovery = &SchemaSourceRecoveryProof{
		Command: command, SourceApplied: f.machine.state.Applied,
		Membership: durable.CheckpointMembershipWitness{
			Sequence: transition.MembershipSequence, Source: transition.MembershipSource,
			Target: transition.MembershipTarget,
		},
		AuthorizationDigest: transition.AuthorizationDigest, CatalogCASDigest: transition.CatalogCASDigest,
	}
	if _, err := OpenBundle(f.binding, f.bootstrap, f.system,
		[]RelationCollection{{Relation: 1, Kind: RelationJSON, Name: "docs", Target: f.user}},
		f.log, options); !errors.Is(err, ErrSchemaTransition) || errors.Is(err, ErrSchemaSourceNotCommitted) {
		t.Fatalf("unwitnessed source recovery = %v", err)
	}
}

func TestSchemaSourceRecoveryAuthenticatesCommittedCheckpoint(t *testing.T) {
	f := newNormalBatchFixture(t, 8, 4)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "target.vdb"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	target, err := durable.Create(file, f.userOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	transition := testSchemaTransition(f.binding, f.machine.manifestDigest, f.machine.applyContract,
		sha256.Sum256([]byte("target-manifest")), sha256.Sum256([]byte("target-contract")), f.machine.state.ReplicaSetVersion)
	transition.FromPlacementDigest = f.machine.state.RelationPlacementDigest
	transition.ToPlacementDigest = sha256.Sum256([]byte("target-placement"))
	witness, err := f.group.PrepareMembershipTransition([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: f.system.Collection}, {Name: "docs", Collection: target},
	}, transition.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	transition.MembershipSequence, transition.MembershipSource, transition.MembershipTarget = witness.Sequence, witness.Source, witness.Target
	command, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	options := f.machineOptions
	options.SchemaSourceRecovery = &SchemaSourceRecoveryProof{Command: command, Membership: witness,
		AuthorizationDigest: transition.AuthorizationDigest, CatalogCASDigest: transition.CatalogCASDigest, SourceApplied: 1}
	open := func(options Options) (*Machine, error) {
		return Open(f.binding, f.bootstrap, f.system,
			UserCollection{Name: "docs", Target: f.user}, f.log, options)
	}
	if _, err := open(options); !errors.Is(err, ErrSchemaSourceNotCommitted) {
		t.Fatalf("prepared cut=%v", err)
	}
	if _, err := f.machine.ApplyNormal(normalMeta(2), command); err != nil {
		t.Fatal(err)
	}
	if _, err := open(f.machineOptions); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("ordinary source reopen=%v", err)
	}
	for _, finalized := range []bool{false, true} {
		if finalized {
			if err := f.group.FinalizeMembershipTransition(witness, transition.RequestDigest, 2, sha256.Sum256(command)); err != nil {
				t.Fatal(err)
			}
		}
		m, err := open(options)
		if err != nil {
			t.Fatalf("finalized=%v source reopen: %v", finalized, err)
		}
		if applied, committed, err := m.ObserveSchemaTransition(command); err != nil || !committed || applied != 2 {
			t.Fatalf("observe=%d,%v,%v", applied, committed, err)
		}
		if _, err := m.Snapshot(); !errors.Is(err, ErrSchemaTransitionPending) {
			t.Fatalf("source snapshot=%v", err)
		}
		for _, mutate := range []func(*SchemaSourceRecoveryProof){
			func(p *SchemaSourceRecoveryProof) { p.SourceApplied++ },
			func(p *SchemaSourceRecoveryProof) { p.AuthorizationDigest[0] ^= 1 },
			func(p *SchemaSourceRecoveryProof) { p.CatalogCASDigest[0] ^= 1 },
			func(p *SchemaSourceRecoveryProof) { p.Membership.Source[0] ^= 1 },
		} {
			bad := *options.SchemaSourceRecovery
			mutate(&bad)
			rejected := options
			rejected.SchemaSourceRecovery = &bad
			if _, err := open(rejected); err == nil || errors.Is(err, ErrSchemaSourceNotCommitted) {
				t.Fatalf("altered proof=%v", err)
			}
		}
	}
}

func TestSchemaSourceRecoveryAuthenticatesBoundEmptyNormalSuffix(t *testing.T) {
	f := newNormalBatchFixture(t, 8, 4)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "target.vdb"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	target, err := durable.Create(file, f.userOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	transition := testSchemaTransition(f.binding, f.machine.manifestDigest, f.machine.applyContract,
		sha256.Sum256([]byte("target-manifest")), sha256.Sum256([]byte("target-contract")), f.machine.state.ReplicaSetVersion)
	transition.FromPlacementDigest = f.machine.state.RelationPlacementDigest
	transition.ToPlacementDigest = sha256.Sum256([]byte("target-placement"))
	witness, err := f.group.PrepareMembershipTransition([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: f.system.Collection}, {Name: "docs", Collection: target},
	}, transition.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	transition.MembershipSequence, transition.MembershipSource, transition.MembershipTarget = witness.Sequence, witness.Source, witness.Target
	command, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	committedTransition := transition
	committedTransition.CatalogCASDigest = sha256.Sum256([]byte("coordinated-catalog-cas"))
	committedCommand, err := AppendSchemaTransition(nil, committedTransition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.machine.ApplyNormal(normalMeta(2), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = f.machine.ApplyNormal(normalMeta(3), nil); err != nil {
		t.Fatal(err)
	}
	if _, err = f.machine.ApplyNormal(normalMeta(4), committedCommand); err != nil {
		t.Fatal(err)
	}
	options := f.machineOptions
	options.SchemaSourceRecovery = &SchemaSourceRecoveryProof{
		Command: command, Membership: witness, SourceApplied: 1, PreCommandApplied: 3,
		CommittedCommand:    committedCommand,
		AuthorizationDigest: transition.AuthorizationDigest, CatalogCASDigest: transition.CatalogCASDigest,
	}
	open := func(options Options) (*Machine, error) {
		return Open(f.binding, f.bootstrap, f.system,
			UserCollection{Name: "docs", Target: f.user}, f.log, options)
	}
	m, err := open(options)
	if err != nil {
		t.Fatal(err)
	}
	if applied, committed, err := m.ObserveSchemaTransition(command); err != nil || !committed || applied != 4 {
		t.Fatalf("observe=%d,%v,%v", applied, committed, err)
	}
	foreign := committedTransition
	foreign.RequestDigest = sha256.Sum256([]byte("foreign-request"))
	foreignCommand, err := AppendSchemaTransition(nil, foreign)
	if err != nil {
		t.Fatal(err)
	}
	badCommand := *options.SchemaSourceRecovery
	badCommand.CommittedCommand = foreignCommand
	rejected := options
	rejected.SchemaSourceRecovery = &badCommand
	if _, err := open(rejected); err == nil || errors.Is(err, ErrSchemaSourceNotCommitted) {
		t.Fatalf("semantically foreign committed command accepted: %v", err)
	}
	for _, preCommandApplied := range []uint64{0, 2, 4} {
		bad := *options.SchemaSourceRecovery
		bad.PreCommandApplied = preCommandApplied
		rejected := options
		rejected.SchemaSourceRecovery = &bad
		if _, err := open(rejected); err == nil || errors.Is(err, ErrSchemaSourceNotCommitted) {
			t.Fatalf("pre-command applied %d accepted: %v", preCommandApplied, err)
		}
	}
}

func TestSchemaSourceRecoveryAuthenticatesLegacyReplicaLocalCommand(t *testing.T) {
	f := newNormalBatchFixture(t, 8, 4)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "target.vdb"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	target, err := durable.Create(file, f.userOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	transition := testSchemaTransition(f.binding, f.machine.manifestDigest, f.machine.applyContract,
		sha256.Sum256([]byte("target-manifest")), sha256.Sum256([]byte("target-contract")), f.machine.state.ReplicaSetVersion)
	transition.FromPlacementDigest = f.machine.state.RelationPlacementDigest
	transition.ToPlacementDigest = sha256.Sum256([]byte("target-placement"))
	witness, err := f.group.PrepareMembershipTransition([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: f.system.Collection}, {Name: "docs", Collection: target},
	}, transition.RequestDigest)
	if err != nil {
		t.Fatal(err)
	}
	transition.MembershipSequence, transition.MembershipSource, transition.MembershipTarget = witness.Sequence, witness.Source, witness.Target
	localCommand, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	committed := transition
	committed.MembershipSequence++
	committed.MembershipSource = sha256.Sum256([]byte("group-coordination-source"))
	committed.MembershipTarget = sha256.Sum256([]byte("group-coordination-target"))
	committedCommand, err := AppendSchemaTransition(nil, committed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.machine.ApplyNormal(normalMeta(2), committedCommand); err != nil {
		t.Fatal(err)
	}
	options := f.machineOptions
	options.SchemaSourceRecovery = &SchemaSourceRecoveryProof{
		Command: localCommand, SourceApplied: 1, Membership: witness,
		AuthorizationDigest: transition.AuthorizationDigest, CatalogCASDigest: transition.CatalogCASDigest,
	}
	m, err := Open(f.binding, f.bootstrap, f.system,
		UserCollection{Name: "docs", Target: f.user}, f.log, options)
	if err != nil {
		t.Fatal(err)
	}
	if applied, ok, observeErr := m.ObserveSchemaTransition(localCommand); observeErr != nil || !ok || applied != 2 {
		t.Fatalf("legacy local observation=%d,%t,%v", applied, ok, observeErr)
	}
	altered := append([]byte(nil), localCommand...)
	altered[len(altered)-1] ^= 1
	if _, ok, observeErr := m.ObserveSchemaTransition(altered); observeErr == nil || ok {
		t.Fatalf("altered local observation=%t,%v", ok, observeErr)
	}
}

func TestSchemaRetiredSourceRejectsServingOperations(t *testing.T) {
	f := newMachineFixture(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	transition := testSchemaTransition(f.binding, f.machine.manifestDigest,
		f.machine.applyContract, sha256.Sum256([]byte("target-manifest")), sha256.Sum256([]byte("target-contract")),
		f.machine.state.ReplicaSetVersion)
	command, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(normalMeta(2), command); err != nil {
		t.Fatal(err)
	}
	m := f.machine
	mutation := testCommand(f.binding, 1, replication.Mutation{Kind: replication.MutationDelete, Key: []byte("key")})
	tests := []struct {
		name string
		run  func() error
	}{
		{"snapshot", func() error { _, e := m.Snapshot(); return e }},
		{"snapshot-base", func() error { _, _, e := m.BuildBundleSnapshotBase(); return e }},
		{"snapshot-manifest", func() error { _, e := m.BuildSnapshotBaseForManifest(SnapshotArtifactManifest{}); return e }},
		{"install-snapshot", func() error { _, e := m.InstallSnapshot(f.bootstrap); return e }},
		{"admit", func() error { return m.AdmitCommand(command) }},
		{"completion", func() error { _, e := m.LookupCompletion(mutation); return e }},
		{"point-read", func() error { _, e := m.PointReadInto(1, []byte("key"), 1, 32, nil); return e }},
		{"capacity", func() error { _, e := m.SessionCapacityState(); return e }},
		{"manifest", func() error { _, e := m.RelationManifestDigest(); return e }},
		{"contract", func() error { _, e := m.ApplyContractDigest(); return e }},
		{"placement", func() error { _, e := m.RelationPlacementDigest(); return e }},
		{"route-status", func() error { _, e := m.RouteGateStatus(); return e }},
		{"configuration", func() error {
			_, e := m.ApplyConfiguration(normalMeta(3), f.bootstrap.GetMetadata().GetConfState())
			return e
		}},
		{"apply", func() error { _, e := m.ApplyNormal(normalMeta(3), nil); return e }},
		{"batch", func() error {
			_, _, e := m.ApplyNormalBatch([]raftmodel.NormalApply{{Meta: normalMeta(3)}}, make([][32]byte, 1))
			return e
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, ErrSchemaTransitionPending) {
				t.Fatalf("retired source serving operation = %v", err)
			}
		})
	}
	if applied, committed, err := m.ObserveSchemaTransition(command); err != nil || !committed || applied != 2 {
		t.Fatalf("observation damaged by refusals: %d %v %v", applied, committed, err)
	}
}
