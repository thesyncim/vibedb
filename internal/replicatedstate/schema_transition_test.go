package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func testSchemaTransition(
	from Binding,
	fromManifest, fromContract, toManifest, toContract [sha256.Size]byte,
	replicaSetVersion uint64,
) SchemaTransition {
	return SchemaTransition{
		From: from, ToSchemaGeneration: from.SchemaGeneration + 1,
		ExpectedReplicaSetVersion: replicaSetVersion,
		MembershipSequence:        7,
		MembershipSource:          sha256.Sum256([]byte("membership-source")),
		MembershipTarget:          sha256.Sum256([]byte("membership-target")),
		FromManifest:              fromManifest,
		FromApplyContract:         fromContract,
		ToManifest:                toManifest,
		ToApplyContract:           toContract,
		RequestDigest:             sha256.Sum256([]byte("rollout-request")),
		AuthorizationDigest:       sha256.Sum256([]byte("rollout-authorization")),
		CatalogCASDigest:          sha256.Sum256([]byte("catalog-old-to-new-cas")),
	}
}

func TestSchemaTransitionCodecIsCanonicalAndBounded(t *testing.T) {
	if MaxSchemaTransitionBytes > replication.MaxCommandBytes {
		t.Fatalf("schema transition bound %d exceeds command bound %d",
			MaxSchemaTransitionBytes, replication.MaxCommandBytes)
	}
	transition := testSchemaTransition(
		testBinding(), sha256.Sum256([]byte("from-manifest")),
		sha256.Sum256([]byte("from-contract")), sha256.Sum256([]byte("to-manifest")),
		sha256.Sum256([]byte("to-contract")), 3,
	)
	encoded, err := AppendSchemaTransition([]byte("prefix"), transition)
	if err != nil {
		t.Fatal(err)
	}
	frame := encoded[len("prefix"):]
	opened, err := OpenSchemaTransition(frame)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := AppendSchemaTransition(nil, opened.SchemaTransition)
	if err != nil || !bytes.Equal(reencoded, frame) || !bytes.Equal(opened.Bytes(), frame) {
		t.Fatalf("canonical round trip failed: %v", err)
	}
	corrupt := bytes.Clone(frame)
	corrupt[485] = 1
	sealRecord(corrupt, schemaTransitionChecksumDomain)
	if _, err := OpenSchemaTransition(corrupt); !errors.Is(err, ErrSchemaTransition) {
		t.Fatalf("authenticated reserved byte = %v", err)
	}
	stale := transition
	stale.ToSchemaGeneration = stale.From.SchemaGeneration
	before := []byte("unchanged")
	if got, err := AppendSchemaTransition(before, stale); !errors.Is(err, ErrSchemaTransition) ||
		!bytes.Equal(got, before) {
		t.Fatalf("stale append = %q, %v", got, err)
	}
}

func TestMachineSchemaTransitionFencesOldBundleAndReopensExactTarget(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	if contract, err := fixture.machine.ApplyContractDigest(); err != nil ||
		contract != fixture.machine.applyContract {
		t.Fatalf("old apply contract = %x, %v", contract, err)
	}
	toBinding := fixture.binding
	toBinding.SchemaGeneration++
	relationSpecs := []RelationCollection{{
		Relation: 1, Kind: RelationJSON, Name: "docs", Target: fixture.user,
	}}
	toRelations, toManifest, err := prepareRelationCollections(toBinding, relationSpecs)
	if err != nil {
		t.Fatal(err)
	}
	toContract, err := bundleApplyContractDigest(
		toManifest, toRelations, fixture.machine.options.MaxSessions,
		fixture.machine.options.RetryWindow,
		fixture.machine.options.RequestLedgerCapacityBytes,
		fixture.machine.options.RequestLedgerCleanupReserveBytes,
		fixture.machine.options.RequestLedgerRange, routeGateRecordLimit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	transition := testSchemaTransition(
		fixture.binding, fixture.machine.manifestDigest, fixture.machine.applyContract,
		toManifest, toContract, fixture.machine.state.ReplicaSetVersion,
	)
	encoded, err := AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit schema transition: %v", err)
	}
	meta := normalMeta(2)
	publication, err := fixture.machine.ApplyNormal(meta, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if publication.Applied != 2 || fixture.machine.state.Binding != toBinding ||
		fixture.machine.state.ApplyContractDigest != toContract ||
		fixture.machine.state.LastKind != RecordSchema ||
		fixture.machine.state.DataChainDigest == ([sha256.Size]byte{}) {
		t.Fatalf("schema publication = %+v state=%+v", publication, fixture.machine.state)
	}
	if _, err := fixture.machine.RelationManifestDigest(); !errors.Is(err, ErrSchemaTransitionPending) {
		t.Fatalf("old bundle remained usable: %v", err)
	}
	if applied, committed, err := fixture.machine.ObserveSchemaTransition(encoded); err != nil ||
		!committed || applied != 2 {
		t.Fatalf("observe exact transition applied=%d committed=%t err=%v", applied, committed, err)
	}
	changedCommand := append([]byte(nil), encoded...)
	changedCommand[len(changedCommand)-1] ^= 1
	if _, committed, err := fixture.machine.ObserveSchemaTransition(changedCommand); err == nil || committed {
		t.Fatalf("observe corrupt transition committed=%t err=%v", committed, err)
	}
	if _, err := fixture.machine.ApplyContractDigest(); !errors.Is(err, ErrSchemaTransitionPending) {
		t.Fatalf("old apply contract remained observable: %v", err)
	}
	if replay, err := fixture.machine.ApplyNormal(meta, encoded); err != nil || replay.Applied != 2 {
		t.Fatalf("exact replay = %+v, %v", replay, err)
	}
	changed := bytes.Clone(encoded)
	changed[len(changed)-1] ^= 1
	if _, err := fixture.machine.ApplyNormal(meta, changed); !errors.Is(err, ErrSchemaTransitionPending) {
		t.Fatalf("mixed replay = %v", err)
	}
	if _, err := OpenBundle(
		toBinding, fixture.bootstrap, fixture.system, relationSpecs,
		fixture.log, fixture.machine.options,
	); !errors.Is(err, ErrSchemaTransition) {
		t.Fatalf("target reopen without cross-certificate witness = %v", err)
	}

	targetOptions := fixture.machine.options
	targetOptions.SchemaTransition = encoded
	targetOptions.SchemaMembershipWitness = durable.CheckpointMembershipWitness{
		Sequence: transition.MembershipSequence,
		Source:   transition.MembershipSource, Target: transition.MembershipTarget,
	}
	targetOptions.SchemaAuthorizationDigest = transition.AuthorizationDigest
	targetOptions.SchemaCatalogCASDigest = transition.CatalogCASDigest
	target, err := OpenBundle(
		toBinding, fixture.bootstrap, fixture.system, relationSpecs,
		fixture.log, targetOptions,
	)
	if err != nil {
		t.Fatalf("open exact target: %v", err)
	}
	if target.manifestDigest != toManifest || target.applyContract != toContract ||
		target.state.Binding != toBinding {
		t.Fatal("target opened with wrong schema identity")
	}
	if _, err := target.ApplyNormal(normalMeta(3), nil); err != nil {
		t.Fatalf("target did not resume ordered apply: %v", err)
	}
	if _, err := OpenBundle(
		fixture.binding, fixture.bootstrap, fixture.system, relationSpecs,
		fixture.log, targetOptions,
	); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("old generation reopen = %v", err)
	}
}
