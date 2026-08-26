package gateway

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestDurableRequestTypedExecutionContextBindsWideReplay(t *testing.T) {
	key, program := durableLogicalStreamFixture(t, 4097, 3)
	measurement, pages := durableLogicalStreamBuild(t, key, program)
	reader, err := openDurableRequestRecipeStream(
		key, measurement.descriptor(), durableRequestPlanPageSource(pages),
	)
	if err != nil {
		t.Fatal(err)
	}
	home, err := requestledger.Home(key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	recipe := DurableRequestRecipe{
		CatalogGeneration: reader.CatalogGeneration,
		Identity:          reader.Identity,
		Contract:          reader.Contract,
		Tenant:            bytes.Clone(reader.Tenant),
		KeyDigest:         reader.KeyDigest,
		RequestID:         reader.RequestID,
		RequestDigest:     reader.RequestDigest,
		ParticipantCount:  reader.ParticipantCount,
		ParticipantStream: reader,
	}
	execution, err := NewDurableRequestTypedExecutionContext(
		DurableRequestLedgerHome{
			Identity: replication.Digest{1}, Point: home, TopologyGeneration: 9,
		},
		key,
		recipe,
	)
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		if pass != 0 {
			if err = execution.Participants.Reset(); err != nil {
				t.Fatal(err)
			}
		}
		var count uint64
		for execution.Participants.Next() {
			count++
			if execution.Participants.BufferedBytes() > durableRequestReaderMaxLiveBytes {
				t.Fatalf("live bytes exceeded fixed bound")
			}
		}
		if err = execution.Participants.Err(); err != nil ||
			!execution.Participants.Complete() || count != 4097 {
			t.Fatalf("pass=%d count=%d complete=%v err=%v", pass, count, execution.Participants.Complete(), err)
		}
	}
}

func TestDurableRequestTerminalAuthorityIsStableAndContractBound(t *testing.T) {
	execution := typedExecutionFixture(t)
	commit := []byte("commit-cursor")
	abort := []byte("abort-cursor")
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(
		requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commit),
	)
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(
		requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abort),
	)
	release := terminalAuthorityRelease(t, execution)
	bindingDigest, err := executionpin.BindingDigest(release.Binding)
	if err != nil {
		t.Fatal(err)
	}
	execution.Recipe.Contract.PinDigest = replication.Digest(bindingDigest)
	key := DurableRequestAckDerivationKey{1}
	first, err := NewDurableRequestTerminalAuthority(execution, key, commit, abort, release)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewDurableRequestTerminalAuthority(execution, key, commit, abort, release)
	if err != nil || first.AckToken != second.AckToken ||
		!bytes.Equal(first.CommitCursor, commit) || !bytes.Equal(first.AbortCursor, abort) {
		t.Fatal("terminal authority was not restart-stable")
	}
	drifted := bytes.Clone(commit)
	drifted[0] ^= 1
	if _, err = NewDurableRequestTerminalAuthority(execution, key, drifted, abort, release); err == nil {
		t.Fatal("terminal cursor drift accepted")
	}
	release.Binding.RequestDigest[0] ^= 1
	if _, err = NewDurableRequestTerminalAuthority(execution, key, commit, abort, release); err == nil {
		t.Fatal("execution-pin drift accepted")
	}
}

func typedExecutionFixture(t testing.TB) DurableRequestTypedExecutionContext {
	return typedExecutionFixtureCount(t, 3)
}

func typedExecutionFixtureCount(t testing.TB, count int) DurableRequestTypedExecutionContext {
	t.Helper()
	key, program := durableLogicalStreamFixture(t, count, 3)
	measurement, pages := durableLogicalStreamBuild(t, key, program)
	reader, err := openDurableRequestRecipeStream(
		key, measurement.descriptor(), durableRequestPlanPageSource(pages),
	)
	if err != nil {
		t.Fatal(err)
	}
	home, err := requestledger.Home(key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := NewDurableRequestTypedExecutionContext(
		DurableRequestLedgerHome{Identity: replication.Digest{1}, Point: home}, key,
		DurableRequestRecipe{
			CatalogGeneration: reader.CatalogGeneration, Identity: reader.Identity,
			Contract: reader.Contract, Tenant: bytes.Clone(reader.Tenant),
			KeyDigest: reader.KeyDigest, RequestID: reader.RequestID,
			RequestDigest: reader.RequestDigest, ParticipantCount: reader.ParticipantCount,
			ParticipantStream: reader,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return execution
}

func terminalAuthorityRelease(
	t testing.TB,
	execution DurableRequestTypedExecutionContext,
) executionpin.Command {
	t.Helper()
	participant := execution.Participants
	if !participant.Next() {
		t.Fatalf("participant: %v", participant.Err())
	}
	logical := participant.Current()
	if err := participant.Reset(); err != nil {
		t.Fatal(err)
	}
	binding := executionpin.Binding{
		RequestKeyDigest:  executionpin.Digest(execution.Recipe.KeyDigest),
		RequestDigest:     executionpin.Digest(execution.Recipe.RequestDigest),
		CatalogGeneration: execution.Recipe.CatalogGeneration,
		SchemaGeneration:  logical.SchemaGeneration,
		SchemaManifestDigest: executionpin.Digest(
			execution.Recipe.Contract.SchemaManifestDigest,
		),
		SchemaCertificateDigest: executionpin.Digest(
			execution.Recipe.Contract.RouteSchemaCertificateDigest,
		),
		LogicalGroup:   executionpin.ID(logical.Group.GroupID),
		LogicalRange:   executionpin.ID(logical.Group.ShardIncarnation),
		MutationDigest: executionpin.Digest(logical.MutationDigest),
	}
	pin, err := executionpin.DerivePinID(binding)
	if err != nil {
		t.Fatal(err)
	}
	return executionpin.Command{
		Operation: executionpin.OperationRelease, Binding: binding, PinID: pin,
		AuthorityNode: executionpin.ID{1}, AuthorityGeneration: 1,
		ExpectedController: executionpin.ID{2}, ExpectedControllerEpoch: 1,
		ExpectedLeaseAppliedThrough: 2, ExpectedLeaseRevision: 1,
		AcquireCertificateDigest: executionpin.Digest{3},
	}
}

func TestDurableRequestTypedExecutionContextRejectsIdentityDrift(t *testing.T) {
	key, program := durableLogicalStreamFixture(t, 3, 3)
	measurement, pages := durableLogicalStreamBuild(t, key, program)
	reader, err := openDurableRequestRecipeStream(
		key, measurement.descriptor(), durableRequestPlanPageSource(pages),
	)
	if err != nil {
		t.Fatal(err)
	}
	home, err := requestledger.Home(key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	recipe := DurableRequestRecipe{
		CatalogGeneration: reader.CatalogGeneration,
		Identity:          reader.Identity,
		Contract:          reader.Contract,
		Tenant:            bytes.Clone(reader.Tenant),
		KeyDigest:         reader.KeyDigest,
		RequestID:         reader.RequestID,
		RequestDigest:     reader.RequestDigest,
		ParticipantCount:  reader.ParticipantCount,
		ParticipantStream: reader,
	}
	validHome := DurableRequestLedgerHome{Identity: replication.Digest{1}, Point: home}
	tests := []struct {
		name   string
		mutate func(*DurableRequestLedgerHome, *DurableRequestLedgerKey, *DurableRequestRecipe)
	}{
		{"home", func(home *DurableRequestLedgerHome, _ *DurableRequestLedgerKey, _ *DurableRequestRecipe) {
			home.Point[0] ^= 1
		}},
		{"digest", func(_ *DurableRequestLedgerHome, key *DurableRequestLedgerKey, _ *DurableRequestRecipe) {
			key.Digest[0] ^= 1
		}},
		{"request", func(_ *DurableRequestLedgerHome, _ *DurableRequestLedgerKey, recipe *DurableRequestRecipe) {
			recipe.RequestID[0] ^= 1
		}},
		{"tenant", func(_ *DurableRequestLedgerHome, _ *DurableRequestLedgerKey, recipe *DurableRequestRecipe) {
			recipe.Tenant[0] ^= 1
		}},
		{"count", func(_ *DurableRequestLedgerHome, _ *DurableRequestLedgerKey, recipe *DurableRequestRecipe) {
			recipe.ParticipantCount++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driftedHome, driftedKey, driftedRecipe := validHome, key, recipe
			driftedRecipe.Tenant = bytes.Clone(recipe.Tenant)
			test.mutate(&driftedHome, &driftedKey, &driftedRecipe)
			if _, err := NewDurableRequestTypedExecutionContext(
				driftedHome, driftedKey, driftedRecipe,
			); err == nil {
				t.Fatal("identity drift accepted")
			}
		})
	}
}
