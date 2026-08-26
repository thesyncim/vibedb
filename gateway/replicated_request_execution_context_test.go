package gateway

import (
	"bytes"
	"testing"

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
