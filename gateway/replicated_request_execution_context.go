package gateway

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// DurableRequestTypedExecutionContext binds a sealed recipe to its exact RF3
// ledger identity. The older DurableRequestRunner surface receives neither
// Home nor RequestKey and therefore cannot safely drive typed lifecycle CASes.
// New in-process orchestration uses this context directly.
type DurableRequestTypedExecutionContext struct {
	Home         DurableRequestLedgerHome
	Key          DurableRequestLedgerKey
	Recipe       DurableRequestRecipe
	Participants DurableRequestReplayableParticipantStream
}

// DurableRequestTypedRunner is the production runner boundary for the typed
// requestledger lifecycle. It is intentionally separate from the coarse
// StepJournal adapter until the shipped executor is switched atomically.
type DurableRequestTypedRunner interface {
	RunTyped(
		context.Context,
		DurableRequestTypedExecutionContext,
	) (DurableRequestTerminalResult, error)
}

func NewDurableRequestTypedExecutionContext(
	home DurableRequestLedgerHome,
	key DurableRequestLedgerKey,
	recipe DurableRequestRecipe,
) (DurableRequestTypedExecutionContext, error) {
	participants, ok := recipe.ParticipantStream.(DurableRequestReplayableParticipantStream)
	if !ok || participants == nil || len(recipe.Participants) != 0 ||
		home.Identity == (replication.Digest{}) || home.Point == (requestledger.LedgerHome{}) ||
		!key.RequestKey.Valid() || key.Digest == (replication.Digest{}) ||
		len(recipe.Tenant) == 0 || len(recipe.Tenant) > replication.MaxIdentityBytes ||
		recipe.RequestID == (replication.ID128{}) || recipe.RequestDigest == (replication.Digest{}) ||
		recipe.KeyDigest == (replication.Digest{}) || recipe.ParticipantCount == 0 {
		return DurableRequestTypedExecutionContext{}, ErrDurableRequest
	}
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	derivedHome, homeErr := requestledger.Home(key.RequestKey)
	if err != nil || homeErr != nil || derivedHome != home.Point ||
		replication.Digest(keyDigest) != recipe.KeyDigest || key.Digest != recipe.RequestDigest ||
		recipe.RequestID != replication.ID128(key.RequestKey.Request) ||
		requestledger.Digest(sha256.Sum256(recipe.Tenant)) != key.RequestKey.TenantDigest ||
		recipe.Contract.KeyDigest != recipe.KeyDigest ||
		recipe.Contract.RequestDigest != recipe.RequestDigest ||
		recipe.Contract.ParticipantCount != recipe.ParticipantCount ||
		recipe.Contract.CatalogGeneration != recipe.CatalogGeneration ||
		recipe.Identity.CatalogGeneration != recipe.CatalogGeneration ||
		recipe.Identity.ID == ([16]byte{}) {
		return DurableRequestTypedExecutionContext{}, errors.Join(err, homeErr, ErrDurableRequestConflict)
	}
	recipe.ParticipantStream = participants
	return DurableRequestTypedExecutionContext{
		Home: home, Key: key, Recipe: recipe, Participants: participants,
	}, nil
}
