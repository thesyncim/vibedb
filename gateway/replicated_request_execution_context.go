package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/executionpin"
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

// DurableRequestTerminalAuthority is the deterministic, restart-reconstructible
// authority needed after the distributed transaction is retired. Cursor bytes
// are authenticated by the sealed recipe. AckToken is derived with a stable
// deployment key instead of generated at the outcome-unknown boundary.
type DurableRequestTerminalAuthority struct {
	CommitCursor []byte
	AbortCursor  []byte
	AckToken     requestledger.AckToken
	Release      executionpin.Command
}

type DurableRequestAckDerivationKey [sha256.Size]byte

func NewDurableRequestTerminalAuthority(
	execution DurableRequestTypedExecutionContext,
	ackKey DurableRequestAckDerivationKey,
	commitCursor []byte,
	abortCursor []byte,
	release executionpin.Command,
) (DurableRequestTerminalAuthority, error) {
	contract := execution.Recipe.Contract
	if ackKey == (DurableRequestAckDerivationKey{}) || len(commitCursor) == 0 ||
		len(commitCursor) > requestledger.MaxContinuationCursorBytes || len(abortCursor) == 0 ||
		len(abortCursor) > requestledger.MaxContinuationCursorBytes ||
		requestledger.NextStateDigest(contract.CommitTransitionTag, commitCursor) !=
			requestledger.Digest(contract.CommitTerminalStateDigest) ||
		requestledger.NextStateDigest(contract.AbortTransitionTag, abortCursor) !=
			requestledger.Digest(contract.AbortTerminalStateDigest) ||
		release.Operation != executionpin.OperationRelease ||
		release.PrepareTerminalDigest != (executionpin.Digest{}) ||
		release.Binding.RequestKeyDigest != executionpin.Digest(execution.Recipe.KeyDigest) ||
		release.Binding.RequestDigest != executionpin.Digest(execution.Recipe.RequestDigest) ||
		release.Binding.CatalogGeneration != execution.Recipe.CatalogGeneration ||
		release.Binding.SchemaManifestDigest != executionpin.Digest(contract.SchemaManifestDigest) ||
		release.Binding.SchemaCertificateDigest != executionpin.Digest(contract.RouteSchemaCertificateDigest) {
		return DurableRequestTerminalAuthority{}, ErrDurableRequestConflict
	}
	bindingDigest, err := executionpin.BindingDigest(release.Binding)
	if err != nil || bindingDigest != executionpin.Digest(contract.PinDigest) {
		return DurableRequestTerminalAuthority{}, errors.Join(err, ErrDurableRequestConflict)
	}
	check := release
	check.PrepareTerminalDigest = executionpin.Digest{1}
	if !check.Valid() {
		return DurableRequestTerminalAuthority{}, ErrDurableRequestConflict
	}
	mac := hmac.New(sha256.New, ackKey[:])
	_, _ = mac.Write([]byte("vibedb/durable-request/ack-token/typed-1\x00"))
	_, _ = mac.Write(execution.Key.RequestKey.TenantDigest[:])
	_, _ = mac.Write(execution.Recipe.KeyDigest[:])
	_, _ = mac.Write(execution.Recipe.RequestDigest[:])
	_, _ = mac.Write(execution.Recipe.Contract.TerminalContractDigest[:])
	var token requestledger.AckToken
	_ = mac.Sum(token[:0])
	if token == (requestledger.AckToken{}) {
		return DurableRequestTerminalAuthority{}, ErrDurableRequest
	}
	return DurableRequestTerminalAuthority{
		CommitCursor: append([]byte(nil), commitCursor...),
		AbortCursor:  append([]byte(nil), abortCursor...),
		AckToken:     token,
		Release:      release,
	}, nil
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
