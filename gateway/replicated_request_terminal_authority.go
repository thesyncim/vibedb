package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var durableRequestTerminalAuthorityKeyDomain = [...]byte{
	'v', 'i', 'b', 'e', 'd', 'b', '/', 'd', 'u', 'r', 'a', 'b', 'l', 'e', '-',
	'r', 'e', 'q', 'u', 'e', 's', 't', '/', 't', 'e', 'r', 'm', 'i', 'n', 'a',
	'l', '-', 'a', 'u', 't', 'h', 'o', 'r', 'i', 't', 'y', '-', 'k', 'e', 'y', 0,
}

// NativeDurableRequestTerminalAuthorityProvider reconstructs terminal
// authority without random input or process-local state. The base key and
// authenticated service principal are deployment inputs; every request input
// is taken from the sealed aggregate execution-pin certificate chain.
type NativeDurableRequestTerminalAuthorityProvider struct {
	ackKey    DurableRequestAckDerivationKey
	principal serviceauthz.Authority
}

func NewNativeDurableRequestTerminalAuthorityProvider(
	ackKey DurableRequestAckDerivationKey,
	principal serviceauthz.Authority,
) (*NativeDurableRequestTerminalAuthorityProvider, error) {
	if ackKey == (DurableRequestAckDerivationKey{}) || !principal.Valid() {
		return nil, ErrDurableRequest
	}
	return &NativeDurableRequestTerminalAuthorityProvider{
		ackKey: ackKey, principal: principal,
	}, nil
}

func (provider *NativeDurableRequestTerminalAuthorityProvider) TerminalAuthority(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
) (DurableRequestTerminalAuthority, error) {
	if provider == nil || ctx == nil || provider.ackKey == (DurableRequestAckDerivationKey{}) ||
		!provider.principal.Valid() {
		return DurableRequestTerminalAuthority{}, ErrDurableRequest
	}
	binding, err := BuildDurableRequestExecutionPinBinding(execution)
	if err != nil {
		return DurableRequestTerminalAuthority{}, err
	}
	bindingDigest, err := executionpin.BindingDigest(binding)
	if err != nil || replication.Digest(bindingDigest) != execution.Recipe.Contract.PinDigest {
		return DurableRequestTerminalAuthority{}, errors.Join(err, ErrDurableRequestConflict)
	}
	if cut := execution.terminalCut; cut != nil {
		if err := validateDurableRequestPreparedCut(execution, *cut); err != nil {
			return DurableRequestTerminalAuthority{}, err
		}
		if cut.SchemaPin.Revision != 0 {
			_, release, err := durableRequestTerminalReleaseCommand(execution, *cut)
			if err != nil {
				return DurableRequestTerminalAuthority{}, err
			}
			release.PrepareTerminalDigest = executionpin.Digest{}
			result, err := NewDurableRequestTerminalAuthority(execution, provider.ackKey,
				appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedCommitted}),
				appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedAborted}), release)
			if err != nil {
				return DurableRequestTerminalAuthority{}, err
			}
			// This is immutable terminal recovery, not renewed side-effect
			// authority. The prepared row already owns the client capability.
			result.AckToken = cut.Prepared.AckToken
			return result, nil
		}
	}

	acquire, lease := execution.ExecutionPinAcquire, execution.ExecutionPinLease
	acquireDigest, acquireErr := executionpin.AcquireCertificateDigest(acquire)
	leaseDigest, leaseErr := executionpin.LeaseCertificateDigest(lease)
	homeRoute := execution.Home.borrowedRoute()
	if acquireErr != nil || leaseErr != nil || acquire.Binding != binding ||
		acquire.PinID != lease.PinID || acquireDigest != lease.AcquireCertificateDigest ||
		lease.Controller != executionpin.ID(provider.principal.Node) ||
		!validReplicatedRoute(execution.ExecutionPinRoute) ||
		!sameReplicatedCatalogRoute(execution.ExecutionPinRoute, homeRoute) {
		return DurableRequestTerminalAuthority{}, errors.Join(
			acquireErr, leaseErr, ErrDurableRequestConflict,
		)
	}

	release := executionpin.Command{
		Operation:                   executionpin.OperationRelease,
		Binding:                     binding,
		PinID:                       lease.PinID,
		AuthorityNode:               executionpin.ID(provider.principal.Node),
		AuthorityGeneration:         provider.principal.Generation,
		ExpectedController:          lease.Controller,
		ExpectedControllerEpoch:     lease.ControllerEpoch,
		ExpectedLeaseAppliedThrough: lease.LeaseAppliedThrough,
		ExpectedLeaseRevision:       lease.Revision,
		AcquireCertificateDigest:    acquireDigest,
	}
	check := release
	check.PrepareTerminalDigest = executionpin.Digest{1}
	if !check.Valid() {
		return DurableRequestTerminalAuthority{}, ErrDurableRequestConflict
	}

	commitCursor := appendDurableDistributedState(nil, durableDistributedState{
		branch: durableDistributedCommitted,
	})
	abortCursor := appendDurableDistributedState(nil, durableDistributedState{
		branch: durableDistributedAborted,
	})
	derivedKey := provider.deriveAckKey(
		bindingDigest, acquireDigest, leaseDigest,
	)
	result, err := NewDurableRequestTerminalAuthority(
		execution, derivedKey, commitCursor, abortCursor, release,
	)
	if err == nil && execution.terminalCut != nil {
		result.AckToken = execution.terminalCut.Prepared.AckToken
	}
	return result, err
}

func (provider *NativeDurableRequestTerminalAuthorityProvider) deriveAckKey(
	binding executionpin.Digest,
	acquire executionpin.Digest,
	lease executionpin.Digest,
) DurableRequestAckDerivationKey {
	mac := hmac.New(sha256.New, provider.ackKey[:])
	_, _ = mac.Write(durableRequestTerminalAuthorityKeyDomain[:])
	_, _ = mac.Write(binding[:])
	_, _ = mac.Write(acquire[:])
	_, _ = mac.Write(lease[:])
	_, _ = mac.Write(provider.principal.Node[:])
	var generation [8]byte
	binary.LittleEndian.PutUint64(generation[:], provider.principal.Generation)
	_, _ = mac.Write(generation[:])
	var derived DurableRequestAckDerivationKey
	_ = mac.Sum(derived[:0])
	return derived
}

var _ DurableRequestTerminalAuthorityProvider = (*NativeDurableRequestTerminalAuthorityProvider)(nil)

// Compile-time checks keep terminal cursors inside the replicated ledger's
// fixed continuation bound if either grammar changes.
const _ = uint(requestledger.MaxContinuationCursorBytes - durableDistributedCursorBytes)
