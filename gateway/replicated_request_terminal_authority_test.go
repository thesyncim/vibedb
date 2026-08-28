package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestNativeDurableRequestTerminalAuthorityProviderIsRestartDeterministic(t *testing.T) {
	execution, principal := terminalAuthorityProviderFixture(t)
	provider, err := NewNativeDurableRequestTerminalAuthorityProvider(
		DurableRequestAckDerivationKey{0xa1}, principal,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := provider.TerminalAuthority(context.Background(), execution)
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.TerminalAuthority(context.Background(), execution)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.CommitCursor, second.CommitCursor) ||
		!bytes.Equal(first.AbortCursor, second.AbortCursor) ||
		first.AckToken != second.AckToken || first.Release != second.Release {
		t.Fatal("terminal authority changed across reconstruction")
	}
	if len(first.CommitCursor) != durableDistributedCursorBytes ||
		len(first.AbortCursor) != durableDistributedCursorBytes ||
		bytes.Equal(first.CommitCursor, first.AbortCursor) {
		t.Fatalf("noncanonical terminal cursors: commit=%x abort=%x", first.CommitCursor, first.AbortCursor)
	}
	committed, commitErr := openDurableDistributedState(first.CommitCursor)
	aborted, abortErr := openDurableDistributedState(first.AbortCursor)
	if commitErr != nil || abortErr != nil || committed.branch != durableDistributedCommitted ||
		aborted.branch != durableDistributedAborted || committed.conflict || aborted.conflict ||
		committed.affected != 0 || aborted.affected != 0 {
		t.Fatalf("terminal cursor state mismatch: commit=%+v abort=%+v errors=%v/%v",
			committed, aborted, commitErr, abortErr)
	}

	binding, err := BuildDurableRequestExecutionPinBinding(execution)
	if err != nil {
		t.Fatal(err)
	}
	acquireDigest, err := executionpin.AcquireCertificateDigest(execution.ExecutionPinAcquire)
	if err != nil {
		t.Fatal(err)
	}
	want := executionpin.Command{
		Operation: executionpin.OperationRelease, Binding: binding,
		PinID:         execution.ExecutionPinLease.PinID,
		AuthorityNode: executionpin.ID(principal.Node), AuthorityGeneration: principal.Generation,
		ExpectedController:          execution.ExecutionPinLease.Controller,
		ExpectedControllerEpoch:     execution.ExecutionPinLease.ControllerEpoch,
		ExpectedLeaseAppliedThrough: execution.ExecutionPinLease.LeaseAppliedThrough,
		ExpectedLeaseRevision:       execution.ExecutionPinLease.Revision,
		AcquireCertificateDigest:    acquireDigest,
	}
	if first.Release != want || first.Release.PrepareTerminalDigest != (executionpin.Digest{}) {
		t.Fatalf("release template did not preserve exact lease authority: got=%+v want=%+v", first.Release, want)
	}
}

func TestNativeDurableRequestTerminalAuthorityProviderFailsClosedOnAuthorityDrift(t *testing.T) {
	execution, principal := terminalAuthorityProviderFixture(t)
	key := DurableRequestAckDerivationKey{0xb1}
	provider, err := NewNativeDurableRequestTerminalAuthorityProvider(key, principal)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := provider.TerminalAuthority(context.Background(), execution)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("controller", func(t *testing.T) {
		drifted := principal
		drifted.Node[0] ^= 1
		other, err := NewNativeDurableRequestTerminalAuthorityProvider(key, drifted)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = other.TerminalAuthority(context.Background(), execution); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("controller drift error=%v", err)
		}
	})
	t.Run("acquire", func(t *testing.T) {
		drifted := execution
		drifted.ExecutionPinAcquire.AuthorityDigest[0] ^= 1
		if _, err := provider.TerminalAuthority(context.Background(), drifted); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("acquire drift error=%v", err)
		}
	})
	t.Run("binding", func(t *testing.T) {
		drifted := execution
		drifted.Recipe.Contract.TransactionManifestDigest[0] ^= 1
		if _, err := provider.TerminalAuthority(context.Background(), drifted); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("binding drift error=%v", err)
		}
	})
	t.Run("terminal-state", func(t *testing.T) {
		drifted := execution
		drifted.Recipe.Contract.CommitTerminalStateDigest[0] ^= 1
		if _, err := provider.TerminalAuthority(context.Background(), drifted); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("terminal-state drift error=%v", err)
		}
	})
	t.Run("principal-generation-binds-token", func(t *testing.T) {
		drifted := principal
		drifted.Generation++
		other, err := NewNativeDurableRequestTerminalAuthorityProvider(key, drifted)
		if err != nil {
			t.Fatal(err)
		}
		got, err := other.TerminalAuthority(context.Background(), execution)
		if err != nil {
			t.Fatal(err)
		}
		if got.AckToken == baseline.AckToken || got.Release.AuthorityGeneration != drifted.Generation {
			t.Fatal("principal generation did not bind terminal authority")
		}
	})
}

func TestNativeDurableRequestTerminalAuthorityProviderRejectsInvalidConfiguration(t *testing.T) {
	principal := serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}
	if _, err := NewNativeDurableRequestTerminalAuthorityProvider(DurableRequestAckDerivationKey{}, principal); err == nil {
		t.Fatal("zero derivation key accepted")
	}
	if _, err := NewNativeDurableRequestTerminalAuthorityProvider(DurableRequestAckDerivationKey{1}, serviceauthz.Authority{}); err == nil {
		t.Fatal("zero principal accepted")
	}
}

func terminalAuthorityProviderFixture(
	t testing.TB,
) (DurableRequestTypedExecutionContext, serviceauthz.Authority) {
	t.Helper()
	execution := typedExecutionFixture(t)
	_, _, route := lifecycleRunnerFixture(t)
	commit := appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedCommitted})
	abort := appendDurableDistributedState(nil, durableDistributedState{branch: durableDistributedAborted})
	execution.Recipe.Contract.CommitTerminalStateDigest = replication.Digest(
		requestledger.NextStateDigest(execution.Recipe.Contract.CommitTransitionTag, commit),
	)
	execution.Recipe.Contract.AbortTerminalStateDigest = replication.Digest(
		requestledger.NextStateDigest(execution.Recipe.Contract.AbortTransitionTag, abort),
	)
	execution, _ = bindTypedExecutionPin(t, execution, route)
	principal := serviceauthz.Authority{
		Node: rafttransport.NodeID(execution.ExecutionPinLease.Controller), Generation: 17,
	}
	return execution, principal
}
