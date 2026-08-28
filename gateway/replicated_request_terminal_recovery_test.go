package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type terminalRecoveryPin struct {
	*terminalCoordinatorPin
	delegated serviceauthz.Authority
}

func (pin *terminalRecoveryPin) RetryExact(ctx context.Context, exact []byte) (ReplicatedResult, error) {
	pin.delegated, _ = serviceauthz.FromContext(ctx)
	return pin.terminalCoordinatorPin.RetryExact(ctx, exact)
}

type terminalRecoveryLedger struct {
	*terminalCoordinatorLedger
	pin *terminalRecoveryPin
}

func (ledger *terminalRecoveryLedger) ApplyCAS(ctx context.Context, home DurableRequestLedgerHome, key requestledger.RequestKey, cas DurableRequestLifecycleCAS) (DurableRequestLifecycleCASResult, error) {
	result, err := ledger.terminalCoordinatorLedger.ApplyCAS(ctx, home, key, cas)
	if cas.Operation == requestledger.OperationBeginSchemaPinRelease && ledger.head.Revision == cas.Revision {
		outer, openErr := replication.OpenCommand(cas.SchemaPin.Command)
		command, nestedErr := outer.OpenExecutionPin()
		if openErr != nil || nestedErr != nil {
			return DurableRequestLifecycleCASResult{}, errors.Join(openErr, nestedErr)
		}
		// Model the production atomic ledger-intent + pin-freeze transition,
		// including an outcome-unknown response after both replacements.
		ledger.pin.record, openErr = executionpin.FreezeRelease(ledger.pin.record, command)
		if openErr != nil {
			return DurableRequestLifecycleCASResult{}, openErr
		}
	}
	return result, err
}

func runBuiltTerminalRecoveryCases(t *testing.T, execution DurableRequestTypedExecutionContext, authority DurableRequestTerminalAuthority, state durableDistributedState, head requestledger.HeadRecord, continuation requestledger.ContinuationRecord, record executionpin.Record) {
	t.Helper()
	for _, test := range []struct {
		name string
		op   requestledger.Operation
	}{
		{"prepared_new_lease", requestledger.OperationPrepareTerminal},
		{"retained_release_new_gateway", requestledger.OperationBeginSchemaPinRelease},
		{"retained_proof_new_gateway", requestledger.OperationRecordSchemaPinReleased},
	} {
		t.Run(test.name, func(t *testing.T) {
			pin := &terminalRecoveryPin{terminalCoordinatorPin: &terminalCoordinatorPin{
				t: t, route: execution.Home.borrowedRoute(), tenant: execution.Recipe.Tenant,
				retryHome: execution.Recipe.Identity.RetryHome, clientID: replication.ID128{3}, epoch: 2, sequence: 2, record: record}}
			ledger := &terminalRecoveryLedger{terminalCoordinatorLedger: &terminalCoordinatorLedger{
				head: head, continuation: continuation, fault: test.op}, pin: pin}
			coordinator, err := newDurableRequestTerminalCoordinator(ledger, pin)
			if err != nil {
				t.Fatal(err)
			}
			runner := &DurableRequestDistributedRunner{terminal: coordinator}
			if _, err := runner.completeTerminal(t.Context(), execution, authority, state); !errors.Is(err, errLifecycleRunnerFault) {
				t.Fatalf("missing authentic transition cut: %v", err)
			}
			cut := durableRequestTerminalReadCut{Head: ledger.head, Continuation: ledger.continuation,
				Prepared: ledger.prepared, SchemaPin: ledger.release, Applied: ledger.head.Revision + 100}
			preparedBytes, err := requestledger.AppendPreparedTerminal(nil, cut.Prepared)
			if err != nil {
				t.Fatal(err)
			}
			resumed := execution
			resumed.terminalCut = &cut
			principal := serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 5}
			if test.op == requestledger.OperationPrepareTerminal {
				// A prepared result without a release intent may extend its live
				// lease. That must not regenerate its already-owned ACK capability.
				renew := authority.Release
				renew.Operation, renew.NextController, renew.NextControllerEpoch = executionpin.OperationRenew, record.Controller, record.ControllerEpoch
				renew.NextLeaseSpan = 1000
				changed := executionpin.Apply(pin.record, true, renew, 11, executionpin.Digest{13}, executionpin.Digest{14})
				if changed.Reason != executionpin.ReasonApplied {
					t.Fatal("lease renewal fixture failed")
				}
				pin.record = changed.Record
				resumed.ExecutionPinLease, _ = changed.Record.LeaseCertificate()
				principal.Node = rafttransport.NodeID(record.Controller)
			}
			provider, err := NewNativeDurableRequestTerminalAuthorityProvider(DurableRequestAckDerivationKey{0xf1}, principal)
			if err != nil {
				t.Fatal(err)
			}
			for name, mutate := range map[string]func(*durableRequestTerminalReadCut){
				"request":  func(value *durableRequestTerminalReadCut) { value.Head.Key.Principal[0] ^= 1 },
				"binding":  func(value *durableRequestTerminalReadCut) { value.Head.PinDigest[0] ^= 1 },
				"prepared": func(value *durableRequestTerminalReadCut) { value.Head.PreparedTerminalDigest[0] ^= 1 },
				"terminal": func(value *durableRequestTerminalReadCut) { value.Terminal.Revision = 1 },
				"ack":      func(value *durableRequestTerminalReadCut) { value.Prepared.AckToken[0] ^= 1 },
				"result": func(value *durableRequestTerminalReadCut) {
					value.Prepared.Result = bytes.Clone(value.Prepared.Result)
					value.Prepared.Result[0] ^= 1
				},
			} {
				t.Run("reject_"+name, func(t *testing.T) {
					bad, altered := cut, resumed
					mutate(&bad)
					altered.terminalCut = &bad
					if _, err := provider.TerminalAuthority(t.Context(), altered); !errors.Is(err, ErrDurableRequestConflict) {
						t.Fatalf("forged terminal cut accepted: %v", err)
					}
				})
			}
			if cut.SchemaPin.Revision != 0 {
				bad, altered := cut, resumed
				bad.SchemaPin.Command = bytes.Clone(bad.SchemaPin.Command)
				bad.SchemaPin.Command[len(bad.SchemaPin.Command)-1] ^= 1
				altered.terminalCut = &bad
				if _, err := provider.TerminalAuthority(t.Context(), altered); !errors.Is(err, ErrDurableRequestConflict) {
					t.Fatalf("changed retained release accepted: %v", err)
				}
				if executionpin.ValidateSideEffectFence(resumed.ExecutionPinLease, pin.record, pin.record.LastApplied) == nil {
					t.Fatal("terminal recovery lease became fresh side-effect authority")
				}
			}
			if cut.SchemaPin.Phase == requestledger.SchemaPinReleased {
				bad, altered := cut, resumed
				bad = terminalCutWithWrongProofFormat(t, bad)
				altered.terminalCut = &bad
				if _, err := provider.TerminalAuthority(t.Context(), altered); !errors.Is(err, ErrDurableRequestConflict) {
					t.Fatalf("canonical wrong-format terminal proof accepted: %v", err)
				}
			}
			fresh, err := provider.TerminalAuthority(t.Context(), resumed)
			if err != nil || fresh.AckToken != cut.Prepared.AckToken {
				t.Fatalf("persisted terminal authority was replaced: %v", err)
			}
			// The coordinator independently uses the prepared capability; a
			// caller's newly derived deployment token cannot rewrite it.
			fresh.AckToken[0] ^= 0x80
			result, err := runner.completeTerminal(t.Context(), resumed, fresh, state)
			if err != nil || result.Terminal.Revision == 0 || result.Terminal.AckToken != authority.AckToken {
				t.Fatalf("terminal recovery: %v", err)
			}
			after, err := requestledger.AppendPreparedTerminal(nil, ledger.prepared)
			if err != nil || !bytes.Equal(preparedBytes, after) {
				t.Fatal("terminal recovery rewrote the immutable prepared result", err)
			}
			if test.op == requestledger.OperationBeginSchemaPinRelease {
				if pin.delegated.Node != rafttransport.NodeID(authority.Release.AuthorityNode) || pin.delegated.Generation != authority.Release.AuthorityGeneration ||
					len(pin.attempts) != 1 || !bytes.Equal(pin.attempts[0], cut.SchemaPin.Command) {
					t.Fatal("retained release changed original authority or exact command bytes")
				}
			}
		})
	}
}

// Rebuild all enclosing ledger digests using the real kernels, so this tests
// the proof format boundary rather than just a corrupt envelope checksum.
func terminalCutWithWrongProofFormat(t *testing.T, cut durableRequestTerminalReadCut) durableRequestTerminalReadCut {
	t.Helper()
	c, err := replication.OpenCompletion(cut.SchemaPin.Completion)
	if err != nil {
		t.Fatal(err)
	}
	const format = replicatedstate.ResultFormatMutation
	wrong, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: c.ClusterID, ClusterIncarnation: c.ClusterIncarnation, TopologyRecoveryEpoch: c.TopologyRecoveryEpoch,
		Distribution: c.Distribution, Shard: c.Shard, AllocationGeneration: c.AllocationGeneration,
		ShardIncarnation: c.ShardIncarnation, GroupID: c.GroupID, ReplicaSetVersion: c.ReplicaSetVersion,
		ActivePolicyGeneration: c.ActivePolicyGeneration, ProtectionEpoch: c.ProtectionEpoch,
		RoutingVersion: c.RoutingVersion, RouteGeneration: c.RouteGeneration,
		Tenant: c.Tenant, ClientID: c.ClientID, ClientEpoch: c.ClientEpoch, ClientSequence: c.ClientSequence,
		Fingerprint: c.Fingerprint, RetryHome: c.RetryHome, AppliedSequence: c.AppliedSequence,
		ResultCode: c.ResultCode, ResultFormat: format, Storage: c.Storage, ResultLength: c.ResultLength,
		ResultDigest: replication.CompletionResultDigest(c.ResultCode, format, c.InlineResult), InlineResult: c.InlineResult,
	})
	if err != nil {
		t.Fatal(err)
	}
	prior := cut.Head
	prior.Revision, prior.SchemaPinReleaseCertificateDigest = cut.Prepared.Revision, requestledger.Digest{}
	intent, err := requestledger.NewSchemaPinRelease(prior, cut.Prepared, prior.Revision+1, cut.SchemaPin.Command)
	if err != nil {
		t.Fatal(err)
	}
	prior, err = requestledger.InstallSchemaPinRelease(prior, cut.Prepared, intent)
	if err != nil {
		t.Fatal(err)
	}
	cut.SchemaPin, err = requestledger.RecordVerifiedSchemaPinReleased(intent, intent.Revision+1, wrong)
	if err != nil {
		t.Fatal(err)
	}
	cut.Head, err = requestledger.MarkSchemaPinReleased(prior, cut.Prepared, intent, cut.SchemaPin)
	if err != nil {
		t.Fatal(err)
	}
	return cut
}
