package gateway

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func runBuiltTerminalRecoveryCases(t *testing.T, execution DurableRequestTypedExecutionContext, authority DurableRequestTerminalAuthority, state durableDistributedState, head requestledger.HeadRecord, continuation requestledger.ContinuationRecord, record executionpin.Record) {
	t.Helper()
	for _, test := range []struct {
		name string
		op   requestledger.Operation
	}{
		{"prepared_new_lease", requestledger.OperationPrepareTerminal},
		{"atomic_release_new_gateway", requestledger.OperationReleaseSchemaPin},
	} {
		t.Run(test.name, func(t *testing.T) {
			pin := &terminalCoordinatorPin{record: record}
			ledger := &terminalCoordinatorLedger{
				pin:  pin,
				head: head, continuation: continuation, fault: test.op}
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
				// A prepared result without a committed release may extend its live
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
			if test.op == requestledger.OperationReleaseSchemaPin {
				if len(pin.attempts) != 1 || !bytes.Equal(pin.attempts[0], cut.SchemaPin.Command) {
					t.Fatal("gateway replacement repeated the committed release")
				}
			}
		})
	}
}

// Rebuild all enclosing ledger digests using the real kernels, so this tests
// the proof format boundary rather than just a corrupt envelope checksum.
func terminalCutWithWrongProofFormat(t *testing.T, cut durableRequestTerminalReadCut) durableRequestTerminalReadCut {
	t.Helper()
	c, err := executionpin.OpenCompletion(cut.SchemaPin.Completion)
	if err != nil {
		t.Fatal(err)
	}
	c.Operation = executionpin.OperationAcquire
	c.Status = executionpin.StatusActive
	c.Terminal = executionpin.TerminalCertificate{}
	wrong, err := executionpin.AppendCompletion(nil, c)
	if err != nil {
		t.Fatal(err)
	}
	prior := cut.Head
	prior.Revision, prior.SchemaPinReleaseCertificateDigest = cut.Prepared.Revision, requestledger.Digest{}
	intent, err := requestledger.NewSchemaPinRelease(prior, cut.Prepared, prior.Revision+1, cut.SchemaPin.Command)
	if err != nil {
		t.Fatal(err)
	}
	cut.Head, cut.SchemaPin, err = requestledger.CompleteSchemaPinRelease(prior, cut.Prepared, intent, wrong)
	if err != nil {
		t.Fatal(err)
	}

	return cut
}
