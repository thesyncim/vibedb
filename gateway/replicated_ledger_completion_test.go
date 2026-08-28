package gateway

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestNativeLedgerCompletionBindsFixedIdentityWithoutAllocation(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)
	head, _, _ := lifecycleHead(t)
	home, err := requestledger.Home(head.Key)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := requestledger.AppendHead(nil, head)
	if err != nil {
		t.Fatal(err)
	}
	innerBytes, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation: requestledger.OperationCreate, Revision: head.Revision,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, SubjectDigest: head.TerminalContractDigest,
		ExpectedRangeIdentity: requestledger.Digest{6}, Home: home, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := requestledger.OpenCommandInto(innerBytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := ReplicatedRequestLedgerRF3{service: serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}, serviceTenant: []byte("service")}
	encoded, err := client.appendOuter(nil, route, inner, innerBytes)
	if err != nil {
		t.Fatal(err)
	}
	command, err := replication.OpenCommand(encoded)
	if err != nil {
		t.Fatal(err)
	}
	// Separate callers must not share the outer proposal waiter: only the
	// inner replicated Create can determine which caller created the row.
	first, err := client.appendOuterAttempt(nil, route, inner, innerBytes, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := client.appendOuterAttempt(nil, route, inner, innerBytes, [32]byte{1})
	if err != nil || !bytes.Equal(first, retry) {
		t.Fatalf("transport retry changed envelope: %v", err)
	}
	second, err := client.appendOuterAttempt(nil, route, inner, innerBytes, [32]byte{2})
	if err != nil || bytes.Equal(first, second) {
		t.Fatalf("independent Create callers shared envelope: %v", err)
	}
	for _, raw := range [][]byte{first, second} {
		outer, err := replication.OpenCommand(raw)
		if err != nil || !bytes.Equal(outer.RequestLedgerBytes(), innerBytes) || outer.ClientID != command.ClientID ||
			outer.Kind() != command.Kind() || outer.AuthorityClass != command.AuthorityClass {
			t.Fatalf("attempt changed inner Create or authority: %v", err)
		}
	}
	result := replicatedstate.RequestLedgerCompletionResult{
		Operation: inner.Operation, Phase: requestledger.PhasePlanning,
		ResultCode: replicatedstate.ResultApplied, Revision: inner.Revision,
		KeyDigest: inner.KeyDigest, RequestDigest: inner.RequestDigest,
		PlanRoot: inner.PlanRoot, RangeIdentity: inner.ExpectedRangeIdentity,
		StateDigest: requestledger.Digest{7}, PlanningLeaseExpiryIndex: 100,
	}
	completionFor := func(result replicatedstate.RequestLedgerCompletionResult) replication.CompletionView {
		t.Helper()
		raw, err := replicatedstate.AppendRequestLedgerCompletionResult(nil, result)
		if err != nil {
			t.Fatal(err)
		}
		completion := appendNativeTransactionCompletion(t, command, result.ResultCode, raw)
		completion.ResultFormat = replicatedstate.ResultFormatRequestLedger
		completion.AppliedSequence = 1 // Raft index is independent of the derived client epoch.
		return completion
	}
	completion := completionFor(result)
	if !nativeCompletionMatches(command, completion) {
		t.Fatal("valid ledger settlement rejected")
	}
	if got := testing.AllocsPerRun(1000, func() {
		if !nativeCompletionMatches(command, completion) {
			panic("settlement rejected")
		}
	}); got != 0 {
		t.Fatalf("completion allocations=%v, want zero", got)
	}
	for _, test := range []struct {
		name   string
		change func(*replicatedstate.RequestLedgerCompletionResult)
	}{
		{"operation", func(r *replicatedstate.RequestLedgerCompletionResult) {
			r.Operation = requestledger.OperationSeal
			r.PlanningLeaseExpiryIndex = 0
		}},
		{"key", func(r *replicatedstate.RequestLedgerCompletionResult) { r.KeyDigest[0] ^= 1 }},
		{"request", func(r *replicatedstate.RequestLedgerCompletionResult) { r.RequestDigest[0] ^= 1 }},
		{"plan", func(r *replicatedstate.RequestLedgerCompletionResult) { r.PlanRoot[0] ^= 1 }},
		{"range", func(r *replicatedstate.RequestLedgerCompletionResult) { r.RangeIdentity[0] ^= 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrong := result
			test.change(&wrong)
			if nativeCompletionMatches(command, completionFor(wrong)) {
				t.Fatal("foreign settlement accepted")
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*replication.CompletionView)
	}{
		{"format", func(c *replication.CompletionView) { c.ResultFormat = replicatedstate.ResultFormatMutation }},
		{"length", func(c *replication.CompletionView) { c.ResultLength-- }},
		{"truncated", func(c *replication.CompletionView) { c.InlineResult = c.InlineResult[:len(c.InlineResult)-1] }},
		{"code", func(c *replication.CompletionView) { c.ResultCode = replicatedstate.ResultIndexConflict }},
		{"unapplied", func(c *replication.CompletionView) { c.AppliedSequence = 0 }},
		{"epoch", func(c *replication.CompletionView) { c.ClientEpoch++ }},
		{"fingerprint", func(c *replication.CompletionView) { c.Fingerprint[0] ^= 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			wrong := completion
			test.change(&wrong)
			if nativeCompletionMatches(command, wrong) {
				t.Fatal("invalid settlement accepted")
			}
		})
	}
}
