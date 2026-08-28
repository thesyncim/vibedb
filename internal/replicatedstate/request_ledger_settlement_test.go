package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestRequestLedgerOriginalSettlementAndDurableReplay(t *testing.T) {
	for _, issuer := range []bool{false, true} {
		name := "create"
		if issuer {
			name = "open_issuer"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newRequestLedgerMachineFixture(t, 64<<20)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			key := requestledger.RequestKey{Scope: requestledger.ScopeAuthenticated,
				TenantDigest: requestledger.Digest{0x11}, Principal: requestledger.PrincipalID{0x21},
				Request: requestledger.RequestID{0x31}}
			command, _ := requestLedgerCreateCommand(t, fixture, key)
			if issuer {
				key = issuerPlannerKey(1, 0x61)
				highwater, err := requestledger.NewIssuerHighwater(key)
				if err != nil {
					t.Fatal(err)
				}
				payload, err := requestledger.AppendIssuerHighwater(nil, highwater)
				if err != nil {
					t.Fatal(err)
				}
				inner, err := requestledger.AppendCommand(nil, requestledger.Command{
					Operation: requestledger.OperationOpenIssuerLane, Revision: 1,
					KeyDigest: highwater.IssuerDigest, RequestDigest: highwater.HighwaterDigest,
					PlanRoot: highwater.HighwaterDigest, SubjectDigest: highwater.HighwaterDigest,
					ExpectedRangeIdentity: fixture.machine.options.RequestLedgerRange.Identity,
					Home:                  highwater.Home, Payload: payload,
				})
				if err != nil {
					t.Fatal(err)
				}
				outer := commandValue(fixture.binding, 1)
				outer.Kind, outer.AuthorityClass = replication.CommandRequestLedger, replication.CommandAuthorityRequestLedger
				outer.Batches, outer.RequestLedger, outer.Fingerprint = nil, inner, sha256.Sum256(inner)
				command = encodeCommand(t, outer)
			}
			check := func(raw []byte, applied uint64, duplicate bool) RequestLedgerCompletionResult {
				t.Helper()
				completion, err := replication.OpenCompletion(raw)
				if err != nil || completion.AppliedSequence != applied {
					t.Fatalf("completion at %d: %+v, %v", applied, completion, err)
				}
				result, err := OpenRequestLedgerCompletionResult(completion.ResultCode, completion.InlineResult)
				if err != nil || result.ResultCode != ResultApplied || result.ExactDuplicate != duplicate {
					t.Fatalf("completion duplicate=%v: %+v, %v", duplicate, result, err)
				}
				return result
			}
			publication, original, err := fixture.machine.ApplyNormalWithCompletion(normalMeta(2), command)
			if err != nil || publication.Applied != 2 || cap(original) > raftmodel.MaxNormalApplyCompletionBytes {
				t.Fatalf("original apply: %+v bytes=%d cap=%d, %v", publication, len(original), cap(original), err)
			}
			fresh := check(original, 2, false)
			originalCopy := bytes.Clone(original)
			lookup, err := fixture.machine.LookupCompletion(command)
			if err != nil {
				t.Fatal(err)
			}
			if replay := check(lookup.Bytes, 2, true); replay.StateDigest != fresh.StateDigest {
				t.Fatal("durable replay changed result state")
			}
			// Reapplying the already-published log entry has no original result;
			// its caller falls back to durable replay, including after restart.
			if _, replay, err := fixture.machine.ApplyNormalWithCompletion(normalMeta(2), command); err != nil || replay != nil {
				t.Fatalf("same-entry replay bytes=%x: %v", replay, err)
			}
			reopened, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
			if err != nil {
				t.Fatal(err)
			}
			lookup, err = reopened.LookupCompletion(command)
			if err != nil {
				t.Fatal(err)
			}
			check(lookup.Bytes, 2, true)
			if _, duplicate, err := reopened.ApplyNormalWithCompletion(normalMeta(3), command); err != nil {
				t.Fatal(err)
			} else {
				check(duplicate, 3, true)
			}
			if !bytes.Equal(original, originalCopy) {
				t.Fatal("later apply mutated the owned original result")
			}
			if _, result, err := reopened.ApplyNormalWithCompletion(normalMeta(4), nil); err != nil || result != nil {
				t.Fatalf("no-op result=%x: %v", result, err)
			}
		})
	}
}
