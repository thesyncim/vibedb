package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store/durable"
)

// Seed the fixture at a fully authenticated prepared cut; the tested release
// intent and takeover both execute through the real Machine.ApplyNormal and
// its single durable system transaction, not through a fake CAS implementation.
func seedPreparedPinLedger(t *testing.T, f machineFixture) (requestledger.HeadRecord, requestledger.PreparedTerminalRecord, executionpin.Record) {
	t.Helper()
	head, continuation, prepared, binding := requestLedgerPreparedForExecutionPinRows(t)
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, f.machine, 2, executionPinSessionPrototype(f.binding, id128(0xd0)))
	pinID, _ := executionpin.DerivePinID(binding)
	acquire := executionpin.Command{Operation: executionpin.OperationAcquire, Binding: binding, PinID: pinID,
		AuthorityNode: executionpin.ID(id128(0xd0)), AuthorityGeneration: 7,
		NextController: executionpin.ID(id128(0xe0)), NextControllerEpoch: 1, NextLeaseSpan: 1}
	if _, err := f.machine.ApplyNormal(normalMeta(3), executionPinCommand(f.binding, id128(0xd0), 2, 2, acquire)); err != nil {
		t.Fatal(err)
	}
	pin, found, err := f.machine.LookupExecutionPin(pinID)
	if err != nil || !found {
		t.Fatal("acquire", err)
	}
	home, _ := requestledger.Home(head.Key)
	headRaw, _ := requestledger.AppendHead(nil, head)
	contRaw, _ := requestledger.AppendContinuation(nil, continuation)
	preparedRaw, _ := requestledger.AppendPreparedTerminal(nil, prepared)
	rows := []transactionRowMutation{
		newTransactionPut(requestledger.AppendHeadKey(nil, home, head.KeyDigest), headRaw),
		newTransactionPut(requestledger.AppendContinuationKey(nil, home, head.KeyDigest), contRaw),
		newTransactionPut(requestledger.AppendPreparedTerminalKey(nil, home, head.KeyDigest), preparedRaw),
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
	scan := newRequestLedgerImageScanner(f.machine.options.RequestLedgerCapacityBytes, f.machine.options.RequestLedgerCleanupReserveBytes, f.machine.options.RequestLedgerRange)
	for _, row := range rows {
		if err = scan.observe(row.key, row.value); err != nil {
			t.Fatal(err)
		}
	}
	if err = scan.finishRequest(); err != nil {
		t.Fatal(err)
	}
	next := f.machine.nextState(normalMeta(4), RecordNormal, normalEntryDigest(normalMeta(4), []byte("prepared-fixture")))
	next.RequestLedgerRows, next.RequestLedgerResidentBytes, next.RequestLedgerReservedBytes = scan.rows, scan.resident, scan.reserved
	if err = f.machine.persistTransitionRows(next, nil, commandPlan{dataChainDigest: next.DataChainDigest}, rows); err != nil {
		t.Fatal(err)
	}
	return head, prepared, pin
}

func TestRequestLedgerReleaseFreezeOrdersTakeoverAtomically(t *testing.T) {
	for _, recoverFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "freeze-first", true: "recover-first"}[recoverFirst], func(t *testing.T) {
			f := newRequestLedgerMachineFixture(t, 64<<20)
			head, prepared, pin := seedPreparedPinLedger(t, f)
			acquire, _ := pin.AcquireCertificate()
			acquireDigest, _ := executionpin.AcquireCertificateDigest(acquire)
			release := executionpin.Command{Operation: executionpin.OperationRelease, Binding: pin.Binding, PinID: pin.PinID,
				AuthorityNode: executionpin.ID(id128(0xd0)), AuthorityGeneration: 7,
				ExpectedController: pin.Controller, ExpectedControllerEpoch: pin.ControllerEpoch,
				ExpectedLeaseAppliedThrough: pin.LeaseAppliedThrough, ExpectedLeaseRevision: pin.LeaseRevision,
				PrepareTerminalDigest: executionpin.Digest(prepared.PreparedDigest), AcquireCertificateDigest: acquireDigest}
			releaseBytes := executionPinCommand(f.binding, id128(0xd0), 2, 4, release)
			intent, err := requestledger.NewSchemaPinRelease(head, prepared, head.Revision+1, releaseBytes)
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := requestledger.AppendSchemaPinRelease(nil, intent)
			home, _ := requestledger.Home(head.Key)
			inner, err := requestledger.AppendCommand(nil, requestledger.Command{Operation: requestledger.OperationBeginSchemaPinRelease,
				ExpectedRevision: head.Revision, Revision: intent.Revision, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
				SubjectDigest: intent.RecordDigest, ExpectedRangeIdentity: f.machine.options.RequestLedgerRange.Identity, Home: home, Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			outer := commandValue(f.binding, 1)
			outer.Kind, outer.AuthorityClass, outer.Batches, outer.RequestLedger = replication.CommandRequestLedger, replication.CommandAuthorityRequestLedger, nil, inner
			outer.Fingerprint = sha256.Sum256(inner)
			freezeBytes := encodeCommand(t, outer)
			recover := release
			recover.Operation, recover.PrepareTerminalDigest = executionpin.OperationRecover, executionpin.Digest{}
			recover.NextController, recover.NextControllerEpoch, recover.NextLeaseSpan = executionpin.ID(id128(0xe1)), 2, 1
			recoverBytes := executionPinCommand(f.binding, id128(0xd0), 2, 3, recover)
			first, second := freezeBytes, recoverBytes
			if recoverFirst {
				first, second = recoverBytes, freezeBytes
			}
			before := f.machine.state
			for i, command := range [][]byte{first, second} {
				if _, err = f.machine.ApplyNormal(normalMeta(uint64(5+i)), command); err != nil {
					t.Fatal(err)
				}
			}
			got, found, err := f.machine.LookupExecutionPin(pin.PinID)
			if err != nil || !found {
				t.Fatal(err)
			}
			intentKey := requestledger.AppendSchemaPinReleaseKey(nil, home, head.KeyDigest)
			_, intentFound, err := f.system.Collection.AppendRaw(nil, intentKey)
			if err != nil {
				t.Fatal(err)
			}
			if recoverFirst {
				if got.ControllerEpoch != 2 || got.PrepareTerminalDigest != (executionpin.Digest{}) || intentFound {
					t.Fatal("stale release partially published")
				}
				if f.machine.state.RequestLedgerRows != before.RequestLedgerRows || f.machine.state.RequestLedgerResidentBytes != before.RequestLedgerResidentBytes {
					t.Fatal("rejected release changed ledger budget")
				}
				return
			}
			if !intentFound || got.ControllerEpoch != 1 || got.PrepareTerminalDigest != release.PrepareTerminalDigest {
				t.Fatal("freeze did not fence takeover")
			}
			if f.machine.state.ExecutionPinRecordCount != before.ExecutionPinRecordCount || f.machine.state.ActiveExecutionPinCount != before.ActiveExecutionPinCount || f.machine.state.ExecutionPinResidentBytes != before.ExecutionPinResidentBytes {
				t.Fatal("freeze grew pin space")
			}
			// Close the actual system collection and reopen its journal/image.
			if err = f.system.Collection.Close(); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(filepath.Join(f.dir, "system.vdb"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			collection, err := durable.Open(file, durable.Options{OpaqueValues: true, MaxDocumentBytes: requestledger.MaxCommandBytes, MaxBatchDocuments: requestledger.MaxAckGCDeleteRows + 8, MaxBatchBytes: 128 << 20, ResidentBytes: 192 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer collection.Close()
			f.system = systemTargetOf(collection)
			reopened, err := Open(f.binding, f.bootstrap, f.system, UserCollection{Name: "docs", Target: f.user}, f.log, f.machine.options)
			if err != nil {
				t.Fatal("machine reopen", err)
			}
			active, err := reopened.ScanActiveExecutionPins(pin.Binding.LedgerHomeGroup, executionpin.PinID{}, 1)
			if err != nil || len(active) != 1 || active[0] != got {
				t.Fatal("frozen active index reopen", err)
			}
			if _, err = reopened.ApplyNormal(normalMeta(7), releaseBytes); err != nil {
				t.Fatal(err)
			}
			final, found, err := reopened.LookupExecutionPin(pin.PinID)
			if err != nil || !found || final.Status != executionpin.StatusReleased || reopened.state.ActiveExecutionPinCount != 0 {
				t.Fatal("exact release after restart", err)
			}
		})
	}
}
