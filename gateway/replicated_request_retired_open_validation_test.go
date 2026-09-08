package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestValidateRetiredOpenCutRejectsStaleForeignAndTerminalEvidence(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	identity, err := requestledger.DeriveRouteGateIdentity(
		initial.KeyDigest, initial.RequestDigest, initial.PlanRoot,
		initial.ContinuationDigest, wave.PinID, initial.NextStepOrdinal,
	)
	if err != nil {
		t.Fatal(err)
	}
	clientID, err := durableRouteSessionIdentity(identity, route, wave.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	session := &NativeSession{
		route: route, distribution: string(route.Distribution), shard: string(route.Shard),
		tenant: wave.Tenant, clientID: clientID, retryHome: wave.Identity.RetryHome,
		maxCommand: requestledger.MaxRouteGatePinCommandBytes, epoch: 2, nextSequence: 2, ackThrough: 1,
		proposalCapability: serviceauthz.CapabilityDataWrite, scopedCoordination: true,
		membershipStableCoordination: true,
	}
	acquire, physical, err := appendDurableRequestRouteGateCommand(
		nil, route, wave, initial.KeyDigest, initial.RequestDigest, initial.PlanRoot,
		initial.ContinuationDigest, initial.NextStepOrdinal, routegate.OperationAcquireShared, session,
	)
	if err != nil {
		t.Fatal(err)
	}
	acquiring, err := requestledger.NewRoutePinAcquiring(initial, wave.PinID, wave.Binding, physical, acquire)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := requestledger.RecordVerifiedRoutePinAcquired(
		acquiring, acquiring.Revision+1, lifecycleRunnerCompletion(t, mustOpenCommand(t, acquire)),
	)
	if err != nil {
		t.Fatal(err)
	}
	acquiredHead, err := requestledger.AdvanceHeadRoutePin(initial, acquiring, acquired, initial.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := requestledger.NewPendingWaveWithRoutePin(
		acquiredHead, wave.Build, acquiredHead.Revision+1, acquired, []requestledger.StepRef{wave.Step},
	)
	if err != nil {
		t.Fatal(err)
	}
	cutHead, err := requestledger.InstallPendingWave(acquiredHead, pending, wave.Build, acquired)
	if err != nil {
		t.Fatal(err)
	}
	cut := durableRequestRetiredOpenCut{head: cutHead, route: acquired, pending: pending, applied: cutHead.Revision}
	openHeader := durableRequestSessionOpenHeader(session)
	openCommand, err := durableRequestSessionOpenCommand(openHeader, session.maxCommand)
	if err != nil {
		t.Fatal(err)
	}
	retired := &durableRequestRetiredOpenError{cause: replicatedstate.ErrRetryRetired, openCommand: openCommand}
	if err := validateRetiredOpenCut(wave, initial, route, cut, retired); err != nil {
		t.Fatalf("valid retained cut rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*requestledger.HeadRecord, *requestledger.RoutePinRecord, *requestledger.PendingWaveRecord)
	}{
		{name: "no progress", mutate: func(head *requestledger.HeadRecord, _ *requestledger.RoutePinRecord, _ *requestledger.PendingWaveRecord) {
			head.Revision = initial.Revision
		}},
		{name: "missing pin", mutate: func(_ *requestledger.HeadRecord, pin *requestledger.RoutePinRecord, _ *requestledger.PendingWaveRecord) {
			*pin = requestledger.RoutePinRecord{}
		}},
		{name: "foreign request", mutate: func(_ *requestledger.HeadRecord, pin *requestledger.RoutePinRecord, _ *requestledger.PendingWaveRecord) {
			pin.RequestDigest[0]++
		}},
		{name: "terminal head", mutate: func(head *requestledger.HeadRecord, _ *requestledger.RoutePinRecord, _ *requestledger.PendingWaveRecord) {
			head.Phase = requestledger.PhaseTerminal
		}},
		{name: "acked head", mutate: func(head *requestledger.HeadRecord, _ *requestledger.RoutePinRecord, _ *requestledger.PendingWaveRecord) {
			head.Phase = requestledger.PhaseAcked
		}},
		{name: "foreign pending witness", mutate: func(_ *requestledger.HeadRecord, _ *requestledger.RoutePinRecord, pending *requestledger.PendingWaveRecord) {
			pending.ForwardingWitnessDigest[0]++
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			bad := cut
			bad.head.InlinePlan = bytes.Clone(cut.head.InlinePlan)
			bad.route.Command, bad.route.Completion = bytes.Clone(cut.route.Command), bytes.Clone(cut.route.Completion)
			bad.pending.Steps = append([]requestledger.StepRef(nil), cut.pending.Steps...)
			testCase.mutate(&bad.head, &bad.route, &bad.pending)
			if err := validateRetiredOpenCut(wave, initial, route, bad, retired); !errors.Is(err, ErrDurableRequestConflict) {
				t.Fatalf("invalid cut accepted: %v", err)
			}
		})
	}
}

func TestDurableRequestRetiredOpenRefusalRequiresExactTypedOutcome(t *testing.T) {
	exact := &ReplicatedRefusalError{
		Code:    shardservice.ReplicatedRefusalRetryRetired,
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeRetryRetired},
	}
	for _, testCase := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "exact", err: exact, want: true},
		{name: "joined", err: errors.Join(exact, errors.New("terminal"))},
		{name: "wrapped", err: fmt.Errorf("wrapped: %w", exact)},
		{name: "unknown", err: &ReplicatedRefusalError{
			Code:    shardservice.ReplicatedRefusalRetryRetired,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionActive},
		}},
		{name: "different refusal", err: &ReplicatedRefusalError{
			Code:    shardservice.ReplicatedRefusalDeterministic,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeRetryRetired},
		}},
		{name: "nil", err: nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := durableRequestRetiredOpenRefusal(testCase.err); got != testCase.want {
				t.Fatalf("classifier=%v want=%v err=%v", got, testCase.want, testCase.err)
			}
		})
	}
}

func TestDurableRequestRetiredOpenRefreshRequiresLiveCoherentCut(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	retired := &durableRequestRetiredOpenError{openCommand: []byte{1}}
	noCutRunner := &DurableRequestLifecycleRunner{
		ledger: &lifecycleRunnerLedger{head: initial, events: new(lifecycleRunnerEvents)},
	}
	if _, err := noCutRunner.refreshRetiredOpenCut(
		context.Background(), wave, initial.KeyDigest, initial, route, retired,
	); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("compatibility row reader accepted retired Open recovery: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cutLedger := &retiredOpenWaveLedger{lifecycleRunnerLedger: &lifecycleRunnerLedger{
		head: initial, events: new(lifecycleRunnerEvents),
	}}
	cutRunner := &DurableRequestLifecycleRunner{ledger: cutLedger}
	if _, err := cutRunner.refreshRetiredOpenCut(ctx, wave, initial.KeyDigest, initial, route, retired); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("canceled retired Open recovery err=%v", err)
	}
	if got := cutLedger.readCount(); got != 0 {
		t.Fatalf("canceled recovery read=%d", got)
	}
}

func mustOpenCommand(t testing.TB, raw []byte) replication.CommandView {
	t.Helper()
	command, err := replication.OpenCommand(raw)
	if err != nil {
		t.Fatal(err)
	}
	return command
}
