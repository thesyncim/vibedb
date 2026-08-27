package gateway

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
)

func TestDurableRequestWaveFailureContextPreservesCause(t *testing.T) {
	for _, staged := range []bool{false, true} {
		wave, head, route := lifecycleRunnerFixture(t)
		events := new(lifecycleRunnerEvents)
		ledger := &lifecycleRunnerLedger{head: head, events: events}
		proposer := &lifecycleRunnerProposer{t: t, events: events, faultKind: int(replication.CommandRouteGate),
			faultGate: routegate.OperationAcquireShared, attempts: make(map[replication.CommandKind][][]byte)}
		runner, err := newDurableRequestLifecycleRunner(ledger, &lifecycleRunnerResolver{route: route, events: events}, proposer)
		if err != nil {
			t.Fatal(err)
		}
		wantStage := "wave 0 acquire proposal and proof"
		wantCause := errLifecycleRunnerFault
		if staged {
			wave.Step = requestledger.StepRef{}
			proposer.fenceFaultAt = 1
			_, err = runner.RunStagedWave(t.Context(), wave)
			wantStage = "wave 0 staging execution fence"
			wantCause = ErrDurableRequestConflict
		} else {
			_, err = runner.RunWave(t.Context(), wave)
		}
		if !errors.Is(err, wantCause) || !strings.Contains(err.Error(), wantStage) {
			t.Fatalf("stage=%q err=%v", wantStage, err)
		}
		if strings.Contains(err.Error(), string(wave.Command)) || strings.Contains(err.Error(), string(wave.Tenant)) {
			t.Fatal("failure context exposed request payload or tenant")
		}
	}
}
