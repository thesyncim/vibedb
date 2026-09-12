package gatewayruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

type scalingProgressDirectory struct {
	gateway.DirectoryReader
	gateway.DirectoryWriter
	parent     gateway.ScalingIntent
	rows       []gateway.GroupEnrollmentIntent
	writeCount int
	beforeList func()
}

func (directory *scalingProgressDirectory) ReadScalingIntent(context.Context, [32]byte) (gateway.ScalingIntent, error) {
	return directory.parent, nil
}

func (directory *scalingProgressDirectory) ListEnrollmentIntents(context.Context, raftmember.GroupKey) ([]gateway.GroupEnrollmentIntent, error) {
	if directory.beforeList != nil {
		directory.beforeList()
	}
	return directory.rows, nil
}

func (directory *scalingProgressDirectory) PutScalingIntent(_ context.Context, next gateway.ScalingIntent, expected uint64) error {
	if directory.parent.Revision != expected {
		return gateway.ErrScalingRevision
	}
	directory.parent = next
	directory.writeCount++
	return nil
}

func TestScalingProgressPreservesCompletedChildrenAfterHistoryCollection(t *testing.T) {
	parent := gateway.ScalingIntent{ID: [32]byte{1}, Revision: 4, DirectoryRevision: 4,
		State: gateway.ScalingRunning, PlannedReplicas: 1025, CompletedReplicas: 1025}
	directory := &scalingProgressDirectory{parent: parent}
	controller := &ScalingController{directory: directory, writer: directory,
		catalog: scalingEnrollmentCatalogFixture{snapshot: catalogRouteSeedSnapshot(t, 2, "127.0.0.1:7101")}}
	stale := parent
	stale.Revision--
	stale.CompletedReplicas--
	next, err := controller.reconcileIntentProgress(t.Context(), stale)
	if err != nil || next == nil || next.CompletedReplicas != 1025 || next.PlannedReplicas != 1025 || directory.writeCount != 0 {
		t.Fatalf("recovered progress=%+v writes=%d err=%v", next, directory.writeCount, err)
	}
}

func TestScalingProgressCannotOverwriteConcurrentChildCompletion(t *testing.T) {
	parent := gateway.ScalingIntent{ID: [32]byte{1}, Revision: 4, DirectoryRevision: 4,
		State: gateway.ScalingRunning, PlannedReplicas: 1}
	directory := &scalingProgressDirectory{parent: parent}
	// The child completes atomically with parent accounting after the parent
	// read, so it has disappeared by the time the active directory is read.
	directory.beforeList = func() {
		directory.parent.CompletedReplicas = 1
		directory.parent.Revision++
		directory.parent.DirectoryRevision++
	}
	controller := &ScalingController{directory: directory, writer: directory,
		catalog: scalingEnrollmentCatalogFixture{snapshot: catalogRouteSeedSnapshot(t, 2, "127.0.0.1:7101")}}
	if _, err := controller.reconcileIntentProgress(t.Context(), parent); !errors.Is(err, gateway.ErrScalingRevision) {
		t.Fatalf("concurrent completion err=%v", err)
	}
	if directory.parent.CompletedReplicas != 1 || directory.parent.PlannedReplicas != 1 || directory.writeCount != 0 {
		t.Fatalf("overwrote atomic progress: %+v", directory.parent)
	}
}

func TestScalingRemainingBudgetSurvivesCompletedWaves(t *testing.T) {
	intent := gateway.ScalingIntent{Request: gateway.ScalingIntentRequest{MaxMoves: 5, MaxMigrationBytes: 100},
		PlannedReplicas: 3, CompletedReplicas: 3, AdmittedMigrationBytes: 70}
	remaining, err := scalingRemainingRequest(intent)
	if err != nil || remaining.MaxMoves != 2 || remaining.MaxMigrationBytes != 30 {
		t.Fatalf("remaining=%+v err=%v", remaining, err)
	}
	intent.PlannedReplicas = 5
	if _, err := scalingRemainingRequest(intent); !errors.Is(err, ErrScalingControllerBlocked) {
		t.Fatalf("exhausted moves=%v", err)
	}
	intent.PlannedReplicas = 3
	intent.AdmittedMigrationBytes = 100
	if _, err := scalingRemainingRequest(intent); !errors.Is(err, ErrScalingControllerBlocked) {
		t.Fatalf("exhausted bytes=%v", err)
	}
	intent.Request.MaxMigrationBytes = 0
	if remaining, err := scalingRemainingRequest(intent); err != nil || remaining.MaxMigrationBytes != 0 {
		t.Fatalf("unlimited bytes remaining=%+v err=%v", remaining, err)
	}
}
