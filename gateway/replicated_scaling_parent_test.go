package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func scalingTestRunningParent(t *testing.T, authority *ReplicatedCatalogAuthority, target NodeRecord) ScalingIntent {
	t.Helper()
	return scalingTestRunningParentWithBudget(t, authority, target, 1, 0)
}

func scalingTestRunningParentWithBudget(t *testing.T, authority *ReplicatedCatalogAuthority, target NodeRecord, moves uint16, migrationBytes uint64) ScalingIntent {
	t.Helper()
	request := ScalingIntentRequest{Kind: ScalingScaleOut, RequestID: [32]byte{0xe3},
		Targets: []NodeReference{{NodeID: target.NodeID, Incarnation: target.Incarnation}}, MaxMoves: moves, MaxMigrationBytes: migrationBytes}
	parent := ScalingIntent{ID: request.ID(), Request: request, CatalogGeneration: 5,
		Revision: 1, DirectoryRevision: 1, State: ScalingReserved}
	if err := authority.SubmitScalingIntent(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	parent.State, parent.Revision, parent.DirectoryRevision = ScalingRunning, 2, 2
	if err := authority.PutScalingIntent(context.Background(), parent, 1); err != nil {
		t.Fatal(err)
	}
	return parent
}

func TestScalingParentCancellationFencesEnrollmentAdmission(t *testing.T) {
	for _, phase := range []string{"before-admission", "admission-during-cancel", "reserved", "claimed", "prepared"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			authority, client, current := newCatalogAuthorityFixture(t)
			peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0xea)
			target := scalingTestNodeRecord([16]byte{0x91}, 1, NodeJoining, 1)
			if err := authority.PutNode(ctx, target, 0); err != nil {
				t.Fatal(err)
			}
			target.Lifecycle, target.Revision = NodeActive, 2
			if err := authority.PutNode(ctx, target, 1); err != nil {
				t.Fatal(err)
			}
			parent := scalingTestRunningParent(t, authority, target)
			child := scalingTestEnrollmentIntent(0x92, target.NodeID[0], target.Revision)
			child.ParentScalingIntentID = parent.ID
			child.ReservedMigrationBytes = 7
			if phase == "admission-during-cancel" {
				var admissionErr error
				client.onRead = func(key []byte) {
					if !bytes.Equal(key, enrollmentDirectoryKey) {
						return
					}
					client.mu.Lock()
					client.onRead = nil
					client.mu.Unlock()
					admissionErr = peer.SubmitEnrollmentIntent(ctx, child)
				}
				if _, err := authority.CancelScalingIntent(ctx, parent.ID, parent.Revision); err == nil {
					t.Fatal("cancellation committed over concurrent child admission")
				}
				if admissionErr != nil {
					t.Fatalf("concurrent admission: %v", admissionErr)
				}
				parent, err := authority.ReadScalingIntent(ctx, parent.ID)
				if err != nil || parent.State != ScalingRunning || parent.PlannedReplicas != 1 {
					t.Fatalf("concurrent child lost running parent: %+v err=%v", parent, err)
				}
				return
			}
			if phase == "before-admission" {
				if _, err := peer.CancelScalingIntent(ctx, parent.ID, parent.Revision); err != nil {
					t.Fatal(err)
				}
				if err := authority.SubmitEnrollmentIntent(ctx, child); !errors.Is(err, ErrScalingState) {
					t.Fatalf("stale child admitted after parent cancellation: %v", err)
				}
				return
			}
			if err := authority.SubmitEnrollmentIntent(ctx, child); err != nil {
				t.Fatal(err)
			}
			var err error
			child, err = authority.ReadEnrollmentIntent(ctx, child.IntentID)
			if err != nil {
				t.Fatal(err)
			}
			if phase != "reserved" {
				child, err = authority.ClaimEnrollmentPreparation(ctx, child.IntentID, child.Revision)
				if err != nil {
					t.Fatal(err)
				}
			}
			if phase == "prepared" {
				prior := child.Revision
				child.State, child.Revision, child.PreparationClaim = EnrollmentPrepared, prior+1, [32]byte{}
				proof := scalingTestPreparedProof(child, target.Revision)
				child.Proof = &proof
				if err := authority.PutEnrollmentIntent(ctx, child, prior); err != nil {
					t.Fatal(err)
				}
			}
			parent, err = peer.ReadScalingIntent(ctx, parent.ID)
			if err != nil || parent.PlannedReplicas != 1 || len(parent.OutstandingMoves) != 0 {
				t.Fatalf("unexpected pre-journal parent=%+v err=%v", parent, err)
			}
			if _, err := peer.CancelScalingIntent(ctx, parent.ID, parent.Revision); !errors.Is(err, ErrScalingState) {
				t.Fatalf("parent cancelled with %s child: %v", phase, err)
			}
			if phase == "reserved" {
				if _, err := authority.CancelEnrollmentIntent(ctx, child.IntentID, child.Revision); err != nil {
					t.Fatal(err)
				}
				parent, err = peer.ReadScalingIntent(ctx, parent.ID)
				if err != nil || parent.PlannedReplicas != 0 || parent.AdmittedMigrationBytes != 0 {
					t.Fatalf("cancelled child retained planned count=%+v err=%v", parent, err)
				}
				if _, err := peer.CancelScalingIntent(ctx, parent.ID, parent.Revision); err != nil {
					t.Fatalf("parent cancellation after child cancellation: %v", err)
				}
			}
		})
	}
}

func TestScalingParentAdmissionBudgetsAreCumulative(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		moves                 uint16
		budget, first, second uint64
		allowed               bool
	}{
		{name: "move-limit", moves: 1, first: 4, second: 1},
		{name: "byte-limit", moves: 2, budget: 5, first: 4, second: 2},
		{name: "byte-limit-exact", moves: 2, budget: 5, first: 4, second: 1, allowed: true},
		{name: "byte-overflow", moves: 2, first: ^uint64(0), second: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			authority, _, _ := newCatalogAuthorityFixture(t)
			target := scalingTestNodeRecord([16]byte{0x93}, 1, NodeJoining, 1)
			if err := authority.PutNode(ctx, target, 0); err != nil {
				t.Fatal(err)
			}
			target.Lifecycle, target.Revision = NodeActive, 2
			if err := authority.PutNode(ctx, target, 1); err != nil {
				t.Fatal(err)
			}
			parent := scalingTestRunningParentWithBudget(t, authority, target, tc.moves, tc.budget)
			first := scalingTestEnrollmentIntent(0x94, target.NodeID[0], target.Revision)
			first.ParentScalingIntentID, first.ReservedMigrationBytes = parent.ID, tc.first
			if err := authority.SubmitEnrollmentIntent(ctx, first); err != nil {
				t.Fatal(err)
			}
			second := scalingTestEnrollmentIntent(0x95, target.NodeID[0], target.Revision)
			second.Distribution = "another-distribution"
			second.ParentScalingIntentID, second.ReservedMigrationBytes = parent.ID, tc.second
			err := authority.SubmitEnrollmentIntent(ctx, second)
			if tc.allowed && err != nil || !tc.allowed && !errors.Is(err, ErrScalingMetadataBound) {
				t.Fatalf("second admission allowed=%v err=%v", tc.allowed, err)
			}
			stored, err := authority.ReadScalingIntent(ctx, parent.ID)
			wantMoves, wantBytes := uint32(1), tc.first
			if tc.allowed {
				wantMoves++
				wantBytes += tc.second
			}
			if err != nil || stored.PlannedReplicas != wantMoves || stored.AdmittedMigrationBytes != wantBytes {
				t.Fatalf("admitted budget=%+v want moves=%d bytes=%d err=%v", stored, wantMoves, wantBytes, err)
			}
		})
	}
}
