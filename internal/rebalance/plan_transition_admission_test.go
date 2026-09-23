package rebalance

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

func TestEnrolledReplicaMoveRejectsIncompleteOwnedTransitionBeforeAdmission(t *testing.T) {
	cut := failedReplicaEnrolledTestCut(t)
	planned, err := PlanFailedReplicaReplacement(cut)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"missing logical schema", "different enrolled member"} {
		t.Run(change, func(t *testing.T) {
			catalog, request := cut.Catalog, planned.Plan.Request()
			if change == "missing logical schema" {
				raw, err := gateway.AppendSnapshotDocument(nil, catalog)
				if err != nil {
					t.Fatal(err)
				}
				digest := catalog.ReplicatedShardDescriptors()[0].LogicalSchemaDigest
				var zero [32]byte
				before := []byte(`"logical_schema_digest":"` + hex.EncodeToString(digest[:]) + `"`)
				after := []byte(`"logical_schema_digest":"` + hex.EncodeToString(zero[:]) + `"`)
				changed := bytes.Replace(raw, before, after, 1)
				if bytes.Equal(changed, raw) {
					t.Fatal("fixture logical schema was not replaced")
				}
				catalog, err = gateway.OpenSnapshotDocument(changed)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				request.TargetMember++
			}
			if plan, err := PlanReplicaMove(catalog, cut.Publication, request); !errors.Is(err, ErrInvalidPlan) || plan != nil {
				t.Fatalf("enrolled move silently fell back to legacy authority: plan=%v err=%v", plan, err)
			}
		})
	}
}
