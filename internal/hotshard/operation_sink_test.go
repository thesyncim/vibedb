package hotshard

import (
	"context"
	"errors"
	"testing"
)

func TestOperationSinkRefusesUnserializedOrUnboundAdmission(t *testing.T) {
	catalog, source, _ := hotCatalog(t)
	sink := OperationSink{Catalog: catalog}
	admission := Admission{CatalogGeneration: catalog.Generation(), AuthorityRevision: 1,
		SplitCount: 1, MoveCount: 1}
	admission.ID[0] = 1
	admission.Splits[0].Candidate.Recommendation.Source = source
	if err := sink.SubmitHotShardAdmission(context.Background(), admission); !errors.Is(err, ErrInvalidPressureCut) {
		t.Fatalf("multi-operation admission=%v", err)
	}
	admission.MoveCount = 0
	if err := sink.SubmitHotShardAdmission(context.Background(), admission); !errors.Is(err, ErrInvalidPressureCut) {
		t.Fatalf("unbound split factory=%v", err)
	}
	admission.CatalogGeneration++
	if err := sink.SubmitHotShardAdmission(context.Background(), admission); !errors.Is(err, ErrInvalidPressureCut) {
		t.Fatalf("stale catalog admission=%v", err)
	}
}
