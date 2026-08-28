package splitcontroller

import (
	"testing"

	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestSplitGlobalRelationCollectionUsesExactSourceSlot(t *testing.T) {
	plan, _, _ := testReplicatedProjectionPlan(t)
	base := sqldriver.ReplicatedShardRelationIdentity{Relation: 1, Kind: sqldriver.ReplicatedShardRelationJSON, Table: plan.partitioner.CollectionName()}
	global := splitGlobalIndexRelation(true)
	global.Table = "emails"
	// This checks the final inventory predicate after NewPlan has authenticated
	// the complete source/child schema digests. The physical composed gateway
	// test exercises those digest checks with real base/local/global SQL stores.
	plan.sourceAuthority.Schema.SQL.Relations = []sqldriver.ReplicatedShardRelationIdentity{base, global}
	if !plan.validRelationCollection(0, base) || !plan.validRelationCollection(1, global) {
		t.Fatal("exact global-index collection differs legitimately from base table")
	}
	for name, mutate := range map[string]func(*sqldriver.ReplicatedShardRelationIdentity){
		"foreign table": func(r *sqldriver.ReplicatedShardRelationIdentity) { r.Table = "foreign_emails" },
		"base alias":    func(r *sqldriver.ReplicatedShardRelationIdentity) { r.Table = base.Table },
		"foreign slot":  func(r *sqldriver.ReplicatedShardRelationIdentity) { r.Relation++ },
		"foreign kind":  func(r *sqldriver.ReplicatedShardRelationIdentity) { r.Kind = sqldriver.ReplicatedShardRelationJSON },
	} {
		t.Run(name, func(t *testing.T) {
			foreign := global
			mutate(&foreign)
			if plan.validRelationCollection(1, foreign) {
				t.Fatal("foreign global collection identity accepted")
			}
		})
	}
	foreignBase := base
	foreignBase.Table = global.Table
	if plan.validRelationCollection(0, foreignBase) {
		t.Fatal("foreign base collection accepted")
	}
	plan.sourceAuthority.Schema.SQL.Relations = plan.sourceAuthority.Schema.SQL.Relations[:1]
	if plan.validRelationCollection(1, global) {
		t.Fatal("global collection accepted without authenticated source slot")
	}
	plan.sourceAuthority = nil
	if plan.validRelationCollection(1, global) {
		t.Fatal("distinct global collection accepted without replicated source authority")
	}
}
