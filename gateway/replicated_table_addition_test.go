package gateway

import "testing"

func TestReplicatedProvisionProfileAcceptsSchemaSuccessor(t *testing.T) {
	origin := ReplicatedTableProfile{Table: "employees", Relation: 1, PrimaryKey: "/id",
		SchemaGeneration: 1, LogicalSchemaDigest: [32]byte{1}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20}
	current := origin
	current.SchemaGeneration = 2
	current.LogicalSchemaDigest = [32]byte{2}
	if !replicatedProvisionProfileMatches(current, origin) {
		t.Fatal("authorized schema successor rejected as conflicting provisioning")
	}
	for _, mutate := range []func(*ReplicatedTableProfile){
		func(p *ReplicatedTableProfile) { p.Relation++ },
		func(p *ReplicatedTableProfile) { p.PrimaryKey = "/other" },
		func(p *ReplicatedTableProfile) { p.MaxDocumentBytes++ },
		func(p *ReplicatedTableProfile) { p.SchemaGeneration = 0 },
	} {
		changed := current
		mutate(&changed)
		if replicatedProvisionProfileMatches(changed, origin) {
			t.Fatal("incompatible provision profile accepted")
		}
	}
	sameGeneration := origin
	sameGeneration.LogicalSchemaDigest[0] ^= 1
	if replicatedProvisionProfileMatches(sameGeneration, origin) {
		t.Fatal("same-generation logical schema conflict accepted")
	}
}

func TestReplicatedTableAdditionResumeAcceptsEvolvedDeclaration(t *testing.T) {
	origin := []ReplicatedTableDeclaration{{Table: "messages", CreateTable: "CREATE TABLE messages (PRIMARY KEY (id))"}}
	evolved := []ReplicatedTableDeclaration{{Table: "messages", CreateTable: "CREATE TABLE messages (id TEXT PRIMARY KEY, city TEXT)"}}
	if !replicatedProvisionDeclarationsMatch(2, 1, evolved, origin) {
		t.Fatal("evolved declaration rejected after authenticated schema successor")
	}
	if replicatedProvisionDeclarationsMatch(1, 1, evolved, origin) {
		t.Fatal("same-generation declaration conflict accepted")
	}
}
