package driver

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

const replicatedEmployeesDDL = `CREATE TABLE employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, team TEXT NOT NULL, city TEXT, score INTEGER NOT NULL, active BOOLEAN NOT NULL)`

func declaredEmployeeSchema(t testing.TB) *store.Schema {
	t.Helper()
	statement, err := query.PrepareDML(replicatedEmployeesDDL)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()
	definition, err := statement.LowerTable()
	if err != nil {
		t.Fatal(err)
	}
	return definition.Schema
}

func TestReplicatedDeclaredSchemaBindReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "employees.vdb")
	binding := testReplicatedBinding(41)
	db, err := InitializeShardStore(path, ShardStoreBinding{
		Distribution: distribution.DistributionName(binding.Distribution), Shard: distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	session, err := db.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, replicatedEmployeesDDL, nil); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := db.BindReplicatedShardStore(binding, "employees")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		core := db.connector.db
		if !identity.IsZero() || core.catalog.ReplicatedShardStore != nil || core.tables["employees"].collection != nil {
			t.Fatal("unsupported allocation retained a partial bind")
		}
		t.Skip("strict allocation requires Linux; exercised in Docker")
	}
	if err != nil {
		t.Fatal(err)
	}
	if identity.Relations[0].SchemaDigest == ([sha256.Size]byte{}) {
		t.Fatal("uncommitted declared schema")
	}
	if err := ValidateReplicatedChildSchema(identity, replicatedEmployeesDDL, nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []string{
		`CREATE TABLE employees (PRIMARY KEY (id))`,
		strings.Replace(replicatedEmployeesDDL, "score INTEGER", "score TEXT", 1),
		strings.Replace(replicatedEmployeesDDL, "team TEXT NOT NULL", "team TEXT", 1),
	} {
		if err := ValidateReplicatedChildSchema(identity, wrong, nil, nil); err == nil {
			t.Fatalf("accepted wrong schema: %s", wrong)
		}
	}
	encoded, err := identity.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded ReplicatedShardStoreIdentity
	if err := decoded.UnmarshalJSON(encoded); err != nil || !decoded.Equal(identity) {
		t.Fatalf("roundtrip: %v", err)
	}
	initial, err := InitialReplicatedLogicalSchemaDigest(binding, testReplicatedApplyOptions().Placement,
		InitialReplicatedRelationSchema{Table: "employees", PrimaryKey: "/id", Limits: identity.UserLimits, Schema: declaredEmployeeSchema(t)})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := ReplicatedRelationManifestDigest(identity)
	if err != nil || actual != initial {
		t.Fatalf("initial and bound schema differ: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStore(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.connector.db.tables["employees"].collection.HasSchema() {
		t.Fatal("lost durable schema on reopen")
	}
	claim, applyIdentity, err := reopened.OpenReplicatedApply(identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, identity, 2)
	_, key, valid := employeeValidator(t)
	for i, value := range [][]byte{valid, []byte(strings.Replace(string(valid), `"score":92`, `"score":"bad"`, 1))} {
		command := testReplicatedApplyCommand(identity, epoch, uint64(i+2), replication.Mutation{Kind: replication.MutationPut, Key: key, Value: value})
		admissionErr := claim.AdmitCommand(command)
		if i == 0 && admissionErr != nil {
			t.Fatalf("valid document admission: %v", admissionErr)
		}
		if i == 1 && admissionErr == nil {
			t.Fatal("invalid declared type passed admission")
		}
		// Replay must validate too: another member may have proposed these
		// bytes, so local pre-admission is not the state machine's authority.
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(uint64(i+3)), command); err != nil {
			t.Fatal(err)
		}
		lookup, err := claim.LookupCompletion(command)
		if err != nil {
			t.Fatal(err)
		}
		completion, err := replication.OpenCompletion(lookup.Bytes)
		want := uint32(replicatedstate.ResultApplied)
		if i == 1 {
			want = replicatedstate.ResultInvalidDocument
		}
		if err != nil || completion.ResultCode != want {
			t.Fatalf("typed apply %d: %+v %v", i, completion, err)
		}
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := OpenReplicatedShardStoreWithApply(path, identity, applyIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	againClaim, _, err := again.OpenReplicatedApply(identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer againClaim.Close()
	value, found, err := again.connector.db.tables["employees"].collection.AppendRaw(nil, key)
	if err != nil || !found || !strings.Contains(string(value), `"score":92`) {
		t.Fatalf("reopen after invalid write: %s %t %v", value, found, err)
	}
}

func employeeValidator(t testing.TB) (*replicatedSQLMutationValidator, []byte, []byte) {
	t.Helper()
	primary, err := vibejson.CompilePointer("/id")
	if err != nil {
		t.Fatal(err)
	}
	identity := ReplicatedShardStoreIdentity{UserPrimaryKey: "/id", UserLimits: ReplicatedShardStoreLimits{MaxKeyBytes: 256, MaxDocumentBytes: 4096}}
	validator := newReplicatedSQLMutationValidator(identity, &table{primary: primary, schema: declaredEmployeeSchema(t)}, testReplicatedApplyOptions().Placement)
	value := []byte(`{"id":"employee-0001","name":"Alex","team":"Platform","city":"Lisbon","score":92,"active":true}`)
	key, err := documentKey(value, "/id", primary, 256)
	if err != nil {
		t.Fatal(err)
	}
	return validator, []byte(key), value
}

func TestReplicatedDeclaredSchemaValidationAllocationFree(t *testing.T) {
	v, key, value := employeeValidator(t)
	owned := testReplicatedApplyOptions().Placement.Range
	if got := v.ValidatePut(key, value); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("valid put: %v", got)
	}
	for _, invalid := range []string{
		strings.Replace(string(value), `"score":92`, `"score":"92"`, 1),
		strings.Replace(string(value), `"name":"Alex",`, ``, 1),
		strings.Replace(string(value), `"active":true`, `"active":null`, 1),
	} {
		if v.ValidatePut(key, []byte(invalid)) != replicatedstate.MutationValidationInvalid ||
			v.ValidatePutOwnership(key, []byte(invalid), owned) != replicatedstate.MutationValidationInvalid {
			t.Fatalf("accepted invalid declared document: %s", invalid)
		}
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if v.ValidatePut(key, value) != replicatedstate.MutationValidationAccept {
			panic("invalid put")
		}
		if v.ValidatePutOwnership(key, value, owned) != replicatedstate.MutationValidationAccept {
			panic("invalid ownership")
		}
	}); allocs != 0 {
		t.Fatalf("warm validation allocations: %g", allocs)
	}
}

func BenchmarkReplicatedDeclaredSchemaValidation(b *testing.B) {
	for _, typed := range []bool{false, true} {
		name := "schema-free"
		if typed {
			name = "declared"
		}
		b.Run(name, func(b *testing.B) {
			v, key, value := employeeValidator(b)
			if !typed {
				v.schema = nil
			}
			v.ValidatePut(key, value)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				v.ValidatePut(key, value)
			}
		})
	}
}
