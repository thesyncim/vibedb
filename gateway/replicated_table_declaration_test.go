package gateway

import (
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestReplicatedTableDeclarationsColdPersistenceAndHotResolution(t *testing.T) {
	config, endpoints, descriptor, profile := testReplicatedTableInput(t)
	descriptor.RequestLedgerRanges = []DurableRequestLedgerRangeDescriptor{{Identity: replication.Digest{0x91}}}
	declarations := []ReplicatedTableDeclaration{{Table: "messages", CreateTable: `CREATE TABLE messages (id TEXT PRIMARY KEY, city TEXT, score INTEGER NOT NULL)`}}
	snapshot, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor}, []ReplicatedTableProfile{profile}, declarations)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := orderedkey.AppendString(nil, []byte("a"), orderedkey.Ascending)
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalar [1024]byte
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, ok := snapshot.ResolveReplicatedTableKey([]byte("messages"), key, scalar[:0], replicas[:0]); !ok {
			panic("unresolved table")
		}
	}); allocations != 0 {
		t.Fatalf("hot routing allocations: %g", allocations)
	}
	encoded, err := AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := OpenSnapshotDocument(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.ReplicatedTableDeclarations(), declarations) {
		t.Fatal("declaration lost in persistence")
	}
	info, ok := loaded.declaredTableInfo("messages")
	if !ok || len(info.Columns) != 3 || info.Columns[0].Path != "/id" || !info.Columns[0].Required || info.Columns[1].Required || !info.Columns[2].Required {
		t.Fatalf("columns: %+v", info)
	}
	info.Columns[0].Path = "/wrong"
	again, _ := loaded.declaredTableInfo("messages")
	if again.Columns[0].Path != "/id" {
		t.Fatal("metadata aliases caller memory")
	}
}
