package gatewayruntime

import (
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadInitialNodeDirectory(t *testing.T) {
	record := gateway.NodeRecord{NodeID: rafttransport.NodeID{1}, Incarnation: 1, ServiceKeyDigest: replication.Digest{2}, DataEndpoint: "peer", NativeEndpoint: "native", ControlEndpoint: "control", DataAddress: "localhost:1", NativeAddress: "localhost:2", ControlAddress: "localhost:3", Roles: gateway.NodeRoleStorage, FailureDomain: "worker", Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1}
	records := []gateway.NodeRecord{record}
	raw, err := vibejson.Marshal(&records)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "directory")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadInitialNodeDirectory(path)
	if err != nil || len(loaded) != 1 || loaded[0] != record {
		t.Fatalf("prepared directory: %v", err)
	}
	for _, invalid := range [][]byte{nil, []byte("[]"), append(append([]byte(nil), raw...), '\n')} {
		if err := os.WriteFile(path, invalid, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadInitialNodeDirectory(path); err == nil {
			t.Fatal("accepted malformed or noncanonical directory")
		}
	}
	if _, err := loadInitialNodeDirectory(path + "-absent"); err == nil {
		t.Fatal("explicit missing directory accepted")
	}
	if loaded, err := loadInitialNodeDirectory(""); err != nil || loaded != nil {
		t.Fatal("omitted preparation path is not optional")
	}
}
