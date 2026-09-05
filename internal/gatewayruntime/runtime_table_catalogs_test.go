package gatewayruntime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	vibejson "github.com/thesyncim/vibejson"
)

func TestRuntimeTableCatalogPathListIsBoundedAndCanonical(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table-catalogs.vibejson")
	paths := make([]string, maxGatewayTableCatalogPaths)
	for index := range paths {
		paths[index] = fmt.Sprintf("/node/table-%d/catalog.vibejson", index)
	}
	for _, expected := range [][]string{{}, paths} {
		raw, err := vibejson.Marshal(&expected)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := loadGatewayTableCatalogPaths(path)
		if err != nil || !slices.Equal(got, expected) {
			t.Fatalf("paths=%v err=%v", got, err)
		}
	}
	overflow := append(slices.Clone(paths), "/extra")
	tooMany, err := vibejson.Marshal(&overflow)
	if err != nil {
		t.Fatal(err)
	}
	invalid := []string{"", "null", "{}", "[1]", " []", "[]\n", `[""]`, `["relative"]`,
		`["/a/../b"]`, `["/same","/same"]`, string(tooMany), strings.Repeat(" ", maxGatewayTableCatalogPathBytes+1)}
	for _, raw := range invalid {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadGatewayTableCatalogPaths(path); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid list accepted (%d bytes): %v", len(raw), err)
		}
	}
	if _, err := loadGatewayTableCatalogPaths(filepath.Dir(path)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("directory: %v", err)
	}
	link := filepath.Join(filepath.Dir(path), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadGatewayTableCatalogPaths(link); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("symlink: %v", err)
	}
}

func TestRuntimeControlParticipantRejectsControllerSettings(t *testing.T) {
	config := runtimeReplicatedConfig(t, &runtimeRejectingTransport{})
	config.DevPlaintext = false
	config.ControlParticipantOnly = true
	config.ReplicaControlManifestPath = "/control.vibejson"
	config.withDefaults()
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*Config){
		"plaintext":         func(c *Config) { c.DevPlaintext = true },
		"static":            func(c *Config) { c.DevStaticCatalog = true },
		"missing-roster":    func(c *Config) { c.ReplicaControlManifestPath = "" },
		"hot-shard":         func(c *Config) { c.HotShardCapacityPath = "/capacity" },
		"backup":            func(c *Config) { c.BackupRepositoryPath = "/backups" },
		"schema-rollout":    func(c *Config) { c.SchemaRolloutPlan = "/plan" },
		"schema-once":       func(c *Config) { c.SchemaRolloutOnce = true },
		"registration-list": func(c *Config) { c.TableCatalogsPath = "/catalog-list" },
		"registration":      func(c *Config) { c.TableCatalogs = []string{"/catalog"} },
		"pg-without-owner":  func(c *Config) { c.PGListenAddress = "127.0.0.1:7100" },
		"owner-without-pg":  func(c *Config) { c.DDLOwnerAddress = "127.0.0.1:7200"; c.DDLOwnerNode = rafttransport.NodeID{9} },
	} {
		t.Run(name, func(t *testing.T) {
			bad := config
			edit(&bad)
			if err := bad.validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("accepted: %v", err)
			}
		})
	}
	config.PGListenAddress, config.PGDDLSocket = "127.0.0.1:7100", "/shared/pg-ddl.sock"
	config.DDLOwnerAddress, config.DDLOwnerNode = "127.0.0.1:7200", rafttransport.NodeID{9}
	if err := config.validate(); err != nil {
		t.Fatalf("participant forwarding with shared supervisor: %v", err)
	}
	config.ControlParticipantOnly = false
	if err := config.validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("owner can also forward: %v", err)
	}
}
