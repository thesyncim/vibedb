package main

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFlagsRejectsUncontrolledShapes(t *testing.T) {
	if _, err := parseFlags([]string{"-engine=vibedb", "-mode=warmish"}); err == nil {
		t.Fatal("accepted unknown cache mode")
	}
	if _, err := parseFlags([]string{"-engine=vibedb", "-exact-indexes=4"}); err == nil {
		t.Fatal("accepted impossible exact index count")
	}
	if _, err := parseFlags([]string{"-engine=vibedb", "-internal-child=timed"}); err == nil {
		t.Fatal("accepted child without prepared directory")
	}
}

func TestIsolatedVerifyAndRepackLifecycleSmoke(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "lifecycle")
	command := exec.Command("go", "build", "-o", binary, ".")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build lifecycle: %v: %s", err, output)
	}
	t.Setenv("VIBEDB_LIFECYCLE_TEST_BINARY", binary)
	for _, mode := range []string{"verify", "repack"} {
		t.Run(mode, func(t *testing.T) {
			cfg, err := parseFlags([]string{"-engine=vibedb", "-mode=" + mode, "-corpus=64",
				"-durability=ordinary-sync", "-exact-indexes=1", "-max-rss-bytes=1073741824",
				"-max-physical-write-bytes=1073741824"})
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := run(cfg, &out); err != nil {
				t.Fatal(err)
			}
			for _, token := range []string{"durability", "exact-indexes", mode + "-ns", "physical-write-known", "vibedb\t" + mode} {
				if !strings.Contains(out.String(), token) {
					t.Fatalf("%s output omits %q:\n%s", mode, token, out.String())
				}
			}
		})
	}
}

func TestParseFlagsRetainsHardBounds(t *testing.T) {
	cfg, err := parseFlags([]string{
		"-engine=vibedb", "-mode=cold", "-max-rss-bytes=123", "-max-physical-write-bytes=456",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxRSSBytes != 123 || cfg.maxPhysicalWriteBytes != 456 {
		t.Fatalf("bounds = %d/%d", cfg.maxRSSBytes, cfg.maxPhysicalWriteBytes)
	}
}
