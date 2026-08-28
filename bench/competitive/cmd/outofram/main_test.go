package main

import "testing"

func TestMinimumOverflowHeavyLogicalBytesIsConservative(t *testing.T) {
	for documents := 1; documents < 40; documents++ {
		got := minimumOverflowHeavyLogicalBytes(documents)
		large := int64(documents/8*7 + max(0, documents%8-1))
		if got != large*(16<<10) {
			t.Fatalf("documents=%d minimum=%d", documents, got)
		}
	}
}

func TestParseFlagsRequiresHardStreamingShape(t *testing.T) {
	if _, err := parseFlags([]string{"-engine=vibedb", "-corpus=10", "-max-loader-bytes=0"}); err == nil {
		t.Fatal("accepted zero loader bound")
	}
	cfg, err := parseFlags([]string{
		"-engine=vibedb", "-corpus=10", "-max-loader-bytes=65536", "-max-rss-bytes=32768",
		"-max-physical-write-bytes=131072",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxLoaderBytes != 65536 || cfg.maxRSSBytes != 32768 || cfg.maxPhysicalWriteBytes != 131072 {
		t.Fatalf("bounds not retained: %+v", cfg)
	}
}
