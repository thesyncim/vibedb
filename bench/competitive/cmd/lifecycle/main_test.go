package main

import "testing"

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
