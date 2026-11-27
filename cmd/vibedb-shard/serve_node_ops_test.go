package main

import (
	"testing"
)

func TestServeNodeRejectsNegativeOpsDurations(t *testing.T) {
	if code := runServeNode([]string{"-manifest", "/nonexistent", "-frontend-drain-grace-period", "-1s"}); code != 2 {
		t.Fatalf("negative drain grace = %d, want 2", code)
	}
	if code := runServeNode([]string{"-manifest", "/nonexistent", "-slow-query-threshold", "-5ms"}); code != 2 {
		t.Fatalf("negative slow-query threshold = %d, want 2", code)
	}
}
