package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryOracleIsIndependentAndBoundToWorkload(t *testing.T) {
	c := config{engine: "vibedb", rows: 2, recoveryOracle: filepath.Join(t.TempDir(), "oracle.json")}
	tables := []string{"test_a", "test_b"}
	scores := [][]int{{1, 2}, {3, 4}}
	if err := writeRecoveryOracle(c, tables, scores); err != nil {
		t.Fatal(err)
	}
	scores[0][0] = 99
	loaded, err := readRecoveryOracle(c, tables)
	if err != nil || loaded[0][0] != 1 {
		t.Fatalf("oracle changed: %v %v", loaded, err)
	}
	if err = writeRecoveryOracle(c, tables, scores); err == nil {
		t.Fatal("overwrote immutable oracle")
	}
	if _, err = readRecoveryOracle(c, []string{"test_b", "test_a"}); err == nil {
		t.Fatal("accepted reordered groups")
	}
	c.rows = 3
	if _, err = readRecoveryOracle(c, tables); err == nil {
		t.Fatal("accepted wrong rows")
	}
	c.rows = 2
	f, err := os.OpenFile(c.recoveryOracle, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("{}")
	_ = f.Close()
	if _, err = readRecoveryOracle(c, tables); err == nil {
		t.Fatal("accepted trailing oracle data")
	}
}
