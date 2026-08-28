package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDurableAckKeyIsStrictSharedFixedWidth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ack.key")
	if err := os.WriteFile(path, []byte(strings.Repeat("01", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := loadDurableAckKey(path)
	if err != nil || key[0] != 1 || key[31] != 1 {
		t.Fatalf("key=%x err=%v", key, err)
	}
	for _, malformed := range []string{
		strings.Repeat("01", 31),
		strings.Repeat("01", 32) + "\n",
		strings.Repeat("0A", 32),
		strings.Repeat("00", 32),
	} {
		if err := os.WriteFile(path, []byte(malformed), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadDurableAckKey(path); !errors.Is(err, errInvalidDurableAckKey) {
			t.Fatalf("malformed %q error=%v", malformed, err)
		}
	}
}
