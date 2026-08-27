//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestWriterLockDiagnosticIdentifiesKernelOwnedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relation.page")
	owner, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	// Bypass the process registry to exercise an operating-system conflict,
	// as with a different process or a surviving kernel file reference.
	if err := lockWriterPlatform(owner); err != nil {
		t.Fatal(err)
	}
	defer unlockWriterPlatform(owner)
	contender, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	err = LockWriter(contender)
	if !errors.Is(err, ErrWriterLocked) || !strings.Contains(err.Error(), strconv.Quote(path)) {
		t.Fatalf("kernel lock conflict lost file identity or sentinel: %v", err)
	}
	if err := unlockWriterPlatform(owner); err != nil {
		t.Fatal(err)
	}
	if err := LockWriter(contender); err != nil {
		t.Fatalf("released kernel lock remains unavailable: %v", err)
	}
	if err := UnlockWriter(contender); err != nil {
		t.Fatal(err)
	}
}
