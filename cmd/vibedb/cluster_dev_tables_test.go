package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDevTableDDLHasRealColumnsAndRejectsUnsafeInputs(t *testing.T) {
	name, primary, err := parseDevTableDDL(`CREATE TABLE employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, score INTEGER NOT NULL)`)
	if err != nil || name != "employees" || primary != "/id" {
		t.Fatalf("declaration: %s %s %v", name, primary, err)
	}
	for _, source := range []string{
		`CREATE TABLE documents (PRIMARY KEY (id))`,
		`CREATE TABLE "../escape" (PRIMARY KEY (id))`,
		`CREATE TABLE employees (PRIMARY KEY (id)); DELETE FROM documents`,
		`CREATE TABLE IF NOT EXISTS employees (PRIMARY KEY (id))`,
		`INSERT INTO employees VALUES ('x')`,
	} {
		if _, _, err := parseDevTableDDL(source); err == nil {
			t.Fatalf("accepted unsafe provisioning: %s", source)
		}
	}
}

func TestRetainDevGroupInventoryManifestPreservesEveryPublishedCut(t *testing.T) {
	root := t.TempDir()
	previous, current := []byte("previous canonical manifest"), []byte("current canonical manifest")
	previousDigest, currentDigest := sha256.Sum256(previous), sha256.Sum256(current)
	state := make([]byte, 64)
	copy(state[:8], "VDBLIVEG")
	copy(state[8:40], previousDigest[:])
	if err := os.WriteFile(filepath.Join(root, "adopted-groups.state"), state, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "serve-multigroup.vibejson"), current, 0600); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "prepared-manifests")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, hex.EncodeToString(previousDigest[:])+".vibejson"), previous, 0600); err != nil {
		t.Fatal(err)
	}
	if err := retainDevGroupInventoryManifest(root, nil); err != nil {
		t.Fatal(err)
	}
	retained, err := os.ReadFile(filepath.Join(directory, hex.EncodeToString(currentDigest[:])+".vibejson"))
	if err != nil || string(retained) != string(current) {
		t.Fatalf("current published cut was not retained: %q %v", retained, err)
	}
}

func TestWaitDevDDLReloadRequiresBothDurableJournalsOnEveryMember(t *testing.T) {
	root := t.TempDir()
	manifests := make([]string, devClusterRF3)
	for index := range manifests {
		memberRoot := filepath.Join(root, "data-member-"+string(rune('1'+index)))
		if err := os.Mkdir(memberRoot, 0700); err != nil {
			t.Fatal(err)
		}
		manifests[index] = filepath.Join(memberRoot, "serve-multigroup.vibejson")
		raw := []byte{byte(index + 1)}
		if err := os.WriteFile(manifests[index], raw, 0600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		inventory, admissions := make([]byte, 40), make([]byte, 48)
		copy(inventory[8:40], digest[:])
		copy(admissions[16:48], digest[:])
		if err := os.WriteFile(filepath.Join(memberRoot, "adopted-groups.state"), inventory, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(memberRoot, "child-preparations.state"), admissions, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := waitDevDDLReload(t.Context(), root, manifests); err != nil {
		t.Fatal(err)
	}
	bad := make([]byte, 48)
	if err := os.WriteFile(filepath.Join(root, "data-member-3", "child-preparations.state"), bad, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := waitDevDDLReload(ctx, root, manifests); err == nil {
		t.Fatal("DDL reload returned before every durable journal acknowledged the manifest")
	}
}
