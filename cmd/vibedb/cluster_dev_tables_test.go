package main

import "testing"

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
