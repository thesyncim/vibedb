package main

import (
	"fmt"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
)

// newRF3MigrationBudget is the single construction hook for a physical RF3
// node. Callers must retain and pass this pointer to every group source and
// target path; constructing one budget per group would multiply the configured
// allowance when a node hosts several groups.
func newRF3MigrationBudget(config migrationbudget.Config) (*migrationbudget.Budget, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: migration budget: %v", errRF3Serving, err)
	}
	budget, err := migrationbudget.New(config)
	if err != nil {
		return nil, fmt.Errorf("%w: migration budget: %v", errRF3Serving, err)
	}
	return budget, nil
}

// openRF3MigrationBudget opens the node-scoped budget persisted in the RF3
// runtime manifest. A zero value is accepted only for in-memory test fixtures;
// parsed shipped manifests always carry the explicit validated object.
func openRF3MigrationBudget(manifest rf3Manifest) (*migrationbudget.Budget, error) {
	config := manifest.ReplicaControl.Migration.config()
	if config == (migrationbudget.Config{}) {
		config = migrationbudget.DefaultConfig()
	}
	return newRF3MigrationBudget(config)
}
