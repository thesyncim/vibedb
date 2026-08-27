package main

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/schemainstall"
)

type schemaStartupSourceProbe struct {
	command                          []byte
	events                           *[]string
	committed, published             bool
	observeErr, publishErr, closeErr error
}

func (s schemaStartupSourceProbe) ObserveReplicatedSchemaTransition(command []byte) (uint64, bool, error) {
	*s.events = append(*s.events, "observe-exact-command")
	if !bytes.Equal(command, s.command) {
		return 0, false, schemainstall.ErrConflict
	}
	return 12, s.committed, s.observeErr
}
func (s schemaStartupSourceProbe) PublishReplicatedSchemaCatalog() (bool, error) {
	*s.events = append(*s.events, "publish-catalog")
	return s.published, s.publishErr
}
func (s schemaStartupSourceProbe) Close() error {
	*s.events = append(*s.events, "close-source")
	return s.closeErr
}

type schemaStartupDatabaseProbe struct {
	events *[]string
	err    error
}

func (d schemaStartupDatabaseProbe) Close() error {
	*d.events = append(*d.events, "close-database")
	return d.err
}

func TestRF3SchemaStartupSettlementClosesFencedSourceBeforeReturn(t *testing.T) {
	command := []byte("exact preauthenticated transition bytes")
	var events []string
	source := schemaStartupSourceProbe{command: command, events: &events, committed: true, published: true}
	if err := settleRF3SchemaSourceBeforeServing(source, schemaStartupDatabaseProbe{events: &events}, command); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(events, []string{"observe-exact-command", "publish-catalog", "close-source", "close-database"}) {
		t.Fatalf("startup source ordering = %v", events)
	}
}

func TestRF3SchemaStartupSettlementFailsClosedAtEveryBoundary(t *testing.T) {
	for _, name := range []string{"uncommitted", "foreign-command", "observation-error", "publication-unknown", "source-close", "database-close"} {
		t.Run(name, func(t *testing.T) {
			command := []byte("original command")
			var events []string
			failure := errors.New("boundary failure")
			source := schemaStartupSourceProbe{command: command, events: &events, committed: true, published: true}
			database := schemaStartupDatabaseProbe{events: &events}
			switch name {
			case "uncommitted":
				source.committed = false
			case "foreign-command":
				command = []byte("substituted command")
			case "observation-error":
				source.observeErr = failure
			case "publication-unknown":
				source.publishErr = failure
			case "source-close":
				source.closeErr = failure
			case "database-close":
				database.err = failure
			}
			if err := settleRF3SchemaSourceBeforeServing(source, database, command); err == nil {
				t.Fatal("startup may adopt despite failed settlement")
			}
			if !slices.Equal(events[len(events)-2:], []string{"close-source", "close-database"}) {
				t.Fatalf("source handles retained: %v", events)
			}
			if (name == "uncommitted" || name == "foreign-command" || name == "observation-error") && slices.Contains(events, "publish-catalog") {
				t.Fatal("unproven source was published")
			}
		})
	}
}
