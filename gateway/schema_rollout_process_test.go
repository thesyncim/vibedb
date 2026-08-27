package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibejson"
)

const schemaRolloutProcessCrashExit = 83

type schemaRolloutProcessRow struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type schemaRolloutProcessState struct {
	Applied           uint64                    `json:"applied"`
	Commit            uint64                    `json:"commit"`
	StateApplied      uint64                    `json:"state_applied"`
	CheckpointApplied uint64                    `json:"checkpoint_applied"`
	Rows              []schemaRolloutProcessRow `json:"rows"`
}

type schemaRolloutProcessEvidence struct {
	Phase         string `json:"phase"`
	Catalog       uint64 `json:"catalog"`
	Operation     uint8  `json:"operation_state"`
	ElapsedNanos  int64  `json:"elapsed_nanos"`
	ProtocolBytes uint64 `json:"protocol_bytes"`
	RuntimeBytes  uint64 `json:"runtime_bytes"`
	StateBytes    uint64 `json:"state_bytes"`
}

func canonicalSchemaProcessWrite[T any](path string, value *T) error {
	raw, err := vibejson.Marshal(value)
	if err != nil {
		return err
	}
	canonical, err := vibejson.AppendCanonicalize(nil, raw)
	clear(raw)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err = os.WriteFile(temporary, canonical, 0o600); err == nil {
		err = os.Rename(temporary, path)
	}
	clear(canonical)
	if err != nil {
		_ = os.Remove(temporary)
	}
	return err
}

func saveSchemaProcessAuthority(path string, client *catalogAuthorityClient) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	state := schemaRolloutProcessState{Applied: client.applied,
		Commit: client.state.Commit, StateApplied: client.state.Applied,
		CheckpointApplied: client.state.CheckpointApplied,
		Rows:              make([]schemaRolloutProcessRow, 0, len(client.rows))}
	for key, value := range client.rows {
		state.Rows = append(state.Rows, schemaRolloutProcessRow{
			Key: []byte(key), Value: bytes.Clone(value),
		})
	}
	slices.SortFunc(state.Rows, func(a, b schemaRolloutProcessRow) int {
		return bytes.Compare(a.Key, b.Key)
	})
	return canonicalSchemaProcessWrite(path, &state)
}

func loadSchemaProcessAuthority(path string, client *catalogAuthorityClient) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	canonical, canonicalErr := vibejson.AppendCanonicalize(nil, raw)
	if canonicalErr != nil || !bytes.Equal(canonical, raw) {
		return errors.Join(canonicalErr, ErrSchemaRollout)
	}
	var state schemaRolloutProcessState
	if err = vibejson.Unmarshal(raw, &state); err != nil {
		return err
	}
	client.mu.Lock()
	client.applied = state.Applied
	client.state.Commit = state.Commit
	client.state.Applied = state.StateApplied
	client.state.CheckpointApplied = state.CheckpointApplied
	client.rows = make(map[string][]byte, len(state.Rows))
	for _, row := range state.Rows {
		client.rows[string(row.Key)] = bytes.Clone(row.Value)
	}
	client.mu.Unlock()
	return nil
}

func schemaProcessProtocolBytes(plans []SchemaRolloutReplicaPlan, activations uint64) uint64 {
	var result uint64
	for _, plan := range plans {
		result += uint64(schemainstall.ControlRequestBytes+schemainstall.ControlResponseBytes) +
			uint64(len(plan.Bundle))
		result += uint64(schemainstall.ControlRequestBytes + schemainstall.ControlResponseBytes)
	}
	result += activations * uint64(schemainstall.ControlRequestBytes+schemainstall.ControlResponseBytes)
	return result
}

func TestSchemaRolloutExternalProcessLeaderLossAndMixedGenerationRecovery(t *testing.T) {
	if os.Getenv("VIBEDB_SCHEMA_PROCESS_MODE") != "" {
		t.Skip("parent-only process gate")
	}
	root := t.TempDir()
	statePath := filepath.Join(root, "catalog-state.vjson")
	evidencePath := filepath.Join(root, "evidence.vjson")
	run := func(mode string, want int) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0],
			"-test.run=^TestSchemaRolloutProcessHelper$", "-test.count=1")
		command.Env = append(os.Environ(),
			"VIBEDB_SCHEMA_PROCESS_MODE="+mode,
			"VIBEDB_SCHEMA_PROCESS_STATE="+statePath,
			"VIBEDB_SCHEMA_PROCESS_EVIDENCE="+evidencePath)
		err := command.Run()
		code := 0
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal(err)
			}
			code = exit.ExitCode()
		}
		if code != want || ctx.Err() != nil {
			t.Fatalf("mode=%s exit=%d want=%d context=%v", mode, code, want, ctx.Err())
		}
	}
	run("leader-loss", schemaRolloutProcessCrashExit)
	info, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= 0 || info.Size() > 1<<20 {
		t.Fatalf("bounded crash state size=%d", info.Size())
	}
	run("recover", 0)
	raw, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var evidence schemaRolloutProcessEvidence
	if err = vibejson.Unmarshal(raw, &evidence); err != nil || evidence.Phase != "recover" ||
		evidence.Operation != uint8(ReplicatedOperationComplete) || evidence.Catalog != 6 ||
		evidence.ElapsedNanos <= 0 || evidence.ElapsedNanos > int64(10*time.Second) ||
		evidence.ProtocolBytes == 0 || evidence.ProtocolBytes > 1<<20 ||
		evidence.RuntimeBytes == 0 || evidence.RuntimeBytes > 512<<20 ||
		evidence.StateBytes == 0 || evidence.StateBytes > 1<<20 {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
}

func TestSchemaRolloutProcessHelper(t *testing.T) {
	mode := os.Getenv("VIBEDB_SCHEMA_PROCESS_MODE")
	if mode == "" {
		t.Skip("process helper")
	}
	statePath := os.Getenv("VIBEDB_SCHEMA_PROCESS_STATE")
	evidencePath := os.Getenv("VIBEDB_SCHEMA_PROCESS_EVIDENCE")
	authority, catalogClient, current := newCatalogAuthorityFixture(t)
	if mode == "recover" {
		if err := loadSchemaProcessAuthority(statePath, catalogClient); err != nil {
			t.Fatal(err)
		}
	}
	target, _ := testSchemaRolloutTarget(t, current)
	id := sha256.Sum256([]byte("external-schema-process"))
	plans := schemaControllerPlans(t, id, current, target)
	client := &schemaControllerClient{authority: authority, base: current.Generation()}
	if mode == "leader-loss" {
		client.failActivate.Store(true)
		client.activateErr = errors.Join(raftservice.ErrOutcomeUnknown,
			errors.New("injected leader loss after replica activation"))
	}
	controller, err := NewSchemaRolloutController(SchemaRolloutControllerOptions{
		Authority: authority, Client: client, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, rolloutErr := controller.Execute(context.Background(), id, target, plans)
	elapsed := time.Since(started)
	if mode == "leader-loss" {
		if rolloutErr == nil || result.Record.State != ReplicatedOperationRunning ||
			client.activated.Load() != 1 {
			t.Fatalf("leader-loss result=%+v activated=%d err=%v",
				result, client.activated.Load(), rolloutErr)
		}
		if _, abortErr := authority.AbortSchemaRollout(context.Background(), id); !errors.Is(abortErr, ErrSchemaRolloutConflict) {
			t.Fatalf("authorized mixed generation rolled back: %v", abortErr)
		}
		catalog, readErr := authority.Read(context.Background())
		if readErr != nil || catalog.Generation() != current.Generation() {
			t.Fatalf("mixed generation published catalog=%d err=%v", catalog.Generation(), readErr)
		}
		if err = saveSchemaProcessAuthority(statePath, catalogClient); err != nil {
			t.Fatal(err)
		}
		os.Exit(schemaRolloutProcessCrashExit)
	}
	if rolloutErr != nil || result.Record.State != ReplicatedOperationComplete {
		t.Fatalf("recovery result=%+v err=%v", result, rolloutErr)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	info, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	evidence := schemaRolloutProcessEvidence{Phase: mode,
		Catalog:   result.Authorization.TargetCatalogGeneration,
		Operation: uint8(result.Record.State), ElapsedNanos: elapsed.Nanoseconds(),
		ProtocolBytes: schemaProcessProtocolBytes(plans, client.activated.Load()),
		RuntimeBytes:  memory.Sys, StateBytes: uint64(info.Size())}
	if err = canonicalSchemaProcessWrite(evidencePath, &evidence); err != nil {
		t.Fatal(err)
	}
}
