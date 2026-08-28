package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedSchemaLineageRejectsCorruptOrSubstitutedAuthority(t *testing.T) {
	path, source, target, marker, activation := schemaFenceFixture(t)
	directory := path + ".tables"
	if err := os.WriteFile(filepath.Join(directory, replicatedSchemaOriginName), source, 0600); err != nil {
		t.Fatal(err)
	}
	record := replicatedSchemaLineage{origin: sha256.Sum256(source), catalog: target, marker: marker, activation: activation}
	raw, err := encodeReplicatedSchemaLineage(record)
	if err != nil {
		t.Fatal(err)
	}
	lineagePath := filepath.Join(directory, replicatedSchemaLineageName)
	for _, test := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing", func(b []byte) []byte { return append(b, 0) }},
		{"checksum", func(b []byte) []byte { b[len(b)-1] ^= 1; return b }},
		{"reserved", func(b []byte) []byte { b[52] = 1; return b }},
		{"oversize-catalog", func(b []byte) []byte {
			for i := 40; i < 44; i++ {
				b[i] = 255
			}
			return b
		}},
		{"origin", func(b []byte) []byte {
			b[8] ^= 1
			d := sha256.Sum256(b[:len(b)-32])
			copy(b[len(b)-32:], d[:])
			return b
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(lineagePath, test.change(bytes.Clone(raw)), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readReplicatedSchemaLineage(directory); err == nil {
				t.Fatal("invalid lineage accepted")
			}
		})
	}
	if err := os.WriteFile(lineagePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, found, err := readReplicatedSchemaLineage(directory); err != nil || !found {
		t.Fatalf("valid lineage: %v", err)
	}
	foreign := record
	foreign.marker.membership.Sequence++
	if _, err := encodeReplicatedSchemaLineage(foreign); err == nil {
		t.Fatal("foreign membership accepted")
	}
	if err := os.Rename(lineagePath, lineagePath+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(lineagePath+".saved", lineagePath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readReplicatedSchemaLineage(directory); err == nil {
		t.Fatal("symlink authority accepted")
	}
}

func TestReplicatedSchemaLineageRepeatedDDLAndRestart(t *testing.T) {
	path, db, binding, _ := prepareReplicatedTestRoot(t, "lineage", false)
	base := requireReplicatedShardStoreBind(t, db, binding, "docs")
	claim, applyID, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = claim.Close(); _ = db.Close() })
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	original, originalApply := base.Clone(), applyID
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	for first, seq := 0, uint64(2); first < 1000; first, seq = first+64, seq+1 {
		var mutations []replication.Mutation
		for i := first; i < min(first+64, 1000); i++ {
			doc := []byte(fmt.Sprintf(`{"id":"employee-%04d","city":"Lisbon","score":%d}`, i, i))
			mutations = append(mutations, replication.Mutation{Kind: replication.MutationPut, Key: testReplicatedApplyKey(t, db, doc), Value: doc})
		}
		command, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, epoch, seq, mutations))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(seq+1), command); err != nil {
			t.Fatal(err)
		}
		if completionResultCode(t, claim, command) != replicatedstate.ResultApplied {
			t.Fatal("seed not applied")
		}
	}
	checkRows := func(rows, indexes int) {
		t.Helper()
		var cut replicatedstate.DataReadCut
		if err := claim.DataReadCutInto(nil, claim.Applied(), &cut); err != nil {
			t.Fatal(err)
		}
		snapshot, _ := cut.Relation(1)
		count := 0
		err := snapshot.RangeRaw(func(_, _ []byte) error { count++; return nil })
		gotIndexes := len(snapshot.AppendIndexes(nil))
		if err = errors.Join(err, cut.Close()); err != nil || count != rows || gotIndexes != indexes {
			t.Fatalf("rows=%d indexes=%d: %v", count, gotIndexes, err)
		}
	}
	reopen := func() {
		t.Helper()
		if err := errors.Join(claim.Close(), db.Close()); err != nil {
			t.Fatal(err)
		}
		db, err = OpenReplicatedShardStoreWithApply(path, base, applyID)
		if err != nil {
			t.Fatal(err)
		}
		claim, _, err = db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
		if err != nil {
			t.Fatal(err)
		}
	}
	for step, test := range []struct {
		sql           string
		rows, indexes int
	}{
		{"CREATE INDEX by_city ON docs (city)", 1000, 1},
		{"CREATE INDEX by_score ON docs (score)", 1000, 2},
		{"DROP INDEX by_city", 1000, 1},
		{"TRUNCATE TABLE docs", 0, 1},
		{"DROP INDEX by_score", 0, 0},
	} {
		t.Run(test.sql, func(t *testing.T) {
			before := claim.Applied()
			target, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), [32]byte{byte(step + 1)}, before, test.sql)
			if err != nil {
				t.Fatal(err)
			}
			request, authorization := [32]byte{byte(step + 1), 0x37}, [32]byte{byte(step + 1), 0x38}
			proof, err := claim.PrepareReplicatedSchemaTarget(target.Catalog, before, request)
			if err != nil {
				t.Fatal(err)
			}
			foreignRequest := request
			foreignRequest[31] ^= 1
			if _, err := claim.PrepareReplicatedSchemaTarget(target.Catalog, before, foreignRequest); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
				t.Fatalf("same image substituted another prepare authority: %v", err)
			}
			if recovered, err := claim.RecoverPreparedReplicatedSchemaTarget(target.Catalog, request); err != nil || recovered != proof {
				t.Fatalf("foreign prepare altered the original receipt: %v", err)
			}
			// Crash after new membership and target files are prepared, while
			// the activation slot may still contain the prior drained command.
			if step > 0 {
				reopen()
			}
			cas, err := claim.ReplicatedSchemaCatalogCASDigest(proof, request, authorization)
			if err != nil {
				t.Fatal(err)
			}
			command, err := claim.AppendReplicatedSchemaTransition(nil, proof, ReplicatedSchemaTransitionAuthority{RequestDigest: request, AuthorizationDigest: authorization, CatalogCASDigest: cas})
			if err != nil {
				t.Fatal(err)
			}
			if err := claim.PersistReplicatedSchemaTransition(command); err != nil {
				t.Fatal(err)
			}
			if step > 0 {
				// Persist-before-proposal must not make the new prepared image
				// the selected identity or lose the exact pending command.
				reopen()
				pending, found, err := ObservePersistedReplicatedSchemaTransition(path)
				if err != nil || !found || !bytes.Equal(pending.Bytes(), command) {
					t.Fatalf("pending transition after restart: %v", err)
				}
			}
			if _, err := claim.ApplyNormal(testReplicatedApplyMeta(before+1), command); err != nil {
				t.Fatal(err)
			}
			testPublishSchemaCatalogFence(t, claim, db.connector.db)
			if published, err := claim.PublishReplicatedSchemaCatalog(); err != nil || !published {
				t.Fatalf("publish=%v: %v", published, err)
			}
			catalog, _, err := openReplicatedSchemaCatalogImage(target.Catalog)
			if err != nil {
				t.Fatal(err)
			}
			previous := base
			base, applyID = catalog.ReplicatedShardStore.Clone(), catalog.ReplicatedApply.identity()
			if err := errors.Join(claim.Close(), db.Close()); err != nil {
				t.Fatal(err)
			}
			testSchemaTargetSelectionFence(t, path, previous, base, applyID)
			db, err = OpenReplicatedShardStoreWithApply(path, base, applyID)
			if err != nil {
				t.Fatal(err)
			}
			claim, _, err = db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			checkRows(test.rows, test.indexes)
			if step == 0 {
				// Building is not authority to overwrite undrained proofs. The
				// same reserved next-operation receipt resumes after this drain.
				next, err := claim.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), [32]byte{2}, claim.Applied(), "CREATE INDEX by_score ON docs (score)")
				if err != nil {
					t.Fatal(err)
				}
				beforeMarker, err := os.ReadFile(filepath.Join(path+".tables", replicatedSchemaStageMarkerName))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := claim.PrepareReplicatedSchemaTarget(next.Catalog, claim.Applied(), [32]byte{2, 0x37}); !errors.Is(err, ErrReplicatedSchemaCatalogImage) {
					t.Fatalf("overwrote undrained proof: %v", err)
				}
				afterMarker, err := os.ReadFile(filepath.Join(path+".tables", replicatedSchemaStageMarkerName))
				if err != nil || !bytes.Equal(beforeMarker, afterMarker) {
					t.Fatalf("undrained marker changed: %v", err)
				}
				originPath := filepath.Join(path+".tables", replicatedSchemaOriginName)
				if err := os.Rename(originPath, originPath+".held"); err != nil {
					t.Fatal(err)
				}
				if drained, err := DrainPublishedReplicatedSchemaSource(path, command); drained || err == nil {
					t.Fatalf("missing origin: drained=%v err=%v", drained, err)
				}
				if drained, err := ObserveDrainedReplicatedSchemaSource(path, command); drained || err != nil {
					t.Fatalf("missing source files mistaken for retained lineage: drained=%v err=%v", drained, err)
				}
				if err := os.Rename(originPath+".held", originPath); err != nil {
					t.Fatal(err)
				}
				injected := errors.New("lineage directory fence interrupted")
				replicatedSchemaDirectorySyncHook = func(directory string) error {
					if directory == path+".tables" {
						if _, err := os.Stat(filepath.Join(directory, replicatedSchemaLineageName)); err == nil {
							return injected
						}
					}
					return nil
				}
				t.Cleanup(func() { replicatedSchemaDirectorySyncHook = nil })
				if drained, err := DrainPublishedReplicatedSchemaSource(path, command); drained || !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
					t.Fatalf("unfenced lineage acknowledged: drained=%v err=%v", drained, err)
				}
				if _, _, _, err := RetainedReplicatedSchemaLineageIdentity(path, original, originalApply); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
					t.Fatalf("readable but unfenced lineage selected: %v", err)
				}
				replicatedSchemaDirectorySyncHook = nil
			}
			if drained, err := DrainPublishedReplicatedSchemaSource(path, command); err != nil || !drained {
				t.Fatalf("drain=%v: %v", drained, err)
			}
			// The original external files remain byte-identical across all DDL.
			retained, retainedApply, found, err := RetainedReplicatedSchemaLineageIdentity(path, original, originalApply)
			if err != nil || !found || !retained.Equal(base) || retainedApply != applyID {
				t.Fatalf("retained identity after step %d: %v", step, err)
			}
			foreign := original.Clone()
			foreign.LogID[0] ^= 1
			if _, _, _, err := RetainedReplicatedSchemaLineageIdentity(path, foreign, originalApply); err == nil {
				t.Fatal("foreign startup origin accepted")
			}
			reopen()
			checkRows(test.rows, test.indexes)
			if drained, err := DrainPublishedReplicatedSchemaSource(path, command); err != nil || !drained {
				t.Fatalf("drain retry=%v: %v", drained, err)
			}
		})
		if t.Failed() {
			return
		}
	}
	raw, err := os.ReadFile(filepath.Join(path+".tables", replicatedSchemaLineageName))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > replicatedSchemaLineageMaxBytes {
		t.Fatal("unbounded history")
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)-1] ^= 1
	if err := os.WriteFile(filepath.Join(path+".tables", replicatedSchemaLineageName), corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := RetainedReplicatedSchemaLineageIdentity(path, original, originalApply); err == nil {
		t.Fatal("corrupt lineage accepted")
	}
}
