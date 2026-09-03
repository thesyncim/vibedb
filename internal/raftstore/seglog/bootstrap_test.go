package seglog

import (
	"errors"
	"testing"
)

func TestBootstrapIncarnationDoesNotWeakenExistingGroupFence(t *testing.T) {
	tests := []struct {
		name string
		edit func(*engineGroup, *ReadyBatch)
	}{
		{"existing-incarnation", func(g *engineGroup, b *ReadyBatch) { g.NodeIncarnation = 1; b.BeginIncarnation = 2 }},
		{"prior-term", func(g *engineGroup, _ *ReadyBatch) { g.Hard.Term = 1 }},
		{"prior-vote", func(g *engineGroup, _ *ReadyBatch) { g.Hard.Vote = 1 }},
		{"prior-checkpoint", func(g *engineGroup, _ *ReadyBatch) { g.Checkpoint = Checkpoint{ID: [16]byte{1}, Index: 1, Term: 1} }},
		{"prior-entry", func(g *engineGroup, _ *ReadyBatch) { g.lastIndex = 1 }},
		{"prior-ready", func(g *engineGroup, _ *ReadyBatch) { g.ReadyID = 1 }},
		{"prior-truncation", func(g *engineGroup, _ *ReadyBatch) { g.TruncateIndex = 1 }},
		{"missing-checkpoint", func(_ *engineGroup, b *ReadyBatch) { b.Checkpoint = nil }},
		{"missing-hardstate", func(_ *engineGroup, b *ReadyBatch) { b.Hard = nil }},
		{"zero-checkpoint-id", func(_ *engineGroup, b *ReadyBatch) { b.Checkpoint.ID = [16]byte{} }},
		{"checkpoint-term-ahead", func(_ *engineGroup, b *ReadyBatch) { b.Hard.Term = 2 }},
		{"commit-behind", func(_ *engineGroup, b *ReadyBatch) { b.Hard.Commit = 40 }},
		{"commit-ahead", func(_ *engineGroup, b *ReadyBatch) { b.Hard.Commit = 42 }},
		{"new-vote", func(_ *engineGroup, b *ReadyBatch) { b.Hard.Vote = 1 }},
		{"skipped-incarnation", func(_ *engineGroup, b *ReadyBatch) { b.BeginIncarnation = 2 }},
		{"appended-entry", func(_ *engineGroup, b *ReadyBatch) { b.Entries = []Entry{{Index: 1, Term: 1}} }},
		{"prefix", func(_ *engineGroup, b *ReadyBatch) { b.TruncateIndex, b.TruncateTerm = 41, 7 }},
		{"suffix", func(_ *engineGroup, b *ReadyBatch) { b.ReplaceFrom = 1 }},
		{"ready", func(_ *engineGroup, b *ReadyBatch) { b.NodeIncarnation, b.ReadyID, b.ReadyDigest = 1, 1, [16]byte{1} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			group := engineGroup{}
			batch := ReadyBatch{GroupID: 1, BeginIncarnation: 1, Checkpoint: &Checkpoint{ID: [16]byte{2}, Index: 41, Term: 7}, Hard: &HardState{Term: 7, Commit: 41}}
			test.edit(&group, &batch)
			if _, err := validateBatch(&group, &batch); !errors.Is(err, ErrRaftState) {
				t.Fatalf("accepted invalid bootstrap: %v", err)
			}
		})
	}
}

func TestBootstrapIncarnationRotatesAndRecoversWithCheckpoint(t *testing.T) {
	dir := t.TempDir()
	key := [32]byte{4, 5, 6}
	engine, err := CreateEngineAuthenticated(dir, testLogID, key, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }()
	if err = engine.Reserve(4096, 16, 8); err != nil {
		t.Fatal(err)
	}
	if err = engine.ReserveGroup(1, 8); err != nil {
		t.Fatal(err)
	}
	checkpoint := Checkpoint{ID: [16]byte{2}, Index: 41, Term: 7}
	hard := HardState{Term: 7, Commit: 41}
	bootstrap := Wave{ID: WaveID{1}, Batches: []ReadyBatch{{GroupID: 1, BeginIncarnation: 1, Checkpoint: &checkpoint, Hard: &hard}}}
	if err = engine.PersistWave(bootstrap); err != nil {
		t.Fatal(err)
	}
	if err = engine.PersistWave(bootstrap); err != nil {
		t.Fatalf("exact wave retry: %v", err)
	}
	if err = engine.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err = engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = OpenEngineAuthenticated(dir, testLogID, key)
	if err != nil {
		t.Fatal(err)
	}
	state, exists := engine.Metadata(1)
	if !exists || state.NodeIncarnation != 1 || state.ReadyID != 0 || state.Checkpoint != checkpoint || state.Hard != hard {
		t.Fatalf("recovered bootstrap=%+v exists=%v", state, exists)
	}
	if err = engine.DeepVerify(); err != nil {
		t.Fatal(err)
	}
}
