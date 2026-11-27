package tin

import (
	"fmt"
	"sync"
	"testing"
)

func TestIndexCloneSharesSealedAndIsIndependent(t *testing.T) {
	orig := NewIndex()
	for i := 0; i < 300; i++ {
		text := fmt.Sprintf("common shared token doc%d", i)
		if i%10 == 0 {
			text += " luxury vintage watch"
		}
		orig.Add(DocID(i), text)
	}
	orig.Seal()

	clone := orig.Clone()
	// Verify sealed posting lists are shared directly with zero byte copy.
	commonHash, _ := FoldTerm("common")
	if orig.post[commonHash].sealed == nil {
		t.Fatal("expected common term to be sealed")
	}
	if clone.post[commonHash].sealed != orig.post[commonHash].sealed {
		t.Fatal("Clone did not share immutable sealedPostings pointer")
	}

	// Mutate clone: remove one doc, update one doc, add a new doc.
	clone.Remove(DocID(0))
	clone.Add(DocID(10), "replaced exclusive sapphire")
	clone.AddBytes(DocID(300), []byte("luxury sapphire watch"))
	clone.Seal()

	// Original must remain untouched.
	if orig.Docs() != 300 {
		t.Fatalf("orig.Docs() = %d, want 300", orig.Docs())
	}
	qSapphireOrig, err := orig.ParseTINQL("sapphire")
	if err != nil {
		t.Fatal(err)
	}
	if hits := orig.Match(qSapphireOrig, nil); len(hits) != 0 {
		t.Fatalf("orig matched sapphire after clone mutation: %v", hits)
	}

	// Compare clone against a freshly built index with the exact same corpus.
	fresh := NewIndex()
	for i := 1; i < 300; i++ {
		if i == 10 {
			fresh.Add(DocID(i), "replaced exclusive sapphire")
			continue
		}
		text := fmt.Sprintf("common shared token doc%d", i)
		if i%10 == 0 {
			text += " luxury vintage watch"
		}
		fresh.Add(DocID(i), text)
	}
	fresh.Add(DocID(300), "luxury sapphire watch")
	fresh.Seal()

	if clone.Docs() != fresh.Docs() {
		t.Fatalf("clone.Docs() = %d, fresh.Docs() = %d", clone.Docs(), fresh.Docs())
	}
	for _, query := range []string{
		"common",
		"luxury",
		"sapphire",
		"luxury AND watch",
		`"luxury sapphire"`,
		"sapph*",
		"doc10",
		"doc0",
	} {
		qc, err := clone.ParseTINQL(query)
		if err != nil {
			t.Fatalf("clone.ParseTINQL(%q): %v", query, err)
		}
		qf, err := fresh.ParseTINQL(query)
		if err != nil {
			t.Fatalf("fresh.ParseTINQL(%q): %v", query, err)
		}
		got := clone.Score(qc, 20, nil)
		want := fresh.Score(qf, 20, nil)
		if len(got) != len(want) {
			t.Fatalf("query %q: clone %d hits, fresh %d hits", query, len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("query %q hit %d: clone=%+v fresh=%+v", query, i, got[i], want[i])
			}
		}
	}
}

func TestIndexCloneConcurrentReadsWhileMutatingClone(t *testing.T) {
	orig := NewIndex()
	for i := 0; i < 200; i++ {
		orig.Add(DocID(i), fmt.Sprintf("alpha beta gamma doc%d", i))
	}
	orig.Seal()
	clone := orig.Clone()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				q, err := orig.ParseTINQL("alpha AND beta")
				if err != nil {
					t.Errorf("ParseTINQL: %v", err)
					return
				}
				if got := orig.Match(q, nil); len(got) != 200 {
					t.Errorf("orig Match len = %d, want 200", len(got))
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		clone.Add(DocID(200+i), fmt.Sprintf("alpha delta doc%d", 200+i))
		clone.Remove(DocID(i))
	}
	clone.Seal()
	wg.Wait()
}
