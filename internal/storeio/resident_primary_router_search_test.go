package storeio

import (
	"bytes"
	"sort"
	"testing"
)

func TestResidentRouteTreeCachedSearchMatchesLexicalFloor(t *testing.T) {
	fences := make([][]byte, 1, 302)
	fences[0] = nil
	for i := 0; i < 300; i++ {
		f := append([]byte("shared-prefix/0123456789/"), byte(i>>8), byte(i), 0, byte(255-i%251))
		fences = append(fences, f)
	}
	sort.Slice(fences[1:], func(i, j int) bool { return bytes.Compare(fences[i+1], fences[j+1]) < 0 })
	entries := make([]residentRouteEntry, len(fences))
	for i, fence := range fences {
		entries[i] = residentRouteEntry{fence: fence, cell: &residentRouteCell{}}
	}
	tree := buildResidentRouteTree(entries)
	probes := [][]byte{nil, []byte("a"), []byte("shared"), []byte("shared-prefix/0123456789"), []byte("shared-prefix/0123456789/\x00\x00\x00"), []byte("shared-prefix/0123456789/\xff"), []byte{0xff}}
	for _, fence := range fences[1:] {
		probes = append(probes, fence)
		probes = append(probes, append(append([]byte(nil), fence...), 0))
	}
	for _, probe := range probes {
		want := sort.Search(len(fences), func(i int) bool { return bytes.Compare(fences[i], probe) > 0 }) - 1
		if want < 0 {
			want = 0
		}
		got, rank, ok := tree.floor(probe)
		if !ok || rank != want || !bytes.Equal(got.fence, fences[want]) {
			t.Fatalf("probe %x: rank=%d ok=%v fence=%x want rank=%d fence=%x", probe, rank, ok, got.fence, want, fences[want])
		}
	}
}
