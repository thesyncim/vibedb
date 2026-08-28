package hotshard

import (
	"bytes"
	"testing"

	vibejson "github.com/thesyncim/vibejson"
)

func TestViewCanonicalRoundTrip(t *testing.T) {
	_, source, nodes := hotCatalog(t)
	view := hotView(source, nodes, 7)
	raw, err := AppendView([]byte("prefix"), view)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenView(raw[len("prefix"):])
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := AppendView(nil, opened)
	if err != nil || !bytes.Equal(reencoded, raw[len("prefix"):]) {
		t.Fatalf("reencode err=%v", err)
	}
	if opened.CatalogGeneration != view.CatalogGeneration ||
		opened.AuthorityRevision != view.AuthorityRevision || len(opened.Reports) != 1 ||
		opened.Reports[0].Recommendation != view.Reports[0].Recommendation ||
		opened.Reports[0].Group != view.Reports[0].Group {
		t.Fatalf("opened=%+v", opened)
	}
}

func TestViewRejectsNoncanonicalAndMalformedCoordinates(t *testing.T) {
	_, source, nodes := hotCatalog(t)
	raw, err := AppendView(nil, hotView(source, nodes, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenView(append(raw, ' ')); err == nil {
		t.Fatal("trailing byte accepted")
	}
	view := hotView(source, nodes, 1)
	view.Reports[0].Group = hotGroup(0)
	view.Reports = append(view.Reports, view.Reports[0])
	if _, err = AppendView(nil, view); err == nil {
		t.Fatal("unordered duplicate group accepted")
	}
	var persisted persistedView
	if err = vibejson.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	persisted.Reports[0].Kind = 255
	raw, err = vibejson.Marshal(&persisted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenView(raw); err == nil {
		t.Fatal("unknown recommendation kind accepted")
	}
}
