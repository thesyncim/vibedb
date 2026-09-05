package query

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

func TestResultOwnershipReview(t *testing.T) {
	for _, test := range []struct {
		name, json, text string
		owned            int
	}{
		{"plain", `"payload"`, "payload", 9},
		{"empty", `""`, "", 2},
		{"unicode", `"café 😀"`, "café 😀", len(`"café 😀"`)},
		{"escaped", `"quote\"slash\\newline\n"`, "quote\"slash\\newline\n", len(`"quote\"slash\\newline\n"`) + len("quote\"slash\\newline\n")},
		{"escaped-unicode", `"\u00e9\ud83d\ude00"`, "é😀", len(`"\u00e9\ud83d\ude00"`) + len("é😀")},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := []byte(test.json)
			var text []byte
			borrowed := cellFromScalar(classifyRawInto(vibejson.RawValue{Src: source}, &text))
			var result Result
			owned := result.ownFileCell(borrowed)
			if len(result.fileData) != test.owned {
				t.Fatalf("retained %d payload bytes, want %d", len(result.fileData), test.owned)
			}
			if resultCellPayloadBytes(owned) != int64(test.owned) {
				t.Fatalf("budget charged %d bytes, want %d", resultCellPayloadBytes(owned), test.owned)
			}
			clear(source)
			clear(text)
			if got, ok := owned.Text(); !ok || got != test.text || string(owned.JSON()) != test.json {
				t.Fatalf("borrowed input mutation changed output: %q %q", owned.JSON(), got)
			}
			if test.name == "plain" && &byteview.Bytes(owned.text)[0] != &owned.raw[1] {
				t.Fatal("plain text does not share its owned JSON storage")
			}
		})
	}
	// Equal contents alone do not prove shared storage. This cell represents
	// independently owned text, which must survive both input buffers changing.
	raw := []byte(`"payload"`)
	text := []byte("payload")
	var result Result
	cell := result.ownFileCell(Cell{kind: TypeString, raw: raw, text: byteview.String(text)})
	clear(raw)
	clear(text)
	if !bytes.Equal(cell.JSON(), []byte(`"payload"`)) || cell.text != "payload" {
		t.Fatal("independently backed text was not owned")
	}
	for _, value := range []scalar{{kind: kindNull}, {kind: kindNull, raw: nullBytes}, {kind: kindBool}, {kind: kindBool, bval: true}} {
		result.fileData = result.fileData[:0]
		borrowed := cellFromScalar(value)
		owned := result.ownFileCell(borrowed)
		if len(result.fileData) != 0 || !bytes.Equal(owned.JSON(), borrowed.JSON()) || owned.flag != borrowed.flag {
			t.Fatal("immutable JSON primitive copied or changed")
		}
	}
}
