package storeio

import "testing"

func TestCanonicalRawUnicodeSeparatorKeysMatchVibejson(t *testing.T) {
	var workspace CanonicalWorkspace
	for _, input := range []string{
		"{\"\u2028\":1}",
		"{\"\u2029\":1}",
		"{\"\u2028\":1,\"\u2029\":2}",
		"{\"a\":{\"\u2028\":1}}",
		"{\"\u2028\":1,\"\\u2028\":2}",
		`{"\u2028":1,"\u2029":2}`,
	} {
		checkCanonicalAgainstLibrary(t, &workspace, []byte(input))
	}
}
