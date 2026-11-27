package jsoncmp

import "testing"

func TestJSONNumberEqual(t *testing.T) {
	equal := [][2]string{
		{"0", "-0"},
		{"0", "0.0"},
		{"0", "0e10"},
		{"100", "1e2"},
		{"100", "1.00e+2"},
		{"0.01", "1e-2"},
		{"-12.3400", "-1.234e1"},
		{"123456789012345678901234567890", "1.2345678901234567890123456789e29"},
	}
	for _, pair := range equal {
		if !JSONNumberEqual([]byte(pair[0]), []byte(pair[1])) {
			t.Fatalf("JSONNumberEqual(%q, %q) = false, want true", pair[0], pair[1])
		}
	}
	notEqual := [][2]string{
		{"1", "-1"},
		{"100", "101"},
		{"0.1", "0.10000000000000001"},
		{"1e2", "1e3"},
		{"123456789012345678901234567890", "123456789012345678901234567891"},
	}
	for _, pair := range notEqual {
		if JSONNumberEqual([]byte(pair[0]), []byte(pair[1])) {
			t.Fatalf("JSONNumberEqual(%q, %q) = true, want false", pair[0], pair[1])
		}
	}
}

func TestJSONStringByteIter(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`hello`, "hello"},
		{`a\nb\tc\"d\\e\/f`, "a\nb\tc\"d\\e/f"},
		{`caf\u00e9`, "café"},
		{`\uD83D\uDE00`, "😀"},
	}
	for _, tc := range cases {
		it := JSONStringByteIter{Raw: []byte(tc.raw)}
		var got []byte
		for {
			b, ok := it.Next()
			if !ok {
				break
			}
			got = append(got, b)
		}
		if string(got) != tc.want {
			t.Fatalf("iter(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}
