package replicaaction

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestRetirementLocatorsCanonicalRoundTrip(t *testing.T) {
	input := []RetirementLocator{{Member: 3, Address: "c:3"}, {Member: 1, Address: "a:1"}, {Member: 2, Address: "b:2"}}
	before := slices.Clone(input)
	raw, err := EncodeRetirementLocators(input)
	if err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString("56524c010300000001000000000000000300613a3102000000000000000300623a3203000000000000000300633a33")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) || !slices.Equal(input, before) {
		t.Fatalf("noncanonical encoding or mutated input: raw=%x input=%+v", raw, input)
	}
	opened, err := OpenRetirementLocators(raw)
	if err != nil || !slices.Equal(opened, []RetirementLocator{input[1], input[2], input[0]}) {
		t.Fatalf("decoded locators=%+v err=%v", opened, err)
	}
	rebuilt, err := EncodeRetirementLocators(opened)
	if err != nil || !bytes.Equal(rebuilt, raw) {
		t.Fatalf("canonical round trip changed bytes: %x %v", rebuilt, err)
	}
}

func TestRetirementLocatorsBounds(t *testing.T) {
	address := strings.Repeat("a", MaxRetirementAddressBytes-2) + ":1"
	locators := []RetirementLocator{{Member: 1, Address: address}, {Member: 2, Address: address}, {Member: 3, Address: address}}
	raw, err := EncodeRetirementLocators(locators)
	if err != nil || len(raw) != MaxRetirementLocatorBytes {
		t.Fatalf("maximum locator encoding size=%d err=%v", len(raw), err)
	}
	if opened, err := OpenRetirementLocators(raw); err != nil || !slices.Equal(opened, locators) {
		t.Fatalf("maximum locator decoding=%+v err=%v", opened, err)
	}
	for name, invalid := range map[string][]RetirementLocator{
		"empty":       nil,
		"four":        append(slices.Clone(locators), RetirementLocator{Member: 4, Address: "d:4"}),
		"zero_member": {{Member: 0, Address: "a:1"}},
		"duplicate":   {{Member: 1, Address: "a:1"}, {Member: 1, Address: "b:2"}},
		"address":     {{Member: 1, Address: "a" + address}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := EncodeRetirementLocators(invalid); !errors.Is(err, ErrControl) {
				t.Fatalf("out-of-bound locators accepted: %v", err)
			}
		})
	}
	if _, err := OpenRetirementLocators(append(raw, 0)); !errors.Is(err, ErrControl) {
		t.Fatalf("oversized wire body accepted: %v", err)
	}
	if _, err := OpenRetirementLocators(rawRetirementLocator("a" + address)); !errors.Is(err, ErrControl) {
		t.Fatalf("oversized address inside bounded body accepted: %v", err)
	}
}

func TestRetirementLocatorsRejectMalformedWire(t *testing.T) {
	raw, err := EncodeRetirementLocators([]RetirementLocator{{Member: 1, Address: "a:1"}, {Member: 2, Address: "b:2"}})
	if err != nil {
		t.Fatal(err)
	}
	const secondMember = 8 + 10 + len("a:1")
	for name, mutate := range map[string]func([]byte) []byte{
		"magic":          func(b []byte) []byte { b[0] ^= 1; return b },
		"version":        func(b []byte) []byte { b[3] = 2; return b },
		"zero_count":     func(b []byte) []byte { b[4] = 0; return b },
		"four_count":     func(b []byte) []byte { b[4] = 4; return b },
		"short_count":    func(b []byte) []byte { b[4] = 1; return b },
		"long_count":     func(b []byte) []byte { b[4] = 3; return b },
		"reserved":       func(b []byte) []byte { b[5] = 1; return b },
		"zero_member":    func(b []byte) []byte { clear(b[8:16]); return b },
		"duplicate":      func(b []byte) []byte { binary.LittleEndian.PutUint64(b[secondMember:], 1); return b },
		"unsorted":       func(b []byte) []byte { binary.LittleEndian.PutUint64(b[8:], 3); return b },
		"empty_address":  func(b []byte) []byte { clear(b[16:18]); return b },
		"address_length": func(b []byte) []byte { binary.LittleEndian.PutUint16(b[16:], 65535); return b },
		"trailing":       func(b []byte) []byte { return append(b, 0) },
	} {
		t.Run(name, func(t *testing.T) {
			opened, err := OpenRetirementLocators(mutate(bytes.Clone(raw)))
			if !errors.Is(err, ErrControl) || len(opened) != 0 {
				t.Fatalf("malformed wire produced locators=%+v err=%v", opened, err)
			}
		})
	}
	for size := 0; size < len(raw); size++ {
		if opened, err := OpenRetirementLocators(raw[:size]); !errors.Is(err, ErrControl) || len(opened) != 0 {
			t.Fatalf("truncated body length %d produced locators=%+v err=%v", size, opened, err)
		}
	}
}

func TestRetirementLocatorsRejectInvalidAddresses(t *testing.T) {
	addresses := []string{"", "host", ":1234", "host:", "::1:1234", "[::1]:",
		"host\xff:1234", "host:12\xff34", "host\u0085:1234", "host\u00a0:1234", "host\u2028:1234", "host:12\u202934"}
	for control := byte(0); control <= 0x20; control++ {
		addresses = append(addresses, "host"+string(control)+":1234", "host:12"+string(control)+"34")
	}
	addresses = append(addresses, "host\x7f:1234", "host:12\x7f34")
	for _, address := range addresses {
		t.Run(fmt.Sprintf("%q", address), func(t *testing.T) {
			if _, err := EncodeRetirementLocators([]RetirementLocator{{Member: 1, Address: address}}); !errors.Is(err, ErrControl) {
				t.Fatalf("invalid address encoded: %q, %v", address, err)
			}
			if opened, err := OpenRetirementLocators(rawRetirementLocator(address)); !errors.Is(err, ErrControl) || len(opened) != 0 {
				t.Fatalf("invalid address decoded: %q, %+v, %v", address, opened, err)
			}
		})
	}
}

func TestRetirementLocatorsRequestCompatibility(t *testing.T) {
	maximum := strings.Repeat("a", MaxRetirementAddressBytes-2) + ":1"
	for name, locators := range map[string][]RetirementLocator{
		"legacy": nil,
		"ipv6":   {{Member: 3, Address: "[::1]:1234"}},
		"maximum": {
			{Member: 1, Address: maximum}, {Member: 2, Address: maximum}, {Member: 3, Address: maximum},
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := actionFixture(t, SourceRetirement)
			if locators != nil {
				var err error
				request.Command, err = EncodeRetirementLocators(locators)
				if err != nil {
					t.Fatal(err)
				}
			}
			raw, err := AppendRequest(nil, request)
			if err != nil || len(raw) > MaxRequestBytes {
				t.Fatalf("request encode length=%d err=%v", len(raw), err)
			}
			opened, err := OpenRequest(raw)
			if err != nil || !equalRequest(opened, request) || !bytes.Equal(opened.Command, request.Command) {
				t.Fatalf("request decode=%+v err=%v", opened, err)
			}
			rebuilt, err := AppendRequest(nil, opened)
			if err != nil || !bytes.Equal(rebuilt, raw) {
				t.Fatalf("request round trip changed bytes: %v", err)
			}
			var stream bytes.Buffer
			if err := WriteRequest(&stream, request); err != nil {
				t.Fatal(err)
			}
			streamed, err := ReadRequest(&stream)
			if err != nil || !equalRequest(streamed, request) || !bytes.Equal(streamed.Command, request.Command) || stream.Len() != 0 {
				t.Fatalf("request stream round trip=%+v remaining=%d err=%v", streamed, stream.Len(), err)
			}
			if len(request.Command) > 0 {
				raw[requestHeaderBytes] ^= 1
				if _, err := OpenRequest(raw); !errors.Is(err, ErrControl) {
					t.Fatalf("request accepted malformed locator payload: %v", err)
				}
			}
		})
	}
}

func rawRetirementLocator(address string) []byte {
	raw := []byte{'V', 'R', 'L', 1, 1, 0, 0, 0}
	raw = binary.LittleEndian.AppendUint64(raw, 1)
	raw = binary.LittleEndian.AppendUint16(raw, uint16(len(address)))
	return append(raw, address...)
}
